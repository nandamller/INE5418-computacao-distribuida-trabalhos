import json
import socket
import threading
import time
from abc import ABC

from flask import Flask, jsonify, request
from pydantic import BaseModel

from process.utils.args import RequestArgs, PrepareArgs, PrepareOKArgs, CommitArgs
from process.utils.exceptions import InvalidStatusError


class NetworkMessage(BaseModel):
    """Envelope usado em toda mensagem trocada no socket cru entre réplicas."""
    sender_port: int
    msg_type: str
    payload: dict


class BaseProcess(ABC):
    """
    Cada réplica atende dois canais ao mesmo tempo, no mesmo processo, em threads
    diferentes (para compartilhar o mesmo estado sem precisar de IPC):

      - socket "cru" (porta `port`)        -> protocolo de replicação ENTRE réplicas
      - HTTP via Flask (porta `flask_port`) -> requisições de clientes/admin

    Além disso, quando esta réplica é a primária, uma terceira thread (heartbeat)
    reenvia COMMIT(view, commit_num) periodicamente pros backups mesmo sem operação
    nova, pra eles não ficarem com o commit_num atrasado esperando a próxima escrita.

    IMPORTANTE: esta classe assume que a subclasse concreta também herda de VRNode
    (ex.: `class Process1(BaseProcess, VRNode)`), porque tanto as rotas Flask quanto
    o dispatch de mensagens de rede chamam métodos do VRNode (self.prepare,
    self.prepare_ok, self.received_request, etc.) que não existem aqui.
    Se um dia BaseProcess precisar servir um processo que NÃO seja um nó VR, vale
    separar essa parte num mixin próprio (ex.: VRNetworkingMixin) em vez de deixá-la
    aqui dentro.

    Use self.lock para proteger qualquer leitura/escrita de estado do VRNode feita
    a partir das rotas Flask ou das conexões de socket, já que cada uma roda em sua
    própria thread.
    """

    def __init__(self, port: int, host: str = '127.0.0.1', flask_port: int = None):
        self.host = host
        self.port = port
        # Porta HTTP separada da porta de replicação (mesma máquina, protocolos diferentes)
        self.flask_port = flask_port if flask_port is not None else port + 1000

        self.is_running = False
        self.lock = threading.Lock()  # protege o estado do VRNode entre as threads
        self.false_fault = False

        self.app = Flask(self.__class__.__name__)
        self._register_common_routes()
        self.register_routes()  # hook para as subclasses adicionarem rotas próprias, se precisarem

    def register_routes(self):
        """Sobrescrever nas subclasses para registrar rotas Flask adicionais, se necessário."""
        pass


    # ---------------------------------------------------------------------
    # HTTP (Flask) - cliente externo fala com a réplica por aqui
    # ---------------------------------------------------------------------

    def _register_common_routes(self):
        @self.app.route('/status', methods=['GET'])
        def status():
            with self.lock:
                return jsonify({
                    "host": self.host,
                    "port": self.port,
                    "flask_port": self.flask_port,
                    "running": self.is_running,
                    "replica_id": getattr(self, "replica_id", None),
                    "view_num": getattr(self, "view_num", None),
                    "is_primary": getattr(self, "is_primary", None),
                })

        @self.app.route('/replica/info', methods=['GET'])
        def replica_info():
            with self.lock:
                return jsonify({
                    "process": self.__class__.__name__,
                    "host": self.host,
                    "port": self.port,
                    "replica_id": self.replica_id,
                    "primary_id": self.primary_id,
                    "view_num": self.view_num,
                    "op_num": self.op_num,
                    "commit_num": self.commit_num,
                })

        @self.app.route('/client/request', methods=['POST'])
        def client_request():
            data = request.get_json(force=True) or {}
            try:
                req_args = RequestArgs(**data)
            except Exception as exc:
                return jsonify({"error": f"payload inválido para RequestArgs: {exc}"}), 400

            with self.lock:
                prepare_msg = self.received_request(req_args)

            if prepare_msg is None:
                return jsonify({"status": "dropped (requisição obsoleta ou duplicada em andamento)"}), 202
            if isinstance(prepare_msg, dict) and prepare_msg["status"] == 409:
                return jsonify({
                    "error": prepare_msg["message"],
                    "primary": prepare_msg["primary_address"],
                }), 409
            if isinstance(prepare_msg, dict) and prepare_msg["status"] == 503:
                return jsonify({
                    "error": prepare_msg["error"],
                    "message": prepare_msg["message"],
                }), 503
            if isinstance(prepare_msg, str):
                # Duplicata de uma requisição já COMMITTED: o resultado em cache volta direto,
                # sem precisar replicar de novo.
                return jsonify({"result": prepare_msg}), 200

            # Replica o PREPARE para os backups e processa as PREPARE_OK que voltarem.
            # Feito FORA do lock: é I/O de rede, não queremos travar outras conexões/rotas
            # enquanto esperamos os backups responderem.
            responses = self.broadcast_to_backups("PREPARE", prepare_msg)
            with self.lock:
                for resp_type, resp_payload in responses:
                    if resp_type == "PREPARE_OK":
                        self.prepare_ok(PrepareOKArgs(**resp_payload))
                client_info = self.client_table.get(req_args.client_id, {})

            if client_info.get("status") == "COMMITTED":
                return jsonify({"result": client_info["result"]}), 200
            return jsonify({"status": "preparing", "op_num": prepare_msg.op_num}), 202

        @self.app.route('/admin/freeze', methods=['POST'])
        def admin_freeze():
            """
            Trava o nó por `seconds` segundos segurando o self.lock.
            Isso bloqueia dispatch_message inteiro (PREPARE, COMMIT, etc.),
            simulando um processo pendurado sem matar o container.

            Body JSON: {"seconds": <float>}   (default: 10)
            """
            data = request.get_json(force=True) or {}
            seconds = float(data.get("seconds", 10))

            def _hold_lock():
                print(f"[{self.__class__.__name__}] FREEZE: travando por {seconds}s...", flush=True)
                self.false_fault = True
                with self.lock:
                    time.sleep(seconds)
                self.false_fault = False
                print(f"[{self.__class__.__name__}] FREEZE: liberado.", flush=True)

            threading.Thread(target=_hold_lock, daemon=True).start()
            return jsonify({"status": "freezing", "seconds": seconds}), 200

    def _run_flask(self):
        # threaded=True: o Flask atende várias requisições HTTP concorrentes.
        # use_reloader=False: obrigatório, o reloader não funciona fora da thread principal real.
        self.app.run(host=self.host, port=self.flask_port, threaded=True, use_reloader=False)

    def start(self):
        """Inicia o socket de replicação, e o Flask na thread principal."""
        self.is_running = True

        socket_thread = threading.Thread(
            target=self.execute,
            name=f"{self.__class__.__name__}-socket",
            daemon=True,
        )
        socket_thread.start()

        try:
            self._run_flask()
        finally:
            self.is_running = False

    # ---------------------------------------------------------------------
    # Socket cru - réplicas conversam entre si por aqui (PREPARE, COMMIT, etc.)
    # ---------------------------------------------------------------------

    def execute(self):
        """Loop do socket de replicação: aceita conexões continuamente, uma thread por conexão."""
        print(f"[{self.__class__.__name__}] Started. Listening on {self.host}:{self.port}...")

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.host, self.port))
            s.listen()

            while self.is_running:
                connection, address = s.accept()
                threading.Thread(
                    target=self._handle_connection,
                    args=(connection, address),
                    daemon=True,
                ).start()

        print(f"[{self.__class__.__name__}] Finished.")

    def _handle_connection(self, connection, address):
        # Framing por linha: cada mensagem é um JSON terminado em "\n". TCP não garante
        # que um recv() traga exatamente uma mensagem completa (pode vir picotada ou
        # mais de uma junta), então acumulamos num buffer e processamos linha a linha.
        buffer = b""
        with connection:
            print(f"[{self.__class__.__name__}] Connection established with {address}")
            while True:
                try:
                    chunk = connection.recv(4096)
                except OSError:
                    break
                if not chunk:
                    break
                buffer += chunk
                while b"\n" in buffer:
                    line, _, buffer = buffer.partition(b"\n")
                    if line.strip():
                        self._process_line(connection, line)

    def _process_line(self, connection, line: bytes):
        try:
            data = json.loads(line.decode("utf-8"))
            msg_type = data["msg_type"]
            payload = data.get("payload", {})
        except (json.JSONDecodeError, KeyError) as exc:
            print(f"[{self.__class__.__name__}] Mensagem inválida descartada: {exc}")
            return

        if self.false_fault:
            print("Pausa FAKE no processo! Vamos pular a requisição!")
            return

        print(f"[{self.__class__.__name__}] Received {msg_type} from sender_port={data.get('sender_port')}")
        reply_type, reply_payload = self.dispatch_message(msg_type, payload)
        if reply_type is not None:
            try:
                connection.sendall(self._serialize(reply_type, reply_payload))
            except OSError as exc:
                print(f"[{self.__class__.__name__}] Falha ao responder: {exc}")

    @staticmethod
    def _to_dict(obj):
        """Converte PrepareArgs/PrepareOKArgs/etc. (dataclass, pydantic v1/v2 ou objeto comum) em dict serializável."""
        if obj is None or isinstance(obj, dict):
            return obj
        if hasattr(obj, "model_dump"):       # pydantic v2
            return obj.model_dump()
        if hasattr(obj, "dict") and callable(obj.dict):  # pydantic v1
            return obj.dict()
        if hasattr(obj, "__dict__"):         # dataclass / objeto comum
            return dict(vars(obj))
        return obj

    def _serialize(self, msg_type: str, payload) -> bytes:
        message = NetworkMessage(sender_port=self.port, msg_type=msg_type, payload=self._to_dict(payload) or {})
        as_json = message.model_dump_json() if hasattr(message, "model_dump_json") else message.json()
        return as_json.encode("utf-8") + b"\n"

    def send_to_replica(self, host: str, port: int, msg_type: str, payload, timeout: float = 2.0):
        """Abre uma conexão nova, envia uma mensagem e espera por exatamente uma resposta. None em falha/timeout."""
        try:
            with socket.create_connection((host, port), timeout=timeout) as s:
                s.sendall(self._serialize(msg_type, payload))
                s.settimeout(timeout)
                buffer = b""
                while b"\n" not in buffer:
                    chunk = s.recv(4096)
                    if not chunk:
                        return None
                    buffer += chunk
                line, _, _ = buffer.partition(b"\n")
                data = json.loads(line.decode("utf-8"))
                return data.get("msg_type"), data.get("payload", {})
        except (ConnectionRefusedError, socket.timeout, OSError, json.JSONDecodeError) as exc:
            print(f"[{self.__class__.__name__}] Falha ao falar com {host}:{port}: {exc}")
            return None

    def broadcast_to_backups(self, msg_type: str, payload, timeout: float = 2.0):
        """Envia para todas as réplicas do topology, exceto a própria. Retorna só as respostas que chegaram."""
        responses = []
        for replica in self.all_replicas:
            if int(replica) == self.port:
                continue  # não manda pra si mesmo

            replica_host = self.all_replicas[replica][0]
            replica_port = int(replica)
            if isinstance(payload, dict) and payload["type"] == "START_VIEW_CHANGE" and payload["primary"] == replica_port:
                print("KKKKKKKKK")
                print(payload["primary"])
                continue # não manda para quem era líder (está parado)

            result = self.send_to_replica(replica_host, replica_port, msg_type, payload, timeout=timeout)
            if result is not None:
                responses.append(result)
        return responses

    def dispatch_message(self, msg_type: str, payload: dict):
        """
        Roteia uma mensagem recebida pro método correspondente do VRNode (disponível em
        self porque as subclasses herdam de BaseProcess + VRNode ao mesmo tempo).
        Retorna (msg_type_da_resposta, payload_da_resposta), ou (None, None) se não
        há nada a responder na mesma conexão.
        """
        try:
            dvc_payload = None       # DO_VIEW_CHANGE a enviar para o novo primário
            sv_payload = None        # START_VIEW a broadcastar (este nó virou o novo primário)
            svc_rebroadcast = None   # START_VIEW_CHANGE a (re)propagar (este nó aderiu a uma view nova)

            with self.lock:
                if msg_type == "REQUEST":
                    result = self.received_request(RequestArgs(**payload))
                    if result is None:
                        return "ACK", {}
                    if isinstance(result, str):
                        return "RESULT", {"result": result}
                    return "PREPARE", self._to_dict(result)

                elif msg_type == "PREPARE":
                    if hasattr(self, "_reset_primary_timeout"):
                        self._reset_primary_timeout()
                    result = self.prepare(PrepareArgs(**payload))
                    return "PREPARE_OK", self._to_dict(result)

                elif msg_type == "PREPARE_OK":
                    self.prepare_ok(PrepareOKArgs(**payload))
                    return "ACK", {}

                elif msg_type == "COMMIT":
                    if hasattr(self, "_reset_primary_timeout"):
                        self._reset_primary_timeout()
                    self.receive_commit(CommitArgs(**payload))
                    return "ACK", {}

                elif msg_type == "START_VIEW_CHANGE":
                    actions = self.receive_start_view_change(payload["view"], payload["replica"], payload["primary"])
                    svc_rebroadcast = actions.get("start_view_change")
                    dvc_payload = actions.get("do_view_change")

                elif msg_type == "DO_VIEW_CHANGE":
                    sv_payload = self.do_view_change(payload["view"], payload["sender"], payload["state"], payload["target_primary"])

                elif msg_type == "START_VIEW":
                    if hasattr(self, "_reset_primary_timeout"):
                        self._reset_primary_timeout()
                    self.receive_start_view(payload["view"], payload["op_log"], payload["op_num"], payload["commit_num"], payload["primary_id"])
                    return "ACK", {}

                else:
                    return "ERROR", {"error": f"unknown msg_type {msg_type}"}

            # As ramificações abaixo ficam FORA do lock de propósito: elas fazem
            # I/O de rede com outras réplicas, e não queremos travar este nó enquanto
            # isso acontece.
            if svc_rebroadcast is not None:
                threading.Thread(
                    target=self.broadcast_to_backups, args=("START_VIEW_CHANGE", svc_rebroadcast), daemon=True
                ).start()

            if dvc_payload is not None:
                target_id = dvc_payload["target_primary"]
                if target_id == self.replica_id:
                    threading.Thread(target=self.dispatch_message, args=("DO_VIEW_CHANGE", dvc_payload), daemon=True).start()
                else:
                    # target_id vem de new_primary() como int, mas all_replicas é
                    # carregado do JSON com chaves string ("6062"). Indexar com str
                    # evita o KeyError que impedia o DO_VIEW_CHANGE de ser encaminhado.
                    target_host = self.all_replicas[str(target_id)][0]
                    target_port = int(target_id)   # porta do socket, não a HTTP
                    threading.Thread(
                        target=self.send_to_replica,
                        args=(target_host, target_port, "DO_VIEW_CHANGE", dvc_payload),
                        daemon=True,
                    ).start()

            if sv_payload is not None:
                threading.Thread(target=self.broadcast_to_backups, args=("START_VIEW", sv_payload), daemon=True).start()

            return "ACK", {}

        except Exception as exc:  # nunca deixa um erro de lógica VR derrubar a conexão
            print(f"[{self.__class__.__name__}] Erro processando {msg_type}: {exc}")
            return "ERROR", {"error": str(exc)}