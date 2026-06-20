from typing import List, Dict, Any

from consensus.utils.Enum import Status
from consensus.utils.args import PrepareArgs, CommitArgs, RequestArgs
from consensus.utils.args import PrepareOKArgs
from consensus.utils.exceptions import InvalidStatusError, ViewMismatchError, LogInconsistencyError


# --- Viewstamped Replication Node State ---
class VRNode:
    def __init__(self, current_address, replica_id: int, all_replicas: List[str], primary_id: int):

        self.host = current_address[0]
        self.port = current_address[1]

        self.replica_id = replica_id
        self.all_replicas = all_replicas  # List of [host, port] pairs, na MESMA ORDEM em todos os nós
                                           # (o índice de cada par é o replica_id daquele nó - ver Process.py)
        self.N = len(all_replicas)
        self.f = (self.N - 1) // 2
        self.quorum_size = self.f + 1

        # VR State variables
        self.op_num = 0
        self.commit_num = 0
        self.view_num = 0
        # Primário inicial é deterministico: view_num % N (convenção round-robin padrão do VR).
        # Precisa ficar consistente com a conta usada em do_view_change.
        self.primary_id = primary_id
        self.status: Status = Status.NORMAL

        self.op_log: List[Dict[str, Any]] = [] # The replicated log: list of {"op": str, "view": int, "client_id": int, "request_num": int}
        self.client_table: Dict[int, Dict[str, Any]] = {} # Tracks client_id -> {"request_num": int, "result": Any, "status": str}

        self.prepare_ok_counts: Dict[int, set] = {} # op_num -> set of replica_ids that sent PREPARE_OK (inclui o próprio primário)
        self.start_view_change_votes: Dict[int, set] = {} # view_num -> set of replica_ids vistos enviando START_VIEW_CHANGE
        self.view_change_votes: Dict[int, List[Dict]] = {} # view_num -> list of received DO_VIEW_CHANGE messages (inclui o do próprio nó)

    @property
    def is_primary(self) -> bool:
        return self.replica_id == self.primary_id

    def received_request(self, args: RequestArgs):
        """Executed ONLY by the Primary. Receives client request and starts replication."""
        if not self.is_primary:
            print(f"[Node {self.replica_id}] Operation rejected: node status is BACKUP. Expected PRIMARY.")

            return {
                "status": 409,
                "message": "Please redirect your requests to the primary node.",
                "primary_address": f"http://{self.all_replicas[self.primary_id][0]}:{self.all_replicas[self.primary_id][1]}"
            }
        if self.status != Status.NORMAL:
            print(f"Operation rejected: node status is {self.status}. Expected NORMAL.")

            return {
                "status": 503,
                "error": "RETRY_LATER",
                "message": "The cluster is currently electing a new primary. Please try again shortly.",
            }

        # Drop ou re-responde requisições duplicadas do mesmo client
        client_id = args.client_id
        if client_id in self.client_table:
            last_known = self.client_table[client_id]
            if args.request_num < last_known["request_num"]:
                print(f"[Node {self.replica_id}] Stale client request dropped.")
                return None
            if args.request_num == last_known["request_num"]:
                if last_known["status"] == "COMMITTED":
                    # Requisição já concluída: reenvia o resultado em cache em vez de
                    # reprocessar (o cliente provavelmente não recebeu a resposta original).
                    print(f"[Node {self.replica_id}] Duplicate (already committed) request: resending cached result.")
                    return last_known["result"]
                print(f"[Node {self.replica_id}] Duplicate request still in flight, dropped.")
                return None

        # Advance operation number and append to local log
        self.op_num += 1
        log_entry = {
            "op": args.op,
            "view": self.view_num,
            "client_id": args.client_id, 
            "request_num": args.request_num
        }
        self.op_log.append(log_entry)
        
        # Update client table status
        self.client_table[client_id] = {
            "request_num": args.request_num,
            "status": "PREPARING",
            "result": None
        }

        # O primário conta o próprio voto para o quorum: precisamos de f votos
        # adicionais dos backups (não de f+1), já que o primário em si conta como 1.
        self.prepare_ok_counts[self.op_num] = {self.replica_id}

        print(f"[Primary Node {self.replica_id}] Processing op_num {self.op_num}: '{args.op}'")
        
        # Trigger communication: In your network loop, you would now send a 
        # PREPARE message containing `PrepareArgs` to all backups.
        prepare_msg = PrepareArgs(
            view=self.view_num,
            op_num=self.op_num,
            op=args.op,
            commit_num=self.commit_num,
            client_id=args.client_id,
            request_num=args.request_num
        )
        return prepare_msg

    def prepare(self, args: PrepareArgs):
        """Executed by Backups. Processes a replication command from the primary."""
        if self.status != Status.NORMAL:
            raise InvalidStatusError(current_status=self.status.value)
        if args.view != self.view_num:
            raise ViewMismatchError(msg_view=args.view, node_view=self.view_num)

        if args.op_num <= self.op_num:
            # PREPARE retransmitido/duplicado (já processado): re-confirma de forma
            # idempotente, sem reprocessar nem duplicar a entrada no log.
            print(f"[Backup Node {self.replica_id}] Duplicate PREPARE for op_num {args.op_num} re-acknowledged.")
            return PrepareOKArgs(view=self.view_num, op_num=args.op_num, replica_id=self.replica_id)

        if args.op_num == self.op_num + 1:
            # Increment op_num and append to backup log
            self.op_num += 1
            self.op_log.append({
                "op": args.op, 
                "view": args.view, 
                "client_id": args.client_id, 
                "request_num": args.request_num
            })

            # Update client table
            self.client_table[args.client_id] = {
                "request_num": args.request_num,
                "status": "PREPARING",
                "result": None
            }

            # Backups can safely execute up to the primary's last known committed transaction
            self.commit(args.commit_num)

            print(f"[Backup Node {self.replica_id}] Prepared op_num {self.op_num}. Sending PREPARE_OK.")
            
            # Trigger communication: Network layer sends this back to the primary
            prepare_ok = PrepareOKArgs(view=self.view_num, op_num=self.op_num, replica_id=self.replica_id)
            return prepare_ok
        else:
            # args.op_num > self.op_num + 1: realmente há um buraco no log (réplica perdeu
            # PREPAREs anteriores) -> precisa de state transfer, não é só duplicata.
            raise LogInconsistencyError(expected_op=self.op_num + 1, received_op=args.op_num)

    def prepare_ok(self, args: PrepareOKArgs):
        """Executed ONLY by the Primary. Tracks quorum for an operation."""
        if not self.is_primary:
            return
        if args.view != self.view_num:
            return # Ignore stale view responses
        
        op_num = args.op_num
        if op_num <= self.commit_num:
            return # Already committed

        if op_num not in self.prepare_ok_counts:
            self.prepare_ok_counts[op_num] = {self.replica_id}

        # Add the replica that acknowledged the operation
        self.prepare_ok_counts[op_num].add(args.replica_id)

        # Quorum Check: o set já inclui o próprio primário, então quorum_size (f+1) aqui
        # já corresponde a uma maioria real de réplicas (primário + f backups).
        if len(self.prepare_ok_counts[op_num]) >= self.quorum_size:
            print(f"[Primary Node {self.replica_id}] Quorum achieved for op_num {op_num}!")
            
            # Commit entries sequentially up to this one
            self.commit(op_num)
            
            # Primary executes the state machine and replies to client
            return self.reply(op_num)

    def commit(self, primary_commit_num: int):
        """Advances the local commit pointer and applies logs to the state machine."""
        # Ensure we commit sequentially and never beyond our current op_num
        target_commit = min(primary_commit_num, self.op_num)
        
        while self.commit_num < target_commit:
            self.commit_num += 1
            entry = self.op_log[self.commit_num - 1] # 0-indexed log array
            
            # State Machine Application Execution Simulation
            result = f"Executed: {entry['op']}" 
            
            # Finalize client table status
            self.client_table[entry["client_id"]] = {
                "request_num": entry["request_num"],
                "status": "COMMITTED",
                "result": result
            }
            print(f"[Node {self.replica_id}] Committed entry op_num {self.commit_num}.")

    def receive_commit(self, args: CommitArgs):
        """
        Executed by Backups. Processa um COMMIT "solto" enviado pelo primário como
        heartbeat, quando não há operação nova para carregar o commit_num via PREPARE.
        (Assume que CommitArgs tem os campos `view` e `commit_num` — ajuste se o
        utils/args.py real usar nomes diferentes.)
        """
        if self.status != Status.NORMAL:
            return
        if args.view != self.view_num:
            return
        self.commit(args.commit_num)

    def reply(self, op_num: int):
        """Executed ONLY by the Primary. Sends response back to the client."""
        if not self.is_primary:
            return

        entry = self.op_log[op_num - 1]
        client_info = self.client_table.get(entry["client_id"])
        
        if client_info and client_info["status"] == "COMMITTED":
            print(f"[Primary Node {self.replica_id}] REPLYing to Client {entry['client_id']}: {client_info['result']}")
            # Devolve o resultado para quem chamou (camada de rede) transmitir ao cliente.
            return client_info["result"]

    def start_view_change(self):
        """Executed when a backup suspects the primary has crashed (Timeout)."""
        # TODO: fazer a lógica de timeout para os processos realmente chamarem view_change
        self.status = Status.VIEW_CHANGE
        self.view_num += 1
        # Já conta o próprio voto: esta réplica está, a partir de agora, em VIEW_CHANGE para essa view.
        self.start_view_change_votes.setdefault(self.view_num, set()).add(self.replica_id)
        print(f"[Node {self.replica_id}] Primary timed out! Starting View Change to View {self.view_num}...")
        
        # Broadcast START_VIEW_CHANGE(v, replica_id) to all replicas
        return {"type": "START_VIEW_CHANGE", "view": self.view_num, "replica": self.replica_id}

    def receive_start_view_change(self, view_num: int, sender_replica: int) -> Dict[str, Dict]:
        """
        Executed by ANY replica upon receiving a START_VIEW_CHANGE from a peer.

        Se esta réplica ainda não tinha aderido a essa view (ou estava numa view
        mais antiga), ela TAMBÉM entra em VIEW_CHANGE e propaga seu próprio
        START_VIEW_CHANGE — é assim que o "boato" se espalha pelo cluster mesmo
        que só uma réplica tenha detectado o timeout do primário.

        Retorna um dict com até duas chaves opcionais (ambas ausentes/vazio se
        não há nada a fazer ainda):
          - "start_view_change": payload para esta réplica broadcastar (primeira
            vez que ela adere a essa view)
          - "do_view_change": payload para enviar ao novo primário, uma única vez,
            quando o quorum de votos é atingido
        """
        if view_num < self.view_num:
            return {}  # voto de view obsoleta, ignora

        if view_num == self.view_num and self.status == Status.NORMAL:
            # Já concluímos a troca para essa view (somos o primário dela, ou já
            # recebemos o START_VIEW correspondente como backup) -- esse voto chegou
            # atrasado (era uma rebroadcast de outra réplica). Nada a fazer, e
            # principalmente: não regredir de volta para VIEW_CHANGE.
            return {}

        actions: Dict[str, Dict] = {}

        if view_num > self.view_num:
            # Estávamos atrasados (ou numa view antiga): adota a view e entra em
            # VIEW_CHANGE também, e propaga o próprio START_VIEW_CHANGE.
            self.view_num = view_num
            self.status = Status.VIEW_CHANGE
            print(f"[Node {self.replica_id}] Recebeu START_VIEW_CHANGE de {sender_replica}; também entrando em VIEW_CHANGE para view {view_num}.")
            actions["start_view_change"] = {"type": "START_VIEW_CHANGE", "view": view_num, "replica": self.replica_id}
        # Se view_num == self.view_num e já estamos em VIEW_CHANGE pra ela, só
        # contamos o voto abaixo -- já mandamos nosso próprio START_VIEW_CHANGE
        # antes, não precisa rebroadcastar de novo.

        votes = self.start_view_change_votes.setdefault(view_num, set())
        votes.add(sender_replica)
        votes.add(self.replica_id)  # a essa altura já estamos em VIEW_CHANGE para essa view

        already_sent = self.start_view_change_votes.setdefault(("dvc_sent", view_num), set())
        if len(votes) >= self.quorum_size and self.replica_id not in already_sent:
            already_sent.add(self.replica_id)  # nunca manda DO_VIEW_CHANGE duas vezes pra mesma view
            next_primary_id = view_num % self.N
            print(f"[Node {self.replica_id}] Quorum de START_VIEW_CHANGE para view {view_num}. Enviando DO_VIEW_CHANGE para o novo primário ({next_primary_id}).")
            actions["do_view_change"] = {
                "type": "DO_VIEW_CHANGE",
                "view": view_num,
                "target_primary": next_primary_id,
                "sender": self.replica_id,
                "state": self.get_state(),
            }

        return actions

    def do_view_change(self, incoming_view: int, sender_replica: int, sender_state: Dict):
        """
        Executed by the deterministic NEXT Primary once it receives enough DO_VIEW_CHANGEs.
        Chamar apenas para mensagens vindas de OUTRAS réplicas: o estado deste próprio nó
        é incluído automaticamente na primeira chamada para essa view.
        """
        # Já assumiu como primário pra essa view (ou uma mais nova) -- DO_VIEW_CHANGE
        # atrasado/duplicado, nada a fazer.
        if self.status == Status.NORMAL and self.view_num >= incoming_view:
            return None

        # Only the next primary acts as the coordinator for the view change aggregation
        next_primary_id = incoming_view % self.N
        if self.replica_id != next_primary_id:
            return None # I am not the designated coordinator for this view change

        if incoming_view not in self.view_change_votes:
            # Inclui o próprio estado do candidato a primário na disputa: ele também
            # pode ter o log mais atualizado, e precisa entrar na comparação do new_state.
            self.view_change_votes[incoming_view] = [
                {"sender": self.replica_id, "state": self.get_state()}
            ]

        # Store state payload sent by changing nodes (idempotente: réplica que reenviar
        # DO_VIEW_CHANGE pra essa view não duplica entrada)
        if not any(v["sender"] == sender_replica for v in self.view_change_votes[incoming_view]):
            self.view_change_votes[incoming_view].append({
                "sender": sender_replica,
                "state": sender_state
            })
        
        # Wait for a quorum of f+1 DO_VIEW_CHANGE payloads (incluindo o próprio)
        if len(self.view_change_votes[incoming_view]) >= self.quorum_size:
            print(f"[Node {self.replica_id}] Received quorum of DO_VIEW_CHANGE. Taking over as Primary!")
            self.view_num = incoming_view
            self.primary_id = self.replica_id

            # Select the most up-to-date log payload from quorum
            self.new_state(self.view_change_votes[incoming_view])
            
            self.status = Status.NORMAL
            return self.start_view()
        return None

    def start_view(self):
        """Executed by the NEW Primary to notify all backups to move to the new view."""
        print(f"[New Primary Node {self.replica_id}] Broadcasting START_VIEW for view {self.view_num} to all backups.")
        # Payload que a camada de rede deve transmitir a todos os backups.
        return {
            "type": "START_VIEW",
            "view": self.view_num,
            "op_log": self.op_log,
            "op_num": self.op_num,
            "commit_num": self.commit_num,
        }

    def receive_start_view(self, view_num: int, op_log: List[Dict], op_num: int, commit_num: int):
        """Executed by Backups upon receiving START_VIEW from the new primary."""
        self.view_num = view_num
        self.primary_id = view_num % self.N
        self.op_log = op_log
        self.op_num = op_num
        self.status = Status.NORMAL
        self.commit(commit_num)
        print(f"[Node {self.replica_id}] Adotou a nova view {self.view_num}; primário agora é {self.primary_id}.")

    def get_state(self) -> Dict[str, Any]:
        """Packaging internal states to send during view changes or recoveries."""
        return {
            "view_num": self.view_num,
            "op_num": self.op_num,
            "commit_num": self.commit_num,
            "op_log": self.op_log,
            "client_table": self.client_table
        }

    def new_state(self, quorum_states: List[Dict]):
        """
        Executed by the new primary to consolidate logs based on the highest op_num received.
        Empates em op_num são resolvidos pelo maior commit_num: o conteúdo do log é o
        mesmo entre candidatos com op_num igual (um log replicado é sempre um prefixo
        consistente), mas o commit_num é só contabilidade local de cada réplica -- vale
        preferir quem está mais "em dia", inclusive pra não fazer o próprio nó que está
        assumindo regredir sua contabilidade caso outro candidato saiba mais.
        """
        highest_op_entry = max(
            quorum_states,
            key=lambda x: (x["state"]["op_num"], x["state"]["commit_num"])
        )
        farthest_state = highest_op_entry["state"]
        
        # Overwrite internal log with the safest log path discovered
        self.op_log = farthest_state["op_log"]
        self.op_num = farthest_state["op_num"]
        self.commit_num = farthest_state["commit_num"]
        self.client_table = farthest_state["client_table"]
        print(f"[Node {self.replica_id}] Local state updated/synchronized to op_num {self.op_num}.")