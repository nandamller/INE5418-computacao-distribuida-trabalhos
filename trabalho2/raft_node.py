"""
Nó (processo) do consenso Raft + aplicação de votação do Oscar.

Cada processo deste arquivo é um nó independente do cluster. Ele:
  1. Implementa o algoritmo de CONSENSO Raft (eleição de líder + replicação de log);
  2. Aplica o log replicado na máquina de estado da aplicação (a votação);
  3. Atende clientes e os demais nós por sockets TCP.

Resumo do Raft (building block):
  - Em cada instante há no máximo um LÍDER por "term" (mandato). Os demais são
    FOLLOWERS. Se um follower fica sem notícias do líder por um tempo aleatório,
    ele vira CANDIDATE e dispara uma eleição.
  - Todo comando do cliente vira uma entrada no log do líder. O líder replica a
    entrada via AppendEntries. Quando a MAIORIA dos nós tem a entrada, ela é
    "commitada" e aplicada na máquina de estado — em todos os nós, na mesma ordem.

Indexação do log: usamos índices baseados em 0. commit_index/last_applied = -1
significa "nada commitado/aplicado ainda".
"""

import argparse
import json
import os
import random
import socket
import threading
import time

from transport import send_line, recv_line, rpc
from oscar import OscarStateMachine

# Parâmetros de tempo (generosos, para que os logs da demo sejam legíveis).
HEARTBEAT_INTERVAL = 0.5      # intervalo entre heartbeats do líder (s)
ELECTION_TIMEOUT_MIN = 1.5    # timeout de eleição mínimo (s)
ELECTION_TIMEOUT_MAX = 3.0    # timeout de eleição máximo (s)
RPC_TIMEOUT = 0.4            # timeout de uma RPC para um peer (s)
CLIENT_WAIT = 5.0           # tempo máximo que o líder espera por consenso (s)

_START = time.time()


class RaftNode:
    def __init__(self, node_id, config_path):
        with open(config_path) as f:
            cfg = json.load(f)
        # Mapa id -> (host, porta) de todos os nós do cluster.
        self.nodes = {n["id"]: (n["host"], n["port"]) for n in cfg["nodes"]}
        if node_id not in self.nodes:
            raise SystemExit(f"id '{node_id}' não está no cluster.json")

        self.id = node_id
        self.host, self.port = self.nodes[node_id]
        self.peers = [nid for nid in self.nodes if nid != node_id]
        self.majority = (len(self.nodes) // 2) + 1   # quórum

        # --- Estado persistente do Raft (em memória, para fins didáticos) ---
        self.current_term = 0
        self.voted_for = None
        self.log = []   # entradas: {"term": int, "cmd": dict}

        # --- Estado volátil em todos os nós ---
        self.role = "follower"     # follower | candidate | leader
        self.commit_index = -1     # maior índice commitado
        self.last_applied = -1     # maior índice aplicado na máquina de estado
        self.leader_id = None      # líder conhecido (usado para redirecionar clientes)

        # --- Estado volátil só do líder ---
        self.next_index = {}       # peer -> próximo índice a enviar
        self.match_index = {}      # peer -> maior índice já replicado naquele peer

        # --- Controle de execução ---
        self.lock = threading.RLock()
        self.election_deadline = 0.0
        self.last_heartbeat_sent = 0.0
        self.running = True

        # Máquina de estado da aplicação (votação do Oscar).
        self.app = OscarStateMachine()

        # Clientes bloqueiam até o comando ser commitado.
        # index do log -> {"event": Event, "result": dict}
        self.pending = {}

    # ------------------------------------------------------------------ logging
    def log_msg(self, msg):
        t = time.time() - _START
        print(f"[t={t:6.2f}s][{self.id}][{self.role.upper():9s} term={self.current_term}] {msg}",
              flush=True)

    # -------------------------------------------------------------- ciclo de vida
    def start(self):
        threading.Thread(target=self._serve, daemon=True).start()
        threading.Thread(target=self._loop, daemon=True).start()
        with self.lock:
            self._reset_election_timer()
        self.log_msg(f"nó iniciado em {self.host}:{self.port} | peers={self.peers} | "
                     f"quórum={self.majority}")
        try:
            while self.running:
                time.sleep(0.5)
        except KeyboardInterrupt:
            self.running = False

    # ------------------------------------------------------- servidor de sockets
    def _serve(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen()
        while self.running:
            try:
                conn, _ = srv.accept()
            except OSError:
                break
            threading.Thread(target=self._handle_conn, args=(conn,), daemon=True).start()

    def _handle_conn(self, conn):
        try:
            f = conn.makefile("rb")
            msg = recv_line(f)
            if msg is None:
                return
            mtype = msg.get("type")
            if mtype == "request_vote":
                resp = self._on_request_vote(msg)
            elif mtype == "append_entries":
                resp = self._on_append_entries(msg)
            elif mtype in ("vote", "open_category", "close_category", "status"):
                resp = self._on_client(msg)
            else:
                resp = {"ok": False, "error": f"tipo desconhecido: {mtype}"}
            send_line(conn, resp)
        except (OSError, ValueError):
            pass
        finally:
            conn.close()

    # ----------------------------------------------------- RPCs recebidas (Raft)
    def _on_request_vote(self, msg):
        """Trata um pedido de voto de um candidato."""
        with self.lock:
            term = msg["term"]
            cand = msg["candidate_id"]
            cand_last_idx = msg["last_log_index"]
            cand_last_term = msg["last_log_term"]

            if term > self.current_term:
                self._become_follower(term)

            grant = False
            if term == self.current_term and self.voted_for in (None, cand):
                # O voto só é concedido se o log do candidato for PELO MENOS tão
                # atualizado quanto o nosso (regra de segurança do Raft).
                my_last_term = self.log[-1]["term"] if self.log else 0
                my_last_idx = len(self.log) - 1
                up_to_date = (cand_last_term > my_last_term) or \
                             (cand_last_term == my_last_term and cand_last_idx >= my_last_idx)
                if up_to_date:
                    grant = True
                    self.voted_for = cand
                    self._reset_election_timer()
                    self.log_msg(f"votou em {cand} (term {term})")

            return {"type": "request_vote_resp",
                    "term": self.current_term, "vote_granted": grant}

    def _on_append_entries(self, msg):
        """Trata AppendEntries do líder (replicação de log + heartbeat)."""
        with self.lock:
            term = msg["term"]
            if term < self.current_term:
                # Líder obsoleto: rejeita e informa nosso term maior.
                return {"type": "append_entries_resp",
                        "term": self.current_term, "success": False}

            if term > self.current_term:
                self._become_follower(term)
            # Reconhece o líder atual e reinicia o timer de eleição.
            self.role = "follower"
            self.leader_id = msg["leader_id"]
            self._reset_election_timer()

            prev_idx = msg["prev_log_index"]
            prev_term = msg["prev_log_term"]
            entries = msg["entries"]
            leader_commit = msg["leader_commit"]

            # Checagem de consistência: precisamos ter a entrada anterior idêntica.
            if prev_idx >= 0:
                if prev_idx >= len(self.log) or self.log[prev_idx]["term"] != prev_term:
                    return {"type": "append_entries_resp",
                            "term": self.current_term, "success": False}

            # Anexa as entradas, truncando em caso de conflito.
            base = prev_idx + 1
            for j, e in enumerate(entries):
                idx = base + j
                if idx < len(self.log):
                    if self.log[idx]["term"] != e["term"]:
                        del self.log[idx:]      # conflito: descarta o resto
                        self.log.append(e)
                else:
                    self.log.append(e)
            if entries:
                self.log_msg(f"replicou {len(entries)} entrada(s) do líder "
                             f"{self.leader_id}; log tem {len(self.log)} entradas")

            # Avança o commit conforme o líder informou.
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log) - 1)
                self._apply_committed()

            match = prev_idx + len(entries)
            return {"type": "append_entries_resp",
                    "term": self.current_term, "success": True, "match_index": match}

    # -------------------------------------------------- transições de papel
    def _become_follower(self, term):
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
        if self.role != "follower":
            self.role = "follower"
            self.log_msg(f"voltou a FOLLOWER (term {term})")
        else:
            self.role = "follower"

    def _become_leader(self):
        self.role = "leader"
        self.leader_id = self.id
        nxt = len(self.log)
        self.next_index = {p: nxt for p in self.peers}
        self.match_index = {p: -1 for p in self.peers}
        self.last_heartbeat_sent = 0.0   # força heartbeat imediato no próximo tick
        self.log_msg(f"*** TORNOU-SE LÍDER (term {self.current_term}) ***")

    def _reset_election_timer(self):
        self.election_deadline = time.time() + \
            random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    # ----------------------------------------------------- laço de temporização
    def _loop(self):
        """Único thread responsável por disparar eleições e heartbeats.

        Como só este thread faz E/S de rede dessas operações, não há concorrência
        entre eleição e heartbeat — simplifica bastante o raciocínio.
        """
        while self.running:
            time.sleep(0.05)
            with self.lock:
                now = time.time()
                role = self.role
                if role == "leader":
                    due = (now - self.last_heartbeat_sent) >= HEARTBEAT_INTERVAL
                else:
                    due = now >= self.election_deadline
            if not due:
                continue
            if role == "leader":
                self._send_heartbeats()
            else:
                self._start_election()

    def _start_election(self):
        with self.lock:
            self.role = "candidate"
            self.current_term += 1
            self.voted_for = self.id
            self._reset_election_timer()
            term = self.current_term
            last_idx = len(self.log) - 1
            last_term = self.log[-1]["term"] if self.log else 0
            peers = list(self.peers)
            self.log_msg(f"timeout de eleição -> CANDIDATE, inicia eleição (term {term})")

        votes = 1   # vota em si mesmo
        req = {"type": "request_vote", "term": term, "candidate_id": self.id,
               "last_log_index": last_idx, "last_log_term": last_term}
        # E/S de rede fora do lock; reaquire para aplicar cada resposta.
        for p in peers:
            host, port = self.nodes[p]
            resp = rpc(host, port, req, RPC_TIMEOUT)
            if not resp:
                continue
            with self.lock:
                if self.role != "candidate" or self.current_term != term:
                    return   # situação mudou (já virou follower/leader)
                if resp.get("term", 0) > self.current_term:
                    self._become_follower(resp["term"])
                    return
                if resp.get("vote_granted"):
                    votes += 1
                    self.log_msg(f"recebeu voto de {p} ({votes}/{len(self.nodes)})")
                    if votes >= self.majority:
                        self._become_leader()
                        return

    def _send_heartbeats(self):
        """Líder envia AppendEntries (com ou sem entradas) para todos os peers."""
        with self.lock:
            if self.role != "leader":
                return
            self.last_heartbeat_sent = time.time()
            term = self.current_term
            commit = self.commit_index
            peers = list(self.peers)
            plan = {}
            for p in peers:
                ni = self.next_index.get(p, len(self.log))
                prev_idx = ni - 1
                prev_term = self.log[prev_idx]["term"] if prev_idx >= 0 else 0
                plan[p] = (prev_idx, prev_term, list(self.log[ni:]), ni)

        for p in peers:
            prev_idx, prev_term, entries, ni = plan[p]
            host, port = self.nodes[p]
            msg = {"type": "append_entries", "term": term, "leader_id": self.id,
                   "prev_log_index": prev_idx, "prev_log_term": prev_term,
                   "entries": entries, "leader_commit": commit}
            resp = rpc(host, port, msg, RPC_TIMEOUT)
            if not resp:
                continue
            with self.lock:
                if self.role != "leader" or self.current_term != term:
                    return
                if resp.get("term", 0) > self.current_term:
                    self._become_follower(resp["term"])
                    return
                if resp.get("success"):
                    self.match_index[p] = resp.get("match_index", prev_idx + len(entries))
                    self.next_index[p] = self.match_index[p] + 1
                    self._advance_commit()
                else:
                    # Log inconsistente: recua e tenta de novo no próximo heartbeat.
                    self.next_index[p] = max(0, ni - 1)

    def _advance_commit(self):
        """Líder avança commit_index para o maior N replicado pela maioria.

        Regra de segurança do Raft: só commitamos entradas do TERM atual por
        contagem de réplicas (entradas de terms anteriores são commitadas de
        forma indireta, quando uma entrada do term atual é commitada).
        """
        for n in range(len(self.log) - 1, self.commit_index, -1):
            if self.log[n]["term"] != self.current_term:
                continue
            count = 1   # o próprio líder
            for p in self.peers:
                if self.match_index.get(p, -1) >= n:
                    count += 1
            if count >= self.majority:
                self.commit_index = n
                self.log_msg(f"maioria replicou até idx={n} -> COMMIT")
                self._apply_committed()
                break

    def _apply_committed(self):
        """Aplica na máquina de estado as entradas commitadas ainda não aplicadas."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            result = self.app.apply(entry["cmd"])
            self.log_msg(f"APLICOU idx={self.last_applied} {entry['cmd']} -> {result['msg']}")
            # Acorda o cliente que aguarda este índice (só ocorre no líder).
            waiter = self.pending.pop(self.last_applied, None)
            if waiter is not None:
                waiter["result"] = result
                waiter["event"].set()

    # ------------------------------------------------- requisições de cliente
    def _on_client(self, msg):
        mtype = msg["type"]

        # Consulta de estado: pode ser respondida por QUALQUER réplica (leitura local).
        if mtype == "status":
            with self.lock:
                snap = self.app.snapshot()
                snap.update({"node": self.id, "role": self.role,
                             "term": self.current_term, "leader": self.leader_id,
                             "log_len": len(self.log), "commit_index": self.commit_index})
            return {"ok": True, "state": snap}

        # Comandos de escrita: precisam passar pelo líder (consenso).
        with self.lock:
            if self.role != "leader":
                return {"ok": False, "redirect": True, "leader": self.leader_id,
                        "leader_addr": self.nodes.get(self.leader_id),
                        "msg": f"não sou líder; líder atual = {self.leader_id}"}
            cmd = {k: v for k, v in msg.items() if k != "type"}
            cmd["op"] = mtype
            self.log.append({"term": self.current_term, "cmd": cmd})
            idx = len(self.log) - 1
            waiter = {"event": threading.Event(), "result": None}
            self.pending[idx] = waiter
            self.last_heartbeat_sent = 0.0   # replica já no próximo tick
            self.log_msg(f"cliente -> log idx={idx}: {cmd}")

        # Espera o consenso (fora do lock) e devolve o resultado ao cliente.
        if waiter["event"].wait(timeout=CLIENT_WAIT):
            return {"ok": True, "result": waiter["result"]}
        with self.lock:
            self.pending.pop(idx, None)
        return {"ok": False, "msg": "timeout aguardando consenso (sem quórum?)"}


def main():
    base = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description="Nó Raft da votação distribuída do Oscar")
    ap.add_argument("node_id", help="id do nó (deve existir no cluster.json)")
    ap.add_argument("--config", default=os.path.join(base, "cluster.json"))
    args = ap.parse_args()
    RaftNode(args.node_id, args.config).start()


if __name__ == "__main__":
    main()
