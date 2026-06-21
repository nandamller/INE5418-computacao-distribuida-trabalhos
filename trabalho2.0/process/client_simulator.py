"""
Simulador de passageiros para o sistema distribuído de reserva de assentos.

Cada "passageiro simulado" roda numa thread própria e manda pedidos de reserva
sequenciais (request_num incremental, como o protocolo exige) pro terminal HTTP que
acredita ser o primário. Se errar (réplica responde 409), segue o redirecionamento
que o próprio terminal devolve. Se o endpoint atual estiver fora do ar (conexão
recusada, timeout -- ex.: durante uma troca de view), roda pro próximo terminal
conhecido. É o cenário NORMAL + CONCORRÊNCIA: vários passageiros reservando ao mesmo
tempo, validando que o cluster ordena tudo de forma consistente.

Variáveis de ambiente:
  CLUSTER_TOPOLOGY   JSON com [[host, porta_socket], ...] -- mesmo formato usado
                     pelos process1/2/3.py. A porta HTTP de cada réplica é
                     SEMPRE porta_socket + 1000 (convenção do BaseProcess).
  NUM_CLIENTS        quantos passageiros simulados rodar em paralelo (default 3)
  OPS_PER_CLIENT     quantas reservas cada passageiro tenta; <=0 = roda pra sempre (default 20)
  MIN_THINK_TIME     espera mínima (s) entre uma reserva e a próxima de um passageiro (default 0.5)
  MAX_THINK_TIME     espera máxima (s) entre uma reserva e a próxima de um passageiro (default 2.0)
  REQUEST_TIMEOUT    timeout (s) de cada chamada HTTP (default 3.0)
"""
import json
import os
import random
import threading
import time

import requests

CLUSTER_TOPOLOGY = json.loads(os.getenv(
    "CLUSTER_TOPOLOGY",
    '[["process1", 7061], ["process2", 7062], ["process3", 7063]]'
))
# Convenção do projeto (ver BaseProcess.__init__): porta HTTP = porta do socket.
ENDPOINTS = [
    f"http://{host}:{int(port)}"
    for host, port in CLUSTER_TOPOLOGY.values()
]

NUM_CLIENTS = int(os.getenv("NUM_CLIENTS", "3"))
OPS_PER_CLIENT = int(os.getenv("OPS_PER_CLIENT", "20"))  # <=0 roda pra sempre
MIN_THINK_TIME = float(os.getenv("MIN_THINK_TIME", "0.5"))
MAX_THINK_TIME = float(os.getenv("MAX_THINK_TIME", "2.0"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "3.0"))

_print_lock = threading.Lock()


def log(tag: str, msg: str):
    with _print_lock:
        print(f"[{tag}] {msg}", flush=True)


class SimulatedClient:
    """Passageiro sequencial: um request_num por vez, nunca em paralelo consigo mesmo."""

    def __init__(self, client_id: int):
        self.client_id = client_id
        self.request_num = 0
        # Começa apontando pra endpoints diferentes entre si, só pra não
        # martelar todo mundo no mesmo nó antes do primeiro redirecionamento.
        self.endpoint_idx = ENDPOINTS[0]

    def send_one(self, op: str) -> bool:
        self.request_num += 1
        payload = {
            "client_id": self.client_id,
            "request_num": self.request_num,
            "op": op,
        }

        # Dá pra dar a volta completa nos endpoints conhecidos (e mais um pouco,
        # pra absorver um redirecionamento extra) antes de desistir dessa operação.
        max_attempts = len(ENDPOINTS) * 2 + 2
        # self.endpoint_idx = ENDPOINTS[self.endpoint_idx]
        for attempt in range(1, max_attempts + 1):
            endpoint = self.endpoint_idx

            url = f"{endpoint}/client/request"
            start = time.monotonic()
            try:
                resp = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
            except requests.exceptions.RequestException as exc:
                print(f"[ERROR] client-{self.client_id}", f"{endpoint} inacessível ({exc.__class__.__name__})")
                break

            elapsed_ms = (time.monotonic() - start) * 1000

            if resp.status_code == 409:
                primary = (resp.json() or {}).get("primary", {})

                log(f"client-{self.client_id}", f"{endpoint} não é primário; redirecionando para replica_id={primary}")
                if isinstance(primary, str) and primary[4:] == "http":
                    self.endpoint_idx = primary
                else:
                    print(f"[ERROR] client-{self.client_id} não informou o nó primário para nova consulta!")
                    break
                continue
            elif resp.status_code == 503:
                print(f"client-{self.client_id}", f"Cluster está em estado de VIEW_CHANGE. Tentando novamente em 2s...")
                time.sleep(2)
                continue

            if resp.status_code == 200:
                result = (resp.json() or {}).get("result")
                log(f"client-{self.client_id}", f"req#{self.request_num} '{op}' -> OK em {elapsed_ms:.0f}ms via {endpoint}: {result}")
                return True

            if resp.status_code == 202:
                log(f"client-{self.client_id}", f"req#{self.request_num} '{op}' -> em andamento (202) via {endpoint}, seguindo adiante")
                return True

            log(f"client-{self.client_id}", f"req#{self.request_num} '{op}' -> resposta inesperada {resp.status_code} via {endpoint}: {resp.text[:200]}")
            return False

        log(f"client-{self.client_id}", f"req#{self.request_num} '{op}' -> FALHOU depois de {max_attempts} tentativas (cluster indisponível?)")
        return False

    def run(self, ops_count: int):
        done = 0
        while ops_count <= 0 or done < ops_count:
            done += 1
            # Pedido de reserva de um assento aleatório (fila 1-30, colunas A-F).
            # A op é apenas uma string opaca para o protocolo: o que importa é a
            # ORDEM em que entra no log replicado, decidida pelo consenso.
            seat = f"{random.randint(1, 30)}{random.choice('ABCDEF')}"
            op = f"RESERVE {seat} passenger=P{self.client_id}"
            self.send_one(op)
            time.sleep(random.uniform(MIN_THINK_TIME, MAX_THINK_TIME))
        log(f"client-{self.client_id}", "terminou.")


def wait_for_cluster(timeout: float = 30.0):
    """Espera os nós responderem /status antes de soltar tráfego de verdade
    (útil em Docker Compose, onde os containers podem demorar pra abrir a porta)."""
    deadline = time.monotonic() + timeout
    pending = set(ENDPOINTS)
    while pending and time.monotonic() < deadline:
        for ep in list(pending):
            try:
                requests.get(f"{ep}/status", timeout=1.0)
                pending.discard(ep)
                log("startup", f"{ep} respondeu, ok")
            except requests.exceptions.RequestException:
                pass
        if pending:
            time.sleep(1.0)
    if pending:
        log("startup", f"AVISO: não consegui confirmar {pending} dentro de {timeout:.0f}s, seguindo de qualquer jeito")


def main():
    log("startup", f"terminais conhecidos: {ENDPOINTS}")
    log("startup", f"{NUM_CLIENTS} passageiros, {OPS_PER_CLIENT if OPS_PER_CLIENT > 0 else 'infinitas'} reservas cada")
    wait_for_cluster()

    threads = []
    for client_id in range(1, NUM_CLIENTS + 1):
        client = SimulatedClient(client_id)
        t = threading.Thread(target=client.run, args=(OPS_PER_CLIENT,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    log("startup", "todos os passageiros terminaram.")


if __name__ == "__main__":
    main()