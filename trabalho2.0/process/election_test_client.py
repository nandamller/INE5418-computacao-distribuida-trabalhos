"""
Teste de eleição de líder com passageiros reais (cenário de FALHA).

Simula a queda do terminal coordenador (primário) de um sistema de reserva de
assentos: enquanto passageiros tentam reservar, o líder é congelado, o cluster
elege um novo coordenador e o terminal que caiu se reintegra como backup.

Roteiro
-------
  Fase 1 — Aquecimento
    Dois passageiros mandam algumas reservas para confirmar que o cluster está
    saudável e que o nó 1 (primário inicial) está respondendo.

  Fase 2 — Falha simulada do líder
    O endpoint POST /admin/freeze é chamado no primário. Ele segura o self.lock
    internamente por N segundos, bloqueando PREPARE, COMMIT e qualquer mensagem
    de replicação — simula um terminal pendurado sem matar o container.

  Fase 3 — Reservas durante a falha
    Os mesmos passageiros continuam tentando reservar. Eles vão colidir com o nó
    travado, iterar pelos outros terminais e esperar o cluster eleger um novo
    primário antes de prosseguir.

  Fase 4 — Verificação da eleição
    Verifica que algum nó sobrevivente assumiu como primário numa view mais alta.

  Fase 5 — Retorno do líder antigo
    O freeze expira sozinho. O nó libera o lock, recebe um state transfer do novo
    primário (PREPARE/COMMIT de view mais nova) e sincroniza seu estado.

  Fase 6 — Verificação do reingresso
    Confirma que o process1 voltou como backup na nova view.

Uso
---
  docker compose --profile election-test up election-test

Variáveis de ambiente
---------------------
  CLUSTER_TOPOLOGY    JSON dict port_key -> [host, http_port]
  STARTUP_TIMEOUT     Segundos para aguardar o cluster (default 40)
  ELECTION_TIMEOUT    Segundos para aguardar a nova eleição (default 35)
  REJOIN_TIMEOUT      Segundos para aguardar o nó reintegrar após o freeze (default 30)
  REQUEST_TIMEOUT     Timeout HTTP por chamada (default 4.0)
  FREEZE_SECONDS      Duração do freeze no primário em segundos (default 20)
"""

import json
import os
import sys
import time
import threading

import requests

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

CLUSTER_TOPOLOGY: dict = json.loads(os.getenv(
    "CLUSTER_TOPOLOGY",
    '{"6061":["process1",7061],"6062":["process2",7062],"6063":["process3",7063]}'
))

ENDPOINTS: dict[str, str] = {
    port_key: f"http://{host}:{http_port}"
    for port_key, (host, http_port) in CLUSTER_TOPOLOGY.items()
}

STARTUP_TIMEOUT  = float(os.getenv("STARTUP_TIMEOUT",  "40"))
ELECTION_TIMEOUT = float(os.getenv("ELECTION_TIMEOUT", "35"))
REJOIN_TIMEOUT   = float(os.getenv("REJOIN_TIMEOUT",   "30"))
REQUEST_TIMEOUT  = float(os.getenv("REQUEST_TIMEOUT",  "4.0"))
FREEZE_SECONDS   = float(os.getenv("FREEZE_SECONDS",   "20"))

_print_lock = threading.Lock()


def log(tag: str, msg: str):
    with _print_lock:
        ts = time.strftime("%H:%M:%S")
        print(f"  {ts} [{tag}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Infra HTTP
# ---------------------------------------------------------------------------

def http_get(endpoint: str, path: str) -> dict | None:
    try:
        r = requests.get(f"{endpoint}{path}", timeout=REQUEST_TIMEOUT)
        return r.json() if r.ok else None
    except requests.exceptions.RequestException:
        return None




# ---------------------------------------------------------------------------
# Passageiro sequencial (estilo client_simulator.py)
# ---------------------------------------------------------------------------

class Client:
    """
    Passageiro com request_num incremental, auto-redirecionamento em 409 e
    retry em 503. Itera pelos terminais conhecidos se o atual não responder.
    """

    def __init__(self, client_id: int, start_endpoint: str):
        self.client_id   = client_id
        self.request_num = 0
        self.endpoint    = start_endpoint
        self._all_eps    = list(ENDPOINTS.values())

    def send(self, op: str, label: str = "") -> dict | None:
        """Envia uma operação e retorna {'status_code': ..., 'body': ...} ou None em falha total."""
        self.request_num += 1
        payload = {
            "client_id":   self.client_id,
            "request_num": self.request_num,
            "op":          op,
        }
        max_attempts = len(self._all_eps) * 3 + 2
        ep_idx = self._all_eps.index(self.endpoint) if self.endpoint in self._all_eps else 0

        for attempt in range(1, max_attempts + 1):
            ep = self._all_eps[ep_idx % len(self._all_eps)]
            try:
                r = requests.post(f"{ep}/client/request", json=payload,
                                  timeout=REQUEST_TIMEOUT)
            except requests.exceptions.RequestException as exc:
                log(f"client-{self.client_id}",
                    f"req#{self.request_num}{' ('+label+')' if label else ''} "
                    f"— {ep} inacessível ({exc.__class__.__name__}), tentando próximo...")
                ep_idx += 1
                time.sleep(0.5)
                continue

            if r.status_code == 409:
                # Redireciona para o primário indicado pelo servidor
                primary_url = (r.json() or {}).get("primary", "")
                log(f"client-{self.client_id}",
                    f"409 em {ep}; redirecionando para {primary_url}")
                if primary_url and primary_url.startswith("http"):
                    self.endpoint = primary_url
                    if primary_url in self._all_eps:
                        ep_idx = self._all_eps.index(primary_url)
                    else:
                        ep_idx += 1
                else:
                    ep_idx += 1
                continue

            if r.status_code == 503:
                log(f"client-{self.client_id}",
                    f"503 — cluster em VIEW_CHANGE; aguardando 2s...")
                time.sleep(2)
                continue

            if r.status_code in (200, 202):
                self.endpoint = ep   # fixa no endpoint que respondeu
                tag = "OK" if r.status_code == 200 else "202"
                log(f"client-{self.client_id}",
                    f"req#{self.request_num}{' ('+label+')' if label else ''} "
                    f"'{op}' -> {tag} via {ep}")
                return {"status_code": r.status_code, "body": r.json()}

            log(f"client-{self.client_id}",
                f"req#{self.request_num} resposta inesperada {r.status_code}: {r.text[:120]}")
            return None

        log(f"client-{self.client_id}",
            f"req#{self.request_num} FALHOU após {max_attempts} tentativas")
        return None


# ---------------------------------------------------------------------------
# Helpers de cluster
# ---------------------------------------------------------------------------

def wait_for_cluster(timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    pending  = dict(ENDPOINTS)
    while pending and time.monotonic() < deadline:
        for k, ep in list(pending.items()):
            if http_get(ep, "/status") is not None:
                log("startup", f"{ep} ok")
                del pending[k]
        if pending:
            time.sleep(1.0)
    if pending:
        log("startup", f"TIMEOUT: {list(pending.values())} não responderam")
        return False
    return True


def find_primary() -> tuple[str, str] | None:
    """Retorna (port_key, endpoint) do primário atual."""
    for k, ep in ENDPOINTS.items():
        s = http_get(ep, "/status")
        if s and s.get("is_primary"):
            return k, ep
    return None


def surviving_endpoints(exclude_key: str) -> dict[str, str]:
    return {k: v for k, v in ENDPOINTS.items() if k != exclude_key}


def wait_for_new_primary(exclude_key: str, old_view: int, timeout: float):
    """Poll nos sobreviventes até um deles mostrar view_num > old_view e is_primary=True."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for k, ep in surviving_endpoints(exclude_key).items():
            s = http_get(ep, "/status")
            if s and s.get("is_primary") and s.get("view_num", 0) > old_view:
                return k, ep, s["view_num"]
        time.sleep(1.0)
    return None, None, None


# ---------------------------------------------------------------------------
# Asserções
# ---------------------------------------------------------------------------

_failures = 0

def check(condition: bool, msg: str):
    global _failures
    if condition:
        print(f"  ✓  {msg}", flush=True)
    else:
        print(f"  ✗  FALHOU: {msg}", flush=True)
        _failures += 1


# ---------------------------------------------------------------------------
# Teste principal
# ---------------------------------------------------------------------------

def main():

    print("##  Reserva de assentos — eleição de líder (Viewstamped Replication)   ##\n")


    # ── 1. Cluster up ──────────────────────────────────────────────────────
    print(" Fase 1: aguardando cluster ")
    ok = wait_for_cluster(STARTUP_TIMEOUT)
    check(ok, "Todos os 3 nós responderam /status")
    if not ok:
        sys.exit(1)

    primary_result = find_primary()
    check(primary_result is not None, "Primário identificado no cluster")
    if primary_result is None:
        sys.exit(1)

    initial_primary_key, initial_primary_ep = primary_result
    initial_info = http_get(initial_primary_ep, "/replica/info")
    initial_view = initial_info["view_num"]
    initial_primary_id = initial_info["replica_id"]
    log("info", f"Primário inicial: endpoint={initial_primary_ep}  "
                f"replica_id={initial_primary_id}  view_num={initial_view}")

    # ── 2. Aquecimento ─────────────────────────────────────────────────────
    print("\n Fase 2: aquecimento (operações normais) ")

    # Dois passageiros, ambos apontando para o primário conhecido
    alice = Client(client_id=1, start_endpoint=initial_primary_ep)
    bob   = Client(client_id=2, start_endpoint=initial_primary_ep)

    for i in range(1, 4):
        r = alice.send(f"RESERVE {i}A passenger=alice", label="warmup")
        check(r is not None and r["status_code"] in (200, 202),
              f"Alice reserva {i} aceita pelo cluster")

    for i in range(1, 4):
        r = bob.send(f"RESERVE {i}B passenger=bob", label="warmup")
        check(r is not None and r["status_code"] in (200, 202),
              f"Bob reserva {i} aceita pelo cluster")

    # ── 3. Falha simulada: freeze via HTTP ─────────────────────────────────
    frozen_key = initial_primary_key
    frozen_ep  = initial_primary_ep
    print(f"\n Fase 3: travando '{frozen_ep}' via POST /admin/freeze ")
    log("chaos", f"Enviando freeze de {FREEZE_SECONDS}s para {frozen_ep}...")
    try:
        fr = requests.post(f"{frozen_ep}/admin/freeze",
                           json={"seconds": FREEZE_SECONDS},
                           timeout=REQUEST_TIMEOUT)
        ok = fr.status_code == 200
    except requests.exceptions.RequestException as exc:
        log("chaos", f"Falha ao enviar freeze: {exc}")
        ok = False
    check(ok, f"POST /admin/freeze aceito por {frozen_ep}")
    freeze_time = time.time()

    # ── 4. Operações durante a falha ───────────────────────────────────────
    print("\n Fase 4: operações enquanto o líder está travado ")
    log("info", "Passageiros continuam reservando; espera-se 409/timeout até a eleição")

    # Alice e Bob tentam em background enquanto monitoramos a eleição
    results_during_failure: list[dict | None] = []
    lock = threading.Lock()

    def client_ops_during_failure(client: Client, n: int):
        for i in range(n):
            r = client.send(f"RESERVE {10 + i}{chr(ord('C') + client.client_id)} passenger=P{client.client_id}", label="during-failure")
            with lock:
                results_during_failure.append(r)
            time.sleep(0.5)

    t_alice = threading.Thread(target=client_ops_during_failure, args=(alice, 3), daemon=True)
    t_bob   = threading.Thread(target=client_ops_during_failure, args=(bob,   3), daemon=True)
    t_alice.start()
    t_bob.start()

    # ── 5. Aguarda eleição ──────────────────────────────────────────────────
    print("\n Fase 5: aguardando eleição do novo primário ")
    new_key, new_ep, new_view = wait_for_new_primary(
        exclude_key=frozen_key,
        old_view=initial_view,
        timeout=ELECTION_TIMEOUT,
    )
    elapsed_election = time.time() - freeze_time

    check(new_key is not None,
          f"Novo primário eleito dentro de {ELECTION_TIMEOUT:.0f}s")
    if new_key is None:
        sys.exit(1)

    check(new_view > initial_view,
          f"view_num avançou de {initial_view} para {new_view}")
    check(new_key != frozen_key,
          "Novo primário NÃO é o nó que foi travado")
    log("info", f"Eleição concluída em {elapsed_election:.1f}s  "
                f"→ novo primário: endpoint={new_ep}  view_num={new_view}")

    # Aguarda as threads de operação terminarem
    t_alice.join(timeout=15)
    t_bob.join(timeout=15)

    successful_during = sum(
        1 for r in results_during_failure if r and r["status_code"] in (200, 202)
    )
    log("info", f"Operações durante falha: {successful_during}/{len(results_during_failure)} bem-sucedidas")

    # ── 6. Operações pós-eleição ────────────────────────────────────────────
    print("\n Fase 6: operações pós-eleição (cluster saudável?) ")
    alice.endpoint = new_ep
    bob.endpoint   = new_ep

    for i in range(1, 4):
        r = alice.send(f"RESERVE {20 + i}A passenger=alice", label="post-election")
        check(r is not None and r["status_code"] in (200, 202),
              f"Alice pós-eleição reserva {i} aceita")

    for i in range(1, 4):
        r = bob.send(f"RESERVE {20 + i}B passenger=bob", label="post-election")
        check(r is not None and r["status_code"] in (200, 202),
              f"Bob pós-eleição reserva {i} aceita")

    # ── 7. Aguarda o freeze expirar e o nó reintegrar ──────────────────────
    print(f"\n Fase 7: aguardando freeze expirar e '{frozen_ep}' reintegrar ")
    remaining = FREEZE_SECONDS - (time.time() - freeze_time)
    if remaining > 0:
        log("info", f"Freeze ainda ativo por ~{remaining:.0f}s, aguardando...")
        time.sleep(remaining + 1)

    rejoin_time = time.time()
    print(f"\n Fase 8: verificando sincronização de '{frozen_ep}' ")
    # /status expõe is_primary (e view_num); /replica/info expõe primary_id.
    # Antes este bloco lia is_primary de /replica/info, que NÃO retorna esse campo,
    # então o .get(..., True) sempre caía no default e reprovava o check do backup.
    rejoined_info = None
    deadline = time.monotonic() + REJOIN_TIMEOUT
    while time.monotonic() < deadline:
        info = http_get(frozen_ep, "/status")
        if info and info.get("view_num", 0) >= new_view:
            rejoined_info = info
            break
        time.sleep(1.0)

    elapsed_rejoin = time.time() - rejoin_time
    check(rejoined_info is not None,
          f"Nó travado sincronizou dentro de {REJOIN_TIMEOUT:.0f}s após o freeze")
    if rejoined_info:
        check(rejoined_info["view_num"] >= new_view,
              f"Nó travado adotou view_num>={new_view} "
              f"(atual: {rejoined_info['view_num']})")
        check(not rejoined_info.get("is_primary", True),
              f"Nó travado voltou como BACKUP (não tentou retomar liderança)")
        # primary_id só está disponível em /replica/info
        replica_info = http_get(frozen_ep, "/replica/info") or {}
        check(replica_info.get("primary_id") != initial_primary_id,
              f"Nó travado reconhece o novo primário ({replica_info.get('primary_id')})")
        log("info", f"Estado do nó reintegrado em {elapsed_rejoin:.1f}s: "
                    f"status={rejoined_info} info={replica_info}")

    # ── 9. Operação final com todos os nós ─────────────────────────────────
    print("\n Fase 9: operação final com cluster completo ")
    carol = Client(client_id=3, start_endpoint=new_ep)
    r = carol.send("RESERVE 30F passenger=carol", label="final")
    check(r is not None and r["status_code"] in (200, 202),
          "Operação final aceita pelo cluster completo")

    # ── Resumo ─────────────────────────────────────────────────────────────
    print("\n╔══════════════════════════════════════════════════════════╗")
    if _failures == 0:
        print("║  ✅  Todos os checks passaram!                          ║")
    else:
        print(f"║  ❌  {_failures} check(s) FALHARAM                             ║")
    print(f"║  • view inicial              : {initial_view:<28}║")
    print(f"║  • view após eleição         : {new_view:<28}║")
    print(f"║  • tempo até eleição         : {elapsed_election:<25.1f}s ║")
    print(f"║  • tempo de reintegração     : {elapsed_rejoin:<25.1f}s ║")
    print(f"║  • ops durante falha OK/total: "
          f"{successful_during}/{len(results_during_failure):<24}║")
    print("╚══════════════════════════════════════════════════════════╝\n")

    sys.exit(0 if _failures == 0 else 1)


if __name__ == "__main__":
    main()