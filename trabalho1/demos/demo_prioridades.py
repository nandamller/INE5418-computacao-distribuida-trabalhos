"""
Demo da fila de prioridades do interceptador.

Dispara várias requisições TCP em paralelo misturando 'encurta' (prioridade=2),
'remove' (prioridade=1) e 'resolve' (prioridade=0). Imprime a ordem de envio e a ordem
de resposta para evidenciar que requisições de prioridade menor "furam fila".

Para que a demonstração seja visível, o proxy precisa estar rodando com
DEMO_DELAY > 0 (ex: 0.5s). Isso faz a fila acumular durante o teste.

Uso (do host):
    DEMO_DELAY=0.5 docker compose up -d proxy
    python demos/demo_prioridades.py
"""

import json
import os
import socket
import threading
import time
import uuid

PROXY_HOST = os.getenv("PROXY_HOST", "localhost")
PROXY_PORT = int(os.getenv("PROXY_PORT", "8080"))


def enviar(payload):
    """Abre socket TCP, envia JSON e devolve resposta + timestamps."""
    t_envio = time.perf_counter()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((PROXY_HOST, PROXY_PORT))
    s.sendall(json.dumps(payload).encode("utf-8"))
    data = s.recv(4096)
    t_resposta = time.perf_counter()
    s.close()
    return t_envio, t_resposta, json.loads(data.decode("utf-8"))


def main():
    # 1. Encurta uma URL para ter um código válido para resolves.
    print("Pré-encurta uma URL para resolves...")
    _, _, resp = enviar({"acao": "encurta", "url": "https://demo.priority"})
    codigo_valido = resp["codigo"]
    print(f"  código pré-existente: {codigo_valido}")

    # 2. Monta a sequência de requisições.
    #    Mistura intencionalmente para dar conjunto não trivial.
    cenario = (
        [("encurta", {"acao": "encurta", "url": f"https://e/{i}"}) for i in range(8)]
        + [("resolve", {"acao": "resolve", "codigo": codigo_valido}) for _ in range(4)]
        + [("remove", {"acao": "remove", "codigo": str(uuid.uuid4())}) for _ in range(3)]
    )
    # Embaralha a ordem de chegada para forçar o teste
    import random
    random.seed(42)
    random.shuffle(cenario)

    # 3. Dispara em paralelo.
    resultados = []
    t0 = time.perf_counter()

    def alvo(idx, tipo, payload):
        t_envio, t_resp, resp = enviar(payload)
        resultados.append({
            "idx": idx,
            "tipo": tipo,
            "prio": {"resolve": 0, "remove": 1, "encurta": 2}[tipo],
            "envio_ms": (t_envio - t0) * 1000,
            "resp_ms": (t_resp - t0) * 1000,
        })

    threads = []
    for idx, (tipo, payload) in enumerate(cenario):
        t = threading.Thread(target=alvo, args=(idx, tipo, payload))
        threads.append(t)

    # Lança todas o mais simultaneamente possível.
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 4. Imprime ordem de envio (cronológica) vs ordem de resposta.
    print("\n=== ORDEM DE ENVIO (cronológica) ===")
    for r in sorted(resultados, key=lambda x: x["envio_ms"]):
        print(f"  T+{r['envio_ms']:7.1f}ms  #{r['idx']:02d}  {r['tipo']:7s} (prio={r['prio']})")

    print("\n=== ORDEM DE RESPOSTA (= ordem de processamento pelo worker) ===")
    for r in sorted(resultados, key=lambda x: x["resp_ms"]):
        print(f"  T+{r['resp_ms']:7.1f}ms  #{r['idx']:02d}  {r['tipo']:7s} (prio={r['prio']})")

    # 5. Estatística simples: a média de tempo de resposta por prioridade.
    print("\n=== TEMPO MÉDIO DE RESPOSTA POR PRIORIDADE ===")
    por_prio = {}
    for r in resultados:
        por_prio.setdefault(r["prio"], []).append(r["resp_ms"])
    for prio in sorted(por_prio):
        media = sum(por_prio[prio]) / len(por_prio[prio])
        nome = {0: "resolve", 1: "remove", 2: "encurta"}[prio]
        print(f"  prio={prio} ({nome:7s}): média {media:7.1f}ms  (n={len(por_prio[prio])})")
    print("\nEsperado: prio=0 (resolve) deve ter MÉDIA MENOR que prio=2 (encurta).")


if __name__ == "__main__":
    main()
