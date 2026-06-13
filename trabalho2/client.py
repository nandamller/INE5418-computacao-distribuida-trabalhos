"""
Cliente do leilão distribuído.

O cliente fala com QUALQUER nó. Para comandos de escrita (start/bid/close), se o
nó contatado não for o líder, ele responde um "redirect" indicando quem é o líder;
o cliente então reenvia ao líder. Para leitura (status), consulta cada réplica
diretamente — útil para mostrar que todas convergiram para o mesmo estado.

Uso:
  python client.py start "Quadro Raro" 100   # inicia leilão (lance mínimo 100)
  python client.py bid alice 150             # registra um lance
  python client.py close                      # encerra o leilão
  python client.py status                     # mostra o estado de cada réplica
  python client.py concurrent 5               # dispara 5 lances concorrentes
"""

import argparse
import json
import os
import random
import threading
import time

from transport import rpc


def load_nodes(path):
    cfg = json.load(open(path))
    return {n["id"]: (n["host"], n["port"]) for n in cfg["nodes"]}


def send_command(nodes, msg, timeout=1.0, retries=12):
    """Envia um comando de escrita, seguindo redirects até achar o líder.

    Retorna (resposta, id_do_no) ou (None, None) se nenhum líder respondeu.
    """
    order = list(nodes.keys())
    random.shuffle(order)
    for _ in range(retries):
        for nid in order:
            host, port = nodes[nid]
            resp = rpc(host, port, msg, timeout)
            if resp is None:
                continue
            if resp.get("redirect"):
                leader = resp.get("leader")
                if leader in nodes:
                    order = [leader] + [n for n in nodes if n != leader]
                break   # recomeça a varredura priorizando o líder
            return resp, nid
        time.sleep(0.3)   # nenhum nó pronto (ex.: eleição em curso) -> espera
    return None, None


def do_status(nodes):
    print("=== Estado de cada réplica ===")
    for nid, (host, port) in nodes.items():
        resp = rpc(host, port, {"type": "status"}, 1.0)
        if resp is None:
            print(f"  {nid}: OFFLINE")
            continue
        s = resp["state"]
        print(f"  {nid}: role={s['role']:9s} term={s['term']} lider={s['leader']} "
              f"commit={s['commit_index']} log={s['log_len']} | "
              f"item={s['item']} maior_lance={s['highest_bid']} "
              f"vencedor={s['highest_bidder']} aberto={s['open']}")


def do_concurrent(nodes, n, base):
    """Dispara N lances ao mesmo tempo para demonstrar concorrência."""
    print(f"Disparando {n} lances concorrentes (valores ~{base}+)...")
    results = []
    lock = threading.Lock()

    def worker(i):
        value = base + random.randint(1, 100)
        resp, _ = send_command(nodes, {"type": "bid", "bidder": f"cli{i}", "value": value})
        with lock:
            results.append((f"cli{i}", value, resp))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for bidder, value, resp in sorted(results, key=lambda x: x[1]):
        out = resp["result"]["msg"] if resp and resp.get("ok") else "FALHA"
        print(f"  {bidder} lance={value}: {out}")
    print("\nAgora rode 'python client.py status': todas as réplicas devem "
          "concordar no MESMO vencedor.")


def main():
    base = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description="Cliente do leilão distribuído (Raft)")
    ap.add_argument("--config", default=os.path.join(base, "cluster.json"))
    sub = ap.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("start", help="inicia um leilão")
    sp.add_argument("item")
    sp.add_argument("min_bid", type=int, nargs="?", default=0)

    bp = sub.add_parser("bid", help="registra um lance")
    bp.add_argument("bidder")
    bp.add_argument("value", type=int)

    sub.add_parser("close", help="encerra o leilão")
    sub.add_parser("status", help="mostra o estado de cada réplica")

    cp = sub.add_parser("concurrent", help="dispara N lances concorrentes")
    cp.add_argument("n", type=int)
    cp.add_argument("--base", type=int, default=100)

    args = ap.parse_args()
    nodes = load_nodes(args.config)

    if args.cmd == "status":
        do_status(nodes)
        return
    if args.cmd == "concurrent":
        do_concurrent(nodes, args.n, args.base)
        return

    if args.cmd == "start":
        msg = {"type": "start_auction", "item": args.item, "min_bid": args.min_bid}
    elif args.cmd == "bid":
        msg = {"type": "bid", "bidder": args.bidder, "value": args.value}
    elif args.cmd == "close":
        msg = {"type": "close_auction"}

    resp, nid = send_command(nodes, msg)
    if resp is None:
        print("FALHA: não foi possível contatar o líder (sem quórum?)")
    elif resp.get("ok"):
        print(f"[via {nid}] {resp['result']['msg']}")
    else:
        print(f"[via {nid}] {resp.get('msg', resp)}")


if __name__ == "__main__":
    main()
