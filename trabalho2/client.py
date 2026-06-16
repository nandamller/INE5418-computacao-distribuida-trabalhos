"""
Cliente da votação distribuída do Oscar.

O cliente fala com QUALQUER nó. Para comandos de escrita (open/vote/close), se o
nó contatado não for o líder, ele responde um "redirect" indicando quem é o líder;
o cliente então reenvia ao líder. Para leitura (status), consulta cada réplica
diretamente — útil para mostrar que todas convergiram para o mesmo estado.

Uso:
  python client.py open "Melhor Filme" Oppenheimer Barbie "Pobres Criaturas"
  python client.py vote alice Oppenheimer     # registra um voto
  python client.py close                        # encerra e define o vencedor
  python client.py status                       # estado de cada réplica
  python client.py concurrent 7                 # 7 votos concorrentes
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
        tally = " ".join(f"{n}={v}" for n, v in s["tally"].items()) or "-"
        print(f"  {nid}: role={s['role']:9s} term={s['term']} lider={s['leader']} "
              f"commit={s['commit_index']} log={s['log_len']} | "
              f"categoria={s['category']} aberto={s['open']} "
              f"lider_votacao={s['leading']} | votos: {tally}")


def do_concurrent(nodes, n, base):
    """Dispara N votos ao mesmo tempo para demonstrar concorrência.

    Cada voto vem de um eleitor distinto (um voto por eleitor) para um indicado
    sorteado entre os da categoria aberta.
    """
    # Descobre os indicados consultando qualquer réplica.
    nominees = None
    for host, port in nodes.values():
        resp = rpc(host, port, {"type": "status"}, 1.0)
        if resp and resp["state"]["nominees"]:
            nominees = resp["state"]["nominees"]
            break
    if not nominees:
        print("Nenhuma categoria aberta. Rode 'python client.py open ...' antes.")
        return

    print(f"Disparando {n} votos concorrentes entre {nominees}...")
    results = []
    lock = threading.Lock()

    def worker(i):
        voter = f"eleitor{base + i}"
        nominee = random.choice(nominees)
        resp, _ = send_command(nodes, {"type": "vote", "voter": voter, "nominee": nominee})
        with lock:
            results.append((voter, nominee, resp))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for voter, nominee, resp in sorted(results):
        out = resp["result"]["msg"] if resp and resp.get("ok") else "FALHA"
        print(f"  {voter} -> {nominee}: {out}")
    print("\nAgora rode 'python client.py status': todas as réplicas devem "
          "concordar na MESMA contagem e no mesmo líder.")


def main():
    base = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description="Cliente da votação distribuída do Oscar (Raft)")
    ap.add_argument("--config", default=os.path.join(base, "cluster.json"))
    sub = ap.add_subparsers(dest="cmd", required=True)

    op = sub.add_parser("open", help="abre uma categoria para votação")
    op.add_argument("category")
    op.add_argument("nominees", nargs="+", help="lista de indicados")

    vp = sub.add_parser("vote", help="registra um voto")
    vp.add_argument("voter")
    vp.add_argument("nominee")

    sub.add_parser("close", help="encerra a votação e define o vencedor")
    sub.add_parser("status", help="mostra o estado de cada réplica")

    cp = sub.add_parser("concurrent", help="dispara N votos concorrentes")
    cp.add_argument("n", type=int)
    cp.add_argument("--base", type=int, default=0)

    args = ap.parse_args()
    nodes = load_nodes(args.config)

    if args.cmd == "status":
        do_status(nodes)
        return
    if args.cmd == "concurrent":
        do_concurrent(nodes, args.n, args.base)
        return

    if args.cmd == "open":
        msg = {"type": "open_category", "category": args.category, "nominees": args.nominees}
    elif args.cmd == "vote":
        msg = {"type": "vote", "voter": args.voter, "nominee": args.nominee}
    elif args.cmd == "close":
        msg = {"type": "close_category"}

    resp, nid = send_command(nodes, msg)
    if resp is None:
        print("FALHA: não foi possível contatar o líder (sem quórum?)")
    elif resp.get("ok"):
        print(f"[via {nid}] {resp['result']['msg']}")
    else:
        print(f"[via {nid}] {resp.get('msg', resp)}")


if __name__ == "__main__":
    main()
