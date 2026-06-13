"""
Máquina de estado da aplicação: um LEILÃO distribuído.

Esta é a "aplicação final" que roda sobre o building block de consenso (Raft).
Cada réplica mantém uma cópia desta máquina de estado. O Raft garante que todas
as réplicas apliquem exatamente a MESMA sequência de comandos (o log replicado),
então todas chegam ao MESMO vencedor — mesmo com lances concorrentes ou falhas.

Pontos importantes para a demonstração:
  - A máquina de estado é DETERMINÍSTICA: aplicar o mesmo log produz o mesmo estado.
  - A ordem dos lances é decidida pelo consenso (a ordem no log), não pela ordem
    de chegada na rede. É isso que torna o resultado consistente entre réplicas.
"""


class AuctionStateMachine:
    def __init__(self):
        self.item = None            # item sendo leiloado
        self.open = False           # leilão aberto para lances?
        self.min_bid = 0            # lance mínimo
        self.highest_bid = 0        # maior lance até agora
        self.highest_bidder = None  # autor do maior lance (vencedor parcial)
        self.history = []           # histórico de lances aceitos (ordem do consenso)

    def apply(self, cmd):
        """Aplica um comando JÁ COMMITADO pelo consenso. Retorna um resumo do efeito.

        Comandos suportados (campo "op"):
          - start_auction: inicia um novo leilão
          - bid:           registra um lance
          - close_auction: encerra o leilão
        """
        op = cmd.get("op")

        if op == "start_auction":
            self.item = cmd["item"]
            self.min_bid = cmd.get("min_bid", 0)
            self.open = True
            self.highest_bid = self.min_bid
            self.highest_bidder = None
            self.history = []
            return {"ok": True,
                    "msg": f"leilão iniciado: '{self.item}' (lance mínimo {self.min_bid})"}

        if op == "bid":
            bidder = cmd["bidder"]
            value = cmd["value"]
            if not self.open:
                return {"ok": False, "msg": "lance recusado: leilão fechado",
                        "winner": self.highest_bidder, "highest_bid": self.highest_bid}
            if value > self.highest_bid:
                self.highest_bid = value
                self.highest_bidder = bidder
                self.history.append({"bidder": bidder, "value": value})
                return {"ok": True, "msg": f"lance ACEITO: {bidder} = {value}",
                        "winner": bidder, "highest_bid": value}
            return {"ok": False,
                    "msg": f"lance recusado: {value} <= maior lance atual {self.highest_bid}",
                    "winner": self.highest_bidder, "highest_bid": self.highest_bid}

        if op == "close_auction":
            self.open = False
            return {"ok": True,
                    "msg": f"leilão encerrado. Vencedor: {self.highest_bidder} "
                           f"com {self.highest_bid}",
                    "winner": self.highest_bidder, "highest_bid": self.highest_bid}

        return {"ok": False, "msg": f"operação desconhecida: {op}"}

    def snapshot(self):
        """Retorna o estado atual do leilão (usado pela consulta 'status')."""
        return {
            "item": self.item,
            "open": self.open,
            "min_bid": self.min_bid,
            "highest_bid": self.highest_bid,
            "highest_bidder": self.highest_bidder,
            "num_bids": len(self.history),
        }
