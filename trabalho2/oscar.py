"""
Máquina de estado da aplicação: VOTAÇÃO da premiação do Oscar.

Esta é a "aplicação final" que roda sobre o building block de consenso (Raft).
Cada réplica mantém uma cópia desta máquina de estado. O Raft garante que todas
as réplicas apliquem exatamente a MESMA sequência de votos (o log replicado),
então todas calculam o MESMO vencedor — mesmo com votos concorrentes ou falhas.

Por que isso é consenso (e não "só contar votos"):
  - Contar votos, sozinho, qualquer servidor único faz. O papel do consenso é
    garantir que TODAS as réplicas concordem no MESMO conjunto e ORDEM de votos.
  - O vencedor é uma FUNÇÃO DETERMINÍSTICA desse log compartilhado. Como o log é
    idêntico em todos os nós, o vencedor calculado é idêntico em todos os nós,
    mesmo que algum caia durante a votação.

Regras desta aplicação:
  - Uma categoria ativa por vez (ex.: "Melhor Filme").
  - Um voto por eleitor (votos repetidos do mesmo eleitor são recusados).
  - Só se vota em indicados válidos.
"""


class OscarStateMachine:
    def __init__(self):
        self.category = None     # categoria em votação (ex.: "Melhor Filme")
        self.open = False        # votação aberta?
        self.nominees = []       # indicados válidos (ordem fixa = desempate determinístico)
        self.tally = {}          # indicado -> número de votos
        self.voters = set()      # eleitores que já votaram (um voto por eleitor)
        self.history = []        # votos aceitos, na ordem decidida pelo consenso

    def apply(self, cmd):
        """Aplica um comando JÁ COMMITADO pelo consenso. Retorna um resumo do efeito.

        Comandos suportados (campo "op"):
          - open_category:  abre uma categoria para votação
          - vote:           registra um voto
          - close_category: encerra a votação e define o vencedor
        """
        op = cmd.get("op")

        if op == "open_category":
            self.category = cmd["category"]
            self.nominees = list(cmd["nominees"])
            self.open = True
            self.tally = {n: 0 for n in self.nominees}
            self.voters = set()
            self.history = []
            return {"ok": True,
                    "msg": f"categoria aberta: '{self.category}' "
                           f"indicados={self.nominees}"}

        if op == "vote":
            voter = cmd["voter"]
            nominee = cmd["nominee"]
            if not self.open:
                return {"ok": False, "msg": "voto recusado: votação fechada"}
            if voter in self.voters:
                return {"ok": False, "msg": f"voto recusado: {voter} já votou"}
            if nominee not in self.tally:
                return {"ok": False, "msg": f"voto recusado: '{nominee}' não é indicado"}
            self.tally[nominee] += 1
            self.voters.add(voter)
            self.history.append({"voter": voter, "nominee": nominee})
            leader, votes = self._leader()
            return {"ok": True, "msg": f"voto ACEITO: {voter} -> {nominee}",
                    "leading": leader, "votes": votes}

        if op == "close_category":
            self.open = False
            winner, votes = self._leader()
            return {"ok": True,
                    "msg": f"votação encerrada. Vencedor: {winner} ({votes} votos)",
                    "winner": winner, "votes": votes}

        return {"ok": False, "msg": f"operação desconhecida: {op}"}

    def _leader(self):
        """Indicado mais votado. Desempate determinístico pela ordem dos indicados,
        garantindo que todas as réplicas escolham o mesmo vencedor em caso de empate.
        """
        best, best_votes = None, -1
        for n in self.nominees:
            c = self.tally.get(n, 0)
            if c > best_votes:
                best, best_votes = n, c
        return best, best_votes

    def snapshot(self):
        """Retorna o estado atual da votação (usado pela consulta 'status')."""
        leader, votes = self._leader()
        return {
            "category": self.category,
            "open": self.open,
            "nominees": self.nominees,
            "tally": self.tally,
            "leading": leader,
            "leading_votes": votes,
            "num_votes": len(self.history),
        }
