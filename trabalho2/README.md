# Trabalho 2 — Votação Distribuída do Oscar com Consenso (Raft)

Aplicação distribuída em que vários nós mantêm, de forma **consistente**, o estado
de uma **votação da premiação do Oscar** (o indicado mais votado vence). O building
block central é o **consenso**, implementado pelo algoritmo **Raft** (eleição de
líder + replicação de log). Toda a comunicação entre processos usa
**Berkeley Sockets (TCP)**.

> INE 5418 — Computação Distribuída — 2026/1

## Por que consenso?

"O mais votado ganha" é apenas uma **contagem** — um servidor único faz isso. O
papel do **consenso** é outro: garantir que **todas as réplicas concordem no MESMO
conjunto e na MESMA ordem de votos**, mesmo com votos concorrentes, atrasos de rede
e falhas de processos. Cada voto vira uma entrada em um **log replicado**; o líder
só considera um voto efetivado quando a **maioria** das réplicas o registrou
(commit). Como todas as réplicas aplicam o log **na mesma ordem**, o vencedor — uma
**função determinística** desse log — é o mesmo em todos os nós. Se o líder cair no
meio da apuração, um novo líder é eleito e o resultado continua íntegro.

> Observação: aqui os eleitores são **honestos** (podem cair, mas não mentem). Nós
> que mentem (bizantinos) exigiriam consenso bizantino (ex.: PBFT), que é outro
> building block — fora do escopo deste trabalho.

## Arquitetura

```
        cliente (client.py)
              │  (envia voto / consulta estado, via socket TCP)
              ▼
   ┌─────────────────────────────────────────────┐
   │   node1        node2        node3   ...       │   ≥ 3 processos Raft
   │  ┌──────┐     ┌──────┐     ┌──────┐           │
   │  │votos │     │votos │     │votos │  ← máquina de estado replicada
   │  ├──────┤     ├──────┤     ├──────┤           │
   │  │ Raft │◄───►│ Raft │◄───►│ Raft │  ← consenso (RequestVote/AppendEntries)
   │  └──────┘     └──────┘     └──────┘           │
   └─────────────────────────────────────────────┘
            Camada de comunicação: Berkeley Sockets (TCP)
```

| Arquivo            | Papel                                                        |
|--------------------|--------------------------------------------------------------|
| `raft_node.py`     | Processo do nó: consenso Raft + servidor de sockets + votação |
| `oscar.py`         | Máquina de estado da aplicação (a votação do Oscar)          |
| `transport.py`     | Camada de comunicação (envio/recepção de mensagens JSON/TCP) |
| `client.py`        | Cliente: envia votos e consulta o estado das réplicas        |
| `cluster.json`     | Configuração do cluster (id, host e porta de cada nó)        |
| `run_cluster.sh`   | Sobe todos os nós em background                              |
| `stop_cluster.sh`  | Encerra todos os nós                                         |

## Requisitos

- **Python 3** (testado com 3.13). **Sem dependências externas** — apenas a biblioteca padrão.

## Como executar

### Opção A — script (todos os nós em background)

```bash
cd trabalho2
./run_cluster.sh            # sobe node1, node2, node3 (logs em logs/<id>.log)
tail -f logs/*.log          # acompanhe a eleição e a replicação
```

### Opção B — um terminal por nó (melhor para ver os logs ao vivo na apresentação)

```bash
python3 raft_node.py node1
python3 raft_node.py node2
python3 raft_node.py node3
```

### Interagindo com a votação (em outro terminal)

```bash
python3 client.py open "Melhor Filme" Oppenheimer Barbie "Pobres Criaturas"
python3 client.py vote alice Oppenheimer    # voto aceito
python3 client.py vote alice Barbie         # recusado (alice já votou)
python3 client.py vote bob Marte            # recusado (indicado inválido)
python3 client.py vote carol Oppenheimer    # voto aceito
python3 client.py status                     # estado de cada réplica
python3 client.py close                      # encerra e anuncia o vencedor
```

Para encerrar tudo (opção A): `./stop_cluster.sh`

## Roteiro da demonstração

### 1. Cenário normal
Suba o cluster e observe nos logs a **eleição do líder** (um nó vira `LEADER`).
Em seguida abra uma categoria, registre alguns votos e rode `status`: todas as
réplicas devem convergir para a **mesma contagem** e o **mesmo líder de votação**
(o `commit` dos followers acompanha o líder no heartbeat seguinte, ~0,5 s depois).

### 2. Cenário de concorrência
```bash
python3 client.py concurrent 6    # 6 votos simultâneos de eleitores distintos
python3 client.py status          # todas as réplicas concordam na mesma contagem
```
Mesmo com vários votos chegando ao mesmo tempo, o consenso impõe uma ordem total —
a contagem é idêntica em todos os nós.

### 3. Cenário de falha (queda do líder)
Descubra o líder com `python3 client.py status` e o derrube:
```bash
# opção A: o PID do líder está em .pids na ordem do cluster.json
kill <pid-do-lider>
```
Observe nos logs a **re-eleição** (novo `term`, novo `LEADER`). A apuração é
**preservada** e novos votos continuam funcionando. Com 3 nós, o sistema tolera a
falha de 1 (mantém quórum de 2). Ao **reiniciar** o nó caído, ele faz *catch-up*
automático do log e volta a concordar com os demais.

## Detalhes do algoritmo (Raft)

- **Papéis:** `follower`, `candidate`, `leader`. Há no máximo um líder por *term*.
- **Eleição:** um follower sem heartbeat por um tempo **aleatório**
  (`1.5–3.0 s`) vira candidato, incrementa o *term* e pede votos. Vence quem obtém
  **maioria**. Timeouts aleatórios evitam empates persistentes.
- **Segurança do voto (de líder):** só se vota em candidato cujo log seja **pelo
  menos tão atualizado** quanto o do votante.
- **Replicação:** o líder envia `AppendEntries` (também servem de heartbeat). Uma
  entrada é **commitada** quando replicada pela maioria, e só então é aplicada na
  máquina de estado de cada nó — sempre na mesma ordem.
- **Reparo de log:** se um follower diverge, o líder recua o `next_index` até achar
  o ponto comum e retransmite as entradas corretas.

> Não confunda os dois "votos": há o **voto interno do Raft** (eleição de líder
> entre os nós) e o **voto da aplicação** (eleitor escolhendo um indicado). São
> mecanismos diferentes — o primeiro coordena as réplicas, o segundo é o conteúdo
> replicado.

### API do building block (consenso)

| Operação (cliente)             | Significado                                       |
|--------------------------------|---------------------------------------------------|
| `open_category(cat, nominees)` | propõe abrir uma categoria (vira entrada no log)  |
| `vote(voter, nominee)`         | propõe um voto (vira entrada no log)              |
| `close_category()`             | propõe encerrar a votação e define o vencedor     |
| `status()`                     | leitura local do estado da réplica contatada      |

Comandos de escrita só são aceitos pelo **líder**; um nó não-líder responde um
*redirect* indicando quem é o líder, e o cliente reenvia automaticamente.

### Regras da aplicação

- Uma **categoria por vez**.
- **Um voto por eleitor** (votos repetidos são recusados de forma consistente em
  todas as réplicas — justamente porque todas compartilham o mesmo log).
- Só se vota em **indicados válidos**.
- **Desempate determinístico** pela ordem dos indicados, para que todas as réplicas
  escolham o mesmo vencedor em caso de empate.

## Limitações

- Estado em memória (sem persistência em disco): reiniciar **todos** os nós zera a
  votação. Um nó reiniciado individualmente se recupera pelo log dos demais.
- Conjunto de nós **fixo** (`cluster.json`); não há *membership* dinâmico.
- Foco didático: timeouts folgados e logs verbosos para facilitar a observação.
