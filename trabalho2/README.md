# Trabalho 2 — Leilão Distribuído com Consenso (Raft)

Aplicação distribuída em que vários nós mantêm, de forma **consistente**, o estado
de um **leilão**. O building block central é o **consenso**, implementado pelo
algoritmo **Raft** (eleição de líder + replicação de log). Toda a comunicação entre
processos usa **Berkeley Sockets (TCP)**.

> INE 5418 — Computação Distribuída — 2026/1

## Por que consenso?

Em um leilão distribuído, vários participantes enviam lances ao mesmo tempo e o
sistema precisa decidir o vencedor de forma **única e consistente**, mesmo com
concorrência, atrasos de rede e falhas de processos. O Raft resolve isso: cada lance
vira uma entrada em um **log replicado**; o líder só considera um lance efetivado
quando a **maioria** das réplicas o registrou (commit). Como todas as réplicas
aplicam o log **na mesma ordem**, todas chegam ao **mesmo vencedor**.

## Arquitetura

```
        cliente (client.py)
              │  (envia lance / consulta estado, via socket TCP)
              ▼
   ┌─────────────────────────────────────────────┐
   │   node1        node2        node3   ...       │   ≥ 3 processos Raft
   │  ┌──────┐     ┌──────┐     ┌──────┐           │
   │  │leilão│     │leilão│     │leilão│  ← máquina de estado replicada
   │  ├──────┤     ├──────┤     ├──────┤           │
   │  │ Raft │◄───►│ Raft │◄───►│ Raft │  ← consenso (RequestVote/AppendEntries)
   │  └──────┘     └──────┘     └──────┘           │
   └─────────────────────────────────────────────┘
            Camada de comunicação: Berkeley Sockets (TCP)
```

| Arquivo            | Papel                                                        |
|--------------------|--------------------------------------------------------------|
| `raft_node.py`     | Processo do nó: consenso Raft + servidor de sockets + leilão |
| `auction.py`       | Máquina de estado da aplicação (o leilão)                    |
| `transport.py`     | Camada de comunicação (envio/recepção de mensagens JSON/TCP) |
| `client.py`        | Cliente: envia lances e consulta o estado das réplicas       |
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

### Interagindo com o leilão (em outro terminal)

```bash
python3 client.py start "Quadro Raro" 100   # inicia o leilão (lance mínimo 100)
python3 client.py bid alice 150             # registra um lance
python3 client.py bid bob 120               # recusado (<= maior lance atual)
python3 client.py bid bob 200               # aceito
python3 client.py status                    # estado de cada réplica
python3 client.py close                     # encerra o leilão
```

Para encerrar tudo (opção A): `./stop_cluster.sh`

## Roteiro da demonstração

### 1. Cenário normal
Suba o cluster e observe nos logs a **eleição do líder** (um nó vira `LEADER`).
Em seguida rode os comandos `start`/`bid` acima e finalize com `status`: todas as
réplicas devem convergir para o **mesmo vencedor** (o `commit` dos followers
acompanha o líder no heartbeat seguinte, ~0,5 s depois).

### 2. Cenário de concorrência
```bash
python3 client.py concurrent 5    # 5 lances simultâneos
python3 client.py status          # todas as réplicas concordam no mesmo vencedor
```
Mesmo com lances de **valor igual** chegando ao mesmo tempo, o consenso impõe uma
ordem total — apenas um é aceito, e todos os nós concordam em qual.

### 3. Cenário de falha (queda do líder)
Descubra o líder com `python3 client.py status` e o derrube:
```bash
# opção A: o PID do líder está em .pids na ordem do cluster.json
kill <pid-do-lider>
```
Observe nos logs a **re-eleição** (novo `term`, novo `LEADER`). O estado do leilão é
**preservado** e novos lances continuam funcionando. Com 3 nós, o sistema tolera a
falha de 1 (mantém quórum de 2). Ao **reiniciar** o nó caído, ele faz *catch-up*
automático do log e volta a concordar com os demais.

## Detalhes do algoritmo (Raft)

- **Papéis:** `follower`, `candidate`, `leader`. Há no máximo um líder por *term*.
- **Eleição:** um follower sem heartbeat por um tempo **aleatório**
  (`1.5–3.0 s`) vira candidato, incrementa o *term* e pede votos. Vence quem obtém
  **maioria**. Timeouts aleatórios evitam empates persistentes.
- **Segurança do voto:** só se vota em candidato cujo log seja **pelo menos tão
  atualizado** quanto o do votante.
- **Replicação:** o líder envia `AppendEntries` (também servem de heartbeat). Uma
  entrada é **commitada** quando replicada pela maioria, e só então é aplicada na
  máquina de estado de cada nó — sempre na mesma ordem.
- **Reparo de log:** se um follower diverge, o líder recua o `next_index` até achar
  o ponto comum e retransmite as entradas corretas.

### API do building block (consenso)

| Operação (cliente)        | Significado                                             |
|---------------------------|--------------------------------------------------------|
| `start_auction(item,min)` | propõe iniciar um leilão (vira entrada no log)          |
| `bid(bidder,value)`       | propõe um lance (vira entrada no log)                   |
| `close_auction()`         | propõe encerrar o leilão                                |
| `status()`                | leitura local do estado da réplica contatada           |

Comandos de escrita só são aceitos pelo **líder**; um nó não-líder responde um
*redirect* indicando quem é o líder, e o cliente reenvia automaticamente.

## Limitações

- Estado em memória (sem persistência em disco): reiniciar **todos** os nós zera o
  leilão. Um nó reiniciado individualmente se recupera pelo log dos demais.
- Conjunto de nós **fixo** (`cluster.json`); não há *membership* dinâmico.
- Foco didático: timeouts folgados e logs verbosos para facilitar a observação.
