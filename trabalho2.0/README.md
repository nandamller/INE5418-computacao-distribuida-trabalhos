# Reserva Distribuída de Assentos de Avião — Consenso com Viewstamped Replication

Aplicação distribuída em que vários **terminais de check-in** mantêm, de forma
**consistente**, o **mapa de assentos de um voo**. Vários **passageiros** tentam
reservar assentos ao mesmo tempo, e o sistema precisa garantir que **dois passageiros
nunca fiquem com o mesmo assento**, mesmo havendo concorrência, atraso de rede e falha
de um terminal.

O *building block* central é o **Consenso / Replicação**, implementado pelo algoritmo
**Viewstamped Replication (VR)** (eleição de líder + replicação de log). Toda a
comunicação entre processos usa **Berkeley Sockets (TCP)**.

> INE 5418 — Computação Distribuída — 2026/1

## Integrantes

> ⚠️ Preencher com os nomes do grupo (exigido pelo enunciado).
>
> - Nome 1
> - Nome 2
> - Nome 3

---

## Por que consenso?

"Reservar um assento" parece simples, mas em um sistema com vários terminais
atendendo passageiros ao mesmo tempo surge o problema clássico de **recurso
compartilhado sob concorrência**: dois passageiros podem pedir o assento `12A`
no mesmo instante, em terminais diferentes.

O papel do **consenso** é garantir que **todas as réplicas concordem na MESMA
sequência de reservas**. Cada pedido vira uma entrada em um **log replicado**; o
líder só considera uma reserva efetivada quando a **maioria** das réplicas a
registrou (*commit*). Como todas as réplicas aplicam o log **na mesma ordem**, o
resultado — quem ficou com cada assento — é o **mesmo em todos os terminais**. Quem
entra primeiro no log fica com o assento; os demais pedidos para aquele assento são
recusados de forma consistente. Se o terminal coordenador cair no meio do
atendimento, um novo é eleito e as reservas já confirmadas continuam íntegras.

> Observação: aqui os terminais são **honestos** (podem cair, mas não mentem). Nós que
> mentem (bizantinos) exigiriam consenso bizantino (ex.: PBFT), que é outro building
> block — fora do escopo deste trabalho.

## Arquitetura

```
        passageiros (client_simulator.py / election_test_client.py)
              │  HTTP (reservar assento / consultar estado)
              ▼
   ┌──────────────────────────────────────────────────────────┐
   │  process1        process2        process3                  │  3 réplicas (terminais)
   │  ┌─────────┐    ┌─────────┐    ┌─────────┐                 │
   │  │ assentos│    │ assentos│    │ assentos│  ← máquina de estado replicada
   │  ├─────────┤    ├─────────┤    ├─────────┤                 │
   │  │   VR    │◄──►│   VR    │◄──►│   VR    │  ← consenso (PREPARE/COMMIT/view change)
   │  └─────────┘    └─────────┘    └─────────┘                 │
   └──────────────────────────────────────────────────────────┘
            Camada de comunicação: Berkeley Sockets (TCP)
```

Cada réplica é um único processo com **duas portas**:

| Canal              | Porta        | Uso                                                              |
| ------------------ | ------------ | --------------------------------------------------------------- |
| Socket TCP cru     | 6061–6063    | Protocolo VR **entre réplicas** (PREPARE, COMMIT, view change…) |
| HTTP (Flask)       | 7061–7063    | **Passageiros** e admin (`/client/request`, `/status`, …)       |

A topologia é injetada via variável de ambiente `CLUSTER_TOPOLOGY` (ver
`docker-compose.yml`). O `replica_id` de cada réplica é a própria porta de socket; o
terminal coordenador (primário) inicial é o `6061`.

## Algoritmo (Viewstamped Replication)

**Reserva normal (caminho feliz):**

1. O passageiro envia `POST /client/request` ao primário (`RESERVE 12A passenger=P1`).
2. O primário registra a operação no log, incrementa o `op_num` e faz *broadcast* de
   `PREPARE` aos backups.
3. Cada backup grava no log e responde `PREPARE_OK`.
4. Ao reunir um **quórum** (`f+1`, contando a si mesmo), o primário faz *commit*,
   aplica na máquina de estado e responde ao passageiro.
5. **Heartbeat:** o primário reenvia `COMMIT` periodicamente, mantendo os backups em
   dia e sinalizando que está vivo.

**Deduplicação:** cada passageiro envia pedidos sequenciais (`request_num` crescente);
um pedido repetido já efetivado devolve o resultado em cache, sem reprocessar.

**Eleição de líder / *view change* (quando o primário cai):**

1. Sem contato com o primário por `PRIMARY_TIMEOUT` (5s), um backup suspeita da falha
   e dispara `START_VIEW_CHANGE`, que se propaga pelo cluster.
2. Com quórum de votos, envia-se `DO_VIEW_CHANGE` (com o log de cada nó) ao próximo
   primário determinístico.
3. O novo primário escolhe o log mais atualizado, assume e faz *broadcast* de
   `START_VIEW`.
4. **Recovery:** um nó atrasado que recebe PREPARE/COMMIT de uma *view* mais nova (ex.:
   o primário antigo voltando de uma falha) pede um **state transfer** ao primário
   atual e se sincroniza, reingressando como backup.

## Como executar

Pré-requisitos: **Docker** e **Docker Compose**.

### Cenário 1 — normal + concorrência (simulação de passageiros)

```bash
docker compose --profile simulation up --build
```

Sobe as 3 réplicas e o `client_simulator`, que dispara vários passageiros
concorrentes reservando assentos. **O que observar:** todas as reservas confirmadas, e
ao final os três `process` no **mesmo `commit_num`** (réplicas consistentes).

### Cenário 2 — falha do líder + eleição + recuperação

```bash
docker compose --profile election-test up --build
```

Sobe as 3 réplicas e o `election_test_client`, que executa um roteiro:
aquecimento → **congela o primário** (`/admin/freeze`) → passageiros continuam tentando
→ **um novo primário é eleito** → o nó congelado volta e **se reintegra como backup via
state transfer**. Imprime asserções `✓/✗` e um resumo ao final.

### Subir só o cluster (sem tráfego sintético)

```bash
docker compose up --build
```

Consultas úteis (réplica `process1`): `GET http://localhost:7061/status` e
`GET http://localhost:7061/replica/info`.

## O que observar na demonstração

- **Replicação consistente:** ao final do cenário normal, os três terminais têm o
  mesmo `commit_num` — todos concordam na mesma sequência de reservas.
- **Eleição de líder:** no cenário de falha, o `view_num` avança e um terminal
  sobrevivente assume como primário em poucos segundos.
- **Recuperação:** o terminal que falhou volta como backup e sincroniza o mapa de
  assentos via *state transfer*, sem tentar retomar a liderança.

## Estrutura do projeto

```
trabalho2.0/
├── consensus/
│   ├── viewstamped_replication.py  # lógica do protocolo VR (o building block)
│   └── utils/                      # args (mensagens), Enum (Status), exceptions
├── process/
│   ├── Process.py                  # camada de rede: socket TCP + Flask + heartbeat
│   ├── process1.py / 2 / 3         # réplicas (terminais) do cluster
│   ├── client_simulator.py         # passageiros concorrentes (cenário normal)
│   ├── election_test_client.py     # roteiro de falha + eleição (cenário de falha)
│   ├── Dockerfile
│   └── requirements.txt
└── docker-compose.yml
```

## Limitações

- **Reserva = operação opaca:** a máquina de estado registra a ordem das reservas no
  log replicado, mas não implementa a regra de negócio de recusar um assento já
  ocupado (foco do trabalho é o consenso/ordenação, não as regras de check-in).
- **Sem persistência em disco:** o estado vive em memória; reiniciar todos os nós zera
  o mapa de assentos.
- **Recovery simplificado:** usa *state transfer* completo do primário, em vez do
  protocolo de recovery incremental do VR original.
- Modelo de falhas **fail-stop** (nós honestos que podem cair), não bizantino.
