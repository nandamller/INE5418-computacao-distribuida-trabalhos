# Encurtador de URLs Distribuído

Trabalho 1 da disciplina **INE5418 – Computação Distribuída** (UFSC, 2026/1).
Implementação de um encurtador de URLs com três componentes que se comunicam por protocolos diferentes:

```
[Clientes Python / JavaScript] --sockets TCP/JSON--> [Interceptador] --HTTP/REST--> [Servidor REST]
```

Para a justificativa das decisões de projeto, exemplos de execução comentados, análise dos padrões aplicados (Cache-Aside + Fila de Prioridades) e discussão de limitações, ver [`RELATORIO.md`](./RELATORIO.md).

**Integrantes:** Fernanda Larissa Müller, Julia Fischer Gazolla, Lucas Tomio Schwchow.

---

## Pré-requisitos

- **Docker** e **Docker Compose v2** instalados.
  Para verificar: `docker --version` e `docker compose version`.
- **Não é necessário** ter Python ou Node.js instalados no host — tudo roda em containers.
  A única exceção é o script de demo da fila de prioridades (seção "Demos" abaixo), que roda no host e requer Python 3.

### Atenção macOS

A porta 5000 é ocupada pelo **AirPlay Receiver** do macOS. Por isso, o `docker-compose.yml` mapeia o servidor REST para a porta **5050** no host (`5050:5000`). Em Linux/Windows você pode trocar para `5000:5000` se quiser usar 5000.

---

## Como rodar (passo a passo)

### 1. Subir o sistema

Na raiz do projeto:

```bash
docker compose up --build
```

O que isso faz:
- Constrói 4 imagens: `server`, `proxy`, `client-python`, `client-js`.
- Sobe os containers `api_rest_servidor` (porta 5050) e `proxy_interceptor` (porta 8080).
- Executa o exemplo embutido de cada cliente (`client.py __main__` e `example.js`) e fecha esses containers em seguida.
- Servidor e proxy seguem rodando em foreground com logs no terminal.

Se preferir rodar em background:

```bash
docker compose up -d --build
```

### 2. Ver os logs em tempo real

```bash
docker compose logs -f
# ou só de um serviço:
docker compose logs -f proxy
docker compose logs -f server
```

### 3. Disparar um cliente manualmente

```bash
# Python — executa clients/python/client.py
docker compose run --rm client-python

# JavaScript — executa clients/javascript/example.js
docker compose run --rm client-js
```

Ambos rodam um cenário completo: encurtar URLs, resolver (mostrando cache miss e hit), remover, e tentar resolver depois do remove. A saída tem prefixos `[CLIENT|Py]` e `[CLIENT|JS]` para distinguir.

### 4. Bater direto na API REST (debug)

```bash
# Listar todas as URLs encurtadas e o contador de acessos
curl http://localhost:5050/urls

# Encurtar uma URL
curl -X POST http://localhost:5050/urls \
     -H "Content-Type: application/json" \
     -d '{"url":"https://www.ufsc.br"}'

# Resolver um código
curl http://localhost:5050/urls/<codigo>

# Remover
curl -X DELETE http://localhost:5050/urls/<codigo>
```

> Lembrando que essas chamadas direto na API **não passam pelo proxy** e portanto não usam o cache nem a fila de prioridades. São úteis só para inspecionar o estado do servidor.

### 5. Encerrar

```bash
docker compose down
```

Isso para e remove todos os containers e a rede `distributed-network`. As imagens ficam guardadas no Docker; para limpar tudo:

```bash
docker compose down --rmi all
```

---

## Estrutura do projeto

```
.
├── docker-compose.yml             # Orquestração dos 4 serviços
├── README.md                      # Este arquivo
├── RELATORIO.md                   # Relatório técnico completo
├── server/                        # API REST em Flask
│   ├── server.py
│   ├── requirements.txt
│   └── Dockerfile
├── proxy/                         # Interceptador (TCP + cache + fila)
│   ├── proxy.py
│   ├── cache_aside.py
│   ├── requirements.txt
│   └── Dockerfile
├── clients/
│   ├── python/                    # Biblioteca cliente Python
│   │   ├── client.py              # contém também um __main__ de exemplo
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   └── javascript/                # Biblioteca cliente Node.js
│       ├── client.js              # módulo exportando EncurtadorClient
│       ├── example.js             # script de exemplo (executado pelo Docker)
│       ├── package.json
│       └── Dockerfile
└── demos/
    └── demo_prioridades.py        # script de carga para evidenciar a fila
```

---

## Componentes em detalhe

### Servidor REST (`server/`)

Implementado em Python com Flask. Roda na porta 5000 dentro do container, exposta em 5050 no host. Endpoints:

| Método | Caminho           | Descrição                                  | Corpo / Resposta                              |
|--------|-------------------|--------------------------------------------|-----------------------------------------------|
| POST   | `/urls`           | Encurta uma URL                            | req: `{"url":"..."}` → res: `{"codigo","url_curta"}` |
| GET    | `/urls/<codigo>`  | Resolve um código curto                    | res: `{"url_original":"..."}` ou 404         |
| DELETE | `/urls/<codigo>`  | Remove um mapeamento                       | res: `{"removido": true}` ou 404             |
| GET    | `/urls`           | Lista todas as URLs com contador de acessos | res: array de objetos                       |

Armazenamento em dicionário em memória. Códigos curtos são gerados com `shortuuid.uuid(name=url)[:8]` — determinístico (mesma URL ⇒ mesmo código).

### Interceptador / Proxy (`proxy/`)

Servidor TCP que escuta na porta 8080. Recebe mensagens JSON e repassa via HTTP ao servidor REST. Implementa dois padrões:

- **Cache-Aside (LRU)**: classe `LRUCache` em `cache_aside.py`, baseada em `collections.OrderedDict`. Capacidade configurável via env `CACHE_CAPACITY` (default 5).
- **Fila de Prioridades**: `queue.PriorityQueue` com três prioridades (`resolve=0`, `remove=1`, `encurta=2`). Thread principal aceita conexões e enfileira; thread worker daemon consome a fila.

### Bibliotecas cliente

Os dois clientes expõem a mesma interface, sobre o mesmo protocolo JSON-sobre-TCP.

**Python** (`clients/python/client.py`):
```python
from client import EncurtadorClient
c = EncurtadorClient()
c.encurta("https://...")           # → {"codigo": "...", "url_curta": "..."}
c.resolve("WZRaUjsF")              # → {"url_original": "..."} ou com "fonte": "cache"
c.remove_url("WZRaUjsF")           # → {"removido": True}
```

**JavaScript** (`clients/javascript/client.js`):
```js
const { EncurtadorClient } = require('./client');
const c = new EncurtadorClient();
await c.encurta("https://...");
await c.resolve("WZRaUjsF");
await c.removeUrl("WZRaUjsF");
```

Cada chamada abre um socket TCP, envia o JSON, lê a resposta e fecha o socket.

---

## Demos

### Verificar cache HIT e invalidação

Rode o exemplo do cliente Python — ele cobre os 4 comportamentos do cache (miss, hit, eviction LRU, invalidação no remove) em uma única execução:

```bash
docker compose run --rm client-python
```

A saída e os logs correspondentes estão analisados na seção 5.7 do `RELATORIO.md`.

### Demonstrar a Fila de Prioridades sob carga

Em uso normal, a fila quase nunca acumula porque o worker processa cada requisição em milissegundos. Para evidenciar o reordenamento por prioridade, o proxy aceita a variável de ambiente **`DEMO_DELAY`** que adiciona um atraso artificial em cada requisição. Default é `0` (sem efeito).

```bash
# 1. Sobe o proxy com atraso de 500ms por requisição
DEMO_DELAY=0.5 docker compose up -d proxy

# 2. Roda o script de carga (do host — requer Python 3 instalado)
python3 demos/demo_prioridades.py
```

O script abre 15 conexões TCP em paralelo misturando os 3 tipos de operação e imprime:
- A ordem cronológica de envio (todas em ~1ms).
- A ordem em que o worker do proxy respondeu.
- Tempo médio por prioridade.

Resultado esperado: todos os `resolve` (prio=0) saem antes dos `remove` (prio=1), que saem antes dos `encurta` (prio=2), independente da ordem cronológica de chegada.

Para voltar ao comportamento normal:

```bash
docker compose up -d proxy   # sem DEMO_DELAY = volta a 0
```

---

## Parâmetros configuráveis

Todos por variável de ambiente, declarados em `docker-compose.yml`:

| Serviço         | Variável             | Default                          | O que faz                                       |
|-----------------|----------------------|----------------------------------|-------------------------------------------------|
| `server`        | `SERVER_HOST`        | `0.0.0.0`                        | Interface em que o Flask escuta                 |
| `server`        | `SERVER_PORT`        | `5000`                           | Porta interna do servidor                       |
| `proxy`         | `PROXY_BIND_HOST`    | `0.0.0.0`                        | Interface em que o proxy escuta                 |
| `proxy`         | `PROXY_BIND_PORT`    | `8080`                           | Porta interna do proxy                          |
| `proxy`         | `REST_SERVER_URL`    | `http://server:5000/urls`        | URL do servidor REST vista pelo proxy           |
| `proxy`         | `CACHE_CAPACITY`     | `5`                              | Capacidade máxima do cache LRU                  |
| `proxy`         | `DEMO_DELAY`         | `0`                              | Atraso artificial por requisição (s) — só demo  |
| `client-python` | `PROXY_HOST`         | `proxy`                          | Hostname do proxy visto pelo cliente            |
| `client-python` | `PROXY_PORT`         | `8080`                           | Porta do proxy                                  |
| `client-js`     | `PROXY_HOST`         | `proxy`                          | (idem)                                          |
| `client-js`     | `PROXY_PORT`         | `8080`                           | (idem)                                          |

Para alterar pontualmente sem editar arquivo:

```bash
CACHE_CAPACITY=20 docker compose up -d proxy
```

---

## Troubleshooting

### "Cannot connect to the Docker daemon"
Docker Desktop não está rodando. Abra ele e espere o ícone da baleia ficar estável:
```bash
open -a Docker        # macOS
```

### "ports are not available: ... 0.0.0.0:5000"
A porta 5000 está em uso (AirPlay no macOS, ou outro processo). O projeto já mapeia para 5050 por padrão. Se o conflito for em outra porta, ajuste o mapeamento em `docker-compose.yml`:
```yaml
ports:
  - "5050:5000"   # host:container — mude o lado esquerdo
```

### Cliente recebe `Falha na comunicação: connection refused`
O proxy não está de pé. Verifique:
```bash
docker compose ps
docker compose logs proxy
```

### Logs do servidor não aparecem em tempo real
Garante que a variável `PYTHONUNBUFFERED=1` está no `environment:` do serviço (já está, mas se você modificou, confira).

### "Could not resolve host: host" ao clicar na `url_curta`
Esperado. A `url_curta` retornada pelo servidor (`http://host/r/<codigo>`) **não é uma URL navegável**. O enunciado define `url_curta` como um campo da resposta REST, mas não exige um endpoint HTTP funcional de redirecionamento. Toda resolução de código curto no nosso sistema é feita via cliente TCP → proxy → servidor; o campo `url_curta` é apenas um identificador formatado. Detalhes na seção 2.8 do `RELATORIO.md`.

### Quero limpar tudo e começar do zero
```bash
docker compose down --rmi all -v
docker compose up --build
```
