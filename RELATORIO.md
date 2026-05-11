# Relatório – Trabalho 1: Encurtador de URLs Distribuído

**Disciplina:** INE5418 – Computação Distribuída\
**Semestre:** 2026/1\
**Universidade:** Universidade Federal de Santa Catarina (UFSC)

**Integrantes do grupo:**
- Fernanda Larissa Müller (21202109)
- Julia Fischer Gazolla (23250586)
- Lucas Tomio Schwchow (23250585)

---

## 1. Visão geral

O sistema implementa um encurtador de URLs distribuído composto por três componentes independentes:

```
[Clientes (Python / JavaScript)] --TCP/JSON--> [Interceptador (proxy)] --HTTP/REST--> [Servidor REST]
```

- **Servidor REST** (Python/Flask): expõe a API HTTP e mantém os mapeamentos `código curto → URL original` em memória.
- **Interceptador (proxy)** (Python): atua como middleware. Recebe requisições TCP dos clientes, aplica **Cache-Aside (LRU)** e uma **fila de prioridades** (segundo padrão), e repassa via HTTP ao servidor. É transparente para o servidor — o servidor não sabe que o proxy existe.
- **Bibliotecas cliente** em **duas linguagens** (Python e JavaScript/Node.js), com a mesma interface (`encurta`, `resolve`, `remove_url`) sobre o mesmo protocolo JSON-sobre-TCP.

A heterogeneidade é satisfeita por implementar clientes em duas linguagens diferentes que **interoperam** (URL encurtada por um cliente Python pode ser resolvida pelo cliente JS, e vice-versa).

---

## 2. Decisões de implementação

### 2.1 Linguagens e dependências
- **Python 3.10** para servidor, proxy e biblioteca cliente. Servidor usa framwork **Flask** (mais simples que `http.server`, idiomático para REST e pela familiaridade técnica da equipe). Proxy usa **`requests`** para chamar o servidor e biblioteca padrão (`socket`, `threading`, `queue`) para o restante.
- **Node.js 20** para a biblioteca cliente em JavaScript. Sem dependências externas — apenas o módulo nativo `net` (sockets) e `fs` (config).

### 2.2 Protocolo cliente ↔ proxy
JSON sobre TCP, uma requisição por conexão:

```jsonc
// requisições
{"acao": "encurta", "url": "https://..."}
{"acao": "resolve", "codigo": "..."}
{"acao": "remove",  "codigo": "..."}

// respostas
{"codigo": "...", "url_curta": "..."}      // encurta
{"url_original": "..."}                     // resolve (cache miss)
{"url_original": "...", "fonte": "cache"}   // resolve (cache hit) ← marcador útil para depuração
{"removido": true}                          // remove
{"erro": "..."}                             // qualquer falha
```

A campo `"fonte": "cache"` é uma decisão deliberada: permite observar empiricamente quando o cache está atendendo a requisição, sem precisar inspecionar logs do proxy.

### 2.3 Cache-Aside com política LRU (`proxy/cache_aside.py`)
- Estrutura: `collections.OrderedDict`. Cada `get` faz `move_to_end`; `put` insere no fim e remove o início (`popitem(last=False)`) quando excede a capacidade.
- **Política de invalidação:** ao receber `remove`, o proxy primeiro encaminha o `DELETE` ao servidor REST e, em caso de sucesso, chama `cache.invalidate(codigo)`. Isso garante coerência: se o servidor falhar, o cache não é invalidado, evitando inconsistência transitória.
- **Capacidade configurável** (`cache_capacity` no `config.txt`, padrão 5).

### 2.4 Segundo padrão: Fila de Prioridades
Justificativa da escolha:
- É um padrão que faz sentido no contexto do projeto de Encurtador de URLs.
- O proxy recebe três tipos de operação com sensibilidades diferentes a latência:
  - `resolve` (GET) — mais frequente e diretamente percebida pelo usuário.
  - `remove` (DELETE) — afeta coerência do cache; precisa correr antes de `encurta` para não corromper estado.
  - `encurta` (POST) — pode tolerar atraso.
- Implementação: `queue.PriorityQueue` com prioridades `resolve=0`, `remove=1`, `encurta=2` (menor número = atendido antes). Uma thread *worker* daemon consome a fila enquanto a thread principal aceita conexões.
- O padrão é discutido em [Microsoft Azure – Priority Queue Pattern](https://learn.microsoft.com/en-us/azure/architecture/patterns/priority-queue) como mecanismo para **garantir SLA de operações sensíveis quando há contenção**.
- Por que não Circuit Breaker ou Throttling: ambos são úteis principalmente em cenários de falha do servidor / sobrecarga de rede; o problema mais natural neste sistema é dar resposta rápida a `resolve` quando há muitas inserções concorrentes — exatamente o que a fila de prioridades resolve.

### 2.5 Configuração centralizada
O arquivo `config.txt` na raiz do projeto é a fonte única da verdade para parâmetros de runtime. Ele é montado como volume `read-only` em todos os containers (`docker-compose.yml`), de modo que **alterar o arquivo + reiniciar o serviço aplica sem rebuild**. Precedência adotada:

```
variável de ambiente  >  config.txt  >  default no código
```

Variáveis de ambiente continuam disponíveis como *escape hatch* (ex.: rodar o cliente fora do Docker apontando para `localhost`).

### 2.6 Independência do servidor
O servidor REST não tem qualquer conhecimento do proxy. Ele aceita requisições HTTP de quem quer que o alcance — o que satisfaz o requisito do enunciado de que "o interceptador deve ser transparente para o servidor".

### 2.7 Identificação de códigos curtos
UUID v4 (`uuid.uuid4()`). Decisão pragmática: garante unicidade sem coordenação distribuída e elimina a necessidade de detecção de colisão. Tradeoff: códigos longos (~36 chars). Em sistema real provavelmente usaríamos hash truncado + verificação de colisão.

---

## 3. Estrutura do projeto

```
Encurtador-de-URLs-Distribuido/
├── config.txt                       # configuração centralizada
├── docker-compose.yml               # orquestração dos 4 serviços
├── server/                          # API REST (Flask)
│   ├── server.py
│   ├── requirements.txt
│   └── Dockerfile
├── proxy/                           # Interceptador (TCP + cache + fila)
│   ├── proxy.py
│   ├── cache_aside.py
│   ├── requirements.txt
│   └── Dockerfile
├── clients/python/                  # Biblioteca cliente em Python
│   ├── client.py
│   ├── requirements.txt
│   └── Dockerfile
└── clientes/javascript/             # Biblioteca cliente em JavaScript
    ├── client.js
    ├── example.js
    ├── package.json
    └── Dockerfile
```

---

## 4. Como compilar e executar

### 4.1 Pré-requisitos
- Docker Desktop (ou Docker Engine + Docker Compose v2).
- macOS: porta 5000 ocupada pelo AirPlay Receiver — por isso o servidor é exposto no host na porta **5050** (interna 5000).

### 4.2 Subir o sistema completo
```bash
docker compose up --build
```
Isso constrói as 4 imagens (server, proxy, client-python, client-js) e sobe a stack. Os clientes executam um exemplo automático e encerram; servidor e proxy ficam rodando.

### 4.3 Acessar do host
- Servidor REST: `http://localhost:5050/urls`
- Proxy TCP: `localhost:8080`

### 4.4 Rodar o cliente Python contra a stack
```bash
docker compose run --rm client-python python -c "
from client import EncurtadorClient
c = EncurtadorClient()
print(c.encurta('https://www.ufsc.br'))
"
```

### 4.5 Rodar o cliente JavaScript contra a stack
```bash
docker compose run --rm client-js
# ou: docker compose run --rm client-js node example.js
```

### 4.6 Derrubar
```bash
docker compose down
```

---

## 5. Exemplos de saídas de execução

### 5.1 Cache HIT (Python)
```
$ docker compose run --rm client-python python -c "
from client import EncurtadorClient
c = EncurtadorClient()
r = c.encurta('https://example.com'); cod = r['codigo']
print('1a:', c.resolve(cod))
print('2a:', c.resolve(cod))
"
1a: {'url_original': 'https://example.com'}
2a: {'url_original': 'https://example.com', 'fonte': 'cache'}
```
Logs do proxy correspondentes (`docker compose logs proxy`):
```
[FILA] Processando requisição de (172.18.0.4, ...) | prioridade=0
[CACHE MISS] Consultando servidor REST para 7cb02c29-...
[FILA] Processando requisição de (172.18.0.4, ...) | prioridade=0
# (sem CACHE MISS na 2a — atendeu pelo cache local)
```

### 5.2 Invalidação de cache (JS)
```
$ docker compose run --rm client-js node -e "..."
1a:        { url_original: 'https://test2.config' }
2a HIT:    { url_original: 'https://test2.config', fonte: 'cache' }
rm:        { removido: true }
apos rm:   { erro: 'Código não encontrado' }
```
A 4ª chamada não encontrou no cache (porque `remove` invalidou) e o servidor já tinha apagado a entrada — exatamente a coerência exigida pelo enunciado.

### 5.3 Heterogeneidade — interop Python/JS
Sequência: **Python encurta → JS resolve → JS remove → Python tenta resolver**.
```
Codigo gerado pelo Python: 826da07c-879b-4b73-b64a-9b713fec51f9
JS resolveu:                   { url_original: 'https://heterogeneidade.test' }
JS resolveu de novo (HIT):     { url_original: 'https://heterogeneidade.test', fonte: 'cache' }
JS removeu:                    { removido: true }
Python resolveu (apos remove): {'erro': 'Código não encontrado'}
```
Confirma que o estado (URLs no servidor + cache no proxy) é compartilhado entre clientes de linguagens diferentes.

### 5.4 Mudança de parâmetro via `config.txt`
Editando `cache_capacity=5 → 99` e reiniciando o proxy:
```
$ docker compose restart proxy
$ docker compose exec proxy python -c "import proxy; print(proxy.CACHE_CAPACITY)"
99
```
Sem rebuild de imagem — confirma que `config.txt` é fonte única da verdade.

### 5.5 Listagem com contador de acessos (servidor)
```
$ curl -s http://localhost:5050/urls
[
  {"acessos": 1, "codigo": "...", "url_original": "https://example.com"},
  ...
]
```
Observação importante: `acessos = 1` mesmo após **duas** resoluções pelo cliente. Isso é prova de que a 2ª resolução foi atendida pelo **cache do proxy** e **não chegou ao servidor** — a economia de tráfego prometida pelo Cache-Aside está acontecendo.

### 5.6 Comportamento da fila de prioridades sob carga

Para tornar visível o reordenamento da fila — que em uso normal não se observa porque o worker consome rápido demais — o proxy aceita a variável `DEMO_DELAY` (em segundos), que aplica um atraso artificial em cada requisição. Default `0`; em produção não muda nada.

O script `demos/demo_prioridades.py` dispara **15 requisições em paralelo** misturando `encurta` (prio=2), `remove` (prio=1) e `resolve` (prio=0), com a ordem cronológica embaralhada para forçar o teste.

```bash
$ DEMO_DELAY=0.5 docker compose up -d proxy
$ python3 demos/demo_prioridades.py
```

Saída resumida:

```
=== ORDEM DE ENVIO (cronológica, todos em ~1ms) ===
  T+0.2ms  #00 resolve (prio=0)
  T+0.3ms  #01 remove  (prio=1)
  T+0.3ms  #02 encurta (prio=2)
  ...
  T+0.9ms  #14 resolve (prio=0)

=== ORDEM DE PROCESSAMENTO (resposta do worker) ===
  T+ 517.8ms  #06 encurta (prio=2)   ← já estava no worker quando a fila começou a encher
  T+1026.9ms  #08 resolve (prio=0)
  T+1530.6ms  #14 resolve (prio=0)
  T+2036.6ms  #11 resolve (prio=0)
  T+2543.6ms  #00 resolve (prio=0)   ← TODOS os resolves saíram primeiro
  T+3051.7ms  #05 remove  (prio=1)
  T+3562.3ms  #04 remove  (prio=1)
  T+4069.4ms  #01 remove  (prio=1)   ← depois TODOS os removes
  T+4579.4ms  #07 encurta (prio=2)
  ...
  T+7637.2ms  #13 encurta (prio=2)   ← encurtas por último

=== TEMPO MÉDIO DE RESPOSTA POR PRIORIDADE ===
  prio=0 (resolve): média 1784.4ms  (n=4)
  prio=1 (remove ): média 3561.1ms  (n=3)
  prio=2 (encurta): média 5410.1ms  (n=8)
```

**Interpretação:** depois que a fila começou a acumular, o worker processou estritamente em ordem de prioridade — todos os 4 resolves antes de qualquer remove, e todos os 3 removes antes de qualquer encurta — independentemente da ordem cronológica de chegada (que era basicamente simultânea, mas embaralhada). Em média, um `resolve` foi atendido **3× mais rápido** que um `encurta`. Isso é exatamente a garantia de SLA que a fila de prioridades promete.

A única requisição "fora de ordem" foi a `#06 (encurta)`: ela foi processada primeiro porque o worker a pegou antes da fila ter outras requisições enfileiradas — não há mecanismo de preempção, e isso é OK em uma fila não-preemptiva. Em regime estacionário (alta carga), o efeito de prioridade domina.

### 5.7 Eviction do cache LRU ao atingir capacidade

O exemplo embutido em `clients/python/client.py` (`__main__`) encurta+resolve **6 URLs distintas**. Como `cache_capacity=5`, a 6ª inserção dispara eviction da entrada menos recentemente usada.

```
$ docker compose run --rm client-python
Resolvendo 6eb12957-... : {'url_original': 'https://www.inf.ufsc.br'}    # 1ª add
Resolvendo 6eb12957-... : {'url_original': 'https://www.inf.ufsc.br', 'fonte': 'cache'}  # HIT
Resolvendo 089be493-... : {'url_original': 'https://www.ppgcc.ufsc.br'}  # 2ª add
Resolvendo 0c167869-... : {'url_original': 'https://www.ine.ufsc.br'}    # 3ª add
Resolvendo 85950d51-... : {'url_original': 'https://www.ufsc.br'}        # 4ª add
Resolvendo d2184733-... : {'url_original': 'https://www.eas.ufsc.br'}    # 5ª add (cache cheia)
Resolvendo 3424b0d8-... : {'url_original': 'https://www.cse.ufsc.br'}    # 6ª add → EVICTION
Testando remoção: {'removido': True}
Resolvendo 3424b0d8-... : {'erro': 'Código não encontrado'}              # invalidação
```

Logs correspondentes do proxy:
```
[CACHE MISS] ... 6eb12957-... (inf)
[CACHE HIT]  ... 6eb12957-... (inf, move para MRU)
[CACHE MISS] ... 089be493-... (ppgcc)
[CACHE MISS] ... 0c167869-... (ine)
[CACHE MISS] ... 85950d51-... (ufsc)
[CACHE MISS] ... d2184733-... (eas)            ← cache atinge 5
[CACHE MISS] ... 3424b0d8-... (cse)            ← chega 6º, dispara eviction
[CACHE] Capacidade atingida. Removendo entrada obsoleta: 6eb12957-...   ← inf é o LRU
[CACHE] Entrada 3424b0d8-... invalidada com sucesso.
[CACHE MISS] ... 3424b0d8-... (cse após remove → 404 do servidor)
```

**Observação importante sobre qual entrada foi expulsa:** o `inf.ufsc.br` foi expulso, e não o `ppgcc.ufsc.br` (que foi adicionado *depois* do inf), porque o HIT no inf, na 2ª resolução, fez `move_to_end` no `OrderedDict` — promovendo inf a *most recently used* naquele momento. Mesmo assim, depois de 4 inserções subsequentes (ppgcc, ine, ufsc, eas), inf voltou a ser o mais antigo e foi corretamente identificado como vítima do eviction. Isso comprova que a política LRU está agindo sobre a janela de **acessos** (gets + puts), não apenas de inserções.

Esta seção, junto com 5.1 (HIT) e 5.2 (invalidação), cobre os **quatro comportamentos** do cache: miss, hit, eviction por capacidade e invalidação por remoção.

---

## 6. Conclusões

### 6.1 Resultados obtidos
- O padrão **Cache-Aside** reduz tráfego ao servidor de forma observável (contador de acessos no servidor cresce só em cache miss).
- A invalidação ativa do cache no `remove` evita dados obsoletos sem complicação extra.
- A fila de prioridades introduz equidade entre tipos de requisição com custo baixo (uma thread *worker* + uma fila).
- Heterogeneidade não é uma característica acoplada à linguagem do servidor: o protocolo JSON-sobre-TCP é simples o suficiente para qualquer linguagem com socket nativo implementar (~30 linhas de código no caso de Node.js).
- Um arquivo `config.txt` montado como volume mostra-se uma estratégia limpa para parametrização sem rebuild.

### 6.2 Limitações observadas
- **Armazenamento volátil:** o servidor REST guarda URLs em memória — reiniciar perde tudo. Suficiente para o exercício, mas inadequado para produção.
- **Cache só no proxy / instância única:** em deployment multi-proxy, cada réplica teria seu próprio cache, podendo retornar versões diferentes para a mesma URL. Solução real: cache distribuído (Redis) ou esquema *cache-coherence* baseado em pub/sub.
- **Conexão TCP não persistente:** cada chamada do cliente abre + fecha um socket. Para alta carga seria interessante manter pool de conexões ou usar WebSocket / gRPC.
- **Sem autenticação ou rate limiting global:** qualquer um pode encurtar/remover. A fila de prioridades trata fairness *interna*, não abuso *externo*.
- **Sem TTL no cache:** uma URL atualizada diretamente no servidor (via `curl`) não invalida o cache automaticamente. Como toda mutação passa pelo proxy no fluxo normal, isso só seria problema em deployments com múltiplos pontos de escrita.
- **`recv(1024)` no proxy** assume que toda requisição cabe em 1024 bytes. URLs muito longas poderiam ser truncadas. Para produção, ler até delimitador (newline) ou usar prefixo de tamanho.
- **Servidor em modo `debug=True`:** Flask reinicia ao detectar mudanças no código — bom para desenvolvimento, péssimo para produção (deveria ser WSGI dedicado, ex.: Gunicorn).

### 6.3 Possíveis extensões
- Trocar a fila de prioridades por **Circuit Breaker** ou **Rate Limiting** seria igualmente válido. A arquitetura atual permite encaixar um segundo padrão *em série* com a fila atual sem grande refatoração e ambos fazem sentido no contexto desse projeto.
- Persistir o servidor REST em SQLite ou Redis.
- Adicionar TTL no cache para reduzir a janela de incoerência em casos de falha do `remove`.
