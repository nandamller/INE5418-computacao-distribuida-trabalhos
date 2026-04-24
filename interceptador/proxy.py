import socket
import threading
import json
import time
import requests
from collections import OrderedDict

# Configurações do Interceptador
REST_API_URL = "http://localhost:8000"
PROXY_HOST = "0.0.0.0"
PROXY_PORT = 9000

# Parâmetros dos Padrões de Projeto
CACHE_CAPACITY = 100       # [Cache-Aside] LRU Max Itens
RATE_LIMIT_REQ_PER_SEC = 10 # [Rate Limiting] max 10 reqs por segundo por IP

# ==========================================
# PADRÃO 1: Cache-Aside (Thread-Safe LRU)
# ==========================================
class ThreadSafeLRUCache:
    """Implementação simples de Cache LRU orientada a Thread-Safety."""
    def __init__(self, capacity: int):
        self.capacity = capacity
        # Usamos OrderedDict pois ele preserva a ordem de inserção (útil para LRU)
        self.cache = OrderedDict()
        self.lock = threading.Lock()

    def get(self, key):
        with self.lock:
            if key in self.cache:
                # Move a chave para o final indicando que foi acessada recentemente
                self.cache.move_to_end(key)
                return self.cache[key]
            return None

    def put(self, key, value):
        with self.lock:
            self.cache[key] = value
            self.cache.move_to_end(key)
            # Se exceder capacidade, remove o mais antigo (no início do OrderedDict)
            if len(self.cache) > self.capacity:
                self.cache.popitem(last=False)

    def remove(self, key):
        with self.lock:
            if key in self.cache:
                del self.cache[key]

# ==========================================
# PADRÃO 2: Rate Limiting (Fixed Window Simples)
# ==========================================
class RateLimiter:
    """Implementa o padrão Throttling/Rate Limiting por IP usando Janela Deslizante de 1s."""
    def __init__(self, max_requests: int, time_window_sec: float):
        self.max_requests = max_requests
        self.time_window_sec = time_window_sec
        # Dicionário: IP -> Lista de timestamps das últimas requisições
        self.clients = {}  
        self.lock = threading.Lock()

    def is_allowed(self, client_ip: str) -> bool:
        with self.lock:
            now = time.time()
            if client_ip not in self.clients:
                self.clients[client_ip] = []
            
            # Remove timestamps obsoletos fora da janela de tempo atual
            self.clients[client_ip] = [t for t in self.clients[client_ip] if now - t < self.time_window_sec]
            
            if len(self.clients[client_ip]) < self.max_requests:
                # Libera o acesso e armazena o momento da requisição
                self.clients[client_ip].append(now)
                return True
            return False

# Inicialização global das instâncias
cache = ThreadSafeLRUCache(CACHE_CAPACITY)
limiter = RateLimiter(RATE_LIMIT_REQ_PER_SEC, 1.0)

def process_request(conn, client_ip, req_str):
    """Lida com a lógica distribuída, se é cacheable e traduz json."""
    try:
        pedido = json.loads(req_str)
        acao = pedido.get("acao")
        
        if acao == "encurta":
            url = pedido.get("url")
            # [Proxy Repassa] -> Não vamos tentar cachear aqui pois é operação de Escrita (Post)
            resp = requests.post(f"{REST_API_URL}/urls", json={"url": url})
            
            if resp.status_code == 200:
                data_resp = resp.json()
                resposta = {"status": "ok", "codigo": data_resp["codigo"], "url_curta": data_resp["url_curta"]}
            else:
                resposta = {"status": "erro", "mensagem": "Falha na API REST"}
                
        elif acao == "resolve":
            codigo = pedido.get("codigo")
            
            # [CACHE ASIDE] - LEITURA
            cached_url = cache.get(codigo)
            if cached_url:
                print(f"[CACHE HIT] Resolve: {codigo}")
                resposta = {"status": "ok", "url_original": cached_url}
            else:
                print(f"[CACHE MISS] Resolve: {codigo} - Acessando API REST...")
                resp = requests.get(f"{REST_API_URL}/urls/{codigo}")
                if resp.status_code == 200:
                    url_original = resp.json()["url_original"]
                    
                    # [CACHE ASIDE] - ARMAZENA DADO NO CACHE
                    cache.put(codigo, url_original)
                    resposta = {"status": "ok", "url_original": url_original}
                else:
                    resposta = {"status": "erro", "mensagem": "URL não encontrada."}
        
        elif acao == "remove":
            codigo = pedido.get("codigo")
            
            # [CACHE ASIDE] - REMOÇÃO (Invalidação)
            print(f"[INVALIÇÃO CACHE] Removendo: {codigo}")
            cache.remove(codigo)
            
            # Propaga para Fonte da Verdade (Server REST)
            resp = requests.delete(f"{REST_API_URL}/urls/{codigo}")
            if resp.status_code == 200:
                resposta = {"status": "ok", "removido": True}
            else:
                resposta = {"status": "erro", "mensagem": "Falha ao remover na API REST ou URL não encontrada"}
        
        else:
            resposta = {"status": "erro", "mensagem": "Ação desconhecida"}
            
        return json.dumps(resposta)
        
    except json.JSONDecodeError:
        return json.dumps({"status": "erro", "mensagem": "Formato JSON inválido"})
    except requests.ConnectionError:
        return json.dumps({"status": "erro", "mensagem": "Conexão com a API REST perdida."})
    except Exception as e:
        print(f"[ERRO Interno] {e}")
        return json.dumps({"status": "erro", "mensagem": "Erro interno no proxy"})

def handle_client(conn, addr):
    """Comunicação via Sokcets TCP com os Clientes da Biblioteca."""
    client_ip = addr[0]
    print(f"[NOVA CONEXÃO] Cliente conectado: {addr}")
    
    with conn:
        try:
            # Esperamos que a biblioteca cliente envie dados num simples disparo.
            data = conn.recv(4096)
            if not data:
                return
            
            # APATMENTO DE RATE LIMITING
            if not limiter.is_allowed(client_ip):
                print(f"[RATE LIMIT] Cliente {client_ip} excedeu {RATE_LIMIT_REQ_PER_SEC} req/s.")
                erro_msg = json.dumps({"status": "erro", "mensagem": "Rate limit excedido. Tente novamente mais tarde."}) + "\n"
                conn.sendall(erro_msg.encode('utf-8'))
                return

            req_str = data.decode('utf-8').strip()
            
            # Processa e captura o JSON Output em String
            resposta_str = process_request(conn, client_ip, req_str) + "\n"
            
            # Responde via TPC Socket puro
            conn.sendall(resposta_str.encode('utf-8'))
            
        except Exception as e:
            print(f"[ERRO Socket] Cliente {addr} desconectado com erro: {e}")

def start_proxy():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server.bind((PROXY_HOST, PROXY_PORT))
        server.listen(50)
        
        print(f"==================================================")
        print(f"[*] Interceptador/Proxy rodando em {PROXY_HOST}:{PROXY_PORT}")
        print(f"[*] Limite de Requisições: {RATE_LIMIT_REQ_PER_SEC} req/s por IP")
        print(f"[*] Capacidade do Cache Limitado (LRU): {CACHE_CAPACITY} itens")
        print(f"[*] Mapeado para Servidor Alvo em: {REST_API_URL}")
        print(f"==================================================")
        
        while True:
            # Main Server loop travante esperando novos clientes
            conn, addr = server.accept()
            # Delegamos o trabalho do cliente a uma nova Thread para nao travar main loop
            thread = threading.Thread(target=handle_client, args=(conn, addr))
            thread.start()
            
    except KeyboardInterrupt:
        print("\n[!] Encerrando Proxy...")
    finally:
        server.close()

if __name__ == "__main__":
    start_proxy()
