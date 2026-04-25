import socket
import json
import requests
import time

from cache_aside import LRUCache

REST_SERVER_URL = "http://localhost:5000/urls"
PROXY_HOST = '0.0.0.0'
PROXY_PORT = 8080
CACHE_CAPACITY = 5  # Capacidade máxima do cache LRU
RATE_LIMIT_SECONDS = 1  # Intervalo mínimo entre requisições por IP (Throttling)

class InterceptorProxy:
    def __init__(self):
        # Padrão Obrigatório: Cache-Aside com política LRU
        self.cache = LRUCache(capacity=CACHE_CAPACITY)
        
        # TODO: Segundo Padrão: Rate Limiting (Throttling)
        # Armazena o timestamp da última requisição de cada cliente
        # self.last_request_time = {}

    def process_request(self, raw_data, client_address):
        """Lógica central: decide entre Cache, API REST ou Bloqueio."""
        try:
            request_json = json.loads(raw_data.decode('utf-8'))
            acao = request_json.get("acao")
            
            if acao == "resolve":
                codigo = request_json.get("codigo")
                # Tenta o Cache Primeiro
                cached_url = self.cache.get(codigo)
                if cached_url:
                    return {"url_original": cached_url, "fonte": "cache"}
                
                # Se Cache Miss, vai ao Servidor [cite: 97, 113]
                print(f"[CACHE MISS] Consultando servidor REST para {codigo}...")
                response = requests.get(f"{REST_SERVER_URL}/{codigo}")
                if response.status_code == 200:
                    url_original = response.json()["url_original"]
                    self.add_to_cache(codigo, url_original)
                    return response.json()
                return response.json()

            # 2. ENCURTAR URL
            elif acao == "encurta":
                response = requests.post(REST_SERVER_URL, json={"url": request_json.get("url")})
                return response.json()

            # 3. REMOVER URL (Invalidação de Cache)
            elif acao == "remove":
                codigo = request_json.get("codigo")
                response = requests.delete(f"{REST_SERVER_URL}/{codigo}")
                self.cache.invalidate(codigo)
                return response.json()

        except Exception as e:
            return {"erro": str(e)}

    def start(self):
        """Inicia o Servidor de Sockets TCP."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((PROXY_HOST, PROXY_PORT))
        server_socket.listen(5)
        print(f"[PROXY] Interceptador rodando em {PROXY_HOST}:{PROXY_PORT}...")

        while True:
            client_conn, addr = server_socket.accept()
            print(f"\n[PROXY] Conexão recebida de {addr}")
            
            data = client_conn.recv(1024)
            if data:
                resposta = self.process_request(data, addr)
                client_conn.send(json.dumps(resposta).encode('utf-8'))
            
            client_conn.close()

if __name__ == '__main__':
    proxy = InterceptorProxy()
    proxy.start()