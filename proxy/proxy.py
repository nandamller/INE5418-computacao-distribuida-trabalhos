import socket
import json
import requests
import time

import threading
import queue

from cache_aside import LRUCache

REST_SERVER_URL = "http://server:5000/urls"
PROXY_HOST = '0.0.0.0'
PROXY_PORT = 8080
CACHE_CAPACITY = 5  # Capacidade máxima do cache LRU
RATE_LIMIT_SECONDS = 1  # Intervalo mínimo entre requisições por IP (Throttling)

# Prioridade definida conforme a ação
PRIORIDADES = {
    "GET": 0,       # maior prioridade posi é a operação mais frequente e sensível a atraso
    "DELETE": 1,    # importante para manter coerência do cache
    "POST": 2       # menor prioridade
}

class InterceptorProxy:
    def __init__(self):
        # Padrão Obrigatório: Cache-Aside com política LRU
        self.cache = LRUCache(capacity=CACHE_CAPACITY)
        
        # Segundo Padrão: Fila de prioridade
        self.fila_prioridade = queue.PriorityQueue()
        self.contador_requisicoes = 0

        # Thread responsável por consumir a fila
        self.worker = threading.Thread(target=self.processar_fila, daemon=True)
        self.worker.start()

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
                
                # Se Cache Miss, vai ao Servidor
                print(f"[CACHE MISS] Consultando servidor REST para {codigo}...")
                response = requests.get(f"{REST_SERVER_URL}/{codigo}")
                if response.status_code == 200:
                    url_original = response.json()["url_original"]
                    self.cache.put(codigo, url_original)
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
        
    def definir_prioridade(self, raw_data):
        """
        Define a prioridade da requisição antes de colocá-la na fila.
        """
        try:
            request_json = json.loads(raw_data.decode("utf-8"))
            acao = request_json.get("acao")
            return PRIORIDADES.get(acao, 99)
        except Exception:
            return 99

    def processar_fila(self):
        """
        Processa continuamente as requisições da fila.
        Requisições com menor valor de prioridade são processadas primeiro.
        """
        while True:
            prioridade, ordem_chegada, raw_data, client_conn, addr = self.fila_prioridade.get()

            print(f"[FILA] Processando requisição de {addr} | prioridade={prioridade}")

            try:
                resposta = self.process_request(raw_data, addr)
                client_conn.send(json.dumps(resposta).encode("utf-8"))
            except Exception as e:
                erro = {"erro": str(e)}
                client_conn.send(json.dumps(erro).encode("utf-8"))
            finally:
                client_conn.close()
                self.fila_prioridade.task_done()

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
                prioridade = self.definir_prioridade(data)

                self.contador_requisicoes += 1
                ordem_chegada = self.contador_requisicoes

                self.fila_prioridade.put(
                    (prioridade, ordem_chegada, data, client_conn, addr)
                )

                print(
                    f"[FILA] Requisição adicionada | "
                    f"addr={addr} | prioridade={prioridade} | posição aproximada={self.fila_prioridade.qsize()}"
                )
            else:
                client_conn.close()
        

if __name__ == '__main__':
    print("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaA")
    proxy = InterceptorProxy()
    print("[PROXY] Iniciando interceptador...")
    proxy.start()