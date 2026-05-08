import os
import socket
import json
import requests
import time

import threading
import queue

from cache_aside import LRUCache


def load_config(path="config.txt"):
    cfg = {}
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                cfg[k.strip()] = v.strip()
    return cfg


CONFIG = load_config()
REST_SERVER_URL = os.getenv("REST_SERVER_URL", CONFIG.get("rest_server_url", "http://server:5000/urls"))
PROXY_HOST = os.getenv("PROXY_BIND_HOST", CONFIG.get("proxy_host", "0.0.0.0"))
PROXY_PORT = int(os.getenv("PROXY_BIND_PORT", CONFIG.get("proxy_port", "8080")))
CACHE_CAPACITY = int(os.getenv("CACHE_CAPACITY", CONFIG.get("cache_capacity", "5")))

# Prioridade definida conforme a ação
PRIORIDADES = {
    "resolve": 0,       # GET - maior prioridade pois é a operação mais frequente e sensível a atraso
    "remove": 1,        # DELETE - importante para manter coerência do cache
    "encurta": 2        # POST - menor prioridade
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
                    print(f"[CACHE HIT] Encontrado na CACHE de {codigo} para cached_url.")
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

            print(f"[FILA] Processando requisição de {addr} | prioridade={prioridade} \n")

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
                    f"addr={addr} | prioridade={prioridade} | posição aproximada={self.fila_prioridade.qsize()} \n"
                )
            else:
                client_conn.close()
        

if __name__ == '__main__':
    print("[DEBUG] O interceptador vai iniciar")
    proxy = InterceptorProxy()
    print("[PROXY] Iniciando interceptador...")
    proxy.start()