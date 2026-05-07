import os
import socket
import json


def _load_config(path="config.txt"):
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


class EncurtadorClient:
    def __init__(self):
        cfg = _load_config()
        self.host = os.getenv("PROXY_HOST", cfg.get("client_proxy_host", "proxy"))
        self.port = int(os.getenv("PROXY_PORT", cfg.get("client_proxy_port", "8080")))

    def _enviar_comando(self, payload):
        """Método privado para gerenciar a conexão de rede via Sockets Berkeley."""
        try:
            # Criação do socket TCP (AF_INET = IPv4, SOCK_STREAM = TCP) 
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.host, self.port))
                # Envia o comando formatado como JSON em string
                s.sendall(json.dumps(payload).encode('utf-8'))
                
                # Aguarda a resposta do Interceptador
                data = s.recv(4096)
                return json.loads(data.decode('utf-8'))
        except Exception as e:
            return {"erro": f"Falha na comunicação: {e}"}

    def encurta(self, url_original):
        """Envia a URL original ao interceptador e recebe o código curto."""
        payload = {"acao": "encurta", "url": url_original}
        return self._enviar_comando(payload)

    def resolve(self, codigo_curto):
        """Envia o código curto ao interceptador e recebe a URL original."""
        payload = {"acao": "resolve", "codigo": codigo_curto}
        return self._enviar_comando(payload)

    def remove_url(self, codigo_curto):
        """Remove o mapeamento de uma URL encurtada."""
        payload = {"acao": "remove", "codigo": codigo_curto}
        return self._enviar_comando(payload)

if __name__ == "__main__":
    c = EncurtadorClient()
    
    # 1. Encurtando
    print("Testando Encurta:", c.encurta("https://www.ufsc.br"))
    
    # 2. Resolvendo (Deve gerar Cache Hit no Interceptador na segunda vez)
    res = c.encurta("https://www.inf.ufsc.br")
    cod = res.get("codigo")
    print(f"Resolvendo {cod}:", c.resolve(cod))