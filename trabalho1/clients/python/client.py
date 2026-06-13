import os
import socket
import json
import time


class EncurtadorClient:
    def __init__(self):
        self.host = os.getenv("PROXY_HOST", "proxy")
        self.port = int(os.getenv("PROXY_PORT", "8080"))

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
    
    # 1. Encurta
    print("[CLIENT|Py] Testando Encurta:", c.encurta("https://www.ufsc.br"))
    
    # 2. Encurta e resolve, que também adiciona na cache
    res = c.encurta("https://www.inf.ufsc.br")
    cod = res.get("codigo")
    print(f"[CLIENT|Py] Resolvendo {cod}:", c.resolve(cod))
    
    # 3. Cache hit
    print(f"[CLIENT|Py] Resolvendo {cod}:", c.resolve(cod))

    # Tempo para outro cliente processar requisições
    time.sleep(2)

    # 4. Resolve outro exemplo
    res = c.encurta("https://www.ppgcc.ufsc.br")
    cod = res.get("codigo")
    print(f"[CLIENT|Py] Resolvendo {cod}:", c.resolve(cod))

    # 5. Resolve outro exemplo e mostra cache cheia
    res = c.encurta("https://www.ine.ufsc.br")
    cod = res.get("codigo")
    print(f"[CLIENT|Py] Resolvendo {cod}:", c.resolve(cod))

    # 5. Resolve outro exemplo e mostra cache cheia
    res = c.encurta("https://www.ufsc.br")
    cod = res.get("codigo")
    print(f"[CLIENT|Py] Resolvendo {cod}:", c.resolve(cod))

    # 6. Quinto resolve - enche o cache (cache_capacity=5)
    res = c.encurta("https://www.eas.ufsc.br")
    cod = res.get("codigo")
    print(f"[CLIENT|Py] Resolvendo {cod}:", c.resolve(cod))

    # 7. Sexto resolve - dispara eviction do LRU (esperado log [CACHE] Capacidade atingida)
    res = c.encurta("https://www.cse.ufsc.br")
    cod = res.get("codigo")
    print(f"[CLIENT|Py] Resolvendo {cod}:", c.resolve(cod))

    # 8. Remove o último e resolve (esperado erro pós-invalidação)
    print("[CLIENT|Py] Testando remoção:", c.remove_url(cod))
    cod = res.get("codigo")
    print(f"[CLIENT|Py] Resolvendo {cod}:", c.resolve(cod))