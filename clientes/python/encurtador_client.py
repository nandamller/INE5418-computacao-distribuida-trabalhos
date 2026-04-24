import socket
import json
import os

class EncurtadorClient:
    """Biblioteca Cliente em Python para o Encurtador de URLs Distribuído."""
    
    def __init__(self, config_path="../config.txt"):
        self.host = "127.0.0.1"
        self.port = 9000
        self._load_config(config_path)
        
    def _load_config(self, filepath):
        """Lê o arquivo de texto config.txt."""
        try:
            with open(filepath, 'r') as f:
                for line in f:
                    if '=' in line:
                        key, val = line.strip().split('=', 1)
                        if key == 'PROXY_HOST':
                            self.host = val
                        elif key == 'PROXY_PORT':
                            self.port = int(val)
        except Exception as e:
            print(f"[!] Aviso: Não foi possível ler {filepath}. Usando proxy padrão {self.host}:{self.port}")
            
    def _send_request(self, payload) -> dict:
        """Abre um Socket TCP, converte os dados para String JSON, envia, recebe e fecha."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.host, self.port))
            # O proxy espera uma string contendo JSON delimitada por quebra de linha.
            msg = json.dumps(payload) + "\n"
            s.sendall(msg.encode('utf-8'))
            
            data = s.recv(4096)
            return json.loads(data.decode('utf-8').strip())

    def encurta(self, url_original: str) -> str:
        """
        Solicita encurtamento de URL via Proxy Distribuído. 
        Retorna o código curto alfanumérico.
        """
        res = self._send_request({"acao": "encurta", "url": url_original})
        if res.get("status") == "ok":
            return res.get("codigo")
        raise Exception(res.get("mensagem", "Erro desconhecido ao encurtar"))

    def resolve(self, codigo_curto: str) -> str:
        """
        Recebe um código curto e busca a URL original. 
        """
        res = self._send_request({"acao": "resolve", "codigo": codigo_curto})
        if res.get("status") == "ok":
            return res.get("url_original")
        raise Exception(res.get("mensagem", "Erro desconhecido ao resolver"))

    def remove_url(self, codigo_curto: str) -> bool:
        """
        Deleta uma URL do ecossistema distribuído pelo seu código informando ao Interceptador.
        """
        res = self._send_request({"acao": "remove", "codigo": codigo_curto})
        return res.get("status") == "ok"
