import socket
import json
import time

def send_request(req):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("127.0.0.1", 9000))
    s.sendall((json.dumps(req) + "\n").encode('utf-8'))
    data = s.recv(4096)
    s.close()
    return json.loads(data.decode('utf-8').strip())

# 1. Encurtar
res1 = send_request({"acao": "encurta", "url": "https://google.com"})
print("Encurtar:", res1)
cod = res1.get("codigo")

# 2. Resolver (Cache Miss)
res2 = send_request({"acao": "resolve", "codigo": cod})
print("Resolver (Miss):", res2)

# 3. Resolver (Cache Hit)
res3 = send_request({"acao": "resolve", "codigo": cod})
print("Resolver (Hit):", res3)

# 4. Remover
res4 = send_request({"acao": "remove", "codigo": cod})
print("Remover:", res4)
