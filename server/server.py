from flask import Flask, request, jsonify
import os
import uuid


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


def cfg_get(cfg, key, default, env_key=None):
    return os.getenv(env_key, cfg.get(key, default)) if env_key else cfg.get(key, default)


CONFIG = load_config()
SERVER_HOST = cfg_get(CONFIG, "server_host", "0.0.0.0", "SERVER_HOST")
SERVER_PORT = int(cfg_get(CONFIG, "server_port", "5000", "SERVER_PORT"))

app = Flask(__name__)

# Simulação de banco de dados em memória (Dicionário)
# Estrutura: { "codigo": {"url_original": "...", "acessos": 0} }
url_storage = {}

# Base da URL curta (configurável)
BASE_HOST = f"http://localhost:{SERVER_PORT}/r/"

@app.route('/urls', methods=['POST'])
def encurtar_url():
    """
    Encurta uma URL.

    Req: {"url": "https://..."}
    Res: {"codigo": "abc123", "url_curta": "http://host/r/abc123"}
    """
    data = request.get_json()
    if not data or 'url' not in data:
        return jsonify({"erro": "URL não fornecida"}), 400

    url_original = data['url']
    
    # Gera um código curto único usando UUID (6 primeiros caracteres)
    codigo = str(uuid.uuid4())
    
    # Armazena os dados no dicionário em memória
    url_storage[codigo] = {
        "url_original": url_original,
        "acessos": 0
    }
    
    print(f"[SERVIDOR] URL encurtada: {url_original} -> {codigo}")
    
    return jsonify({
        "codigo": codigo,
        "url_curta": f"{BASE_HOST}{codigo}"
    }), 201


@app.route('/urls/<codigo>', methods=['GET'])
def resolver_url(codigo):
    """
    Resolve um código curto.

    Res: {"url_original": "https://..."}
    """
    if codigo in url_storage:
        # Incrementa o contador de acessos
        url_storage[codigo]["acessos"] += 1

        return jsonify({
            "url_original": url_storage[codigo]["url_original"]
        }), 200
    
    return jsonify({"erro": "Código não encontrado"}), 404


@app.route('/urls/<codigo>', methods=['DELETE'])
def remover_url(codigo):
    """
    Remove uma URL encurtada.

    Res: {"removido": true}
    """
    if codigo in url_storage:
        del url_storage[codigo]
        print(f"[SERVIDOR] Código {codigo} removido.")
        return jsonify({"removido": True}), 200
    
    return jsonify({"removido": False, "erro": "Código inexistente"}), 404


@app.route('/urls', methods=['GET'])
def listar_urls():
    """
    Lista todas as URLs encurtadas e seus respectivos acessos.

    Res: Lista de objetos com código, url_original e acessos.
    """
    lista = []
    for codigo, info in url_storage.items():
        lista.append({
            "codigo": codigo,
            "url_original": info["url_original"],
            "acessos": info["acessos"]
        })
    
    return jsonify(lista), 200

if __name__ == '__main__':
    print(f"[SERVIDOR] Iniciando API REST em {SERVER_HOST}:{SERVER_PORT}...")
    app.run(host=SERVER_HOST, port=SERVER_PORT, debug=True)