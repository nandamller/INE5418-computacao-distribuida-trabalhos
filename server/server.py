from flask import Flask, request, jsonify

import os
import uuid
import shortuuid


SERVER_HOST = os.getenv("SERVER_HOST", "0.0.0.0")
SERVER_PORT = int(os.getenv("SERVER_PORT", "5000"))

app = Flask(__name__)

# Simulação de banco de dados em memória (Dicionário)
# Estrutura: { "codigo": {"url_original": "...", "acessos": 0} }
url_storage = {}

# Base da URL curta (configurável)
BASE_HOST = f"http://host/r/"

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
    
    # Gera um código curto único usando uuid(name=...) para que a mesma URL gere sempre o mesmo ID
    codigo = shortuuid.uuid(name=url_original)[:8]

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

        print(f"[SERVIDOR] URL do código {codigo} foi resolvida!")
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
    
    print(f"[SERVIDOR] Listagem de URLs enviada!")
    return jsonify(lista), 200

if __name__ == '__main__':
    print(f"[SERVIDOR] Iniciando API REST em {SERVER_HOST}:{SERVER_PORT}...")
    app.run(host=SERVER_HOST, port=SERVER_PORT, debug=True)