from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import string
import random
from typing import Dict

app = FastAPI(
    title="API REST - Encurtador de URLs",
    description="Servidor REST independente para gerenciar URLs encurtadas. Armazenamento em memória.",
    version="1.0.0"
)

# Estruturas de dados em memória
# urls_db: Mapeia o código curto para a URL original
urls_db: Dict[str, str] = {}
# access_db: Mapeia o código curto para a quantidade de acessos (estatística)
access_db: Dict[str, int] = {}

class URLCreate(BaseModel):
    url: str

def gerar_codigo_curto(tamanho=6):
    """Gera um código alfanumérico aleatório de um dado tamanho."""
    caracteres = string.ascii_letters + string.digits
    return ''.join(random.choice(caracteres) for _ in range(tamanho))

@app.post("/urls", summary="Encurta uma URL")
def create_url(data: URLCreate):
    """
    Recebe uma URL original e retorna um código curto para ela.
    Se a URL já existir, retorna o código existente.
    """
    # Verifica se a URL já foi encurtada antes para evitar duplicidade
    for codigo, d_url in urls_db.items():
        if d_url == data.url:
            return {"codigo": codigo, "url_curta": f"http://localhost:8000/urls/{codigo}"}
            
    # Gera um código único
    codigo = gerar_codigo_curto()
    while codigo in urls_db:
        codigo = gerar_codigo_curto()
        
    # Armazena a URL e inicializa o contador de acessos
    urls_db[codigo] = data.url
    access_db[codigo] = 0
    
    return {"codigo": codigo, "url_curta": f"http://localhost:8000/urls/{codigo}"}

@app.get("/urls/{codigo}", summary="Resolve uma URL")
def get_url(codigo: str):
    """
    Recebe um código curto e retorna a URL original correspondente.
    Incrementa o contador de acessos desta URL.
    """
    if codigo not in urls_db:
        raise HTTPException(status_code=404, detail="URL não encontrada.")
    
    # Incrementa o número de acessos
    access_db[codigo] += 1
    
    return {"url_original": urls_db[codigo]}

@app.delete("/urls/{codigo}", summary="Remove uma URL")
def delete_url(codigo: str):
    """
    Remove uma URL do banco de dados em memória.
    """
    if codigo not in urls_db:
        raise HTTPException(status_code=404, detail="URL não encontrada.")
    
    # Remove dos dicionários
    del urls_db[codigo]
    del access_db[codigo]
    
    return {"removido": True}

@app.get("/urls", summary="Lista todas as URLs")
def list_urls():
    """
    Retorna a lista de todas as URLs cadastradas e seus respectivos números de acessos.
    """
    resultado = []
    for codigo, url in urls_db.items():
        resultado.append({
            "codigo": codigo,
            "url_original": url,
            "acessos": access_db[codigo]
        })
    return resultado

if __name__ == "__main__":
    import uvicorn
    # Inicia o servidor na porta 8000
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
