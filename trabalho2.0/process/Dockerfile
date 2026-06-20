FROM python:3.12-slim

WORKDIR /app

# Copia só o requirements primeiro pra aproveitar o cache de camadas do Docker:
# o pip install só roda de novo se as dependências mudarem, não a cada commit.
COPY process/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
# Garante que /app esteja no sys.path, não só a pasta do script (process/).
# Isso é necessário se utils/ ou consensus/ vivem na raiz do projeto e não
# dentro de process/ -- ajuste se sua estrutura de pastas for diferente.
ENV PYTHONPATH=/app

CMD ["python", "process/process1.py"]