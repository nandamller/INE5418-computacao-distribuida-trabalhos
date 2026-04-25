FROM python:3.10-slim

WORKDIR /proxy

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

CMD ["python", "proxy.py"]