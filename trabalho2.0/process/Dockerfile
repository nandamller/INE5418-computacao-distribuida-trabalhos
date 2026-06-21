FROM python:3.12-slim

WORKDIR /app

COPY process/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

ENV PYTHONPATH=/app

CMD ["python", "process/process1.py"]