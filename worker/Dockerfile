FROM python:3.10-slim
WORKDIR /app

RUN pip install --no-cache-dir pika mysql-connector-python psycopg2-binary requests

COPY . .

CMD ["python", "worker.py"]
