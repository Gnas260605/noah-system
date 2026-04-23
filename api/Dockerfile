FROM python:3.10-slim
WORKDIR /app
VOLUME ["/app/data"]

# Install system dependencies for Docker-out-of-Docker
RUN apt-get update && apt-get install -y \
    docker.io \
    docker-compose \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies
RUN pip install --no-cache-dir \
    flask \
    pika \
    requests \
    mysql-connector-python \
    psycopg2-binary \
    gunicorn \
    gevent

# Copy API source into api/ subfolder to preserve package namespace
COPY . api/

# Create __init__.py so Python treats api/ as a package
RUN touch api/__init__.py

# Expose Flask port
EXPOSE 5000

# Start with gunicorn: 1 worker (to prevent race conditions in local threads), gevent async
CMD ["gunicorn", \
     "-w", "1", \
     "-k", "gevent", \
     "--worker-connections", "1000", \
     "-b", "0.0.0.0:5000", \
     "--timeout", "120", \
     "--access-logfile", "-", \
     "api.app:app"]
