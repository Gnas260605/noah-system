FROM python:3.10-slim
WORKDIR /app
VOLUME ["/app/data"]

# Install dependencies and Docker CLI
RUN apt-get update && \
    apt-get install -y ca-certificates curl gnupg build-essential python3-dev libffi-dev && \
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    chmod a+r /etc/apt/keyrings/docker.gpg && \
    echo \
      "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
      "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" | \
      tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt-get update && \
    apt-get install -y docker-ce-cli docker-compose-plugin && \
    pip install --no-cache-dir \
        flask \
        pika \
        requests \
        mysql-connector-python \
        psycopg2-binary \
        gunicorn \
        gevent && \
    rm -rf /var/lib/apt/lists/*

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
