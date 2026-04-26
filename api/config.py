"""
config.py – Tập trung tất cả cấu hình.
LOCAL_MODE=True nếu RABBITMQ_HOST=localhost (chạy không có Docker).
"""
import os

# ── RabbitMQ ──────────────────────────────────────────────────
RABBITMQ_HOST     = os.getenv("RABBITMQ_HOST",     "localhost") # Default to localhost for easy local run
RABBITMQ_QUEUE    = os.getenv("RABBITMQ_QUEUE",    "orders")
RABBITMQ_API_URL  = os.getenv("RABBITMQ_API_URL",  f"http://{RABBITMQ_HOST}:15672/api")
RABBITMQ_USER     = os.getenv("RABBITMQ_USER",     "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

# ── Environment Detection ───────────────────────────────────
# "docker" means we are inside the container network.
# "local" (default) means we are running on the host machine.
ENVIRONMENT = os.getenv("ENVIRONMENT", "local").lower()
LOCAL_MODE  = (ENVIRONMENT != "docker")

# ── RabbitMQ ──────────────────────────────────────────────────
RABBITMQ_HOST     = os.getenv("RABBITMQ_HOST",     "localhost" if LOCAL_MODE else "rabbitmq")
RABBITMQ_QUEUE    = os.getenv("RABBITMQ_QUEUE",    "orders")
RABBITMQ_API_URL  = os.getenv("RABBITMQ_API_URL",  f"http://{RABBITMQ_HOST}:15672/api")
RABBITMQ_USER     = os.getenv("RABBITMQ_USER",     "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

# ── Database Configs ──────────────────────────────────────────
MYSQL_CONFIG = {
    "host":     os.getenv("MYSQL_HOST",     "localhost" if LOCAL_MODE else "mysql"),
    "port":     int(os.getenv("MYSQL_PORT", 3307 if LOCAL_MODE else 3306)),
    "user":     os.getenv("MYSQL_USER",     "root"),
    "password": os.getenv("MYSQL_PASSWORD", "root"),
    "database": os.getenv("MYSQL_DATABASE", "ecommerce"),
    "connection_timeout": 5,
}

POSTGRES_CONFIG = {
    "host":            os.getenv("POSTGRES_HOST",     "localhost" if LOCAL_MODE else "postgres"),
    "port":            int(os.getenv("POSTGRES_PORT", 5432)),
    "database":        os.getenv("POSTGRES_DB",       "finance"),
    "user":            os.getenv("POSTGRES_USER",     "postgres"),
    "password":        os.getenv("POSTGRES_PASSWORD", "root"),
    "connect_timeout": 5,
}
