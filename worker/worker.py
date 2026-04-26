import pika, json, time, os, socket
import mysql.connector
import psycopg2

MAX_RETRY = 3

# Metrics
stats = {
    "processed": 0,
    "retries": 0,
    "duplicates": 0,
    "failed": 0
}

def create_mysql_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "mysql"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", "root"),
        database=os.getenv("MYSQL_DATABASE", "ecommerce")
    )

def create_pg_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        database=os.getenv("POSTGRES_DB", "finance"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "root")
    )

class ResilientWorker:
    def __init__(self):
        self.mysql_conn = None
        self.pg_conn = None
        self.reconnect_dbs()

    def reconnect_dbs(self):
        print("Connecting to databases...")
        while True:
            try:
                if self.mysql_conn: self.mysql_conn.close()
                if self.pg_conn: self.pg_conn.close()
                self.mysql_conn = create_mysql_connection()
                self.pg_conn = create_pg_connection()
                print("Database connections established.")
                break
            except Exception as e:
                print(f"Database connection failed: {e}. Retrying in 5s...")
                time.sleep(5)

    def save_sales_idempotent(self, data):
        # Resilience: generate an ID if missing to prevent KeyError
        msg_id = data.get('message_id') or f"gen-{int(time.time()*1000)}-{os.urandom(4).hex()}"
        u_id = data.get('user_id')
        p_id = data.get('product_id')
        qty = data.get('quantity')
        price = data.get('total_price')

        for attempt in range(MAX_RETRY):
            try:
                start_time = time.time()
                
                # MySQL UPSERT (Business / Operations)
                my_cur = self.mysql_conn.cursor()
                my_cur.execute("""
                    INSERT INTO orders (message_id, user_id, product_id, quantity, total_price) 
                    VALUES (%s, %s, %s, %s, %s) 
                    ON DUPLICATE KEY UPDATE message_id = message_id
                """, (msg_id, u_id, p_id, qty, price))
                mysql_affected = my_cur.rowcount
                self.mysql_conn.commit()
                my_cur.close()

                # Postgres UPSERT (Finance / Audit)
                pg_cur = self.pg_conn.cursor()
                pg_cur.execute("""
                    INSERT INTO transactions (message_id, user_id, product_id, quantity, total_price) 
                    VALUES (%s, %s, %s, %s, %s) 
                    ON CONFLICT (message_id) DO NOTHING
                """, (msg_id, u_id, p_id, qty, price))
                pg_affected = pg_cur.rowcount
                self.pg_conn.commit()
                pg_cur.close()

                duration = time.time() - start_time
                
                if mysql_affected == 0 or pg_affected == 0:
                    stats["duplicates"] += 1
                    print(f"[SKIP] DUPLICATE: {msg_id} ({duration:.3f}s)")
                else:
                    stats["processed"] += 1
                    print(f"[OK] PROCESSED Order: {msg_id} for User {u_id} ({duration:.3f}s)")
                
                return True

            except (mysql.connector.Error, psycopg2.Error) as e:
                stats["retries"] += 1
                wait = 2 ** attempt
                print(f"[RETRY {attempt+1}/{MAX_RETRY}] {msg_id} due to: {e}. Waiting {wait}s...")
                time.sleep(wait)
                if "connection" in str(e).lower() or "closed" in str(e).lower():
                    self.reconnect_dbs()
            except Exception as e:
                print(f"[ERROR] Unexpected: {e}")
                break
        return False

_worker = None

def callback(ch, method, properties, body):
    global _worker
    try:
        data = json.loads(body)
        if _worker.save_sales_idempotent(data):
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            # Route to DLX natively
            msg_id = data.get('message_id', 'unknown')
            print(f"[FAILED] Poison Message: {msg_id}")
            stats["failed"] += 1
            
            # --- BẮT ĐẦU: GHI VÀO DIRTY LOG KHI DỮ LIỆU BỊ TỪ CHỐI ---
            if _worker and hasattr(_worker, 'mysql_conn') and _worker.mysql_conn:
                try:
                    cur = _worker.mysql_conn.cursor()
                    cur.execute("""
                        CREATE TABLE IF docker stop noah-producerNOT EXISTS dirty_log (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            transaction_id VARCHAR(255),
                            reason TEXT,
                            raw_payload TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    cur.execute(
                        "INSERT INTO dirty_log (transaction_id, reason, raw_payload) VALUES (%s, %s, %s)",
                        (msg_id, "Validation failed / Poison Message", body.decode('utf-8', errors='replace'))
                    )
                    _worker.mysql_conn.commit()
                    cur.close()
                    print(f">>> Đã lưu thành công ID {msg_id} vào Dirty Log!")
                except Exception as db_err:
                    print(f"Lỗi khi lưu DB dirty_log: {db_err}")
            # --- KẾT THÚC: GHI VÀO DIRTY LOG ---

            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        print(f"Critical callback error: {e}")
        
        msg_id = "unknown"
        try:
            parsed = json.loads(body)
            if isinstance(parsed, dict):
                msg_id = parsed.get("message_id", "unknown")
        except Exception:
            pass

        if _worker and _worker.mysql_conn:
            try:
                cur = _worker.mysql_conn.cursor()
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS dirty_log (
                        id INT AUTO_INCREMENT PRIMARY KEY, 
                        transaction_id VARCHAR(255), 
                        reason TEXT, 
                        raw_payload TEXT, 
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute(
                    "INSERT INTO dirty_log (transaction_id, reason, raw_payload) VALUES (%s, %s, %s)",
                    (msg_id, str(e), body.decode('utf-8', errors='replace'))
                )
                _worker.mysql_conn.commit()
                cur.close()
            except Exception as db_err:
                print(f"Failed to insert dirty_log: {db_err}")

        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def run_worker():
    global _worker
    if _worker is None:
        _worker = ResilientWorker()
        
    while True:
        try:
            print("Connecting to RabbitMQ...")
            params = pika.ConnectionParameters(
                host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
                heartbeat=600,
                blocked_connection_timeout=300
            )
            conn = pika.BlockingConnection(params)
            ch = conn.channel()
            # Setup Dead Letter Exchange natively
            ch.exchange_declare(exchange='dlx', exchange_type='direct')
            ch.queue_declare(queue='failed_orders', durable=True)
            ch.queue_bind(exchange='dlx', queue='failed_orders', routing_key='failed_orders')
            
            # Link main queue to DLX
            ch.queue_declare(queue='orders', durable=True, arguments={
                'x-dead-letter-exchange': 'dlx',
                'x-dead-letter-routing-key': 'failed_orders'
            })

            
            # Fair dispatch: 1 message per worker
            ch.basic_qos(prefetch_count=1) 
            
            ch.basic_consume(queue='orders', on_message_callback=callback, auto_ack=False)
            print("Worker running... (A+ Version Active)")
            ch.start_consuming()
        except (pika.exceptions.AMQPError, socket.error) as e:
            print(f"[Worker] RabbitMQ connection lost: {e}. Reconnecting in 5s...")
            time.sleep(5)

if __name__ == "__main__":
    # Periodic stats print
    import threading
    def print_stats():
        while True:
            print(f"\n--- [Pipeline Metrics] ---")
            print(f"SUCCESS: {stats['processed']}")
            print(f"DUPLICATES: {stats['duplicates']}")
            print(f"RETRIES: {stats['retries']}")
            print(f"FAILED: {stats['failed']}")
            print(f"--------------------------\n")
            time.sleep(30)
    
    t = threading.Thread(target=print_stats, daemon=True)
    t.start()
    
    run_worker()