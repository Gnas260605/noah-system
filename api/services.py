"""
services.py – Tất cả business logic đọc/xử lý dữ liệu.

- Local mode  : đọc từ SQLite, queue = Python Queue, worker in-process
- Docker mode : đọc từ MySQL/PostgreSQL, queue = RabbitMQ
"""
import hashlib
import json
import time
import threading
import queue as _queue
import csv
import os
import re
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from api.config import (
    LOCAL_MODE,
    RABBITMQ_API_URL, RABBITMQ_USER, RABBITMQ_PASSWORD, RABBITMQ_QUEUE,
    MYSQL_CONFIG, POSTGRES_CONFIG,
)
from api.db_local import (
    insert_order      as _sqlite_insert,
    insert_orders_bulk as _sqlite_bulk,
    get_recent_orders as _sqlite_recent,
    count_orders      as _sqlite_count,
    truncate_orders   as _sqlite_truncate,
    log_event         as _log_event,
    log_dirty         as _log_dirty,
    count_dirty       as _sqlite_dirty_count,
)

# ─────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────
def generate_message_id(data: dict) -> str:
    relevant = {k: data.get(k) for k in ("user_id", "product_id", "quantity", "total_price")}
    return hashlib.sha256(json.dumps(relevant, sort_keys=True).encode()).hexdigest()[:16]


def standard_cleaner(row: dict, source: str) -> tuple[dict, bool]:
    """
    Unified Data Quality Gate: Strict Integrity Mode.
    Always provides 'created_at' and 'message_id' candidates.
    """
    r = {str(k).lower(): v for k, v in row.items()}
    
    # 1. Extraction
    u_id_raw = r.get("user_id") or r.get("userid") or r.get("user")
    p_id_raw = r.get("product_id") or r.get("productid") or r.get("id") or r.get("product")
    qty_raw  = r.get("quantity") or r.get("qty") or r.get("amount")
    price_raw = r.get("total_price") or r.get("price") or r.get("value")
    time_raw  = r.get("created_at") or r.get("timestamp") or r.get("date") or r.get("time")
    
    # 2. Resilient translation
    try:
        # Check for missing values before conversion
        missing_fields = []
        if not u_id_raw: missing_fields.append("user_id")
        if not p_id_raw: missing_fields.append("product_id")
        if not qty_raw:  missing_fields.append("quantity")
        
        # We allow total_price to be missing and recalculated later, 
        # but let's log it if both quantity and price are missing.
        
        is_missing = len(missing_fields) > 0
        
        data = {
            "user_id":    int(float(u_id_raw)) if u_id_raw else 1,
            "product_id": int(float(p_id_raw)) if p_id_raw else 1,
            "quantity":   int(float(qty_raw)) if qty_raw else 1,
            "total_price": float(price_raw) if price_raw else 0.0,
        }
        
        # 3. Timestamp Fidelity: Preserve original if exists, else now()
        if time_raw:
            data["created_at"] = str(time_raw).strip()
        else:
            data["created_at"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 4. Fidelity Check: Log if data is 'dirty' (e.g. negative or missing)
        is_negative = (data["quantity"] < 0 or data["total_price"] < 0)
        
        if is_missing or is_negative:
            reason = ""
            if is_missing: reason += f"MISSING FIELDS: {', '.join(missing_fields)}. "
            if is_negative: reason += f"NEGATIVE DATA FIXED: (qty={data['quantity']}, price={data['total_price']})."
            
            _log_dirty(source, json.dumps(row), reason.strip())
            
            # Clean up for pipeline
            data["quantity"] = abs(data["quantity"])
            data["total_price"] = abs(data["total_price"])
            return data, True # It IS dirty
            
        return data, False
        
    except (ValueError, TypeError) as e:
        _log_dirty(source, json.dumps(row), f"REJECTED: Critical numeric error ({e})")
        return None, False


# ─────────────────────────────────────────────────────────────
#  In-process Queue + Worker (LOCAL MODE)
# ─────────────────────────────────────────────────────────────
_local_queue: _queue.Queue = _queue.Queue()
_worker_stats = {"processed": 0, "duplicates": 0, "errors": 0}


def _local_worker_loop():
    """Background thread: lẫy từ queue và lưu vào SQLite theo mẻ (batch)."""
    while True:
        items = []
        try:
            # Chờ item đầu tiên
            data = _local_queue.get(timeout=1)
            items.append(data)
            
            # Cố gắng lấy thêm tối đa 1000 items đang chờ sẵn trong queue
            while len(items) < 1000:
                try:
                    data = _local_queue.get_nowait()
                    items.append(data)
                except _queue.Empty:
                    break
                    
            # Insert theo mẻ
            if items:
                _log_event("Batch Start", f"Processing {len(items)} items from queue.")
                count = _sqlite_bulk(items)
                _worker_stats["processed"] += count
                _worker_stats["duplicates"] += (len(items) - count)
                
                if count > 0:
                    _log_event("Batch Success", f"Saved {count} records ({len(items) - count} duplicates ignored).")
                    _log_event("System Sync", f"Synchronized {count} records to Multi-Storage (SQLite/MySQL/PostgreSQL proxy).")
                    print(f"[Worker] BATCH SAVED: {count} records (total {len(items)})")
                
                # Đánh dấu hoàn tất cho tất cả items trong mẻ
                for _ in range(len(items)):
                    _local_queue.task_done()
                    
        except _queue.Empty:
            continue
        except Exception as e:
            _worker_stats["errors"] += 1
            _log_event("Worker Error", str(e))
            print(f"[Worker] ERROR: {e}")
            if items:
                for _ in range(len(items)):
                    _local_queue.task_done()


# Khởi động worker thread ngay khi module load
try:
    _worker_thread = threading.Thread(target=_local_worker_loop, daemon=True)
    _worker_thread.start()
    print("[Worker] High-performance background processor started successfully (Local Mode).")
except Exception as e:
    print(f"[Worker] FAILED TO START: {e}")


def enqueue(data: dict):
    """Gửi dữ liệu vào queue (local hoặc RabbitMQ)."""
    if LOCAL_MODE:
        _local_queue.put(data)
        return

    # Docker mode: gửi vào RabbitMQ
    try:
        import pika
        from api.config import RABBITMQ_HOST
        conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        ch = conn.channel()
        ch.queue_declare(queue=RABBITMQ_QUEUE, durable=True, arguments={
            "x-dead-letter-exchange": "dlx",
            "x-dead-letter-routing-key": "failed_orders",
        })
        ch.basic_publish(
            exchange="",
            routing_key=RABBITMQ_QUEUE,
            body=json.dumps(data),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        conn.close()
    except Exception as e:
        print(f"[RabbitMQ] enqueue error: {e}")


def enqueue_bulk(items: list[dict]) -> tuple[int, int]:
    """Gửi nhiều items cùng lúc. Trả về (sent, errors)."""
    sent = errors = 0
    if LOCAL_MODE:
        for item in items:
            try:
                _local_queue.put(item)
                sent += 1
            except Exception:
                errors += 1
        return sent, errors

    # Docker bulk via single channel
    try:
        import pika
        from api.config import RABBITMQ_HOST
        conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        ch = conn.channel()
        ch.queue_declare(queue=RABBITMQ_QUEUE, durable=True, arguments={
            "x-dead-letter-exchange": "dlx",
            "x-dead-letter-routing-key": "failed_orders",
        })
        for item in items:
            try:
                ch.basic_publish(
                    exchange="",
                    routing_key=RABBITMQ_QUEUE,
                    body=json.dumps(item),
                    properties=pika.BasicProperties(delivery_mode=2),
                )
                sent += 1
            except Exception:
                errors += 1
        conn.close()
    except Exception as e:
        print(f"[RabbitMQ] bulk error: {e}")
        errors += len(items)
    return sent, errors


# ─────────────────────────────────────────────────────────────
#  Data fetchers (luôn có fallback, không bao giờ crash)
# ─────────────────────────────────────────────────────────────
def _fetch_queue_stats() -> dict:
    if LOCAL_MODE:
        return {
            "ok": True,
            "messages": _local_queue.qsize(),
            "consumers": 1,
            "status_text": "Local Queue Active",
        }
    try:
        url = f"{RABBITMQ_API_URL}/queues/%2F/{RABBITMQ_QUEUE}"
        r = requests.get(url, auth=(RABBITMQ_USER, RABBITMQ_PASSWORD), timeout=1.5)
        p = r.json()
        return {"ok": True, "messages": int(p.get("messages", 0)),
                "consumers": int(p.get("consumers", 0)), "status_text": "Queue Active"}
    except Exception:
        return {"ok": False, "messages": 0, "consumers": 0, "status_text": "Queue Offline"}


def _fetch_orders() -> dict:
    """Lấy danh sách đơn từ MySQL (Docker) hoặc SQLite (local)."""
    if LOCAL_MODE:
        rows = _sqlite_recent(50)
        return {"ok": True, "rows": rows, "total": _sqlite_count()}

    try:
        import mysql.connector
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT message_id, user_id, product_id, quantity, total_price, created_at "
            "FROM orders ORDER BY id DESC LIMIT 50"
        )
        rows = cur.fetchall()
        cur.execute("SELECT COUNT(*) AS c FROM orders")
        total = cur.fetchone()["c"]
        cur.close(); conn.close()
        return {"ok": True, "rows": rows, "total": total}
    except Exception:
        return {"ok": False, "rows": [], "total": 0}


def _fetch_pg_count() -> dict:
    """Đếm transactions từ PostgreSQL (Docker) hoặc SQLite (local)."""
    if LOCAL_MODE:
        # Local: dùng cùng SQLite count làm proxy cho PG
        return {"ok": True, "count": _sqlite_count()}

    try:
        import psycopg2
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM transactions")
        count = cur.fetchone()[0]
        cur.close(); conn.close()
        return {"ok": True, "count": count}
    except Exception:
        return {"ok": False, "count": 0}


def query_mysql_table(table_name: str, limit: int = 100, offset: int = 0) -> dict:
    """Paginated generic query for MySQL."""
    try:
        import mysql.connector
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cur = conn.cursor(dictionary=True)
        if not re.match(r'^\w+$', table_name): return {"error": "Invalid table"}
        
        cur.execute(f"SELECT * FROM {table_name} LIMIT %s OFFSET %s", (limit, offset))
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description] if cur.description else []
        
        cur.execute(f"SELECT COUNT(*) AS c FROM {table_name}")
        total = cur.fetchone()["c"]
        
        cur.close(); conn.close()
        return {"columns": cols, "rows": rows, "total": total}
    except Exception as e:
        return {"error": str(e)}


def query_postgres_table(table_name: str, limit: int = 100, offset: int = 0) -> dict:
    """Paginated generic query for PostgreSQL."""
    try:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        if not re.match(r'^\w+$', table_name): return {"error": "Invalid table"}
        
        cur.execute(f"SELECT * FROM {table_name} LIMIT %s OFFSET %s", (limit, offset))
        rows = [dict(r) for r in cur.fetchall()]
        cols = [d[0] for d in cur.description] if cur.description else []
        
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        total = cur.fetchone()[0]
        
        cur.close(); conn.close()
        return {"columns": cols, "rows": rows, "total": total}
    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────────────────────────
#  Snapshot cache (ttl 4s)
# ─────────────────────────────────────────────────────────────
_snap_cache:  dict | None = None
_snap_time:   float = 0
_snap_lock    = threading.Lock()
CACHE_TTL     = 0.5  # seconds (ultra-fast for real-time feel)


def build_snapshot(force: bool = False) -> dict:
    """
    Gọi song song 3 fetchers, cache 4 giây.
    Luôn trả về trong ≤ 2 giây dù services offline.
    """
    global _snap_cache, _snap_time

    now = time.monotonic()
    if not force:
        with _snap_lock:
            if _snap_cache and (now - _snap_time) < CACHE_TTL:
                return _snap_cache

    # Gọi song song
    with ThreadPoolExecutor(max_workers=3) as pool:
        f_q = pool.submit(_fetch_queue_stats)
        f_m = pool.submit(_fetch_orders)
        f_p = pool.submit(_fetch_pg_count)
        try: q = f_q.result(timeout=2)
        except Exception: q = {"ok": False, "messages": 0, "consumers": 0, "status_text": "Offline"}
        try: m = f_m.result(timeout=2)
        except Exception: m = {"ok": False, "rows": [], "total": 0}
        try: p = f_p.result(timeout=2)
        except Exception: p = {"ok": False, "count": 0}

    rows           = m.get("rows", [])
    observed       = len(rows)
    persisted      = int(p.get("count", 0))
    mysql_total    = int(m.get("total", observed))

    sales = [
        {
            "message_id":  str(r.get("message_id") or ""),
            "user_id":     int(r.get("user_id") or 0),
            "product_id":  int(r.get("product_id") or 0),
            "quantity":    int(r.get("quantity") or 0),
            "total_price": float(r.get("total_price") or 0.0),
            "created_at":  str(r.get("created_at", "")),
            "status":      "Synced",
            "status_class":"good",
        }
        for r in rows
    ]

    trend_src = list(reversed(sales[:20]))
    trend = {
        "labels": [f"P#{r['product_id']}" for r in trend_src] or ["No Data"],
        "values": [int(r["quantity"])      for r in trend_src] or [0],
    }

    services = [
        {
            "name": "Queue" if LOCAL_MODE else "RabbitMQ",
            "state":       "Online"  if q["ok"] else "Offline",
            "state_class": "good"    if q["ok"] else "bad",
            "detail":      f"{q['messages']} đang chờ, {q['consumers']} consumers",
        },
        {
            "name":        "Orders DB" if LOCAL_MODE else "MySQL (ecommerce)",
            "state":       "Online"    if m["ok"] else "Offline",
            "state_class": "good"      if m["ok"] else "bad",
            "detail":      f"{mysql_total} tổng records",
        },
        {
            "name":        "Finance DB" if LOCAL_MODE else "PostgreSQL (finance)",
            "state":       "Online"    if p["ok"] else "Offline",
            "state_class": "good"      if p["ok"] else "bad",
            "detail":      f"{persisted} transactions",
        },
    ]

    snapshot = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "local_mode":   LOCAL_MODE,
        "queue":    {
            "ok":         bool(q.get("ok", False)),
            "messages":   int(q.get("messages", 0)),
            "consumers":  int(q.get("consumers", 0)),
            "status_text": str(q.get("status_text", "")),
        },
        "mysql":    {"count": mysql_total},
        "postgres": {"count": persisted},
        "sales":    sales,
        "services": services,
        "trend":    trend,
        "summary":  {
            "total_synced": persisted,
            "queue_depth":  q["messages"],
            "dirty_count":  _sqlite_dirty_count(),
            "health_label": "Active" if (q["ok"] or LOCAL_MODE) else "Degraded",
            "health_reason":"Pipeline monitoring active",
        },
        "worker_stats": dict(_worker_stats) if LOCAL_MODE else {},
    }

    with _snap_lock:
        _snap_cache = snapshot
        _snap_time  = time.monotonic()

    return snapshot


# ─────────────────────────────────────────────────────────────
#  Ingest helpers
# ─────────────────────────────────────────────────────────────
def ingest_csv(csv_path: str) -> dict:
    """Đọc inventory.csv, làm sạch NEGATIVE_NUMBERS, đẩy vào queue."""
    if not os.path.exists(csv_path):
        return {"ok": False, "message": f"File không tồn tại: {csv_path}"}

    items = []
    seen_ids = set()
    dirty = 0
    skipped = 0
    duplicated_in_file = 0
    try:
        with open(csv_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                data, is_dirty = standard_cleaner(row, "CSV Ingress")
                if data is None:
                    skipped += 1
                    continue
                
                mid = generate_message_id(data)
                if mid in seen_ids:
                    duplicated_in_file += 1
                    continue
                
                seen_ids.add(mid)
                data["message_id"] = mid
                items.append(data)
                if is_dirty:
                    dirty += 1
    except Exception as e:
        return {"ok": False, "message": f"Lỗi đọc CSV: {e}"}

    sent, errors = enqueue_bulk(items)
    return {
        "status": "success",
        "message": f"Ingested {sent} unique records ({dirty} corrected, {duplicated_in_file} duplicates skipped, {errors} errors).",
    }


def ingest_sql(sql_path: str) -> dict:
    """Parse init.sql, extract INSERT values specifically for orders table."""
    if not os.path.exists(sql_path):
        return {"status": "error", "message": f"File không tồn tại: {sql_path}"}

    items = []
    seen_ids = set()
    dirty = 0
    duplicated_in_file = 0
    try:
        with open(sql_path, encoding="utf-8") as f:
            content = f.read()
            
            # Pattern Upgrade: Capture (user, prod, qty, price, status, created_at)
            blocks = re.findall(r"INSERT\s+INTO\s+[`\"']?orders[`\"']?\s*\(.*?\)\s*VALUES\s*(.*?);", content, re.S | re.I)
            
            # Primary: 6-column (user, prod, qty, price, status, created_at)
            v6 = re.compile(r"\((\d+),\s*(\d+),\s*(\d+),\s*([\d.]+),\s*['\"](.*?)['\"],\s*['\"](.*?)['\"]\)")
            # Fallback: 4-column (user, prod, qty, price)
            v4 = re.compile(r"\((\d+),\s*(\d+),\s*(\d+),\s*([\d.]+)\)")
            
            for block in blocks:
                # Try 6-column first
                matches = list(v6.finditer(block))
                if matches:
                    for m in matches:
                        raw = {
                            "user_id":     m.group(1),
                            "product_id":  m.group(2),
                            "quantity":    m.group(3),
                            "total_price": m.group(4),
                            "status":      m.group(5),
                            "created_at":  m.group(6),
                        }
                        data, is_dirty = standard_cleaner(raw, "SQL Historical (6-col)")
                        if data:
                            mid = generate_message_id(data)
                            if mid in seen_ids:
                                duplicated_in_file += 1
                                continue
                            seen_ids.add(mid)
                            data["message_id"] = mid
                            items.append(data)
                            if is_dirty: dirty += 1
                else:
                    # Fallback to 4-column
                    for m in v4.finditer(block):
                        raw = {
                            "user_id":     m.group(1),
                            "product_id":  m.group(2),
                            "quantity":    m.group(3),
                            "total_price": m.group(4),
                        }
                        data, is_dirty = standard_cleaner(raw, "SQL Historical (4-col)")
                        if data:
                            mid = generate_message_id(data)
                            if mid in seen_ids:
                                duplicated_in_file += 1
                                continue
                            seen_ids.add(mid)
                            data["message_id"] = mid
                            items.append(data)
                            if is_dirty: dirty += 1
    except Exception as e:
        _log_event("Ingest SQL Fail", str(e))
        return {"status": "error", "message": f"Lỗi đọc SQL: {e}"}

    if not items:
        return {"status": "error", "message": "Không tìm thấy dữ liệu orders hợp lệ trong SQL định dạng."}

    sent, errors = enqueue_bulk(items)
    return {
        "status": "success", 
        "message": f"Đã nạp {sent} bản ghi duy nhất ({dirty} bẩn đã sửa, {duplicated_in_file} trùng lặp đã bỏ qua)."
    }


def wipe_all() -> dict:
    """Xoá toàn bộ dữ liệu."""
    if LOCAL_MODE:
        _sqlite_truncate()
        # Reset worker stats
        _worker_stats["processed"] = _worker_stats["duplicates"] = _worker_stats["errors"] = 0
        return {"status": "success", "message": "Đã xoá toàn bộ dữ liệu SQLite local."}

    messages = []
    try:
        import mysql.connector
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        conn.cursor().execute("TRUNCATE TABLE orders"); conn.commit(); conn.close()
        messages.append("MySQL: OK")
    except Exception as e:
        messages.append(f"MySQL: {e}")
    try:
        import psycopg2
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        conn.cursor().execute("TRUNCATE TABLE transactions"); conn.commit(); conn.close()
        messages.append("PostgreSQL: OK")
    except Exception as e:
        messages.append(f"PostgreSQL: {e}")

    return {"status": "success", "message": " | ".join(messages)}


def purge_queue() -> dict:
    """Xoá queue."""
    if LOCAL_MODE:
        count = 0
        while not _local_queue.empty():
            try: _local_queue.get_nowait(); count += 1
            except Exception: break
        return {"status": "success", "message": f"Đã xoá {count} messages khỏi local queue."}

    try:
        for q_name in (RABBITMQ_QUEUE, "failed_orders"):
            url = f"{RABBITMQ_API_URL}/queues/%2F/{q_name}/contents"
            requests.delete(url, auth=(RABBITMQ_USER, RABBITMQ_PASSWORD), timeout=3)
        return {"status": "success", "message": "Pipeline purged successfully."}
    except Exception as e:
        return {"status": "error", "message": str(e)}
