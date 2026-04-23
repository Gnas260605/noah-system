"""
db_local.py – SQLite adapter cho local development.
Tự động khởi tạo schema, thread-safe.
"""
import os
import sqlite3
import threading
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "local.db")

_lock = threading.Lock()


def _get_conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # High-performance pragmas
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA cache_size=-64000") # 64MB cache
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn


def init_db():
    """Tạo bảng nếu chưa có."""
    with _lock:
        conn = _get_conn()
        # Table Đơn hàng
        conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT    UNIQUE NOT NULL,
                user_id    INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity   INTEGER NOT NULL,
                total_price REAL   NOT NULL,
                created_at TEXT    DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now','localtime'))
            )
        """)
        # Table Nhật ký Worker
        conn.execute("""
            CREATE TABLE IF NOT EXISTS system_logs (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                event      TEXT NOT NULL,
                message    TEXT,
                created_at TEXT    DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now','localtime'))
            )
        """)
        # Table Dữ liệu lỗi
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dirty_records (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                source     TEXT NOT NULL,
                payload    TEXT,
                reason     TEXT,
                created_at TEXT    DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now','localtime'))
            )
        """)
        conn.commit()
        conn.close()


def log_event(event: str, message: str = ""):
    """Ghi log hoạt động hệ thống."""
    with _lock:
        conn = _get_conn()
        try:
            conn.execute("INSERT INTO system_logs (event, message) VALUES (?, ?)", (event, message))
            conn.commit()
        finally:
            conn.close()


def log_dirty(source: str, payload: str, reason: str):
    """Ghi log dữ liệu bẩn/lỗi."""
    with _lock:
        conn = _get_conn()
        try:
            conn.execute(
                "INSERT INTO dirty_records (source, payload, reason) VALUES (?, ?, ?)",
                (source, payload, reason)
            )
            conn.commit()
        finally:
            conn.close()


def get_system_logs(limit: int = 50) -> list:
    """Lấy N logs hệ thống mới nhất."""
    with _lock:
        conn = _get_conn()
        try:
            rows = conn.execute("SELECT * FROM system_logs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


def get_dirty_records(limit: int = 50) -> list:
    """Lấy N records lỗi mới nhất."""
    with _lock:
        conn = _get_conn()
        try:
            rows = conn.execute("SELECT * FROM dirty_records ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


def count_dirty() -> int:
    """Đếm số bản ghi bẩn."""
    with _lock:
        conn = _get_conn()
        try:
            return conn.execute("SELECT COUNT(*) FROM dirty_records").fetchone()[0]
        except Exception:
            return 0
        finally:
            conn.close()


def insert_order(data: dict) -> bool:
    """
    Lưu một đơn hàng vào SQLite (idempotent qua message_id).
    Trả về True nếu insert thành công, False nếu duplicate.
    """
    with _lock:
        conn = _get_conn()
        try:
            conn.execute(
                """INSERT OR IGNORE INTO orders
                   (message_id, user_id, product_id, quantity, total_price, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    data["message_id"],
                    data["user_id"],
                    data["product_id"],
                    data["quantity"],
                    data["total_price"],
                    data.get("created_at") # Now mandatory from cleaner
                )
            )
            affected = conn.execute("SELECT changes()").fetchone()[0]
            conn.commit()
            return affected > 0
        except Exception as e:
            print(f"[SQLite] insert_order error: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()


def insert_orders_bulk(items: list[dict]) -> int:
    """Commit multiple items in a single transaction (extremely fast)."""
    if not items: return 0
    with _lock:
        conn = _get_conn()
        try:
            conn.execute("BEGIN TRANSACTION")
            cur = conn.cursor()
            cur.executemany(
                """INSERT OR IGNORE INTO orders
                   (message_id, user_id, product_id, quantity, total_price, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                [
                    (
                        data["message_id"],
                        data["user_id"],
                        data["product_id"],
                        data["quantity"],
                        data["total_price"],
                        data.get("created_at")
                    )
                    for data in items
                ]
            )
            affected = cur.execute("SELECT changes()").fetchone()[0]
            conn.commit()
            return max(0, affected)
        except Exception as e:
            print(f"[SQLite BULK] error: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()


def get_recent_orders(limit: int = 50) -> list:
    """Lấy N đơn hàng mới nhất."""
    with _lock:
        conn = _get_conn()
        try:
            rows = conn.execute(
                """SELECT message_id, user_id, product_id, quantity, total_price, created_at
                   FROM orders ORDER BY id DESC LIMIT ?""",
                (limit,)
            ).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []
        finally:
            conn.close()


def count_orders() -> int:
    """Đếm tổng số đơn hàng."""
    with _lock:
        conn = _get_conn()
        try:
            return conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        except Exception:
            return 0
        finally:
            conn.close()


def truncate_orders():
    """Xoá toàn bộ dữ liệu."""
    with _lock:
        conn = _get_conn()
        try:
            conn.execute("DELETE FROM orders")
            conn.commit()
        finally:
            conn.close()


def get_tables() -> list[str]:
    """Retrieve all user-defined tables in the SQLite DB."""
    with _lock:
        conn = _get_conn()
        try:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
            return [row["name"] for row in rows]
        except Exception:
            return []
        finally:
            conn.close()


def query_table(table_name: str, limit: int = 100, offset: int = 0) -> dict:
    """Paginated generic query. Returns { 'columns': [...], 'rows': [...], 'total': N }."""
    # Sanitize table name (only alphanumeric and underscore) to prevent injection
    import re
    if not re.match(r'^\w+$', table_name):
        return {"error": "Invalid table name"}

    with _lock:
        conn = _get_conn()
        try:
            # Get data + columns
            cur = conn.execute(f"SELECT * FROM {table_name} LIMIT ? OFFSET ?", (limit, offset))
            cols = [description[0] for description in cur.description]
            rows = [dict(r) for r in cur.fetchall()]

            # Get total count
            total = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

            return {
                "columns": cols,
                "rows": rows,
                "total": total
            }
        except Exception as e:
            return {"error": str(e)}
        finally:
            conn.close()


# Khởi tạo schema ngay khi module được import
init_db()
