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
        # Table Lịch sử Auto-Healing
        conn.execute("""
            CREATE TABLE IF NOT EXISTS heal_log (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                cycle_started  TEXT NOT NULL,
                cycle_finished TEXT,
                detected       INTEGER DEFAULT 0,
                healed_pg      INTEGER DEFAULT 0,
                healed_mysql   INTEGER DEFAULT 0,
                errors         INTEGER DEFAULT 0,
                status         TEXT DEFAULT 'ok',
                detail_json    TEXT,
                created_at     TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now','localtime'))
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


# ─────────────────────────────────────────────────────────────
#  Heal Log functions
# ─────────────────────────────────────────────────────────────

def log_heal_cycle(result: dict):
    """Ghi một chu kỳ heal vào heal_log."""
    import json as _json
    with _lock:
        conn = _get_conn()
        try:
            conn.execute(
                """INSERT INTO heal_log
                   (cycle_started, cycle_finished, detected,
                    healed_pg, healed_mysql, errors, status, detail_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    result.get("started_at", ""),
                    result.get("finished_at", ""),
                    result.get("total_diff", 0),
                    result.get("healed_into_pg", 0),
                    result.get("healed_into_mysql", 0),
                    result.get("errors", 0),
                    result.get("status", "ok"),
                    _json.dumps(result.get("details", [])[:50]),  # max 50 details
                )
            )
            conn.commit()
        except Exception as e:
            print(f"[db_local] log_heal_cycle error: {e}")
        finally:
            conn.close()


def get_heal_log(limit: int = 20) -> list:
    """Lấy lịch sử heal gần nhất."""
    with _lock:
        conn = _get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM heal_log ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []
        finally:
            conn.close()


def get_heal_stats() -> dict:
    """Tổng hợp thống kê heal từ DB."""
    with _lock:
        conn = _get_conn()
        try:
            row = conn.execute(
                """SELECT
                       COUNT(*)          AS cycles,
                       COALESCE(SUM(detected), 0)      AS total_detected,
                       COALESCE(SUM(healed_pg), 0)     AS total_healed_pg,
                       COALESCE(SUM(healed_mysql), 0)  AS total_healed_mysql,
                       COALESCE(SUM(errors), 0)        AS total_errors,
                       MAX(cycle_started)              AS last_run
                   FROM heal_log"""
            ).fetchone()
            return dict(row) if row else {}
        except Exception:
            return {}
        finally:
            conn.close()


def simulate_local_heal() -> dict:
    """
    LOCAL MODE: giả lập heal bằng cách phát hiện orders nào bị thiếu
    created_at (dữ liệu không đầy đủ) và fill lại.
    Trong local mode chỉ có 1 DB nên không có lệch giữa MySQL/PG.
    Hàm này sẽ kiểm tra và sửa các bản ghi bị thiếu trường created_at.
    """
    from datetime import datetime as _dt
    fixed = 0
    with _lock:
        conn = _get_conn()
        try:
            # Tìm các bản ghi thiếu created_at
            rows = conn.execute(
                "SELECT id, message_id FROM orders WHERE created_at IS NULL OR created_at = ''"
            ).fetchall()
            if rows:
                now_str = _dt.now().strftime('%Y-%m-%d %H:%M:%S')
                for row in rows:
                    conn.execute(
                        "UPDATE orders SET created_at = ? WHERE id = ?",
                        (now_str, row["id"])
                    )
                conn.commit()
                fixed = len(rows)
        except Exception as e:
            print(f"[local heal] error: {e}")
            conn.rollback()
        finally:
            conn.close()

    # Build result dict giống Docker mode để UI dùng chung
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    result = {
        "started_at":       now_str,
        "finished_at":      now_str,
        "mysql_total":      count_orders(),
        "pg_total":         count_orders(),
        "missing_in_pg":    0,
        "missing_in_mysql": 0,
        "total_diff":       fixed,
        "healed_into_pg":   0,
        "healed_into_mysql":0,
        "total_healed":     fixed,
        "errors":           0,
        "status":           "ok",
        "note":             f"Local mode: đã sửa {fixed} bản ghi thiếu created_at.",
        "details":          [],
    }
    log_heal_cycle(result)
    return result
