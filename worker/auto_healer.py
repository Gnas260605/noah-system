"""
auto_healer.py – Module 2: Messaging & Consistency → Auto-Healing Engine
=========================================================================
Phát hiện và tự động sửa lỗi lệch dữ liệu giữa MySQL (orders) và
PostgreSQL (transactions).

Luồng hoạt động:
  1. DETECT  – So sánh message_id tồn tại ở hai DB
  2. DIAGNOSE – Phân loại: thiếu ở PG, thiếu ở MySQL, hay conflict
  3. HEAL    – Sao chép bản ghi bị thiếu sang DB đích
  4. AUDIT   – Ghi nhật ký toàn bộ hành động heal

Có thể chạy:
  • Độc lập:  python auto_healer.py
  • Tích hợp: from auto_healer import HealEngine; HealEngine().run_cycle()
  • Docker:   thêm service auto_healer vào docker-compose.yml
"""

import os
import time
import json
import socket
import threading
from datetime import datetime

import mysql.connector
import psycopg2

# ──────────────────────────────────────────────────────────────
#  Config (from env, same as worker.py)
# ──────────────────────────────────────────────────────────────
MYSQL_CFG = dict(
    host=os.getenv("MYSQL_HOST", "mysql"),
    user=os.getenv("MYSQL_USER", "root"),
    password=os.getenv("MYSQL_PASSWORD", "root"),
    database=os.getenv("MYSQL_DATABASE", "ecommerce"),
)
PG_CFG = dict(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    database=os.getenv("POSTGRES_DB", "finance"),
    user=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD", "root"),
)

# Khoảng thời gian giữa mỗi chu kỳ heal (giây)
HEAL_INTERVAL = int(os.getenv("HEAL_INTERVAL_SECONDS", "60"))

# Số bản ghi tối đa sửa mỗi chu kỳ (giới hạn tải)
HEAL_BATCH_LIMIT = int(os.getenv("HEAL_BATCH_LIMIT", "500"))


# ──────────────────────────────────────────────────────────────
#  Global stats (in-memory, reset on restart)
# ──────────────────────────────────────────────────────────────
heal_stats = {
    "cycles_run":         0,
    "total_healed_pg":    0,  # records injected into PostgreSQL
    "total_healed_mysql": 0,  # records injected into MySQL
    "total_detected":     0,  # total discrepancies ever found
    "last_run":           None,
    "last_result":        None,
    "status":             "idle",  # idle | running | ok | error
    "history":            [],      # list of last 20 cycle summaries
}

_stats_lock = threading.Lock()


# ──────────────────────────────────────────────────────────────
#  DB helpers
# ──────────────────────────────────────────────────────────────
def _mysql_conn():
    return mysql.connector.connect(**MYSQL_CFG)

def _pg_conn():
    return psycopg2.connect(**PG_CFG)


# ──────────────────────────────────────────────────────────────
#  HealEngine
# ──────────────────────────────────────────────────────────────
class HealEngine:
    """
    Phát hiện và tự động sửa lỗi lệch dữ liệu giữa MySQL và PostgreSQL.
    """

    def __init__(self):
        self.mysql_conn = None
        self.pg_conn    = None

    # ── Connection management ──────────────────────────────────
    def connect(self):
        """Mở kết nối đến cả hai DB, thử lại nếu thất bại."""
        while True:
            try:
                if self.mysql_conn:
                    try: self.mysql_conn.close()
                    except Exception: pass
                if self.pg_conn:
                    try: self.pg_conn.close()
                    except Exception: pass

                self.mysql_conn = _mysql_conn()
                self.pg_conn    = _pg_conn()
                print("[AutoHealer] Kết nối DB thành công.")
                return
            except Exception as e:
                print(f"[AutoHealer] Lỗi kết nối: {e}. Thử lại sau 5s…")
                time.sleep(5)

    def _ensure_connections(self):
        """Tự động kết nối lại nếu connection bị đóng."""
        try:
            self.mysql_conn.ping(reconnect=True, attempts=1, delay=0)
        except Exception:
            self.mysql_conn = _mysql_conn()
        try:
            self.pg_conn.cursor().execute("SELECT 1")
        except Exception:
            self.pg_conn = _pg_conn()

    # ── Step 1: DETECT discrepancies ──────────────────────────
    def detect(self) -> dict:
        """
        Lấy toàn bộ message_id từ cả hai DB rồi so sánh.

        Returns:
            {
                "mysql_ids":    set,   # message_ids trong MySQL
                "pg_ids":       set,   # message_ids trong PostgreSQL
                "missing_in_pg":    list,  # có trong MySQL, thiếu ở PG
                "missing_in_mysql": list,  # có trong PG, thiếu ở MySQL
                "total_diff":   int,
            }
        """
        self._ensure_connections()

        # Lấy IDs từ MySQL
        my_cur = self.mysql_conn.cursor()
        my_cur.execute("SELECT message_id FROM orders")
        mysql_ids = {row[0] for row in my_cur.fetchall()}
        my_cur.close()

        # Lấy IDs từ PostgreSQL
        pg_cur = self.pg_conn.cursor()
        pg_cur.execute("SELECT message_id FROM transactions")
        pg_ids = {row[0] for row in pg_cur.fetchall()}
        pg_cur.close()

        missing_in_pg    = list(mysql_ids - pg_ids)
        missing_in_mysql = list(pg_ids - mysql_ids)

        return {
            "mysql_ids":        mysql_ids,
            "pg_ids":           pg_ids,
            "missing_in_pg":    missing_in_pg,
            "missing_in_mysql": missing_in_mysql,
            "total_diff":       len(missing_in_pg) + len(missing_in_mysql),
        }

    # ── Step 2: HEAL missing in PostgreSQL ────────────────────
    def heal_into_postgres(self, missing_ids: list) -> dict:
        """
        Sao chép bản ghi từ MySQL → PostgreSQL cho các message_id bị thiếu.

        Returns: { "healed": int, "errors": int, "details": [...] }
        """
        if not missing_ids:
            return {"healed": 0, "errors": 0, "details": []}

        batch = missing_ids[:HEAL_BATCH_LIMIT]
        healed = 0
        errors = 0
        details = []

        self._ensure_connections()
        my_cur = self.mysql_conn.cursor(dictionary=True)
        pg_cur = self.pg_conn.cursor()

        for msg_id in batch:
            try:
                # Fetch từ MySQL
                my_cur.execute(
                    "SELECT message_id, user_id, product_id, quantity, total_price "
                    "FROM orders WHERE message_id = %s",
                    (msg_id,)
                )
                row = my_cur.fetchone()
                if not row:
                    continue

                # Inject vào PostgreSQL
                pg_cur.execute(
                    """
                    INSERT INTO transactions
                        (message_id, user_id, product_id, quantity, total_price)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (message_id) DO NOTHING
                    """,
                    (row["message_id"], row["user_id"], row["product_id"],
                     row["quantity"], row["total_price"])
                )
                self.pg_conn.commit()
                healed += 1
                details.append({
                    "message_id": msg_id,
                    "action": "HEAL→PG",
                    "status": "ok",
                })
                print(f"[AutoHealer] ✔ HEAL→PG: {msg_id}")

            except Exception as e:
                errors += 1
                self.pg_conn.rollback()
                details.append({
                    "message_id": msg_id,
                    "action": "HEAL→PG",
                    "status": f"error: {e}",
                })
                print(f"[AutoHealer] ✘ HEAL→PG FAILED {msg_id}: {e}")

        my_cur.close()
        pg_cur.close()
        return {"healed": healed, "errors": errors, "details": details}

    # ── Step 3: HEAL missing in MySQL ─────────────────────────
    def heal_into_mysql(self, missing_ids: list) -> dict:
        """
        Sao chép bản ghi từ PostgreSQL → MySQL cho các message_id bị thiếu.

        Returns: { "healed": int, "errors": int, "details": [...] }
        """
        if not missing_ids:
            return {"healed": 0, "errors": 0, "details": []}

        batch = missing_ids[:HEAL_BATCH_LIMIT]
        healed = 0
        errors = 0
        details = []

        self._ensure_connections()
        pg_cur = self.pg_conn.cursor()
        my_cur = self.mysql_conn.cursor()

        for msg_id in batch:
            try:
                # Fetch từ PostgreSQL
                pg_cur.execute(
                    "SELECT message_id, user_id, product_id, quantity, total_price "
                    "FROM transactions WHERE message_id = %s",
                    (msg_id,)
                )
                row = pg_cur.fetchone()
                if not row:
                    continue

                msg_id_v, u_id, p_id, qty, price = row

                # Inject vào MySQL
                my_cur.execute(
                    """
                    INSERT INTO orders
                        (message_id, user_id, product_id, quantity, total_price)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE message_id = message_id
                    """,
                    (msg_id_v, u_id, p_id, qty, price)
                )
                self.mysql_conn.commit()
                healed += 1
                details.append({
                    "message_id": msg_id,
                    "action": "HEAL→MySQL",
                    "status": "ok",
                })
                print(f"[AutoHealer] ✔ HEAL→MySQL: {msg_id}")

            except Exception as e:
                errors += 1
                self.mysql_conn.rollback()
                details.append({
                    "message_id": msg_id,
                    "action": "HEAL→MySQL",
                    "status": f"error: {e}",
                })
                print(f"[AutoHealer] ✘ HEAL→MySQL FAILED {msg_id}: {e}")

        pg_cur.close()
        my_cur.close()
        return {"healed": healed, "errors": errors, "details": details}

    # ── Full heal cycle ───────────────────────────────────────
    def run_cycle(self) -> dict:
        """
        Chạy một chu kỳ detect + heal đầy đủ.

        Returns: dict tóm tắt kết quả chu kỳ
        """
        started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with _stats_lock:
            heal_stats["status"] = "running"

        print(f"\n[AutoHealer] ═══ Bắt đầu chu kỳ heal lúc {started_at} ═══")

        try:
            # 1. DETECT
            disc = self.detect()
            total_diff = disc["total_diff"]
            missing_pg    = disc["missing_in_pg"]
            missing_mysql = disc["missing_in_mysql"]

            print(f"[AutoHealer] Phát hiện: thiếu PG={len(missing_pg)}, "
                  f"thiếu MySQL={len(missing_mysql)}, tổng={total_diff}")

            # 2. HEAL
            res_pg    = self.heal_into_postgres(missing_pg)
            res_mysql = self.heal_into_mysql(missing_mysql)

            healed_pg    = res_pg["healed"]
            healed_mysql = res_mysql["healed"]
            total_healed = healed_pg + healed_mysql
            total_errors = res_pg["errors"] + res_mysql["errors"]

            # 3. Build result
            result = {
                "started_at":      started_at,
                "finished_at":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "mysql_total":     len(disc["mysql_ids"]),
                "pg_total":        len(disc["pg_ids"]),
                "missing_in_pg":   len(missing_pg),
                "missing_in_mysql":len(missing_mysql),
                "total_diff":      total_diff,
                "healed_into_pg":  healed_pg,
                "healed_into_mysql":healed_mysql,
                "total_healed":    total_healed,
                "errors":          total_errors,
                "status":          "ok" if total_errors == 0 else "partial",
                "details":         res_pg["details"] + res_mysql["details"],
            }

            # 4. Update global stats
            with _stats_lock:
                heal_stats["cycles_run"]         += 1
                heal_stats["total_healed_pg"]    += healed_pg
                heal_stats["total_healed_mysql"] += healed_mysql
                heal_stats["total_detected"]     += total_diff
                heal_stats["last_run"]            = started_at
                heal_stats["last_result"]         = result
                heal_stats["status"]              = result["status"]
                # Keep last 20 summaries
                summary = {k: v for k, v in result.items() if k != "details"}
                heal_stats["history"].insert(0, summary)
                heal_stats["history"] = heal_stats["history"][:20]

            print(f"[AutoHealer] ✔ Chu kỳ hoàn tất: "
                  f"+{healed_pg} PG, +{healed_mysql} MySQL, "
                  f"{total_errors} lỗi\n")
            return result

        except Exception as e:
            err_result = {
                "started_at":  started_at,
                "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "status":      "error",
                "error":       str(e),
                "total_diff":  0, "total_healed": 0, "errors": 1,
            }
            with _stats_lock:
                heal_stats["status"]      = "error"
                heal_stats["last_result"] = err_result
                heal_stats["history"].insert(0, err_result)
                heal_stats["history"] = heal_stats["history"][:20]
            print(f"[AutoHealer] ✘ Lỗi nghiêm trọng trong chu kỳ: {e}")
            return err_result

    def close(self):
        try:
            if self.mysql_conn: self.mysql_conn.close()
            if self.pg_conn:    self.pg_conn.close()
        except Exception:
            pass


# ──────────────────────────────────────────────────────────────
#  Background daemon (dùng khi chạy standalone / Docker service)
# ──────────────────────────────────────────────────────────────
def run_daemon():
    """
    Chạy HealEngine theo chu kỳ liên tục.
    Dùng cho Docker service hoặc standalone process.
    """
    print(f"[AutoHealer] Daemon khởi động. "
          f"Chu kỳ={HEAL_INTERVAL}s, giới hạn batch={HEAL_BATCH_LIMIT}")

    engine = HealEngine()
    engine.connect()

    while True:
        try:
            engine.run_cycle()
        except Exception as e:
            print(f"[AutoHealer] Daemon error: {e}. Tiếp tục sau {HEAL_INTERVAL}s…")
            try:
                engine.connect()
            except Exception:
                pass
        time.sleep(HEAL_INTERVAL)


# ──────────────────────────────────────────────────────────────
#  Entrypoint
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import threading

    def _print_stats():
        while True:
            time.sleep(30)
            with _stats_lock:
                s = heal_stats.copy()
            print(f"\n── [AutoHealer Stats] ──────────────────")
            print(f"   Chu kỳ đã chạy  : {s['cycles_run']}")
            print(f"   Tổng phát hiện  : {s['total_detected']}")
            print(f"   Healed → PG     : {s['total_healed_pg']}")
            print(f"   Healed → MySQL  : {s['total_healed_mysql']}")
            print(f"   Trạng thái      : {s['status']}")
            print(f"   Lần chạy cuối   : {s['last_run']}")
            print(f"───────────────────────────────────────\n")

    t = threading.Thread(target=_print_stats, daemon=True)
    t.start()

    run_daemon()
