"""
notifier.py – Module 3: Notification System (UI Toast – Fire-and-Forget)
========================================================================
Sau khi Worker sync thành công, ghi notification vào bảng `notifications`
trong PostgreSQL. Dashboard sẽ đọc và hiển thị toast popup realtime qua SSE.

Worker KHÔNG bị block – mọi lỗi DB đều được log, không raise exception.
"""

import os
import threading
import logging
import psycopg2
from datetime import datetime

# ── Logger ────────────────────────────────────────────────────────────────────
logger = logging.getLogger("notifier")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# ── PostgreSQL config (đọc từ env) ───────────────────────────────────────────
def _get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        database=os.getenv("POSTGRES_DB", "finance"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "root"),
        connect_timeout=5,
    )

def _ensure_table():
    """Tạo bảng notifications nếu chưa có (chạy 1 lần khi khởi động)."""
    try:
        conn = _get_pg_conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS notifications (
                id          SERIAL PRIMARY KEY,
                order_id    VARCHAR(128) NOT NULL,
                user_id     VARCHAR(64),
                total       DECIMAL(15,2),
                message     TEXT,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"[NOTIFY] Could not ensure notifications table: {e}")


def _write_notification(order_id: str, user_id, total):
    """
    Ghi 1 dòng vào bảng notifications.
    Chạy trong daemon thread – Worker không bao giờ bị block.
    """
    try:
        now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        message = (
            f"Xin chào User {user_id}, đơn hàng #{order_id} "
            f"trị giá ${total} đã được xác nhận thanh toán thành công lúc {now}."
        )
        conn = _get_pg_conn()
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO notifications (order_id, user_id, total, message)
               VALUES (%s, %s, %s, %s)""",
            (str(order_id), str(user_id), float(total or 0), message)
        )
        conn.commit()
        cur.close()
        conn.close()

        logger.info(
            f"[INFO] Order #{order_id} synced. Notification sent to user_id={user_id}"
        )
    except Exception as e:
        logger.warning(f"[NOTIFY] Order #{order_id} – write failed (non-critical): {e}")


class NotificationService:
    """
    Service thông báo bất đồng bộ (Fire-and-Forget).
    Mỗi lần gọi send_async() sẽ spawn 1 daemon thread và return ngay.
    """

    def __init__(self):
        # Đảm bảo bảng tồn tại khi khởi động
        t = threading.Thread(target=_ensure_table, daemon=True)
        t.start()

    def send_async(self, order_data: dict):
        """
        Trigger ghi notification trong thread riêng.
        order_data keys: order_id, user_id, total
        """
        order_id = order_data.get("order_id", "N/A")
        user_id  = order_data.get("user_id",  "N/A")
        total    = order_data.get("total",    0)

        t = threading.Thread(
            target=_write_notification,
            args=(str(order_id), user_id, total),
            daemon=True
        )
        t.start()
