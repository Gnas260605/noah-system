"""
legacy/reader.py – Module 1: Legacy Adapter (Polling Daemon)
============================================================
Tự động quét thư mục /app/input/ mỗi 10 giây để tìm inventory.csv.
Khi tìm thấy:
  1. Đọc CSV, validate dữ liệu
  2. UPDATE bảng products trong MySQL (cập nhật tồn kho)
  3. Di chuyển file sang /app/processed/inventory_<timestamp>.csv
  4. Log kết quả đúng format yêu cầu

YÊU CẦU ĐÁP ỨNG:
  ✅ Cơ chế Polling (5-10 giây / lần)
  ✅ Validate qty < 0 → BỎ QUA + log warning (không sửa)
  ✅ Bỏ qua dòng thiếu/sai định dạng
  ✅ UPDATE products SET stock=... (không đẩy vào RabbitMQ)
  ✅ Di chuyển file sang /processed sau khi xử lý
  ✅ Log: [INFO] Processed X records. Skipped Y invalid records.
"""

import csv
import os
import shutil
import time
import mysql.connector
from datetime import datetime

# ── Config ───────────────────────────────────────────────────────────────────
MYSQL_HOST     = os.getenv("MYSQL_HOST",     "mysql")
MYSQL_USER     = os.getenv("MYSQL_USER",     "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "ecommerce")

INPUT_DIR     = os.getenv("INPUT_DIR",     "/app/input")
PROCESSED_DIR = os.getenv("PROCESSED_DIR", "/app/processed")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "10"))   # giây


# ── MySQL helper ─────────────────────────────────────────────────────────────
def get_mysql_conn():
    """Kết nối MySQL với retry vô hạn."""
    while True:
        try:
            return mysql.connector.connect(
                host=MYSQL_HOST,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DATABASE,
                connection_timeout=5,
            )
        except mysql.connector.Error as e:
            print(f"[Module 1] MySQL connection failed: {e}. Retrying in 5s...")
            time.sleep(5)


# ── Core processing ──────────────────────────────────────────────────────────
def process_csv(csv_path: str, conn) -> tuple[int, int]:
    """
    Đọc CSV và UPDATE bảng products trong MySQL.

    Returns:
        (processed_count, skipped_count)
    """
    processed = 0
    skipped   = 0
    cur = conn.cursor()

    try:
        with open(csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    # ── Extraction ───────────────────────────────────────────
                    raw_pid = row.get("product_id") or row.get("id")
                    raw_qty = row.get("quantity")   or row.get("stock") or row.get("amount")

                    # ── Validate: thiếu dữ liệu → bỏ qua ───────────────────
                    if raw_pid is None or raw_qty is None or str(raw_pid).strip() == "" or str(raw_qty).strip() == "":
                        print(f"[Module 1][WARN] Bỏ qua dòng thiếu dữ liệu: {row}")
                        skipped += 1
                        continue

                    p_id = int(float(str(raw_pid).strip()))
                    qty  = int(float(str(raw_qty).strip()))

                    # ── Validate: qty < 0 → BỎ QUA (không sửa) ─────────────
                    if qty < 0:
                        print(f"[Module 1][WARN] Bỏ qua sản phẩm {p_id}: Số lượng âm ({qty}) không hợp lệ.")
                        skipped += 1
                        continue

                    # ── UPDATE products trong MySQL ──────────────────────────
                    cur.execute(
                        "UPDATE products SET stock = %s WHERE id = %s",
                        (qty, p_id)
                    )

                    if cur.rowcount > 0:
                        processed += 1
                    else:
                        # Sản phẩm không tồn tại → INSERT mới
                        cur.execute(
                            "INSERT INTO products (id, name, price, stock) "
                            "VALUES (%s, %s, %s, %s) "
                            "ON DUPLICATE KEY UPDATE stock = %s",
                            (p_id, f"Product {p_id}", 0.0, qty, qty)
                        )
                        processed += 1

                except (ValueError, TypeError) as e:
                    print(f"[Module 1][WARN] Bỏ qua dòng sai định dạng: {row} – {e}")
                    skipped += 1
                    continue

        conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"[Module 1][ERROR] Lỗi xử lý CSV: {e}")
        raise
    finally:
        cur.close()

    return processed, skipped


def move_to_processed(csv_path: str):
    """Di chuyển file đã xử lý sang thư mục /processed với timestamp."""
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = os.path.basename(csv_path)
    name, ext = os.path.splitext(base_name)
    dest = os.path.join(PROCESSED_DIR, f"{name}_{timestamp}{ext}")
    shutil.move(csv_path, dest)
    print(f"[Module 1][INFO] File đã chuyển sang: {dest}")


# ── Polling daemon ────────────────────────────────────────────────────────────
def run_polling_daemon():
    """
    Vòng lặp polling chính: kiểm tra thư mục /app/input/ mỗi POLL_INTERVAL giây.
    Không cần trigger thủ công – tự động hoàn toàn.
    """
    print(f"[Module 1][INFO] Legacy Adapter khởi động. Poll interval={POLL_INTERVAL}s")
    print(f"[Module 1][INFO] Thư mục đầu vào : {INPUT_DIR}")
    print(f"[Module 1][INFO] Thư mục đã xử lý: {PROCESSED_DIR}")

    os.makedirs(INPUT_DIR,     exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    conn = get_mysql_conn()
    print("[Module 1][INFO] Đã kết nối MySQL thành công.")

    while True:
        try:
            # ── Quét thư mục tìm file CSV ────────────────────────────────
            csv_files = [
                os.path.join(INPUT_DIR, f)
                for f in os.listdir(INPUT_DIR)
                if f.lower().endswith(".csv")
            ]

            if not csv_files:
                # Không có file → sleep và tiếp tục
                time.sleep(POLL_INTERVAL)
                continue

            for csv_path in csv_files:
                print(f"\n[Module 1][INFO] Phát hiện file: {csv_path}")
                try:
                    # Đảm bảo MySQL vẫn kết nối
                    conn.ping(reconnect=True, attempts=3, delay=1)

                    processed, skipped = process_csv(csv_path, conn)

                    # ── Log đúng format yêu cầu ──────────────────────────
                    print(
                        f"[Module 1][INFO] Processed {processed} records. "
                        f"Skipped {skipped} invalid records."
                    )

                    # ── Di chuyển file sang /processed ───────────────────
                    move_to_processed(csv_path)

                except Exception as e:
                    print(f"[Module 1][ERROR] Lỗi xử lý {csv_path}: {e}")
                    # Thử reconnect MySQL
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = get_mysql_conn()

        except Exception as e:
            print(f"[Module 1][ERROR] Polling error: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run_polling_daemon()