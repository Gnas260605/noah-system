"""
app.py (api/app.py) – Flask application chính.

Routes:
  GET  /                        → redirect /dashboard
  GET  /dashboard               → trang chủ
  GET  /dashboard/data          → JSON snapshot
  GET  /send-order              → form gửi đơn
  POST /send-order              → gửi đơn qua form HTML
  POST /sales                   → API gửi 1 đơn (JSON)
  POST /bulk-orders             → API gửi nhiều đơn (JSON)
  GET  /report                  → trang đối soát
  GET  /report/data             → JSON report
  GET  /pipeline                → trang pipeline logs
  POST /api/ops/ingest-legacy   → đọc inventory.csv → queue
  POST /api/ops/ingest-historical → parse init.sql → queue
  POST /api/ops/purge-queue     → xoá queue
  POST /api/ops/wipe-databases  → xoá DB
"""
import os
import json
import uuid
import time
import random
import threading
import subprocess
from decimal import Decimal
from datetime import datetime
from flask import Flask, jsonify, redirect, render_template, request, url_for, Response

from api.config import LOCAL_MODE
from api.services import (
    build_snapshot,
    enqueue,
    enqueue_bulk,
    generate_message_id,
    ingest_csv,
    ingest_sql,
    wipe_all,
    purge_queue,
    run_auto_heal,
    get_heal_history,
    get_heal_summary,
    fetch_dirty_records,
)
from api.db_local import (
    get_tables, query_table,
    get_system_logs, log_event as _log_event
)

app = Flask(__name__)


# ─────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────
from flask.json.provider import DefaultJSONProvider

class NoahJSONProvider(DefaultJSONProvider):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if hasattr(obj, 'strftime'):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        return super().default(obj)

app.json = NoahJSONProvider(app)

def _serialize(obj):
    return app.json.dumps(obj)

def _make_order(user_id, product_id, quantity, total_price) -> dict:
    data = {
        "user_id":     int(user_id),
        "product_id":  int(product_id),
        "quantity":    int(quantity),
        "total_price": float(total_price),
        "created_at":  datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    # Content-based hash for deduplication
    data["message_id"] = generate_message_id(data)
    return data


def _json_resp(status, message, data=None, meta=None, code=200):
    return jsonify({
        "status": status,
        "message": message,
        "data": data,
        "meta": meta
    }), code


def _ok(msg, data=None, meta=None):
    return _json_resp("success", msg, data, meta)


def _err(msg, code=400):
    return _json_resp("error", msg, code=code)


# ─────────────────────────────────────────────────────────────
#  Main pages
# ─────────────────────────────────────────────────────────────
@app.route("/admin/database")
def admin_database():
    return render_template("pages/admin_database.html")


@app.route("/")
def home():
    return redirect(url_for("dashboard"))


@app.route("/dashboard")
def dashboard():
    return render_template(
        "pages/dashboard.html",
        page_title="Product Sales Pipeline",
        current_page="dashboard",
        local_mode=LOCAL_MODE,
        snapshot=build_snapshot(),
        toast_type="",
        toast_message="",
    )


@app.route("/dashboard/data")
def dashboard_data():
    return _ok("Snapshot loaded", build_snapshot())


@app.route("/api/stream/dashboard")
def stream_dashboard():
    """SSE: Pushing real-time snapshots to dashboard."""
    def event_stream():
        try:
            while True:
                snapshot = build_snapshot()
                data = _serialize(snapshot)
                yield f"data: {data}\n\n"
                time.sleep(1.0)
        except GeneratorExit:
            # Client disconnected
            pass
    return Response(event_stream(), mimetype="text/event-stream")


@app.route("/report")
def report_page():
    snap = build_snapshot()
    mysql_count    = snap["mysql"]["count"]
    postgres_count = snap["postgres"]["count"]
    diff           = abs(mysql_count - postgres_count)
    max_c          = max(mysql_count, postgres_count, 1)
    synced_pct     = round(min(mysql_count, postgres_count) / max_c * 100, 1)

    # Lấy dữ liệu bẩn thực tế
    dirty_records = fetch_dirty_records(100)
    
    # Tính toán thống kê lỗi (Top issues)
    issue_counts = {}
    for dr in dirty_records:
        reason = dr.get("reason", "Unknown Error")
        issue_counts[reason] = issue_counts.get(reason, 0) + 1
    
    rejection_summary = sorted(
        [{"reason": k, "count": v} for k, v in issue_counts.items()],
        key=lambda x: x["count"], reverse=True
    )
    
    report = {
        "generated_at":  snap["generated_at"],
        "status":        "OK" if diff == 0 else "MISMATCH",
        "mysql_count":   mysql_count,
        "postgres_count":postgres_count,
        "diff":          diff,
        "dlq_count":     snap["queue"].get("messages", 0) if not snap["queue"]["ok"] else 0,
        "dirty_log":     dirty_records,
        "summary":       rejection_summary[:5], # Top 5 issues
        "reconciliation":{
            "synced_pct": synced_pct,
            "result": f"Orders DB={mysql_count}, Finance DB={postgres_count}, Chênh lệch={diff}",
        },
    }
    return render_template(
        "pages/report.html",
        page_title="Đối Soát Dữ Liệu – Noah Retail",
        current_page="report",
        local_mode=LOCAL_MODE,
        report=report,
    )


@app.route("/report/data")
def report_data():
    snap = build_snapshot()
    mysql_count    = snap["mysql"]["count"]
    postgres_count = snap["postgres"]["count"]
    diff           = abs(mysql_count - postgres_count)
    max_c          = max(mysql_count, postgres_count, 1)
    synced_pct     = round(min(mysql_count, postgres_count) / max_c * 100, 1)
    
    dirty_records = fetch_dirty_records(100)
    
    data = {
        "generated_at":   snap["generated_at"],
        "status":         "OK" if diff == 0 else "MISMATCH",
        "mysql_count":    mysql_count,
        "postgres_count": postgres_count,
        "diff":           diff,
        "dlq_count":      0,
        "dirty_log":      dirty_records,
        "reconciliation": {
            "synced_pct": synced_pct,
            "result":     f"Orders DB={mysql_count}, Finance DB={postgres_count}, Diff={diff}",
        },
    }
    return _ok("Report data loaded", data)


@app.route("/pipeline")
def pipeline_page():
    return render_template(
        "pages/pipeline.html",
        page_title="Pipeline Logs – Noah Retail",
        current_page="pipeline",
        local_mode=LOCAL_MODE,
        toast_type="",
        toast_message="",
        snapshot=build_snapshot(),
    )


@app.route("/send-order", methods=["GET", "POST"])
def send_order_form():
    if request.method == "POST":
        try:
            uid   = int(request.form.get("user_id")   or random.randint(100, 999))
            pid   = int(request.form.get("product_id") or random.randint(100, 200))
            qty   = int(request.form.get("quantity")   or 1)
            price = float(request.form.get("total_price") or qty * 150_000)
            data  = _make_order(uid, pid, qty, price)
            enqueue(data)
            return redirect(url_for(
                "send_order_form",
                status="success",
                message=f"Đã gửi đơn P#{pid} vào queue",
            ))
        except Exception as e:
            return redirect(url_for("send_order_form", status="error", message=str(e)))

    return render_template(
        "pages/send_order.html",
        page_title="Gửi Đơn Hàng",
        current_page="send_order",
        local_mode=LOCAL_MODE,
        toast_type=request.args.get("status", ""),
        toast_message=request.args.get("message", ""),
    )


# ─────────────────────────────────────────────────────────────
#  JSON APIs
# ─────────────────────────────────────────────────────────────
@app.route("/sales", methods=["POST"])
def create_sale():
    body = request.get_json(silent=True) or {}
    try:
        uid   = int(body.get("user_id")    or random.randint(100, 999))
        pid   = int(body.get("product_id") or body.get("id") or 100)
        qty   = int(body.get("quantity")   or body.get("amount") or 1)
        price = float(body.get("total_price") or body.get("price") or (qty * 100000))
        data  = _make_order(uid, pid, qty, price)
        enqueue(data)
        return jsonify({"status": "success", "message": "queued", "data": data})
    except Exception as e:
        return _err(str(e))


@app.route("/bulk-orders", methods=["POST"])
def bulk_orders():
    body  = request.get_json(silent=True) or {}
    count = int(body.get("count", 10))
    # Remove artificial limits
    
    def background_injector(n):
        try:
            chunk_size = 5000
            total_queued = 0
            _log_event("Bulk Start", f"Starting background injection of {n} records.")
            
            while total_queued < n:
                current_chunk = min(chunk_size, n - total_queued)
                items = []
                for _ in range(current_chunk):
                    uid   = random.randint(100, 999)
                    pid   = random.randint(100, 200)
                    qty   = random.randint(1, 50)
                    price = round(random.uniform(50_000, 2_000_000), 2)
                    items.append(_make_order(uid, pid, qty, price))
                
                enqueue_bulk(items)
                total_queued += current_chunk
                if total_queued % 25000 == 0 or total_queued >= n:
                    _log_event("Bulk Progress", f"Queued {total_queued}/{n} records.")
                
                time.sleep(0.1)
                
            _log_event("Bulk Complete", f"Successfully queued all {n} records.")
        except Exception as e:
            _log_event("Bulk Error", f"Background injector failed: {str(e)}")
            
    thread = threading.Thread(target=background_injector, args=(count,))
    thread.daemon = True
    thread.start()
    
    # [REAL-TIME] Invalidate cache immediately to show progress start
    import api.services as svc
    from api.services import _snap_lock
    with _snap_lock: svc._snap_cache = None
        
    return _ok(f"Bắt đầu bơm siêu tốc {count} đơn hàng (Chạy ngầm)...", data={"status": "started", "count": count})


# ─────────────────────────────────────────────────────────────
#  Operations API
# ─────────────────────────────────────────────────────────────
@app.route("/api/ops/ingest-legacy", methods=["POST"])
@app.route("/api/ops/ingest-csv", methods=["POST"])
def ops_ingest_csv():
    # Correct path to root directory
    base_dir = os.path.dirname(os.path.dirname(__file__))
    csv_path = os.path.join(base_dir, "legacy", "inventory.csv")
    result = ingest_csv(csv_path)
    if result["status"] == "success":
        # invalidate snapshot cache
        from api.services import _snap_lock
        import api.services as svc
        with _snap_lock:
            svc._snap_cache = None
        return _ok(result["message"])
    return _err(result["message"])


@app.route("/api/ops/ingest-historical", methods=["POST"])
@app.route("/api/ops/ingest-sql", methods=["POST"])
def ops_ingest_historical():
    base_dir = os.path.dirname(os.path.dirname(__file__))
    sql_path = os.path.join(base_dir, "db", "init.sql")
    result = ingest_sql(sql_path)
    if result["status"] == "success":
        # [REAL-TIME] Invalidate cache
        import api.services as svc
        from api.services import _snap_lock
        with _snap_lock: svc._snap_cache = None
        return _ok(result["message"])
    return _err(result["message"])


@app.route("/api/ops/purge-queue", methods=["POST"])
def ops_purge_queue():
    result = purge_queue()
    if result["status"] == "success":
        return _ok(result["message"])
    return _err(result["message"])


# ─────────────────────────────────────────────────────────────
#  Service Management API (Docker Compose Bridge - DooD)
# ─────────────────────────────────────────────────────────────
def run_docker_compose(cmd_list):
    """Helper to run docker compose commands within the project context."""
    try:
        # Intelligent path detection
        if os.path.exists("/app/docker-compose.yml"):
            root_dir = "/app"
        else:
            root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            
        compose_file = os.path.join(root_dir, "docker-compose.yml")
        
        # LOG FOR TRANSPARENCY: User will see this in their Terminal
        full_command = ["docker", "compose", "-f", compose_file] + cmd_list
        print(f"[DOCKER_CONTROL] Executing: {' '.join(full_command)}")
        
        try:
            # Try Docker Compose V2 (Plugin)
            result = subprocess.run(full_command, cwd=root_dir, capture_output=True, text=True, check=True)
            return True, result.stdout.strip()
        except (subprocess.CalledProcessError, FileNotFoundError):
            # Fallback to Docker Compose V1 (Hyphenated)
            full_command_v1 = ["docker-compose", "-f", compose_file] + cmd_list
            print(f"[DOCKER_CONTROL] V2 failed, trying V1: {' '.join(full_command_v1)}")
            result = subprocess.run(full_command_v1, cwd=root_dir, capture_output=True, text=True, check=True)
            return True, result.stdout.strip()

    except Exception as e:
        error_msg = getattr(e, 'stderr', str(e))
        print(f"[DOCKER_CONTROL] FAILED: {error_msg}")
        return False, f"Docker CLI Error: {error_msg.strip()}"

@app.route("/api/ops/service-status", methods=["GET"])
def ops_service_status():
    """Returns the running status of dockerized services using docker compose ps."""
    statuses = {"worker": False, "producer": False, "legacy": False}
    
    for svc in statuses.keys():
        # --status running -q returns the container ID if running, empty if not
        success, output = run_docker_compose(["ps", svc, "--status", "running", "-q"])
        if success and output:
            statuses[svc] = True
            
    return _ok("Status fetched", data=statuses)


@app.route("/api/ops/service-toggle", methods=["POST"])
def ops_service_toggle():
    """Starts or stops a specific docker service."""
    body = request.get_json(silent=True) or {}
    service_name = body.get("service")
    action = body.get("action")       # "start" or "stop"
    
    if service_name not in ["worker", "producer", "legacy"]:
        return _err("Dịch vụ không hợp lệ")
    
    # Map 'start'/'stop' to docker compose commands
    if action not in ["start", "stop", "restart"]:
        return _err("Hành động không hợp lệ")

    def run_toggle():
        run_docker_compose([action, service_name])
    
    # Run in background to prevent UI block/timeout
    threading.Thread(target=run_toggle, daemon=True).start()
    
    return _ok(f"Đã gửi lệnh {action} tới hệ thống {service_name}...")


@app.route("/api/ops/wipe-databases", methods=["POST"])
def ops_wipe_databases():
    # Support both old and new confirmation styles
    body = request.get_json(silent=True) or {}
    if body.get("confirm") != "WIPE":
        return _err("Cần xác nhận trong payload: { 'confirm': 'WIPE' }")
    result = wipe_all()
    if result["status"] == "success":
        import api.services as svc
        from api.services import _snap_lock
        with _snap_lock: svc._snap_cache = None
        return _ok(result["message"])
    return _err(result["message"])


@app.route("/api/ops/database-explorer")
def api_db_explorer():
    table = request.args.get("table", "orders")
    limit = min(int(request.args.get("limit", 100)), 100) # Strict safety limit
    offset = int(request.args.get("offset", 0))

    res = query_table(table, limit, offset)
    if "error" in res:
        return _err(res["error"])
    
    import math
    total_pages = math.ceil(res["total"] / limit)
    
    return _json_resp("success", f"Loaded {len(res['rows'])} records from {table}", 
                     data={"columns": res["columns"], "rows": res["rows"]},
                     meta={
                         "total": res["total"],
                         "total_pages": total_pages,
                         "current_page": (offset // limit) + 1,
                         "limit": limit
                     })


@app.route("/api/ops/database-tables")
def api_db_tables():
    tables = get_tables()
    return _ok("Tables loaded", tables)


@app.route("/api/ops/worker-logs")
def api_worker_logs():
    return _ok("Worker logs loaded", get_system_logs(50))


@app.route("/api/ops/dirty-data")
def api_dirty_data():
    return _ok("Dirty data loaded", fetch_dirty_records(100))


# ─────────────────────────────────────────────────────────────
#  Module 2: Auto-Healing API
# ─────────────────────────────────────────────────────────────

@app.route("/api/ops/auto-heal", methods=["POST"])
def api_auto_heal():
    """Kích hoạt một chu kỳ auto-heal ngay lập tức."""
    try:
        result = run_auto_heal()
        status = result.get("status", "ok")
        if status == "error":
            return _err(f"Heal thất bại: {result.get('error', 'unknown')}", result)
        msg = (
            f"Heal hoàn tất: phát hiện {result.get('total_diff',0)} lệch, "
            f"đã sửa {result.get('total_healed',0)} bản ghi, "
            f"{result.get('errors',0)} lỗi."
        )
        return _ok(msg, result)
    except Exception as e:
        return _err(f"Lỗi auto-heal: {e}")


@app.route("/api/ops/heal-log", methods=["GET"])
def api_heal_log():
    """Lịch sử các chu kỳ heal gần nhất."""
    limit = min(int(request.args.get("limit", 20)), 100)
    history = get_heal_history(limit)
    summary = get_heal_summary()
    return _ok("Heal log loaded", {"summary": summary, "history": history})


@app.route("/api/ops/heal-status", methods=["GET"])
def api_heal_status():
    """Trạng thái tổng hợp của hệ thống heal."""
    summary = get_heal_summary()
    snap = build_snapshot()
    diff = abs(snap["mysql"]["count"] - snap["postgres"]["count"])
    summary["current_diff"] = diff
    summary["needs_heal"] = diff > 0
    return _ok("Heal status", summary)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
