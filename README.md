# 🏗️ NOAH - Hệ thống Tích hợp Dữ liệu Bán lẻ (Retail Integration System)

> **Dự án tích hợp hệ thống dữ liệu toàn diện** cho mô hình bán lẻ, kết hợp dữ liệu từ nhiều nguồn (CSV Legacy, MySQL, PostgreSQL) thông qua RabbitMQ và Kong API Gateway.

---

## 📖 Giới thiệu Dự án

**NOAH** là một giải pháp backend hoàn chỉnh được thiết kế để giải quyết bài toán đồng bộ hóa dữ liệu giữa các hệ thống cũ (Legacy) và hiện đại. Hệ thống cho phép xử lý hàng chục nghìn bản ghi dữ liệu lịch sử, làm sạch dữ liệu lỗi (như số lượng âm, sai định dạng) và đưa vào hệ thống giám sát thời gian thực.

Dự án này nằm trong khuôn khổ môn học **Tích hợp hệ thống (Platform Integration)** - CMUCS 445.

---

## 🌊 Luồng hoạt động của hệ thống (Project Flow)

Luồng dữ liệu trong NOAH được thiết kế theo mô hình **Event-Driven Architecture**:

1.  **Nguồn dữ liệu (Ingestion Layer):**
    *   **Legacy CSV:** Hệ thống đọc file `inventory.csv` (hơn 5,000 bản ghi), áp dụng chiến lược `NEGATIVE_NUMBERS` (chuyển số âm thành số dương) và gửi vào RabbitMQ.
    *   **Historical SQL:** Parser đọc dữ liệu từ file SQL cũ (hơn 20,000 bản ghi) và đẩy vào hàng đợi.
    *   **Mock Producer:** Tự động tạo đơn hàng giả lập mỗi giây để kiểm thử tải hệ thống.
    *   **Manual Entry:** Người dùng nhập đơn hàng trực tiếp qua Dashboard.

2.  **Trung chuyển (Messaging Layer):**
    *   **RabbitMQ:** Đóng vai trò là "trái tim" điều phối. Dữ liệu từ các nguồn khác nhau được đẩy vào queue `orders`. Nếu xử lý lỗi quá 3 lần, dữ liệu sẽ được chuyển sang **Dead Letter Queue (DLQ)** để kiểm tra sau.

3.  **Xử lý & Lưu trữ (Processing Layer):**
    *   **Worker:** Lắng nghe hàng đợi RabbitMQ, giải mã dữ liệu và thực hiện ghi đồng thời vào:
        *   **MySQL:** Lưu thông tin đơn hàng đầy đủ (E-commerce DB).
        *   **PostgreSQL:** Lưu thông tin giao dịch tài chính (Finance DB).
    *   Hệ thống sử dụng `message_id` (SHA-256) để đảm bảo tính **Idempotency** (không bị trùng lặp dữ liệu khi gửi lại).

4.  **Cổng kết nối (API Gateway):**
    *   **Kong Gateway:** Đóng vai trò bảo mật và kiểm soát lưu lượng (Rate Limiting 60 req/min). Mọi truy cập vào endpoint `/api/sales` đều phải đi qua Kong với API Key.

5.  **Giám sát (Presentation Layer):**
    *   **Dashboard:** Hiển thị biểu đồ realtime, trạng thái các service và thống kê đơn hàng.
    *   **Reconciliation Report:** Đối soát dữ liệu giữa MySQL và PostgreSQL để phát hiện sai sót.

---

## 📂 Cấu trúc thư mục

```text
noah-system/
├── api/             # Flask Backend & Dashboard (Giao diện API)
├── worker/          # Consumer xử lý hàng đợi và ghi DB
├── producer/        # Script giả lập tạo đơn hàng tự động
├── legacy/          # Logic đọc và làm sạch dữ liệu CSV
├── db/              # Chứa các script khởi tạo database (SQL)
├── docker-compose.yml # File cấu hình chạy toàn bộ hệ thống
└── requirements.txt # Danh sách thư viện Python
```

---

## 🚀 Hướng dẫn chạy dự án

### Cách 1: Sử dụng Docker (Khuyên dùng)
Đây là cách nhanh nhất để chạy toàn bộ 9 dịch vụ (MySQL, Postgres, RabbitMQ, Kong, API...).

1.  **Khởi động các dịch vụ:**
    ```bash
    docker-compose up --build -d
    ```

2.  **Cấu hình Gateway (Chỉ cần chạy 1 lần):**
    Chờ khoảng 30 giây để Kong Gateway khởi động hoàn toàn, sau đó chạy:
    ```bash
    python init_kong.py
    ```

3.  **Truy cập hệ thống:**
    *   **Dashboard:** [http://localhost:5000](http://localhost:5000)
    *   **Báo cáo đối soát:** [http://localhost:5000/report](http://localhost:5000/report)
    *   **RabbitMQ Manager:** [http://localhost:15672](http://localhost:15672) (guest/guest)

---

### Cách 2: Chạy Local (Cho mục đích Debug)
*Lưu ý: Bạn cần cài đặt sẵn Python 3.10+, MySQL, PostgreSQL và RabbitMQ trên máy.*

1.  **Tạo môi trường ảo:**
    ```bash
    python -m venv .venv
    ./.venv/Scripts/activate # Windows
    ```

2.  **Cài đặt thư viện:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Chạy ứng dụng:**
    ```bash
    python app.py
    ```

---

## 🛠️ Các tính năng nổi bật

*   **Làm sạch dữ liệu:** Tự động sửa lỗi số lượng âm trong file CSV cũ.
*   **Chống trùng lặp:** Đảm bảo một đơn hàng dù gửi bao nhiêu lần cũng chỉ lưu một bản ghi duy nhất.
*   **Kháng lỗi (Resilience):** Cơ chế Retry 3 lần và Dead Letter Queue giúp hệ thống không bao giờ mất dữ liệu.
*   **Bảo mật:** Giới hạn tốc độ truy cập thông qua Kong API Gateway.
*   **Báo cáo thông minh:** Tự động đối chiếu số dư và số lượng giao dịch giữa các database khác nhau để tìm ra chênh lệch.

---

---

## 🔧 Troubleshooting: worker logs empty

Nếu bạn chạy `docker logs -f noah-worker` mà không thấy gì (mặc dù container vẫn đang chạy), đó là do cơ chế **Python stdout buffering**.

**Cách khắc phục:**
1.  Đảm bảo trong `docker-compose.yml` đã có biến môi trường `PYTHONUNBUFFERED: "1"`.
2.  Chạy lệnh sau để khởi động lại và áp dụng cấu hình:
    ```bash
    docker-compose up -d --build
    ```
3.  Kiểm tra lại log:
    ```bash
    docker logs -f noah-worker
    ```

---
1. Khi bạn tắt "Producer System" hoặc "Legacy Reader":
Kết quả: Hệ thống chỉ đơn giản là ngừng tạo ra các đơn hàng giả lập tự động.
Tác động: Bình thường. Bạn vẫn có thể nạp dữ liệu thủ công bằng nút "SQL Ingest" hoặc "CSV Inventory". Việc tắt Producer thường được làm khi bạn muốn kiểm tra dữ liệu thật mà không bị các đơn hàng ảo làm nhiễu.
2. Khi bạn tắt "Worker Service":
Kết quả: Các đơn hàng (từ Producer hoặc từ file nạp vào) sẽ không được ghi vào Database ngay lập tức.
Tác động: An toàn (Không mất dữ liệu). RabbitMQ sẽ đóng vai trò là "bộ nhớ đệm". Các đơn hàng sẽ nằm chờ sẵn trong hàng đợi (Queue).
Khi bật lại: Ngay khi bạn bật lại Worker, nó sẽ "ngốn" sạch các tin nhắn đang chờ trong hàng đợi và cập nhật đầy đủ vào Database. Bạn sẽ thấy số lượng bản ghi tăng vọt lên để bắt kịp dữ liệu.
3. Khi bạn tắt API Dashboard (api container):
Kết quả: Bạn không thể truy cập giao diện web.
Tác động: Các dịch vụ ngầm vẫn chạy. Worker vẫn tiếp tục xử lý đơn hàng từ Producer và lưu vào DB bình thường. Khi bạn bật lại API, bạn sẽ thấy dữ liệu đã được cập nhật xong xuôi.
Tóm lại: Hệ thống của bạn có tính kháng lỗi (Resilience) rất cao. Bạn có thể tự do bật/tắt các thành phần để quan sát luồng dữ liệu mà không sợ làm mất dữ liệu hay gây crash toàn bộ hệ thống. Đó chính là ưu điểm lớn nhất của việc sử dụng RabbitMQ làm trung gian.
## 👥 Thông tin nhóm thực hiện


*   **Nhóm:** Team 1
*   **Môn học:** CMUCS 445 - Platform Integration Systems
*   **Trường:** Đại học Duy Tân (DTU)

---
*Chúc bạn có trải nghiệm tốt với hệ thống NOAH!*
