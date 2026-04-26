# 🏗️ NOAH - Hệ thống Tích hợp Dữ liệu Bán lẻ (Retail Integration System)

> **Dự án tích hợp hệ thống dữ liệu toàn diện** cho mô hình bán lẻ, kết hợp dữ liệu từ nhiều nguồn (CSV Legacy, MySQL, PostgreSQL) thông qua RabbitMQ..

---

## 📖 Giới thiệu Dự án

**NOAH** là một giải pháp backend hoàn chỉnh giúp đồng bộ hóa dữ liệu giữa các hệ thống cũ và hiện đại. Hệ thống xử lý hàng chục nghìn bản ghi, làm sạch dữ liệu lỗi và giám sát thời gian thực.

---

## 🌊 Luồng hoạt động của hệ thống (Project Flow)

1.  **Nguồn dữ liệu:** CSV Legacy, SQL cũ, hoặc tạo đơn hàng giả lập.
2.  **Trung chuyển:** RabbitMQ điều phối đơn hàng vào queue `orders`. Xử lý lỗi qua Dead Letter Queue.
3.  **Xử lý:** Worker ghi dữ liệu đồng thời vào MySQL (Bán hàng) và PostgreSQL (Tài chính).
4.  **Giám sát:** Dashboard thời gian thực và Báo cáo đối soát (Reconciliation).

---

## 📂 Cấu trúc thư mục

```text
noah-system/
├── api/             # Flask Backend & Dashboard
├── worker/          # Consumer xử lý hàng đợi
├── producer/        # Giả lập đơn hàng
├── legacy/          # Xử lý dữ liệu CSV/SQL cũ
├── db/              # Script khởi tạo DB
└── docker-compose.yml
```

---

## 🚀 Hướng dẫn chạy nhanh

### Sử dụng Docker (Khuyên dùng)
```bash
# Khởi động hệ thống (Chỉ 7 container thiết yếu)
docker-compose up --build -d

# Truy cập Dashboard
http://localhost:5000
```

---

## 🛠️ Các tính năng chính

*   **Làm sạch dữ liệu:** Sửa số lượng âm (Negative Numbers fix).
*   **Chống trùng lặp:** Dựa trên SHA-256 message_id.
*   **Đồng bộ DB kép:** Đảm bảo tính nhất quán giữa MySQL và Postgres.
*   **Database Explorer:** Truy vấn trực tiếp các bảng dữ liệu từ giao diện.
*   **Control Panel:** Bật/tắt các service trực tiếp từ Dashboard.

---

## 👥 Thông tin nhóm
*   **Nhóm:** Team 1 - CMUCS 445

---
*Chúc bạn có trải nghiệm tốt với hệ thống NOAH!*
