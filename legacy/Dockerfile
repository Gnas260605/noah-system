FROM python:3.10-slim
WORKDIR /app

RUN pip install --no-cache-dir pika mysql-connector-python

COPY . .

# Tạo thư mục input và processed (nếu chưa tồn tại)
RUN mkdir -p /app/input /app/processed

# Chạy polling daemon tự động
CMD ["python", "reader.py"]