# Hướng dẫn dashboard real-time (Grafana)

Mô phỏng hệ thống thật: dữ liệu chảy liên tục → xử lý real-time → dashboard live.
```
stream_producer.py → Kafka → spark_stream_dashboard.py → PostgreSQL → Grafana
```

## Bước 0 — Dồn RAM (chỉ cần Kafka + Postgres + Spark local + Grafana)
Streaming dùng Spark local, KHÔNG cần YARN/HDFS/NiFi. Có thể tắt slave luôn.
```bash
stop-yarn.sh 2>/dev/null
~/nifi-1.28.1/bin/nifi.sh stop 2>/dev/null
# Giữ: Kafka, PostgreSQL
sudo systemctl start postgresql
~/kafka_2.13-3.7.1/bin/kafka-server-start.sh -daemon ~/kafka_2.13-3.7.1/config/kraft/server.properties
sleep 5
free -h
```

## Bước 1 — Tạo bảng trong PostgreSQL
```bash
PGPASSWORD=erp123 psql -h localhost -U erp -d erp <<'SQL'
CREATE TABLE IF NOT EXISTS rt_thongke (
  thoi_diem TIMESTAMP, doanh_thu DOUBLE PRECISION,
  loi_nhuan DOUBLE PRECISION, so_gd INTEGER
);
CREATE TABLE IF NOT EXISTS rt_canhbao (
  thoi_diem TIMESTAMP, txn_id TEXT, store_id TEXT, region TEXT,
  revenue DOUBLE PRECISION, cost DOUBLE PRECISION, profit DOUBLE PRECISION
);
SQL
```

## Bước 2 — Cài thư viện cho producer
```bash
pip3 install kafka-python
ls ~/postgresql-42.7.3.jar      # driver Postgres cho Spark (đã tải lúc làm NiFi)
```

## Bước 3 — Cài Grafana (1 lần)
```bash
sudo apt-get install -y apt-transport-https software-properties-common wget
sudo mkdir -p /etc/apt/keyrings/
wget -q -O - https://apt.grafana.com/gpg.key | gpg --dearmor | sudo tee /etc/apt/keyrings/grafana.gpg >/dev/null
echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://apt.grafana.com stable main" | sudo tee /etc/apt/sources.list.d/grafana.list
sudo apt-get update && sudo apt-get install -y grafana
sudo systemctl enable --now grafana-server
```
→ Mở `http://192.168.79.131:3000` (đăng nhập `admin`/`admin`, đổi mật khẩu).

## Bước 4 — Chạy (2 terminal)

**Terminal 1 — Spark Streaming → Postgres:**
```bash
cd ~/big-data-final-project
spark-submit --master local[2] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 \
  --jars /home/hduser/postgresql-42.7.3.jar \
  notebooks/spark_stream_dashboard.py
```

**Terminal 2 — Producer bắn giao dịch liên tục:**
```bash
cd ~/big-data-final-project/data_generator
python3 stream_producer.py
```
→ Dữ liệu chảy: producer → Kafka → Spark → Postgres mỗi 5 giây.

Kiểm tra Postgres có dữ liệu:
```bash
PGPASSWORD=erp123 psql -h localhost -U erp -d erp -c "SELECT * FROM rt_thongke ORDER BY thoi_diem DESC LIMIT 5;"
```

## Bước 5 — Cấu hình Grafana

### 5.1 Thêm data source PostgreSQL
Grafana → **Connections → Data sources → Add → PostgreSQL**:
- Host: `localhost:5432`
- Database: `erp`
- User: `erp` · Password: `erp123`
- TLS/SSL Mode: **disable**
- Save & test → "Database Connection OK".

### 5.2 Tạo dashboard + các panel

**Panel 1 — Doanh thu & lợi nhuận theo thời gian (Time series):**
```sql
SELECT thoi_diem AS "time", doanh_thu, loi_nhuan
FROM rt_thongke ORDER BY thoi_diem
```

**Panel 2 — Số giao dịch/5s (Time series hoặc Stat):**
```sql
SELECT thoi_diem AS "time", so_gd FROM rt_thongke ORDER BY thoi_diem
```

**Panel 3 — Cảnh báo gần đây (Table):**
```sql
SELECT thoi_diem, store_id, region, revenue, cost, profit
FROM rt_canhbao ORDER BY thoi_diem DESC LIMIT 20
```

**Panel 4 — Tổng số cảnh báo (Stat):**
```sql
SELECT COUNT(*) FROM rt_canhbao
```

### 5.3 Bật auto-refresh
Góc trên phải dashboard → chọn refresh **5s**. → Biểu đồ tự cập nhật khi producer bắn dữ liệu → **nhìn như sản phẩm thật**.

---

## Tắt khi xong
```bash
# Ctrl+C ở Terminal 1 (streaming) và Terminal 2 (producer)
sudo systemctl stop grafana-server      # nếu muốn
```

## Lỗi thường gặp
- **Spark báo thiếu driver Postgres** → kiểm `--jars /home/hduser/postgresql-42.7.3.jar` đúng đường dẫn.
- **Grafana không kết nối Postgres** → kiểm pg_hba cho phép localhost md5; user erp/erp123 đúng.
- **Dashboard trống** → producer chưa chạy, hoặc bảng rt_thongke chưa có dữ liệu (đợi vài batch 5s).
- **Hết RAM** → tắt slave + YARH + NiFi (demo này chỉ cần Kafka+Postgres+Spark local+Grafana).
