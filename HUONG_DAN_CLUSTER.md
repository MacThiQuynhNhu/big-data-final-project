# Hướng dẫn dựng cụm master/slave (2 phương án)

Cả hai đều mô phỏng cụm phân tán bằng nhiều container Docker trên 1 máy.
Mỗi container = 1 node. Chạy được trên laptop 16GB.

| | Phương án A | Phương án B |
|---|---|---|
| File | `docker-compose.spark.yml` | `docker-compose.hadoop.yml` |
| Phân tán | Spark (master + 2 worker) | HDFS + YARN + Spark |
| Lưu trữ | volume Docker | HDFS thật |
| RAM | ~7GB | ~9–10GB |
| Khớp với thầy dạy (Hadoop) | Một phần | Hoàn toàn |

> Chỉ chạy MỘT phương án tại một thời điểm (trùng cổng 8080/7077).
> Trước khi chạy, nhớ đã có dữ liệu: `cd data_generator && python prepare_from_kaggle.py`.

---

## PHƯƠNG ÁN A — Cụm Spark Standalone

### Chạy
```bash
docker compose -f docker-compose.spark.yml up -d
docker compose -f docker-compose.spark.yml ps
```

### Kiểm chứng phân tán (để chụp đưa vào báo cáo)
1. Mở **http://localhost:8080** (Spark Master UI) → mục *Workers* phải thấy
   `spark-worker-1` và `spark-worker-2` trạng thái ALIVE.
2. Xác nhận bằng dòng lệnh:
   ```bash
   docker exec spark-master jps        # thấy tiến trình Master
   docker exec spark-worker-1 jps      # thấy tiến trình Worker
   ```

### Chạy job phân tán
```bash
docker exec -e DATA_DIR=/data spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/work/spark_analysis.py
```
Trong lúc chạy, mở lại UID 8080 → mục *Running Applications* sẽ thấy job
`bao-cao-kinh-doanh` đang chia task cho 2 worker. Đây chính là bằng chứng phân tán.

### Tắt
```bash
docker compose -f docker-compose.spark.yml down
```

---

## PHƯƠNG ÁN B — Cụm Hadoop đầy đủ + Spark

### Bước 1 — bật cụm
```bash
docker compose -f docker-compose.hadoop.yml up -d
```
Lần đầu tải image bde2020 khá lâu. Chờ ~2 phút cho các node bắt tay nhau.

### Bước 2 — kiểm chứng phân tán
- **http://localhost:9870** → tab *Datanodes*: phải thấy `datanode1`, `datanode2`.
- **http://localhost:8088** → *Nodes*: thấy `nodemanager1` (RUNNING).
- **http://localhost:8080** → 2 Spark worker ALIVE.
- Dòng lệnh:
  ```bash
  docker exec namenode jps          # NameNode
  docker exec datanode1 jps         # DataNode
  docker exec resourcemanager jps   # ResourceManager
  docker exec nodemanager1 jps      # NodeManager
  ```

### Bước 3 — nạp dữ liệu vào HDFS
```bash
docker exec namenode hdfs dfs -mkdir -p /input
docker exec namenode hdfs dfs -put -f /data/erp_sales.csv /input/
docker exec namenode hdfs dfs -ls /input          # kiểm tra
```
Vào lại http://localhost:9870 tab *Utilities → Browse the file system* sẽ thấy file,
và nó được chia khối nhân bản trên 2 DataNode (replication=2).

### Bước 4 — chạy Spark đọc HDFS
```bash
docker exec -e DATA_DIR=hdfs://namenode:9000/input spark-master \
  /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/spark_analysis.py
```
Job đọc dữ liệu từ HDFS (phân tán) và tính trên Spark worker (phân tán).

### Tắt
```bash
docker compose -f docker-compose.hadoop.yml down -v
```

---

## Ghi chú RAM (quan trọng với 16GB)

- **Phương án A** gồm cả NiFi + Kafka nên chạy thẳng end-to-end được.
- **Phương án B** chỉ gồm cụm Hadoop+Spark. Phần thu thập (NiFi/Kafka) chạy
  riêng bằng `docker-compose.yml`: bật NiFi đẩy dữ liệu xong thì `down`,
  rồi mới bật cụm Hadoop. Tránh bật cùng lúc kẻo hết RAM.
- Nếu vẫn nặng: trong `docker-compose.hadoop.yml` bỏ bớt `datanode2` và
  `spark-worker-2` (chỉ còn 1 slave mỗi loại) — vẫn đủ minh họa master/slave.

## Lỗi thường gặp

- **Image bde2020 kéo chậm/timeout**: chạy lại `docker compose ... up -d`,
  Docker sẽ tiếp tục tải phần còn thiếu.
- **Worker không đăng ký với master**: thường do master chưa sẵn sàng; đợi
  10–20 giây rồi `docker compose ... restart spark-worker-1 spark-worker-2`.
- **spark-submit không thấy gói Kafka (PA B)**: bản bde2020 không tự tải;
  với phần đọc Kafka hãy dùng Phương án A hoặc thêm `--packages` có mạng.
- **Cổng trùng**: đảm bảo đã `down` phương án kia trước khi bật phương án mới.
