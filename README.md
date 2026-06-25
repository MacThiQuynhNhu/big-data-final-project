# Tổng hợp báo cáo kinh doanh đa nguồn

Đồ án Big Data: thu thập dữ liệu bán hàng từ **nhiều nguồn** (POS, ERP, CRM, Kho) với
nhiều **định dạng/cách kết nối** (file JSON, database, API, Excel), làm sạch và chuẩn hóa
bằng **Apache NiFi**, truyền qua **Kafka**, lưu trữ phân tán trên **HDFS/Hive**, rồi phân
tích bằng **Spark SQL** và **Spark MLlib** trên cụm Hadoop 3 node.

## Kiến trúc (Lambda)

```
                         ┌─→ Kafka → Spark Streaming → Cảnh báo real-time   (speed layer)
  4 nguồn → Apache NiFi ─┤
  (POS/ERP/             └─→ HDFS → Hive → Spark SQL + MLlib → Báo cáo       (batch layer)
   Kho/CRM)
```

- **Nhánh batch**: gom dữ liệu → HDFS → bảng Hive phân vùng (nguồn/tháng) → Spark SQL ra
  báo cáo doanh thu/chi phí/lợi nhuận/tồn kho + Spark MLlib dự báo & phân cụm.
- **Nhánh tốc độ**: giao dịch đẩy qua Kafka → Spark Structured Streaming phát hiện đơn
  lỗ nặng và cảnh báo tức thì.

Sơ đồ chi tiết: `kien_truc_lambda.svg` (xem `kien_truc_lambda.png`).

## Công nghệ

| Tầng | Công nghệ |
|------|-----------|
| Thu thập / ETL | Apache NiFi 1.28 |
| Message bus | Apache Kafka 3.7 (KRaft) |
| Lưu trữ phân tán | HDFS (Hadoop 3.3) |
| Kho dữ liệu | Hive (qua Spark Hive support) |
| Xử lý / ML | Apache Spark 3.1.1 (SQL + MLlib + Structured Streaming) |
| Database nguồn | PostgreSQL (đóng vai ERP) |
| Hạ tầng | Cụm 3 node (1 master + 2 slave) trên VMware |

## Nguồn dữ liệu

Dùng dataset **Superstore** (Kaggle, ~9994 giao dịch thật), tách thành 4 nguồn mô phỏng
4 hệ thống phòng ban:

| Nguồn | Định dạng | Cách NiFi đọc | Nội dung |
|-------|-----------|---------------|----------|
| POS | JSON (file) | GetFile | giao dịch: sản phẩm, qty, doanh thu, khuyến mãi |
| ERP | Database | QueryDatabaseTable | tài chính: doanh thu, chi phí, vùng |
| Kho | Excel | GetFile + ConvertExcelToCSV | tồn kho theo cửa hàng/sản phẩm |
| CRM | API (HTTP) | InvokeHTTP | khách hàng: tên, phân khúc |

## Cấu trúc thư mục

```
.
├── data_generator/
│   ├── prepare_from_kaggle.py   # tạo 4 nguồn từ Superstore
│   ├── inspect_data.py          # soi nhanh / thống kê data
│   └── load_erp.py              # nạp ERP vào PostgreSQL
├── notebooks/
│   ├── spark_to_hive.py         # HDFS → Hive (bảng phân vùng) — nhánh batch
│   ├── spark_report_hive.py     # Hive → báo cáo SQL (doanh thu/chi phí/tồn kho)
│   ├── spark_analysis.py        # Spark MLlib: dự báo (LinearRegression), phân cụm (KMeans)
│   └── spark_stream_alert.py    # Spark Streaming: cảnh báo giao dịch lỗ real-time
├── HUONG_DAN_VM.md              # dựng cụm Hadoop 3 node trên VMware
├── HUONG_DAN_CHAY_BATCH.md      # runbook chạy luồng batch (clear + chạy lại từ đầu)
└── kien_truc_lambda.svg/.png    # sơ đồ kiến trúc
```

## Cách chạy

### 1. Chuẩn bị dữ liệu
```bash
cd data_generator
pip install -r requirements.txt
# Tải Superstore -> data/raw/superstore.csv  (xem prepare_from_kaggle.py)
python prepare_from_kaggle.py        # tạo 4 nguồn trong data/
```

### 2. Dựng cụm Hadoop + cài NiFi/Kafka/Postgres
Xem `HUONG_DAN_VM.md` — hướng dẫn dựng cụm Hadoop 3 node + cài NiFi/Kafka/Postgres.

### 3. Thu thập (NiFi)
Dựng flow NiFi 4 nhánh: thu thập → ReplaceText (làm sạch) → ConvertRecord (chuẩn schema)
→ RouteOnAttribute (lọc thiếu) → UpdateAttribute (gắn nguồn/thời gian) → PublishKafka + PutHDFS.

### 4. Batch: nạp Hive + báo cáo
```bash
# HDFS → Hive (nhánh batch)
spark-submit --master yarn notebooks/spark_to_hive.py
# Hive → báo cáo
spark-submit --master yarn notebooks/spark_report_hive.py
# MLlib (đọc từ Hive)
spark-submit --master yarn notebooks/spark_analysis.py
```

### 5. Streaming: cảnh báo real-time
```bash
# Terminal 1: lắng nghe Kafka
spark-submit --master local[2] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 \
  notebooks/spark_stream_alert.py
# Terminal 2: bơm 1 giao dịch lỗ -> cảnh báo bật ngay
echo '{"source":"erp","txn_id":"TEST","store_id":"Texas","region":"Central","revenue":100,"cost":600,"txn_date":"2026-06-24"}' \
  | ~/kafka_2.13-3.7.1/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic sales-report-clean
```

## Kết quả (báo cáo)

- Doanh thu / chi phí / lợi nhuận theo cửa hàng & tháng
- Lợi nhuận theo khu vực, top sản phẩm bán chạy
- Cảnh báo hàng tồn dưới ngưỡng tái đặt
- Gộp đa nguồn: lợi nhuận theo sản phẩm (JOIN POS + ERP)
- Dự báo doanh thu tháng tới, phân cụm cửa hàng theo hành vi
- Cảnh báo giao dịch lỗ nặng theo thời gian thực
