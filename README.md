# Tổng hợp báo cáo kinh doanh đa nguồn (omnichannel)

Đồ án Big Data: thu thập dữ liệu bán hàng của một chuỗi bán lẻ từ **5 hệ thống** theo mô
hình **đa kênh (omnichannel)** — bán **offline** (POS tại cửa hàng, ERP tài chính) và bán
**online** (thương mại điện tử: website + app) — cùng **Kho (WMS)** và **CRM**. Mỗi nguồn
nối theo đúng cách hệ thống thật phát sinh dữ liệu (file JSON, database, API). Dữ liệu được
làm sạch/chuẩn hóa bằng **Apache NiFi**, truyền qua **Kafka**, lưu phân tán trên
**HDFS/Hive**, rồi phân tích bằng **Spark SQL** và **Spark MLlib** trên cụm Hadoop 3 node.

## Kiến trúc (Lambda)

```
                         ┌─→ Kafka → Spark Streaming → Cảnh báo real-time   (speed layer)
  5 nguồn → Apache NiFi ─┤
  (POS/ERP/E-commerce/  └─→ HDFS → Hive → Spark SQL + MLlib → Báo cáo       (batch layer)
   Kho/CRM)
```

- **Nhánh batch**: gom dữ liệu → HDFS → bảng Hive phân vùng (nguồn/tháng) → Spark SQL ra
  báo cáo doanh thu/chi phí/lợi nhuận/tồn kho + Spark MLlib dự báo & phân cụm.
- **Nhánh tốc độ**: giao dịch đẩy qua Kafka → Spark Structured Streaming phát hiện đơn
  lỗ nặng và cảnh báo tức thì.

Sơ đồ chi tiết: `kien_truc_chuan.svg` (xem `kien_truc_chuan.png`).

## Công nghệ

| Tầng | Công nghệ |
|------|-----------|
| Thu thập / ETL | Apache NiFi 1.28 |
| Message bus | Apache Kafka 3.7 (KRaft) |
| Lưu trữ phân tán | HDFS (Hadoop 3.3) |
| Kho dữ liệu | Hive (qua Spark Hive support) |
| Xử lý / ML | Apache Spark 3.1.1 (SQL + MLlib + Structured Streaming) |
| Database nguồn | PostgreSQL (ERP + E-commerce + Kho/WMS) |
| Hạ tầng | Cụm 3 node (1 master + 2 slave) trên VMware |

## Nguồn dữ liệu

Dữ liệu phát sinh **liên tục** bởi `data_generator/source_feeder.py` (mô phỏng giao dịch
live đổ về từng nguồn), dựa trên danh mục sản phẩm/cửa hàng của dataset **Superstore**
(Kaggle). 5 nguồn map đúng hệ thống thật của một chuỗi bán lẻ:

| Nguồn | Kênh | Định dạng | Cách NiFi đọc | Nội dung |
|-------|------|-----------|---------------|----------|
| POS | offline | API (HTTP) | ListenHTTP | **hóa đơn** nhiều dòng: sản phẩm, qty, giá bán (máy bán hàng push) |
| ERP | offline+online | Database | QueryDatabaseTable (incremental) | tài chính **cả 2 kênh** theo dòng: doanh thu, **COGS**, vùng, `kenh` |
| E-commerce | online | Database | QueryDatabaseTable (incremental) | đơn web/app (vận hành): khách, thiết bị, thanh toán |
| Kho (WMS) | — | Database | QueryDatabaseTable (incremental) | **kho tổng**: chuyển động nhập/xuất + tiền (tồn = số dư) |
| CRM | — | API (HTTP) | InvokeHTTP | khách hàng: tên, phân khúc (động) |

> **Dimension:** bảng `san_pham` (product_id, **unit_cost/giá vốn**, reorder_level) — nối
> **kho ↔ tài chính**: mọi COGS và giá trị nhập/tồn kho đều tính từ giá vốn này.

> **Vì sao mapping này thực tế?** Mọi nguồn đều là *api/database* (không file/dump tĩnh):
> ERP/WMS/E-commerce bản chất là *database*; POS và CRM cung cấp *API HTTP* (máy bán hàng
> chi nhánh **push** giao dịch real-time; CRM phục vụ danh sách khách). POS/ERP là kênh
> offline, E-commerce là kênh online → phân tích doanh thu theo **kênh**.
> **Vận hành vs tài chính** (nối bằng `txn_id`): POS (offline) và E-commerce (online) là hệ
> **vận hành** (bán gì, cho ai) — KHÔNG giữ giá vốn. **ERP là hệ tài chính của CẢ HAI kênh**:
> mỗi giao dịch offline (từ POS) và online (từ E-commerce) đều được ghi nhận tài chính trong
> ERP (revenue/COGS/`kenh`), tổng hợp **báo cáo theo ngày** ở tầng batch → báo cáo doanh thu/
> lợi nhuận **theo kênh** lấy từ ERP. Tồn kho tính real-time từ **sự kiện chuyển động** vào
> **kho tổng** (event-sourcing); giá vốn từ `san_pham` cho **giá trị tồn kho** và **COGS** —
> đúng cách một WMS + ERP vận hành.

## Cấu trúc thư mục

```
.
├── data_generator/
│   ├── setup_db.sql             # tạo bảng nguồn: sales, ecommerce_orders, kho_chuyendong
│   └── source_feeder.py         # sinh dữ liệu LIVE cho CẢ 5 nguồn (POS/ERP/Ecom/Kho/CRM)
├── notebooks/
│   ├── spark_to_hive.py         # HDFS → Hive (bảng phân vùng) — nhánh batch
│   ├── spark_report_hive.py     # Hive → báo cáo SQL (doanh thu/chi phí/tồn kho)
│   ├── spark_analysis.py        # Spark MLlib: dự báo, phân cụm + kế hoạch nhập hàng
│   ├── spark_stream_alert.py    # Spark Streaming: cảnh báo giao dịch lỗ real-time
│   └── spark_stream_dashboard.py # Spark Streaming → PostgreSQL (cho Grafana)
├── HUONG_DAN_VM.md              # dựng cụm Hadoop 3 node trên VMware
├── HUONG_DAN_CHAY_BATCH.md      # runbook chạy luồng batch (clear + chạy lại từ đầu)
├── HUONG_DAN_DASHBOARD.md       # runbook dashboard real-time (Grafana)
├── HUONG_DAN_POS_INVOICE.md     # luồng NiFi POS (ListenHTTP + Jolt tách dòng hóa đơn)
├── HUONG_DAN_ECOMMERCE.md       # luồng NiFi E-commerce (QueryDatabaseTable + Jolt)
└── kien_truc_chuan.svg/.png     # sơ đồ kiến trúc thống nhất (chuẩn)
```

## Cách chạy

### 1. Chuẩn bị dữ liệu (sinh LIVE)
```bash
cd data_generator
pip3 install -r requirements.txt
PGPASSWORD=erp123 psql -h localhost -U erp -d erp -f setup_db.sql   # tạo bảng nguồn (1 lần)
python3 source_feeder.py             # đổ dữ liệu LIVE cho cả 5 nguồn (Ctrl+C để dừng)
```
> Không còn nguồn tĩnh: mọi nguồn (POS file, ERP/Ecom/Kho database, CRM api) đều do
> `source_feeder.py` sinh liên tục.

### 2. Dựng cụm Hadoop + cài NiFi/Kafka/Postgres
Xem `HUONG_DAN_VM.md` — hướng dẫn dựng cụm Hadoop 3 node + cài NiFi/Kafka/Postgres.

### 3. Thu thập (NiFi)
Dựng flow NiFi 5 nhánh: thu thập → ReplaceText (làm sạch) → ConvertRecord (chuẩn schema)
→ RouteOnAttribute (lọc thiếu) → UpdateAttribute (gắn nguồn/thời gian) → PublishKafka.
ConsumeKafka → PutHDFS (`/lake`) làm landing chung cho nhánh batch.
Luồng e-commerce (nguồn online) xem `HUONG_DAN_ECOMMERCE.md`.

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

- Doanh thu / chi phí / lợi nhuận theo cửa hàng & tháng (ERP, COGS từ giá vốn)
- Lợi nhuận theo khu vực & **theo sản phẩm**, top sản phẩm bán chạy (cả 2 kênh)
- **Doanh thu/lợi nhuận theo kênh (online vs offline)** + đơn online theo thiết bị/thanh toán
- **Doanh thu theo phân khúc khách** (E-commerce × CRM)
- Cảnh báo hàng tồn **kho tổng** dưới ngưỡng tái đặt + **giá trị tồn kho** (tồn × giá vốn)
- **Snapshot tồn kho theo ngày** (lưu lịch sử để cuộn ngày → tuần → tháng)
- Dự báo doanh thu tháng tới, phân cụm cửa hàng theo hành vi
- **Dự báo nhu cầu sản phẩm → KẾ HOẠCH NHẬP HÀNG** (đề xuất số lượng nhập + chi phí dự kiến)
- Cảnh báo giao dịch lỗ nặng theo thời gian thực
