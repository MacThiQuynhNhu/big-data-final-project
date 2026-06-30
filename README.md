# Tổng hợp báo cáo kinh doanh đa nguồn (Omnichannel)

Đồ án Big Data: thu thập dữ liệu bán lẻ từ **5 hệ thống** theo mô hình **đa kênh** (offline +
online), làm sạch bằng **NiFi**, truyền qua **Kafka**, lưu **HDFS/Hive**, phân tích bằng
**Spark SQL + MLlib**, trực quan hóa trên **Grafana** — chạy trên cụm Hadoop 3 node (VMware).
Kiến trúc **Lambda** (batch + speed layer).

Dữ liệu nền: danh mục sản phẩm/cửa hàng từ dataset **Superstore (Kaggle)**;
`data_generator/source_feeder.py` sinh giao dịch **LIVE** liên tục cho cả 5 nguồn, mô phỏng
đúng cách mỗi hệ thống thật phát sinh dữ liệu (API / database).

## Kiến trúc

```
                                         ┌─→ Spark Streaming → rt_* → PostgreSQL ─┐  (speed layer)
 5 nguồn → NiFi → Kafka ──────────────┤                                         ├─→ Grafana
 POS·ERP·Ecom·Kho·CRM   (làm sạch)     └─→ HDFS /lake → Hive → Spark SQL+MLlib ──┘  (batch layer)
                                              (cron 15') → bc_*/agg_* → PostgreSQL
```

- **Batch layer:** ConsumeKafka đổ dữ liệu xuống HDFS `/lake` → `spark_to_hive` nạp Hive →
  cuộn ngày/tuần/tháng/quý/năm + báo cáo SQL + MLlib → đẩy kết quả sang PostgreSQL cho Grafana.
  Lập lịch tự động bằng **cron mỗi 15 phút** (`run_batch.sh`).
- **Speed layer:** Spark Structured Streaming đọc thẳng Kafka, cập nhật `rt_thongke`/`rt_canhbao`
  mỗi vài giây (cảnh báo giao dịch lỗ real-time).

Sơ đồ chi tiết: [`kien_truc_chuan.png`](kien_truc_chuan.png).

## 5 nguồn dữ liệu

| Nguồn | Kênh | Định dạng | NiFi đọc bằng | Vai trò |
|---|---|---|---|---|
| **POS** | offline | API HTTP (push) | ListenHTTP `:9998` | Hóa đơn nhiều dòng — máy bán hàng đẩy real-time |
| **ERP** | offline + online | Postgres `sales` | QueryDatabaseTable | **Tài chính cả 2 kênh** (revenue, COGS, cột `kenh`) |
| **E-commerce** | online | Postgres `ecommerce_orders` | QueryDatabaseTable | Đơn web/app (khách, thiết bị, thanh toán) |
| **Kho/WMS** | — | Postgres `kho_chuyendong` | QueryDatabaseTable | Kho tổng, tồn = SUM(qty) chuyển động (event-sourcing) |
| **CRM** | — | API HTTP | InvokeHTTP `:8000` | Khách hàng + phân khúc (dimension) |

> **ERP là hệ tài chính của CẢ hai kênh.** POS (offline) và E-commerce (online) là hệ *vận hành*
> (không giữ giá vốn); mỗi giao dịch của chúng được ghi nhận tài chính trong ERP (revenue/COGS/
> `kenh`, nối qua `txn_id`) → báo cáo doanh thu/lợi nhuận theo kênh lấy từ ERP, tránh đếm trùng.
> Dimension `san_pham` (product_id, **unit_cost/giá vốn**, reorder_level) nối **kho ↔ tài chính**.

## Công nghệ (phiên bản thật trên cụm)

| Tầng | Công nghệ | Phiên bản |
|---|---|---|
| Thu thập / ETL | Apache NiFi | 1.28.1 |
| Message bus | Apache Kafka (KRaft) | 3.7.1 |
| Lưu trữ phân tán | HDFS (Hadoop) | 3.3.0 |
| Kho dữ liệu | Apache Hive (metastore MySQL) | 2.3.x |
| Xử lý / ML / Streaming | Apache Spark | **3.1.1** |
| Database nguồn + serving | PostgreSQL | 14 |
| Trực quan hóa | Grafana | 10.x |
| Điều phối | Bash + Cron (15') | — |
| Java | OpenJDK | 8 (batch ép java-8) |

## Cấu trúc thư mục

```
.
├── data_generator/
│   ├── setup_db.sql              # tạo bảng nguồn Postgres (sales, ecommerce_orders, kho_chuyendong, san_pham)
│   └── source_feeder.py          # sinh giao dịch LIVE cho cả 5 nguồn
├── notebooks/                    # các job Spark
│   ├── spark_to_hive.py          # [1] HDFS /lake → Hive (incremental ingest + archive)
│   ├── spark_incremental.py      # [2] cuộn ngày→tuần/tháng→quý/năm (agg_*, watermark)
│   ├── spark_report_hive.py      # [3] báo cáo SQL (bc_*)
│   ├── spark_analysis.py         # [4] MLlib: dự báo, phân cụm, kế hoạch nhập
│   ├── spark_marts_to_pg.py      # [5] đẩy bc_*/agg_* sang PostgreSQL cho Grafana
│   ├── spark_stream_dashboard.py # speed layer → rt_* (PostgreSQL/Grafana)
│   └── spark_stream_alert.py     # speed layer bản console (demo cảnh báo)
├── start_all.sh                  # bật toàn bộ stack
├── run_batch.sh                  # 1 lượt batch 5 bước (cron 15', có flock)
├── reset_all.sh                  # reset sạch, sinh lại từ 2026-07-01
├── thu-thap-da-nguon.xml         # NiFi flow template (5 processor)
├── generate_v2.py · gen_report_images.py   # sinh báo cáo Word + ảnh minh chứng
├── kien_truc_chuan.svg/.png      # sơ đồ kiến trúc
└── docs/                         # tài liệu chi tiết (xem dưới)
```

## Bắt đầu nhanh (Quick Start)

**A. Chỉ muốn xem kết quả** (cụm đang chạy): mở Grafana `http://192.168.79.131:3000`
(admin/admin) — dashboard *sales-report-batch* (phân tích) và *sales-report* (streaming).
Chi tiết đọc kết quả: [`docs/RESULTS.md`](docs/RESULTS.md).

**B. Bật toàn bộ hệ thống** (đã dựng cụm):
```bash
ssh hduser@192.168.79.131
cd ~/big-data-final-project && bash start_all.sh      # HDFS, YARN, Kafka, NiFi, feeder, streaming
# Vào NiFi UI Start tất cả processor; cài cron để run_batch chạy mỗi 15'
```

**C. Chạy 1 lượt batch thủ công:**
```bash
bash run_batch.sh        # spark_to_hive → incremental → report → analysis → marts_to_pg
```

**D. Reset sạch, sinh lại từ đầu:** `bash reset_all.sh` (xem cảnh báo trong script).

## Web UI

| Dịch vụ | URL |
|---|---|
| HDFS NameNode | http://192.168.79.131:9870 |
| YARN ResourceManager | http://192.168.79.131:8088 |
| NiFi | https://192.168.79.131:8443/nifi |
| Grafana | http://192.168.79.131:3000 |

## Tài liệu chi tiết (`docs/`)

| File | Nội dung |
|---|---|
| [docs/SETUP.md](docs/SETUP.md) | Dựng cụm Hadoop 3 node + cài NiFi/Kafka/Hive/PostgreSQL/Grafana |
| [docs/PIPELINE.md](docs/PIPELINE.md) | Cách vận hành: cron tự động, chạy demo thủ công, streaming, quản lý RAM, lỗi thường gặp |
| [docs/NIFI_FLOWS.md](docs/NIFI_FLOWS.md) | 5 luồng NiFi (ListenHTTP/QueryDatabaseTable/InvokeHTTP + Jolt) |
| [docs/RESULTS.md](docs/RESULTS.md) | Cách đọc kết quả: 2 dashboard Grafana + các bảng mart + chỉ số |
| [docs/CHUP_ANH_BAO_CAO.md](docs/CHUP_ANH_BAO_CAO.md) | (Nội bộ) checklist chụp ảnh minh chứng cho báo cáo Word |

Báo cáo cuối kỳ: **`BAO_CAO_BIGDATA_v2.docx`** (sinh bằng `python generate_v2.py`).
