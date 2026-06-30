# CLAUDE.md

Hướng dẫn cho Claude Code khi làm việc trong repo này. **Viết phản hồi bằng tiếng Việt.**

## Đồ án là gì

Đồ án cuối kỳ Big Data (cao học MTA): **"Tổng hợp báo cáo kinh doanh đa nguồn (omnichannel)"**.
Thu thập dữ liệu bán lẻ từ **5 hệ thống** (POS, ERP, E-commerce, Kho/WMS, CRM) theo mô hình
đa kênh online/offline → làm sạch bằng **NiFi** → **Kafka** → lưu **HDFS/Hive** → phân tích
**Spark SQL + MLlib** trên cụm Hadoop 3 node. Kiến trúc **Lambda** (batch + speed layer).

Dữ liệu nền: dataset **Superstore (Kaggle)**; `source_feeder.py` sinh giao dịch LIVE liên tục
cho cả 5 nguồn. Người dùng là **sinh viên** (giỏi Python + SQL), không có hệ thống ERP/CRM thật
nên mọi nguồn được **giả lập đúng cách hệ thống thật phát sinh dữ liệu** (file/database/API).

## Môi trường: code ở Windows, CHẠY trên VM

- **Repo gốc (nơi Claude sửa file):** `D:\Studying\MTA-Master\year_1\BigData\big-data-final-project` (Windows, git).
- **Nơi chạy thật:** cụm VMware 3 node — `master` + `slave01` + `slave02`, user `hduser`.
- **Claude TRUY CẬP ĐƯỢC VM trực tiếp qua SSH key** (đã cài key, không cần mật khẩu):
  ```bash
  ssh hduser@192.168.79.131 "bash -lc '<lệnh>'"      # dùng bash -lc để có PATH Hadoop/Spark
  ```
  IP master: **192.168.79.131** (host-only ens38). Còn `10.0.2.195` (ens33/NAT) không tới được từ Windows.
- **Sync code lên VM** sau khi sửa:
  ```bash
  scp <file> hduser@192.168.79.131:/home/hduser/big-data-final-project/<path>
  ```
  Project trên VM ở `/home/hduser/big-data-final-project`.
- Web UI: HDFS `http://192.168.79.131:9870`, YARN `:8088`, NiFi `https://...:8443/nifi`,
  Grafana `:3000`.

## Phiên bản thật trên VM (đã verify)

| Thành phần | Phiên bản | Ghi chú |
|---|---|---|
| Spark | **3.1.1** (`/usr/local/spark`) | Log runtime xác nhận "Running Spark version 3.1.1"; package kafka `:3.1.1` khớp |
| Hadoop | **3.3.0** (`/usr/local/hadoop`) | HDFS replication=2, block 128MB |
| Java | mặc định **11**, nhưng batch ÉP **java-8** | `run_batch.sh` export `JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64` |
| Hive metastore | **MySQL** (`hivedb`) | warehouse HDFS `/user/hive/warehouse/bao_cao.db` (database `bao_cao`) |
| Kafka | 3.7.1 (KRaft) | topics `sales-report-clean`, `inventory-events` |
| NiFi | 1.28.1 | flow lưu ở `~/nifi-1.28.1/conf` |
| PostgreSQL | nguồn ERP/Ecom/Kho + marts cho Grafana | user `erp`/`erp123`, db `erp` |

## 5 nguồn dữ liệu

| Nguồn | Kênh | Định dạng | NiFi đọc | Vai trò |
|---|---|---|---|---|
| POS | offline | API HTTP | ListenHTTP | hóa đơn nhiều dòng (vận hành real-time) |
| ERP | offline+online | Postgres `sales` | QueryDatabaseTable | **tài chính cả 2 kênh** (revenue/COGS/`kenh`) |
| E-commerce | online | Postgres `ecommerce_orders` | QueryDatabaseTable | đơn web/app (khách, thiết bị, thanh toán) |
| Kho/WMS | — | Postgres `kho_chuyendong` | QueryDatabaseTable | kho tổng, tồn = SUM(qty) chuyển động |
| CRM | — | API HTTP | InvokeHTTP | khách hàng + phân khúc (dimension) |

Dimension `san_pham` (product_id, **unit_cost/giá vốn**, reorder_level) nối kho ↔ tài chính.

## Bản đồ file

- `data_generator/source_feeder.py` — sinh dữ liệu LIVE cho cả 5 nguồn. `setup_db.sql` — tạo bảng nguồn.
- `notebooks/spark_to_hive.py` — HDFS `/lake` → Hive `bao_cao` (sales_report, inventory, dim, snapshot). **Luồng batch chính.**
- `notebooks/spark_report_hive.py` — Hive → báo cáo `bc_*` (doanh thu/chi phí/tồn kho).
- `notebooks/spark_analysis.py` — MLlib: dự báo (LinearRegression), phân cụm (KMeans), kế hoạch nhập hàng.
- `notebooks/spark_incremental.py` — tổng hợp ngày/tuần/tháng/quý/năm, CHỈ kỳ mới đã đóng (`agg_*`, append).
- `notebooks/spark_marts_to_pg.py` — đẩy marts sang PostgreSQL cho Grafana.
- `notebooks/spark_stream_alert.py` — Streaming: cảnh báo giao dịch lỗ real-time. `spark_stream_dashboard.py` — Streaming → Postgres `rt_*`.
- **Scripts điều phối (chạy trên VM):** `start_all.sh` (bật tất cả service), `run_batch.sh` (1 lần batch, có flock, cron 15'), `reset_all.sh` (reset sạch, data từ 2026-07-01), `tune_ram.sh`.
- Runbook: `HUONG_DAN_VM.md`, `HUONG_DAN_CHAY_BATCH.md`, `HUONG_DAN_DASHBOARD.md`, `HUONG_DAN_POS_INVOICE.md`, `HUONG_DAN_ECOMMERCE.md`. Sơ đồ: `kien_truc_chuan.svg/.png`.

## Lệnh hay dùng (trên VM)

```bash
# Chạy 1 lượt batch (đúng môi trường cron)
ssh hduser@192.168.79.131 "bash -lc 'cd ~/big-data-final-project && bash run_batch.sh'"
# Xem log batch / feeder / streaming
ssh hduser@192.168.79.131 "tail -50 ~/batch_cron.log"
# Kiểm tra cụm
ssh hduser@192.168.79.131 "jps"
ssh hduser@192.168.79.131 "bash -lc 'hdfs dfs -ls /lake; hdfs dfs -ls /user/hive/warehouse/bao_cao.db'"
```

`run_batch.sh` chạy 5 bước: spark_to_hive → spark_report_hive → spark_analysis → spark_incremental → spark_marts_to_pg.
RAM nhỏ (executor 640m/driver 512m) để chạy chung với streaming + NiFi.

## Quy ước & cạm bẫy (ĐỌC trước khi sửa Spark)

- **JDBC chỉ chạy ở driver, KHÔNG ở executor.** Executor ở slave; `localhost:5432` trên slave trỏ nhầm
  (Postgres chỉ ở master). Vì vậy `spark_to_hive.py` hard-code `dim_sanpham` trong code thay vì đọc JDBC.
  Khi cần ghi Postgres → chạy `--master local[1]` ở master (xem `spark_marts_to_pg.py`).
- **Bất biến lịch sử:** `sales_report` là transaction fact (có `txn_date`) → cuộn ngày/tuần/tháng tự do, mart overwrite OK.
  Tồn kho là TRẠNG THÁI → phải **snapshot append** (`snapshot_tonkho`, dynamic `partitionOverwriteMode`+`insertInto`).
  `agg_*` incremental chỉ thêm **kỳ mới đã đóng**, kỳ cũ KHÔNG sửa. **Đừng bao giờ làm mất `/lake` + fact chi tiết.**
- **Spark ép java-8** qua `JAVA_HOME` (xem `run_batch.sh`); chạy tay phải export tương tự nếu không qua script.
- **ERP phủ cả 2 kênh:** báo cáo doanh thu/lợi nhuận theo kênh lấy từ ERP (có cả online+offline + COGS).
  Báo cáo theo cửa hàng lọc `kenh='offline'`; KMeans lọc `store_id NOT NULL`. Không double-count POS×ERP.
- Hive metastore (MySQL) và HDFS warehouse phải đồng bộ — nếu xóa 1 bên sẽ lỗi "location already exists" → dọn HDFS dir.

## Khi sửa code

1. Sửa file trong repo Windows. 2. `scp` lên VM. 3. Chạy/verify qua SSH. 4. Báo kết quả thật (log/output), không phỏng đoán.
Repo đã lên GitHub: https://github.com/MacThiQuynhNhu/big-data-final-project (branch `main`).
