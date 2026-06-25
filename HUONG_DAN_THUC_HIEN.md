# Hướng dẫn thực hiện: Tổng hợp báo cáo kinh doanh đa nguồn

Lộ trình end-to-end chạy hoàn toàn trên máy cá nhân (16GB RAM) bằng Docker.
Bạn không cần ERP/CRM/POS thật — tất cả nguồn được **giả lập** bằng dữ liệu sinh tự động.

## Kiến trúc

```
POS(JSON) ERP(Postgres) CRM(JSON) Kho(Excel)
            -> NiFi (thu thập, làm sạch, chuẩn hóa)
            -> Kafka (topic: sales-report-clean)
            -> Spark SQL (tổng hợp) + Spark MLlib (dự báo, phân cụm)
            -> HDFS/Postgres -> Dashboard
```

Xem sơ đồ: `kien_truc_bao_cao_kinh_doanh_da_nguon.svg`

---

## Yêu cầu cài đặt trước

1. **Docker Desktop** (Windows) — đây là thứ duy nhất phải cài thủ công.
   Tải tại docker.com, bật WSL2 backend khi được hỏi.
2. **Python 3.10+** (đã có sẵn, bạn dùng được).

---

## Giai đoạn 0 — Chuẩn bị dữ liệu (15 phút)

Có 2 cách. **Khuyến nghị cách A** (dữ liệu thật, báo cáo thuyết phục hơn).

### Cách A — Dataset thật từ Kaggle (Superstore)
```bash
cd data_generator
pip install -r requirements.txt
# Tải Sample - Superstore.csv -> đặt vào data/raw/superstore.csv
#   hoặc: kaggle datasets download -d vivek468/superstore-dataset-final -p ../data/raw --unzip
python prepare_from_kaggle.py                 # dùng nguyên ~10k dòng thật
python prepare_from_kaggle.py --target 200000 # gen thêm cho đủ "big data"
```
Map cột: revenue=Sales, cost=Sales−Profit, region=Region, store=State,
promotion=Discount>0. Tồn kho (inventory) được gen thêm vì dataset không có.

### Cách B — Sinh hoàn toàn giả (không cần tải gì)
```bash
cd data_generator
pip install -r requirements.txt
python generate.py
```

Cả hai cách đều cho ra 4 file trong `data/`: `pos_transactions.json`,
`erp_sales.csv`, `crm_customers.json`, `inventory.xlsx`.

---

## Giai đoạn 1 — Dựng hạ tầng bằng Docker (20 phút)

```bash
docker compose up -d
```
Lần đầu Docker tải image mất 10–15 phút. Kiểm tra:
```bash
docker compose ps
```
Các dịch vụ và cổng truy cập:
| Dịch vụ   | Địa chỉ                 | Đăng nhập              |
|-----------|-------------------------|------------------------|
| NiFi      | https://localhost:8443  | admin / bigdataproject2026 |
| Jupyter   | http://localhost:8888   | token: `bigdata`       |
| Postgres  | localhost:5432          | erp / erp123 (db: erp) |
| Kafka     | localhost:29092         | (từ host)              |

Nạp dữ liệu ERP vào Postgres:
```bash
cd data_generator
python load_erp.py
```

> Mẹo RAM: nếu máy nặng, có thể tắt tạm `nifi` lúc test Spark, hoặc ngược lại.

---

## Giai đoạn 2 — Test phần phân tích TRƯỚC (30 phút)

Làm phần dễ trước để có kết quả sớm, chưa cần NiFi/Kafka.

1. Mở http://localhost:8888 (token `bigdata`).
2. Tạo notebook mới, dán nội dung `notebooks/spark_analysis.py` (đang ở **chế độ A** — đọc thẳng CSV).
3. Chạy → bạn sẽ thấy ngay bảng tổng hợp doanh thu, dự báo Linear Regression, phân cụm KMeans.

Đạt được bước này nghĩa là Spark SQL + MLlib đã xong. Phần còn lại là nối ống dẫn.

---

## Giai đoạn 3 — NiFi: thu thập, làm sạch, biến đổi (khó nhất, 2–4 giờ)

Vào NiFi (https://localhost:8443, bỏ qua cảnh báo SSL). Dựng flow:

**Nhánh ERP (database):**
- `QueryDatabaseTable` → trỏ tới Postgres (tạo DBCPConnectionPool:
  URL `jdbc:postgresql://postgres:5432/erp`, driver `org.postgresql.Driver`,
  user `erp`, pass `erp123`). Bảng `sales`.

**Nhánh POS / Kho hàng (file):**
- `GetFile` → thư mục `/opt/nifi/nifi-current/ingest` (chính là `data/` của bạn).
- File Excel cần `ConvertExcelToCSVProcessor` trước khi xử lý tiếp.

**Làm sạch (chung):**
- `ReplaceText` → chuẩn hóa tên cột (vd `Doanh_Thu` → `revenue`, `Ngay` → `txn_date`).
- `RouteOnAttribute` → loại bản ghi thiếu trường bắt buộc.

**Biến đổi:**
- `ConvertRecord` → đưa tất cả về schema chuẩn:
  `store_id, txn_date, region, product_id, revenue, cost, profit`.
- `UpdateAttribute` → thêm `import_time = ${now()}` và `source = pos|erp|crm|kho`.

**Đẩy ra Kafka:**
- `PublishKafkaRecord_2_6` → broker `kafka:9092`, topic `sales-report-clean`.

> Đây là phần tốn thời gian nhất. Làm xong 1 nhánh (vd ERP) chạy thông
> trước, rồi nhân bản cho các nguồn khác.

---

## Giai đoạn 4 — Nối Spark vào Kafka (30 phút)

Trong notebook, chuyển sang **chế độ B** (bỏ comment khối Kafka, comment khối CSV).
Spark sẽ đọc topic `sales-report-clean` thay vì đọc file. Kết quả phân tích như cũ
nhưng giờ là luồng end-to-end thật.

---

## Giai đoạn 5 — Lưu trữ + Dashboard (1–2 giờ)

- Ghi kết quả tổng hợp xuống Postgres: `df_ketqua.write.jdbc(...)`.
- Trực quan hóa: cài **Apache Superset** (thêm 1 service vào compose) hoặc đơn giản
  dùng **Power BI Desktop** kết nối Postgres, hoặc vẽ biểu đồ ngay trong notebook
  bằng `matplotlib`/`pandas` (đủ cho báo cáo).

---

## Thứ tự ưu tiên nếu thiếu thời gian

1. Giai đoạn 0 + 2 (sinh dữ liệu + Spark SQL/MLlib) — **chiếm phần lớn điểm kỹ thuật**.
2. Giai đoạn 1 + 3 (Docker + NiFi) — thể hiện kỹ năng pipeline.
3. Giai đoạn 4 (Kafka) — chốt tính "end-to-end".
4. Giai đoạn 5 (dashboard) — điểm cộng trình bày.

## Lỗi thường gặp

- **NiFi không vào được**: chờ ~2 phút sau khi container chạy; dùng `https` không phải `http`.
- **Spark không đọc được Kafka**: thiếu gói `spark-sql-kafka` — đã khai báo trong
  `spark.jars.packages`, lần chạy đầu cần mạng để tải.
- **Kafka từ host vs trong Docker**: trong mạng Docker dùng `kafka:9092`,
  từ máy host dùng `localhost:29092`.
- **Hết RAM**: tắt bớt service không dùng (`docker compose stop nifi`).
