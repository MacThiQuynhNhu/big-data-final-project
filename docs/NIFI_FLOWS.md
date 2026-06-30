# NIFI_FLOWS — 5 luồng thu thập

NiFi thu thập từ 5 nguồn bằng processor phù hợp với từng cách hệ thống phát sinh dữ liệu, làm
sạch/chuẩn hóa về schema chung, rồi đẩy vào Kafka (batch + speed dùng chung). Flow lưu trong
`thu-thap-da-nguon.xml` (import vào NiFi) — UI: `https://192.168.79.131:8443/nifi`.

```
POS        (api)  ListenHTTP :9998      → Jolt (tách dòng + source=pos)   ─┐
ERP        (db)   QueryDatabaseTable    → ConvertRecord → SplitJson        ─┤
E-commerce (db)   QueryDatabaseTable    → ConvertRecord → SplitJson        ─┼→ Kafka: sales-report-clean
                    → Jolt (đổi tên + source=ecommerce)                     │        │
Kho/WMS    (db)   QueryDatabaseTable    → ConvertRecord → SplitJson        ─┘        └→ ConsumeKafka → HDFS /lake
                                                                          ──→ Kafka: inventory-events
CRM        (api)  InvokeHTTP (định kỳ)  → HDFS /clean/crm   (dimension, KHÔNG qua Kafka)
```

Quy ước chung: trường **`source`** (`pos`/`erp`/`ecommerce`) phải nằm TRONG nội dung JSON (Spark
đọc bằng `from_json`, phân biệt nguồn/kênh theo trường này). 3 nguồn bán hàng đổ chung topic
`sales-report-clean`; ConsumeKafka → PutHDFS landing tại `/lake/transactions` (batch).

---

## 1. POS — ListenHTTP + Jolt (tách dòng hóa đơn)
Máy bán hàng **push 1 hóa đơn** (nhiều dòng) lên `http://localhost:9998/pos`. Hóa đơn feeder gửi:
```json
{ "invoice_id":"INV000000001", "store_id":"California", "region":"West", "Ngay":"2026-06-26",
  "lines":[ {"txn_id":"L000000001","product_id":"TEC-CO-10004722","qty":2,"Doanh_Thu":1200,"promotion":1} ] }
```
- **ListenHTTP** — Listening Port `9998`, Base Path `pos` → mỗi POST = 1 FlowFile (nguyên hóa đơn).
- **JoltTransformJSON** — tách hóa đơn lồng nhau thành **mảng dòng phẳng**, đưa header vào từng
  dòng, đổi tên cột bẩn (`Doanh_Thu`→`revenue`, `Ngay`→`txn_date`), gắn `source=pos`:
```json
[
  { "operation":"shift", "spec": { "lines": { "*": {
        "txn_id":"[&1].txn_id", "product_id":"[&1].product_id", "qty":"[&1].qty",
        "Doanh_Thu":"[&1].revenue", "promotion":"[&1].promotion",
        "@(2,invoice_id)":"[&1].invoice_id", "@(2,store_id)":"[&1].store_id",
        "@(2,region)":"[&1].region", "@(2,Ngay)":"[&1].txn_date" } } } },
  { "operation":"default", "spec": { "*": { "source":"pos" } } }
]
```
- **SplitJson** (`$.*`) → **PublishKafka** (`sales-report-clean`).
> POS chỉ giữ **giá bán** (revenue), KHÔNG có giá vốn — đúng thực tế (máy bán hàng không biết
> COGS). Giá vốn nằm ở ERP/`san_pham`. `txn_id` trùng giữa POS↔ERP để join; `invoice_id` gom giỏ.

---

## 2. ERP — QueryDatabaseTable (tài chính cả 2 kênh)
Feeder ghi MỌI giao dịch (offline từ POS + online từ E-commerce) vào Postgres `sales` kèm
`revenue`, `cost`, `kenh`.
- **QueryDatabaseTable** — Table `sales`, Maximum-value Columns `id` (đọc incremental dòng mới).
- **ConvertRecord** (Avro→JSON) → **SplitJson** (`$.*`) → **PublishKafka** (`sales-report-clean`).
- Gắn `source=erp` (UpdateAttribute hoặc Jolt default).
> ERP là **hệ tài chính của cả 2 kênh** → báo cáo doanh thu/lợi nhuận theo `kenh` lấy từ đây.

---

## 3. E-commerce — QueryDatabaseTable + Jolt (đổi tên)
Đơn online trong Postgres `ecommerce_orders` (có `device`, `payment_method`, KHÔNG có `cost`).
- **QueryDatabaseTable** — Table `ecommerce_orders`, Max-value `id`. → **ConvertRecord** → **SplitJson**.
- **JoltTransformJSON** — đổi `order_id`→`txn_id`, `order_date`→`txn_date`, gắn `source=ecommerce`:
```json
[
  { "operation":"shift", "spec": {
      "order_id":"txn_id", "customer_id":"customer_id", "region":"region", "product_id":"product_id",
      "qty":"qty", "revenue":"revenue", "device":"device", "payment_method":"payment_method",
      "order_date":"txn_date" } },
  { "operation":"default", "spec": { "source":"ecommerce" } }
]
```
- **PublishKafka** (`sales-report-clean`).
> E-commerce = **vận hành online** (đơn, khách, thiết bị), KHÔNG giữ `cost`. Tài chính đơn online
> nằm ở ERP (feeder ghi vào `sales` với `kenh='online'`, link qua `txn_id = order_id`).

---

## 4. Kho/WMS — QueryDatabaseTable → topic riêng
Chuyển động nhập/xuất trong Postgres `kho_chuyendong` (event-sourcing: tồn = SUM(qty)).
- **QueryDatabaseTable** — Table `kho_chuyendong`, Max-value `id`. → **ConvertRecord** → **SplitJson**.
- **PublishKafka** → topic **`inventory-events`** (riêng với bán hàng).
- ConsumeKafka → PutHDFS `/lake/inventory`. `spark_to_hive` tính tồn kho + giá trị tồn (× giá vốn).

---

## 5. CRM — InvokeHTTP (dimension)
Danh sách khách hàng + phân khúc, phục vụ qua HTTP (`python3 -m http.server 8000` trong `data/`).
- **InvokeHTTP** — gọi API CRM định kỳ → ghi thẳng HDFS **`/clean/crm`** (KHÔNG qua Kafka, vì là
  dimension ít thay đổi). `spark_to_hive` đọc tạo `dim_khachhang` (join với đơn online theo
  `customer_id` → báo cáo doanh thu theo phân khúc).

---

## Kết quả sau thu thập
`spark_to_hive.py` nạp `bao_cao.sales_report` (gộp 3 nguồn, có `source`/`kenh`), `inventory`
(từ `inventory-events`), `dim_khachhang` (từ `/clean/crm`). Kiểm nhanh không cần chạy lại Spark:
```bash
hdfs dfs -ls /lake/transactions /lake/inventory /clean/crm
spark-sql -e "SELECT source, COUNT(*) FROM bao_cao.sales_report GROUP BY source;"
```
