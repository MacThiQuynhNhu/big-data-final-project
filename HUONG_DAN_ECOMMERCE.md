# Thêm nguồn E-COMMERCE (kênh online) vào NiFi

Nguồn thứ 5 — thương mại điện tử (website + app). Đơn online nằm trong bảng PostgreSQL
`ecommerce_orders`, NiFi đọc **incremental** rồi đẩy vào Kafka (cùng topic với POS/ERP) để
chảy chung cả 2 nhánh speed + batch.

Luồng này **giống hệt luồng ERP** (đều `QueryDatabaseTable` trên Postgres), chỉ khác bảng
nguồn và bước đổi tên trường cho khớp schema chung.

> **E-commerce = VẬN HÀNH online** (đơn, khách, thiết bị, thanh toán), KHÔNG giữ `cost` —
> giống POS không giữ giá vốn. **Tài chính của đơn online nằm ở ERP**: feeder ghi mỗi đơn
> online vào bảng `sales` với `kenh='online'` (link qua `txn_id = order_id`). Nhờ vậy ERP là
> hệ tài chính của **cả hai kênh**; báo cáo doanh thu/lợi nhuận theo kênh lấy từ ERP. Luồng
> NiFi của ERP (`QueryDatabaseTable` trên `sales`) tự nhận luôn các dòng online này.

## 0. Chuẩn bị

```bash
# tạo bảng (1 lần)
PGPASSWORD=erp123 psql -h localhost -U erp -d erp -f data_generator/setup_db.sql
# chạy feeder để có đơn online đổ về liên tục
python3 data_generator/source_feeder.py
```

## 1. QueryDatabaseTable (đọc đơn online incremental)

| Thuộc tính | Giá trị |
|---|---|
| Database Connection Pooling Service | `DBCPConnectionPool` (Postgres erp — dùng lại của ERP) |
| Table Name | `ecommerce_orders` |
| Maximum-value Columns | `id` |

→ Mỗi lần chỉ lấy dòng `id` mới (near-real-time, như CDC-lite). Output: **Avro**.

## 2. ConvertRecord (Avro → JSON)

- Record Reader: **AvroReader**
- Record Writer: **JsonRecordSetWriter** (Output Grouping = *One Line Per Object* hoặc array)

## 3. SplitJson (tránh message Kafka > 1MB)

- JsonPath Expression: `$.*` (tách mảng thành từng đơn riêng)

## 4. JoltTransformJSON (đổi tên trường → schema chung)

Đơn online có `order_id`, `order_date`; schema chung dùng `txn_id`, `txn_date`, và cần
gắn `source=ecommerce`. Jolt Specification:

```json
[
  {
    "operation": "shift",
    "spec": {
      "order_id": "txn_id",
      "customer_id": "customer_id",
      "region": "region",
      "product_id": "product_id",
      "qty": "qty",
      "revenue": "revenue",
      "device": "device",
      "payment_method": "payment_method",
      "order_date": "txn_date"
    }
  },
  {
    "operation": "default",
    "spec": { "source": "ecommerce" }
  }
]
```

> `source` phải nằm TRONG nội dung JSON (không phải attribute) vì Spark đọc bằng
> `from_json` và phân biệt nguồn/kênh theo trường này.

## 5. PublishKafka

| Thuộc tính | Giá trị |
|---|---|
| Kafka Brokers | `localhost:9092` |
| Topic Name | `sales-report-clean` (CÙNG topic POS/ERP) |

→ Đơn online vào chung event bus → Spark Streaming thấy ngay (speed) và ConsumeKafka đổ
xuống `/lake/transactions` (batch).

## 6. Kết quả

Sau khi chạy `spark_to_hive.py`, bảng `bao_cao.sales_report` có thêm:
- `source = 'ecommerce'`, `kenh = 'online'`
- các trường `customer_id`, `device`, `payment_method`

Báo cáo omnichannel (chạy `spark_report_hive.py`):
- `bc_doanhthu_kenh` — doanh thu online vs offline
- `bc_online_thietbi` — đơn online theo thiết bị (web/app) & thanh toán

```sql
-- xem nhanh, không cần chạy lại Spark
hive -e "SELECT * FROM bao_cao.bc_doanhthu_kenh;"
```

## Sơ đồ 5 luồng NiFi hiện có

```
POS         (api)    ListenHTTP       → làm sạch → PublishKafka ─┐
ERP         (db)     QueryDatabaseTbl → ConvertRecord → Split   ─┤
E-COMMERCE  (db)     QueryDatabaseTbl → ConvertRecord → Split   ─┼→ Kafka: sales-report-clean
                       → JoltTransform (đổi tên + source)         │        │
KHO/WMS     (db)     QueryDatabaseTbl → ...                     ─┘        └→ ConsumeKafka → /lake
CRM         (api)    InvokeHTTP (định kỳ) → /clean/crm  (dimension, không qua Kafka)
```
