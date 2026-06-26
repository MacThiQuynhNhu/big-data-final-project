# Luồng NiFi cho POS — HÓA ĐƠN qua API (ListenHTTP)

POS = máy bán hàng chi nhánh **push 1 hóa đơn** (invoice gồm nhiều dòng sản phẩm) lên API
trung tâm. NiFi nhận (ListenHTTP), **tách dòng + làm sạch** (Jolt), rồi đẩy Kafka.

Hóa đơn feeder gửi (POST `http://localhost:9998/pos`):
```json
{
  "invoice_id": "INV000000001",
  "store_id": "California",
  "region": "West",
  "Ngay": "2026-06-26",
  "lines": [
    {"txn_id": "L000000001", "product_id": "TEC-CO-10004722", "qty": 2, "Doanh_Thu": 1200, "promotion": 1},
    {"txn_id": "L000000002", "product_id": "OFF-BI-10003527", "qty": 1, "Doanh_Thu": 14,   "promotion": 0}
  ]
}
```

## 1. ListenHTTP (nhận hóa đơn từ máy bán hàng)

| Thuộc tính | Giá trị |
|---|---|
| Listening Port | `9998` |
| Base Path | `pos` |

→ Mỗi POST = 1 FlowFile chứa nguyên hóa đơn (JSON).

## 2. JoltTransformJSON (TÁCH DÒNG + làm sạch + gắn source)

Biến hóa đơn lồng nhau → **mảng các dòng phẳng**: đưa header (invoice_id/store_id/region/
ngày) vào từng dòng, đổi tên cột bẩn (`Doanh_Thu`→`revenue`, `Ngay`→`txn_date`), thêm
`source=pos`. Jolt Specification:

```json
[
  {
    "operation": "shift",
    "spec": {
      "lines": {
        "*": {
          "txn_id": "[&1].txn_id",
          "product_id": "[&1].product_id",
          "qty": "[&1].qty",
          "Doanh_Thu": "[&1].revenue",
          "promotion": "[&1].promotion",
          "@(2,invoice_id)": "[&1].invoice_id",
          "@(2,store_id)": "[&1].store_id",
          "@(2,region)": "[&1].region",
          "@(2,Ngay)": "[&1].txn_date"
        }
      }
    }
  },
  {
    "operation": "default",
    "spec": { "*": { "source": "pos" } }
  }
]
```

Kết quả (mảng dòng phẳng):
```json
[
  {"txn_id":"L000000001","invoice_id":"INV000000001","store_id":"California","region":"West","txn_date":"2026-06-26","product_id":"TEC-CO-10004722","qty":2,"revenue":1200,"promotion":1,"source":"pos"},
  ...
]
```

## 3. SplitJson (1 message Kafka / 1 dòng)

- JsonPath Expression: `$.*`

## 4. PublishKafka

| Thuộc tính | Giá trị |
|---|---|
| Kafka Brokers | `localhost:9092` |
| Topic Name | `sales-report-clean` (chung POS/ERP/Ecom) |

→ Mỗi dòng hóa đơn vào event bus. Spark phân biệt nguồn/kênh theo `source`.

## Ghi chú
- POS chỉ giữ **giá bán** (revenue), KHÔNG có giá vốn — đúng thực tế (máy bán hàng không
  biết COGS). Giá vốn nằm ở ERP/`san_pham`.
- `txn_id` (id dòng) trùng giữa POS và ERP → join được; `invoice_id` để gom giỏ hàng.
