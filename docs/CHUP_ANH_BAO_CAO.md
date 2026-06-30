# Hướng dẫn chụp ảnh minh chứng (đúng bản chất: data ở đâu chụp ở đó)

**Nguyên tắc:**
- **Nguồn THÔ** (feeder ghi vào) → **PostgreSQL**: `sales`, `ecommerce_orders`, `kho_chuyendong`, CRM.
- **Đã XỬ LÝ** (Spark ghi vào kho dữ liệu) → **HIVE** (`spark-sql`): `sales_report`, `agg_*`, `bc_*`.
- **Streaming** (ghi thẳng) → **PostgreSQL**: `rt_*`.
- **Serving** (chứng minh đã đẩy sang PG cho Grafana) → PostgreSQL.

Chụp → lưu **đúng tên** vào `report_images/` → chạy `python generate_v2.py` là ảnh tự vào báo cáo.

> Mẹo psql: chạy `export PGPASSWORD=erp123` một lần, sau đó bỏ được tiền tố `PGPASSWORD=erp123`.
> Mẹo Hive: mỗi lệnh `spark-sql` mất ~30–60s + in nhiều log; **kéo xuống cuối thấy bảng kết quả** rồi chụp.

---

## BƯỚC 0 — Bật dịch vụ (master + 2 slave)
```bash
start-dfs.sh
start-yarn.sh
sudo systemctl start postgresql grafana-server
~/nifi-1.28.1/bin/nifi.sh start
```
> Ảnh Hive **bắt buộc** cần DataNode sống (`start-dfs.sh`). Không cần feeder/streaming (rt_* đã có data cũ).

---

## PHẦN A — NGUỒN THÔ → PostgreSQL

### erp_sales.png  (nguồn ERP)
```bash
PGPASSWORD=erp123 psql -h localhost -U erp -d erp -c "SELECT txn_id,product_id,store_id,qty,revenue,cost,kenh,txn_date FROM sales ORDER BY id LIMIT 5;"
```

### ecommerce_orders.png  (nguồn E-commerce)
```bash
PGPASSWORD=erp123 psql -h localhost -U erp -d erp -c "SELECT order_id,customer_id,product_id,qty,revenue,device,payment_method,order_date FROM ecommerce_orders ORDER BY id LIMIT 5;"
```

### kho_chuyendong.png  (nguồn Kho)
```bash
PGPASSWORD=erp123 psql -h localhost -U erp -d erp -c "SELECT product_id,loai,qty,cost,thoi_diem FROM kho_chuyendong ORDER BY id LIMIT 5;"
```

### crm_api.png  (nguồn CRM — file JSON)
```bash
python3 -c "import json; d=json.load(open('/home/hduser/big-data-final-project/data/crm_customers.json')); print(json.dumps(d[:6] if isinstance(d,list) else dict(list(d.items())[:6]), indent=2, ensure_ascii=False))"
```

### pos_sample.png  (nguồn POS — push HTTP, cần NiFi ListenHTTP chạy)
```bash
curl -i -X POST http://localhost:9998/pos -H "Content-Type: application/json" -d '{"invoice_id":"POS-DEMO","store_id":"California","txn_date":"2027-04-01","items":[{"product_id":"TEC-CO-10004722","qty":2,"price":450}]}'
```
Chụp khung có dòng `HTTP/1.1 200 OK`.

---

## PHẦN B — ĐÃ XỬ LÝ → HIVE (`spark-sql`)

**Định nghĩa helper 1 lần** (ẩn log WARN + căn cột cho gọn) rồi dùng `hq "<SQL>"` cho mọi lệnh:
```bash
hq() { spark-sql --master local[1] -e "$1" 2>/dev/null | column -t -s $'\t'; }
```
> `2>/dev/null` bỏ log WARN (kết quả vẫn giữ); `column -t` căn cột; `CAST(... AS BIGINT)` bỏ số kiểu khoa học.
> Nếu chạy không ra gì → tạm bỏ `2>/dev/null` để xem lỗi.

### sales_report.png  (bảng fact đa nguồn đã chuẩn hóa — CHỈ có ở Hive, mục 2.3)
```bash
hq "SELECT txn_id, product_id, store_id, qty, CAST(revenue AS BIGINT) revenue, CAST(cost AS BIGINT) cost, kenh, source, txn_date FROM bao_cao.sales_report LIMIT 8;"
```

### agg_thang.png  (cascade rollup theo tháng — mục 2.4)
```bash
hq "SELECT ky, CAST(doanh_thu AS BIGINT) doanh_thu, CAST(loi_nhuan AS BIGINT) loi_nhuan, so_dong FROM bao_cao.agg_thang ORDER BY ky;"
```

### bc_doanhthu_cuahang.png  (mục 3.1.1)
```bash
hq "SELECT store_id, thang, CAST(doanh_thu AS BIGINT) doanh_thu, CAST(chi_phi AS BIGINT) chi_phi, CAST(loi_nhuan AS BIGINT) loi_nhuan FROM bao_cao.bc_doanhthu_cuahang ORDER BY thang, store_id LIMIT 12;"
```

### bc_loinhuan_vung.png  (mục 3.1.2)
```bash
hq "SELECT region, CAST(doanh_thu AS BIGINT) doanh_thu, CAST(loi_nhuan AS BIGINT) loi_nhuan FROM bao_cao.bc_loinhuan_vung ORDER BY doanh_thu DESC;"
```

### bc_top_sanpham.png  (mục 3.1.3)
```bash
hq "SELECT product_id, CAST(doanh_thu AS BIGINT) doanh_thu, so_luong_ban FROM bao_cao.bc_top_sanpham ORDER BY doanh_thu DESC;"
```

### forecast.png  (dự báo Linear Regression — mục 3.2.1)
```bash
hq "SELECT thang_t, CAST(doanh_thu_dubao AS BIGINT) doanh_thu_dubao FROM bao_cao.bc_dubao;"
```

### kehoach_nhap.png  (kế hoạch nhập — mục 3.2.3)
```bash
hq "SELECT product_id, ten_sp, CAST(du_bao_thang AS BIGINT) du_bao_thang, ton_hien_tai, reorder_level, de_xuat_nhap, CAST(chi_phi_nhap_du_kien AS BIGINT) chi_phi_nhap_du_kien FROM bao_cao.bc_kehoach_nhaphang ORDER BY de_xuat_nhap DESC;"
```

### phancum.png  (K-Means — mục 3.2.2, TÙY CHỌN)
Bảng `bc_phancum_cuahang` đang RỖNG (MLlib chết lúc OOM). Muốn có thì chạy lại MLlib trước:
```bash
spark-submit --master yarn notebooks/spark_analysis.py
hq "SELECT * FROM bao_cao.bc_phancum_cuahang ORDER BY cluster, store_id;"
```
Bỏ qua cũng được (chỗ đó giữ caption).

---

## PHẦN C — STREAMING → PostgreSQL (`rt_*` ghi thẳng PG)

### rt_thongke.png  (mục 3.3)
```bash
PGPASSWORD=erp123 psql -h localhost -U erp -d erp -c "SELECT * FROM rt_thongke ORDER BY thoi_diem DESC LIMIT 10;"
```

### rt_canhbao.png  (mục 3.3)
```bash
PGPASSWORD=erp123 psql -h localhost -U erp -d erp -c "SELECT * FROM rt_canhbao ORDER BY thoi_diem DESC LIMIT 5;"
```

---

## PHẦN D — SERVING → PostgreSQL (chứng minh đã đẩy marts sang PG, mục 2.5)

### marts_output.png
```bash
PGPASSWORD=erp123 psql -h localhost -U erp -d erp -c "SELECT relname AS bang, n_live_tup AS so_dong FROM pg_stat_user_tables WHERE relname LIKE 'bc_%' OR relname LIKE 'agg_%' OR relname IN ('inventory','dim_khachhang') ORDER BY relname;"
```
> Đây là các bảng `spark_marts_to_pg.py` đã copy từ Hive sang PostgreSQL để Grafana đọc.

---

## PHẦN E — WEB (mở trình duyệt rồi chụp)

| Lưu tên | URL / thao tác |
|---|---|
| `nifi_flow.png` | `https://192.168.79.131:8443/nifi` (chấp nhận cert) → chụp canvas 5 processor |
| `hdfs_lake.png` | `http://192.168.79.131:9870` → Utilities → Browse the file system → `/lake` |
| `grafana_batch.png` | `http://192.168.79.131:3000` (admin/admin) → dashboard **sales-report-batch**; time range **2026-07-01 → 2027-05-01** |
| `grafana_streaming.png` | Grafana → dashboard **sales-report** (streaming); time range phủ **2026-06-27** |

⚠️ Hai dashboard chỉnh time-range KHÁC nhau mới thấy data: batch = ngày mô phỏng (2026-07 → 2027-04); streaming `rt_*` = giờ thật (~2026-06-27).

---

## BƯỚC CUỐI — Chèn ảnh vào báo cáo
1. Copy tất cả PNG vào `report_images/` (máy Windows chạy Python). **Tên file phải khớp.**
2. Chạy `python generate_v2.py` → tái sinh `BAO_CAO_BIGDATA_v2.docx`.

## Ghi chú
- `kien_truc_chuan.png` (sơ đồ kiến trúc) đã có sẵn — không cần chụp.
- 2 ảnh **mới** `sales_report.png` (mục 2.3) và `agg_thang.png` (mục 2.4) hiện CHƯA có ô trong báo cáo → nếu bạn chụp, báo Claude thêm 2 ô `add_image` vào `generate_v2.py` (mình làm nhanh).
- Ảnh Claude render sẵn (kiểu Ubuntu) đang làm fallback; ảnh thật trùng tên sẽ ghi đè.
