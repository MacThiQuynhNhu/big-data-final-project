# RESULTS — Đọc kết quả

Có thể xem kết quả **không cần chạy lại pipeline** — kết quả nằm sẵn trong PostgreSQL (cho
Grafana) và Hive (kho dữ liệu).

## Hai dashboard Grafana — `http://192.168.79.131:3000` (admin/admin)

### 1. `sales-report-batch` — phân tích kinh doanh (batch)
Nguồn: các bảng `agg_*` / `bc_*` (PostgreSQL, do `spark_marts_to_pg` đẩy từ Hive).

| Panel | Loại | Bảng |
|---|---|---|
| Tổng năm / Dự báo tháng tới | Stat | `agg_nam` / `bc_dubao` |
| Doanh thu & LN theo **ngày/tuần/tháng/quý** | Timeseries | `agg_ngay` / `agg_tuan` / `agg_thang` / `agg_quy` |
| Doanh thu theo kênh (online/offline) | Pie | `bc_doanhthu_kenh` |
| Phân khúc khách | Pie | `bc_doanhthu_segment` |
| Lợi nhuận theo vùng · Top sản phẩm · Giá trị tồn | Bar | `bc_loinhuan_vung` / `bc_top_sanpham` / `bc_giatri_ton` |
| Doanh thu theo cửa hàng-tháng | Table | `bc_doanhthu_cuahang` |

> ⏱ **Time range:** dữ liệu dùng **ngày nghiệp vụ mô phỏng** (2026-07-01 → ...). Chọn range tuyệt
> đối phủ khoảng này (vd `2026-07-01 → 2027-05-01`) mới thấy đồ thị.

### 2. `sales-report` — giám sát real-time (streaming)
Nguồn: `rt_thongke` / `rt_canhbao` (PostgreSQL, do `spark_stream_dashboard` ghi).

| Panel | Loại | Bảng |
|---|---|---|
| Doanh thu & Lợi nhuận · Số giao dịch | Timeseries | `rt_thongke` |
| Cảnh báo gần đây · Tổng cảnh báo | Table / Stat | `rt_canhbao` |

> ⏱ **Time range:** `rt_*` dùng **GIỜ XỬ LÝ THỰC** (không phải ngày nghiệp vụ). Dùng range tương
> đối **"Last 15 minutes"** khi streaming đang chạy. Hai dashboard có 2 trục thời gian khác nhau —
> đúng bản chất speed layer (real-time) vs batch layer (ngày nghiệp vụ).

## Các bảng mart (Hive → PostgreSQL)

**Tổng hợp theo thời gian** (`agg_*`, cuộn bậc thang, mỗi kỳ tính 1 lần):
`agg_ngay`, `agg_tuan`, `agg_thang`, `agg_quy`, `agg_nam` — cột `ky, doanh_thu, loi_nhuan, so_dong`.

**Báo cáo** (`bc_*`):

| Bảng | Ý nghĩa |
|---|---|
| `bc_doanhthu_cuahang` | Doanh thu/chi phí/lợi nhuận theo cửa hàng-tháng (ERP offline) |
| `bc_loinhuan_vung` | Lợi nhuận theo khu vực |
| `bc_top_sanpham` · `bc_loinhuan_sanpham` | Top sản phẩm theo doanh thu / lợi nhuận |
| `bc_doanhthu_kenh` | Doanh thu, lợi nhuận, số GD theo kênh online/offline |
| `bc_doanhthu_segment` | Doanh thu theo phân khúc khách (Ecom × CRM) |
| `bc_online_thietbi` | Đơn online theo thiết bị + phương thức thanh toán |
| `bc_giatri_ton` · `bc_canhbao_tonkho` | Giá trị tồn (× giá vốn) · cảnh báo dưới ngưỡng tái đặt |
| `bc_xuhuong_tonkho` · `bc_kinhdoanh_ngay` | Xu hướng tồn kho · chuỗi thời gian doanh thu theo ngày |
| `bc_dubao` | Dự báo doanh thu tháng tới (Linear Regression) |
| `bc_phancum_cuahang` | Phân cụm cửa hàng theo hành vi (KMeans) |
| `bc_kehoach_nhaphang` | Kế hoạch nhập hàng (dự báo cầu × 1.5 − tồn → đề xuất nhập + chi phí) |
| `bc_tuongquan` | Tương quan khuyến mãi vs lợi nhuận |

## Truy vấn trực tiếp

**PostgreSQL** (nhanh, serving):
```bash
PGPASSWORD=erp123 psql -h localhost -U erp -d erp -c "SELECT * FROM agg_thang ORDER BY ky;"
PGPASSWORD=erp123 psql -h localhost -U erp -d erp -c "SELECT * FROM bc_kehoach_nhaphang ORDER BY de_xuat_nhap DESC;"
```

**Hive** (kho dữ liệu — nơi data đã xử lý thật sự nằm; log nhiều, gọn lại bằng `2>/dev/null`):
```bash
spark-sql --master local[1] -e "SELECT * FROM bao_cao.agg_thang ORDER BY ky;" 2>/dev/null | column -t -s $'\t'
spark-sql --master local[1] -e "SHOW TABLES IN bao_cao;" 2>/dev/null
```
> Hive giữ TẤT CẢ bảng đã xử lý (`sales_report`, `agg_*`, `bc_*`, `inventory`, `dim_*`).
> PostgreSQL chỉ là **bản sao** của `bc_*`/`agg_*` (để Grafana đọc nhanh) + 3 bảng **nguồn thô**
> (`sales`, `ecommerce_orders`, `kho_chuyendong`) + `rt_*` (streaming).
