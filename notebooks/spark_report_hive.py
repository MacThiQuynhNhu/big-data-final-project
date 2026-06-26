"""
Spark SQL — tạo BÁO CÁO từ Hive, vừa IN ra vừa GHI kết quả thành BẢNG HIVE.
Đúng yêu cầu thầy: "Hive: lưu dữ liệu báo cáo".

Sau khi chạy, xem lại báo cáo bất cứ lúc nào KHÔNG cần chạy lại Spark:
  hive -e "SELECT * FROM bao_cao.bc_loinhuan_vung;"

Chạy:
  spark-submit --master yarn --deploy-mode client notebooks/spark_report_hive.py
"""
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("bao-cao-hive")
         .config("spark.sql.warehouse.dir", "hdfs://master:9000/user/hive/warehouse")
         .enableHiveSupport()
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
spark.sql("USE bao_cao")


def in_va_luu(df, ten_bang, so_dong=20):
    """In ra màn hình rồi lưu thành bảng Hive bao_cao.<ten_bang>."""
    df.show(so_dong)
    df.write.mode("overwrite").format("parquet").saveAsTable(ten_bang)
    print(f"   -> đã lưu bảng bao_cao.{ten_bang}\n")


# 1. Doanh thu / chi phí / lợi nhuận theo cửa hàng & tháng (chỉ kênh OFFLINE — có cửa hàng)
print("== 1. Doanh thu / chi phí / lợi nhuận theo cửa hàng & tháng ==")
in_va_luu(spark.sql("""
    SELECT store_id, thang,
           ROUND(SUM(revenue), 0)        AS doanh_thu,
           ROUND(SUM(cost), 0)           AS chi_phi,
           ROUND(SUM(revenue - cost), 0) AS loi_nhuan
    FROM sales_report
    WHERE source = 'erp' AND kenh = 'offline' AND cost IS NOT NULL
    GROUP BY store_id, thang
    ORDER BY store_id, thang
"""), "bc_doanhthu_cuahang")

# 2. Lợi nhuận theo khu vực
print("== 2. Lợi nhuận theo khu vực ==")
in_va_luu(spark.sql("""
    SELECT region,
           ROUND(SUM(revenue), 0)        AS doanh_thu,
           ROUND(SUM(revenue - cost), 0) AS loi_nhuan
    FROM sales_report
    WHERE source = 'erp' AND cost IS NOT NULL
    GROUP BY region
    ORDER BY loi_nhuan DESC
"""), "bc_loinhuan_vung")

# 3. Top 10 sản phẩm bán chạy (cả 2 kênh — từ ERP)
print("== 3. Top 10 sản phẩm bán chạy (cả online + offline) ==")
in_va_luu(spark.sql("""
    SELECT product_id,
           ROUND(SUM(revenue), 0) AS doanh_thu,
           SUM(qty)               AS so_luong_ban
    FROM sales_report
    WHERE source = 'erp'
    GROUP BY product_id
    ORDER BY doanh_thu DESC
    LIMIT 10
"""), "bc_top_sanpham")

# 4. Cảnh báo hàng tồn (KHO TỔNG) dưới ngưỡng tái đặt riêng từng sản phẩm
print("== 4. Cảnh báo hàng tồn dưới ngưỡng tái đặt (kho tổng) ==")
in_va_luu(spark.sql("""
    SELECT product_id, ten_sp, stock_qty, reorder_level
    FROM inventory
    WHERE stock_qty < reorder_level
    ORDER BY stock_qty
"""), "bc_canhbao_tonkho", so_dong=15)

# 5. Lợi nhuận theo sản phẩm — từ ERP (đã có product_id + COGS từ giá vốn)
print("== 5. Lợi nhuận theo sản phẩm ==")
in_va_luu(spark.sql("""
    SELECT product_id,
           ROUND(SUM(revenue - cost), 0) AS loi_nhuan,
           COUNT(*)                      AS so_dong_ban
    FROM sales_report
    WHERE source = 'erp' AND cost IS NOT NULL
    GROUP BY product_id
    ORDER BY loi_nhuan DESC
    LIMIT 10
"""), "bc_loinhuan_sanpham")

# 5b. Giá trị tồn kho theo sản phẩm = tồn * giá vốn (LINK kho <-> tài chính)
print("== 5b. Giá trị tồn kho (kho tổng, theo giá vốn) ==")
in_va_luu(spark.sql("""
    SELECT product_id, ten_sp, stock_qty, unit_cost, gia_tri_ton
    FROM inventory
    ORDER BY gia_tri_ton DESC
"""), "bc_giatri_ton")

# 6. Doanh thu + lợi nhuận theo KÊNH (online vs offline) — từ ERP (có cả 2 kênh + COGS)
print("== 6. Doanh thu / lợi nhuận theo kênh (online vs offline) ==")
in_va_luu(spark.sql("""
    SELECT kenh,
           COUNT(*)                      AS so_dong,
           ROUND(SUM(revenue), 0)        AS doanh_thu,
           ROUND(SUM(revenue - cost), 0) AS loi_nhuan
    FROM sales_report
    WHERE source = 'erp' AND kenh IS NOT NULL
    GROUP BY kenh
    ORDER BY doanh_thu DESC
"""), "bc_doanhthu_kenh")

# 7. Đơn ONLINE theo thiết bị & phương thức thanh toán (chỉ kênh thương mại điện tử)
print("== 7. Đơn online theo thiết bị / thanh toán ==")
in_va_luu(spark.sql("""
    SELECT device, payment_method,
           COUNT(*)               AS so_don,
           ROUND(SUM(revenue), 0) AS doanh_thu
    FROM sales_report
    WHERE source = 'ecommerce'
    GROUP BY device, payment_method
    ORDER BY doanh_thu DESC
"""), "bc_online_thietbi")

# 8. Doanh thu online theo PHÂN KHÚC khách (gộp E-commerce + CRM qua customer_id)
print("== 8. Doanh thu online theo phân khúc khách (gộp E-commerce + CRM) ==")
in_va_luu(spark.sql("""
    SELECT k.segment,
           COUNT(*)                 AS so_don,
           ROUND(SUM(e.revenue), 0) AS doanh_thu
    FROM sales_report e JOIN dim_khachhang k ON e.customer_id = k.customer_id
    WHERE e.source = 'ecommerce'
    GROUP BY k.segment
    ORDER BY doanh_thu DESC
"""), "bc_doanhthu_segment")

# 9. Xu hướng giá trị tồn kho theo NGÀY (từ snapshot — cuộn được ngày → tuần → tháng)
print("== 9. Xu hướng tồn kho theo ngày (từ snapshot) ==")
in_va_luu(spark.sql("""
    SELECT ngay_chot,
           ROUND(SUM(gia_tri_ton), 0) AS gia_tri_ton,
           SUM(CASE WHEN stock_qty < reorder_level THEN 1 ELSE 0 END) AS so_sp_duoi_nguong
    FROM snapshot_tonkho
    GROUP BY ngay_chot
    ORDER BY ngay_chot
"""), "bc_xuhuong_tonkho")

print("== Các bảng báo cáo đã lưu trong Hive (database bao_cao) ==")
spark.sql("SHOW TABLES IN bao_cao").show()

spark.stop()
