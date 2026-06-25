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


# 1. Doanh thu / chi phí / lợi nhuận theo cửa hàng & tháng
print("== 1. Doanh thu / chi phí / lợi nhuận theo cửa hàng & tháng ==")
in_va_luu(spark.sql("""
    SELECT store_id, thang,
           ROUND(SUM(revenue), 0)        AS doanh_thu,
           ROUND(SUM(cost), 0)           AS chi_phi,
           ROUND(SUM(revenue - cost), 0) AS loi_nhuan
    FROM sales_report
    WHERE source = 'erp' AND cost IS NOT NULL
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

# 3. Top 10 sản phẩm bán chạy
print("== 3. Top 10 sản phẩm bán chạy ==")
in_va_luu(spark.sql("""
    SELECT product_id,
           ROUND(SUM(revenue), 0) AS doanh_thu,
           COUNT(*)               AS so_lan_ban
    FROM sales_report
    WHERE source = 'pos'
    GROUP BY product_id
    ORDER BY doanh_thu DESC
    LIMIT 10
"""), "bc_top_sanpham")

# 4. Cảnh báo hàng tồn dưới ngưỡng tái đặt (lưu TẤT CẢ, in 15)
print("== 4. Cảnh báo hàng tồn dưới ngưỡng tái đặt ==")
in_va_luu(spark.sql("""
    SELECT store_id, product_id, stock_qty, reorder_level
    FROM inventory
    WHERE stock_qty < reorder_level
    ORDER BY stock_qty
"""), "bc_canhbao_tonkho", so_dong=15)

# 5. Lợi nhuận theo sản phẩm (gộp POS + ERP qua txn_id)
print("== 5. Lợi nhuận theo sản phẩm (gộp đa nguồn) ==")
in_va_luu(spark.sql("""
    SELECT p.product_id,
           ROUND(SUM(e.revenue - e.cost), 0) AS loi_nhuan,
           COUNT(*)                          AS so_giao_dich
    FROM sales_report p
    JOIN sales_report e ON p.txn_id = e.txn_id
    WHERE p.source = 'pos' AND e.source = 'erp' AND e.cost IS NOT NULL
    GROUP BY p.product_id
    ORDER BY loi_nhuan DESC
    LIMIT 10
"""), "bc_loinhuan_sanpham")

print("== Các bảng báo cáo đã lưu trong Hive (database bao_cao) ==")
spark.sql("SHOW TABLES IN bao_cao").show()

spark.stop()
