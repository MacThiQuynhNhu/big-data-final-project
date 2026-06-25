"""
Spark SQL — tạo BÁO CÁO kinh doanh truy vấn TỪ HIVE (bảng phân vùng).
Đúng yêu cầu thầy: "Spark SQL: doanh thu, hàng tồn, chi phí".

Chạy:
  cd ~/big-data-final-project
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

# ============================================================
# 1. DOANH THU / CHI PHÍ / LỢI NHUẬN theo cửa hàng & tháng (ERP)
# ============================================================
print("== 1. Doanh thu / chi phí / lợi nhuận theo cửa hàng & tháng ==")
spark.sql("""
    SELECT store_id, thang,
           ROUND(SUM(revenue), 0)        AS doanh_thu,
           ROUND(SUM(cost), 0)           AS chi_phi,
           ROUND(SUM(revenue - cost), 0) AS loi_nhuan
    FROM sales_report
    WHERE source = 'erp' AND cost IS NOT NULL
    GROUP BY store_id, thang
    ORDER BY store_id, thang
""").show(20)

# ============================================================
# 2. LỢI NHUẬN theo vùng (ERP)
# ============================================================
print("== 2. Lợi nhuận theo khu vực ==")
spark.sql("""
    SELECT region,
           ROUND(SUM(revenue), 0)        AS doanh_thu,
           ROUND(SUM(revenue - cost), 0) AS loi_nhuan
    FROM sales_report
    WHERE source = 'erp' AND cost IS NOT NULL
    GROUP BY region
    ORDER BY loi_nhuan DESC
""").show()

# ============================================================
# 3. TOP sản phẩm bán chạy (POS)
# ============================================================
print("== 3. Top 10 sản phẩm bán chạy ==")
spark.sql("""
    SELECT product_id,
           ROUND(SUM(revenue), 0) AS doanh_thu,
           COUNT(*)               AS so_lan_ban
    FROM sales_report
    WHERE source = 'pos'
    GROUP BY product_id
    ORDER BY doanh_thu DESC
    LIMIT 10
""").show()

# ============================================================
# 4. HÀNG TỒN: sản phẩm dưới ngưỡng tái đặt (cảnh báo nhập hàng)
# ============================================================
print("== 4. Cảnh báo hàng tồn dưới ngưỡng tái đặt ==")
spark.sql("""
    SELECT store_id, product_id, stock_qty, reorder_level
    FROM inventory
    WHERE stock_qty < reorder_level
    ORDER BY stock_qty
    LIMIT 15
""").show()

# ============================================================
# 5. GỘP ĐA NGUỒN: lợi nhuận theo sản phẩm (POS có product, ERP có cost)
#    JOIN POS + ERP qua txn_id
# ============================================================
print("== 5. Lợi nhuận theo sản phẩm (gộp POS + ERP) ==")
spark.sql("""
    SELECT p.product_id,
           ROUND(SUM(e.revenue - e.cost), 0) AS loi_nhuan,
           COUNT(*)                          AS so_giao_dich
    FROM sales_report p
    JOIN sales_report e ON p.txn_id = e.txn_id
    WHERE p.source = 'pos' AND e.source = 'erp' AND e.cost IS NOT NULL
    GROUP BY p.product_id
    ORDER BY loi_nhuan DESC
    LIMIT 10
""").show()

spark.stop()
