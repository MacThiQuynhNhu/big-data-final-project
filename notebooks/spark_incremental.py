"""
TỔNG HỢP INCREMENTAL theo kỳ (ngày/tuần/tháng) — đúng cách batch PRODUCTION:
  - CHỈ tính kỳ MỚI ĐÃ ĐÓNG rồi APPEND; kỳ cũ KHÔNG tính lại (immutable).
  - Watermark = kỳ lớn nhất đã xử lý; kỳ "hiện tại" (đang chạy dở) chưa đóng -> bỏ qua.
Hiệu quả hơn full-recompute và đúng nghiệp vụ chốt sổ theo kỳ.

Chạy SAU spark_to_hive (cần bao_cao.sales_report):
  spark-submit --master yarn notebooks/spark_incremental.py
"""
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .appName("incremental-agg")
         .config("spark.sql.warehouse.dir", "hdfs://master:9000/user/hive/warehouse")
         .enableHiveSupport().getOrCreate())
spark.sparkContext.setLogLevel("WARN")
spark.sql("USE bao_cao")

# "Hôm nay" mô phỏng = ngày lớn nhất trong nguồn (ngày này ĐANG chạy dở -> CHƯA đóng)
cur = spark.sql("SELECT MAX(txn_date) AS d FROM sales_report WHERE source='erp'").head()["d"]
if cur is None:
    print("Chưa có dữ liệu ERP -> bỏ qua.")
    spark.stop(); raise SystemExit

# Mốc "kỳ hiện tại chưa đóng" cho từng cấp (ngày/tuần/tháng)
b = spark.sql(f"""
    SELECT DATE('{cur}')                                  AS d,
           to_date(date_trunc('week',  DATE('{cur}')))    AS w,
           to_date(date_trunc('month', DATE('{cur}')))    AS m
""").head()


def has_table(t):
    return t in [x.name for x in spark.catalog.listTables("bao_cao")]


def incremental(table, ky_sql, ky_cur):
    """Append tổng hợp cho kỳ ĐÃ ĐÓNG (ky < ky_cur) và CHƯA xử lý (ky > watermark)."""
    agg = (spark.sql(f"""
        SELECT {ky_sql}                     AS ky,
               ROUND(SUM(revenue), 0)       AS doanh_thu,
               ROUND(SUM(revenue - cost),0) AS loi_nhuan,
               COUNT(*)                     AS so_dong
        FROM sales_report
        WHERE source = 'erp' AND txn_date IS NOT NULL
        GROUP BY {ky_sql}
    """).where(F.col("ky") < F.lit(ky_cur)))              # chỉ kỳ ĐÃ ĐÓNG
    if has_table(table):
        wm = spark.sql(f"SELECT MAX(ky) AS k FROM {table}").head()["k"]
        if wm is not None:
            agg = agg.where(F.col("ky") > F.lit(wm))       # bỏ kỳ ĐÃ xử lý
    n = agg.count()
    agg.orderBy("ky").write.mode("append").format("parquet").saveAsTable(table)
    print(f"   {table}: +{n} kỳ mới (đã đóng) -> append")


print(f"== Incremental aggregate (ngày hiện tại chưa đóng = {b['d']}) ==")
incremental("agg_kinhdoanh_ngay",  "txn_date",                               b["d"])
incremental("agg_kinhdoanh_tuan",  "to_date(date_trunc('week',  txn_date))", b["w"])
incremental("agg_kinhdoanh_thang", "to_date(date_trunc('month', txn_date))", b["m"])

print("== Số kỳ đã tích lũy ==")
for t in ("agg_kinhdoanh_ngay", "agg_kinhdoanh_tuan", "agg_kinhdoanh_thang"):
    print(f"   {t}: {spark.table(t).count()} kỳ")

spark.stop()
