"""
TỔNG HỢP INCREMENTAL + CUỘN BẬC THANG (cascading rollup) — đúng cách batch PRODUCTION:
  raw -> NGÀY (đóng) ; NGÀY -> TUẦN/THÁNG (đóng) ; THÁNG -> QUÝ/NĂM (đóng).
Mỗi kỳ tính ĐÚNG 1 LẦN khi ĐÓNG rồi APPEND; kỳ cũ KHÔNG tính lại; kỳ lớn cuộn từ kỳ NHỎ
(năm = 12 dòng tháng, không đọc lại raw).

Chạy SAU spark_to_hive:  spark-submit --master yarn notebooks/spark_incremental.py
"""
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .appName("incremental-rollup")
         .config("spark.sql.warehouse.dir", "hdfs://master:9000/user/hive/warehouse")
         .enableHiveSupport().getOrCreate())
spark.sparkContext.setLogLevel("WARN")
spark.sql("USE bao_cao")

# "Hôm nay" mô phỏng = ngày lớn nhất trong nguồn (kỳ chứa nó ĐANG chạy dở -> chưa đóng)
cur = spark.sql("SELECT MAX(txn_date) AS d FROM sales_report WHERE source='erp'").head()["d"]
if cur is None:
    print("Chưa có dữ liệu ERP -> bỏ qua."); spark.stop(); raise SystemExit

# Mốc "kỳ hiện tại chưa đóng" cho từng cấp
b = spark.sql(f"""
    SELECT to_date(date_trunc('week',    DATE('{cur}'))) AS w,
           to_date(date_trunc('month',   DATE('{cur}'))) AS m,
           to_date(date_trunc('quarter', DATE('{cur}'))) AS q,
           to_date(date_trunc('year',    DATE('{cur}'))) AS y
""").head()


def has_table(t):
    return t in [x.name for x in spark.catalog.listTables("bao_cao")]


def _append_closed_new(agg, ky_cur, table, src):
    """Lọc kỳ ĐÃ ĐÓNG (< ky_cur) + CHƯA xử lý (> watermark) rồi APPEND."""
    agg = agg.where(F.col("ky") < F.lit(ky_cur))
    if has_table(table):
        wm = spark.sql(f"SELECT MAX(ky) AS k FROM {table}").head()["k"]
        if wm is not None:
            agg = agg.where(F.col("ky") > F.lit(wm))
    n = agg.count()
    agg.orderBy("ky").write.mode("append").format("parquet").saveAsTable(table)
    print(f"   {table} (cuộn từ {src}): +{n} kỳ mới")


def from_raw(table, ky_expr, ky_cur):
    """NGÀY: tính từ raw sales_report."""
    agg = spark.sql(f"""
        SELECT {ky_expr}                      AS ky,
               ROUND(SUM(revenue), 0)         AS doanh_thu,
               ROUND(SUM(revenue - cost), 0)  AS loi_nhuan,
               COUNT(*)                       AS so_dong
        FROM sales_report WHERE source='erp' AND txn_date IS NOT NULL
        GROUP BY {ky_expr}
    """)
    _append_closed_new(agg, ky_cur, table, "raw")


def rollup(table, src, ky_expr, ky_cur):
    """TUẦN/THÁNG/QUÝ/NĂM: cuộn từ bảng kỳ NHỎ hơn (đo lường cộng dồn được)."""
    agg = spark.sql(f"""
        SELECT {ky_expr}        AS ky,
               SUM(doanh_thu)   AS doanh_thu,
               SUM(loi_nhuan)   AS loi_nhuan,
               SUM(so_dong)     AS so_dong
        FROM {src} GROUP BY {ky_expr}
    """)
    _append_closed_new(agg, ky_cur, table, src)


print(f"== Incremental rollup (kỳ hiện tại chưa đóng, mốc ngày = {cur}) ==")
from_raw("agg_ngay",  "txn_date",                              cur)      # raw  -> NGÀY
rollup("agg_tuan",  "agg_ngay",  "to_date(date_trunc('week',    ky))", b["w"])   # NGÀY  -> TUẦN
rollup("agg_thang", "agg_ngay",  "to_date(date_trunc('month',   ky))", b["m"])   # NGÀY  -> THÁNG
rollup("agg_quy",   "agg_thang", "to_date(date_trunc('quarter', ky))", b["q"])   # THÁNG -> QUÝ
rollup("agg_nam",   "agg_thang", "to_date(date_trunc('year',    ky))", b["y"])   # THÁNG -> NĂM

print("== Số kỳ đã tích lũy ==")
for t in ("agg_ngay", "agg_tuan", "agg_thang", "agg_quy", "agg_nam"):
    if has_table(t):
        print(f"   {t}: {spark.table(t).count()} kỳ")

spark.stop()
