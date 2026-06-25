"""
Spark MLlib — đọc từ HIVE (đúng luồng batch: HDFS → Hive → Spark).
Chỉ làm Machine Learning (báo cáo SQL nằm ở spark_report_hive.py).

  - Linear Regression : dự báo doanh thu tháng kế tiếp
  - KMeans            : phân cụm cửa hàng theo hành vi (có StandardScaler)
  - Correlation       : tương quan khuyến mãi vs lợi nhuận

Yêu cầu: đã chạy spark_to_hive.py để có bảng bao_cao.sales_report.
Chạy:
  spark-submit --master yarn --deploy-mode client notebooks/spark_analysis.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("ml-bao-cao")
         .config("spark.sql.warehouse.dir", "hdfs://master:9000/user/hive/warehouse")
         .enableHiveSupport()
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

spark.sql("USE bao_cao")

# Dữ liệu tài chính (ERP có cost) đọc từ Hive
erp = (spark.sql("""
        SELECT store_id, revenue, cost, txn_date
        FROM sales_report
        WHERE source = 'erp' AND cost IS NOT NULL
    """)
    .withColumn("year", F.year("txn_date"))
    .withColumn("month", F.month("txn_date")))

# ============================================================
# 1. Linear Regression — dự báo doanh thu tháng kế tiếp
#    Dùng chỉ số thời gian liên tục t = (năm - năm_đầu)*12 + tháng
# ============================================================
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

min_year = erp.agg(F.min("year")).head()[0]
monthly = (erp.groupBy("year", "month")
           .agg(F.sum("revenue").alias("revenue"))
           .withColumn("t", (F.col("year") - F.lit(min_year)) * 12 + F.col("month"))
           .orderBy("t"))

asm = VectorAssembler(inputCols=["t"], outputCol="features")
model = LinearRegression(labelCol="revenue").fit(
    asm.transform(monthly).select("features", "revenue"))

next_t = monthly.agg(F.max("t")).head()[0] + 1
pred = model.predict(asm.transform(
    spark.createDataFrame([(next_t,)], ["t"])).head().features)
print("Hệ số:", model.coefficients, "| Chặn:", round(model.intercept, 1))
print(f"Dự báo doanh thu tháng kế tiếp (t={next_t}):", round(pred, 0))
# Lưu dự báo vào Hive
(spark.createDataFrame([(int(next_t), float(round(pred, 0)))],
                       ["thang_t", "doanh_thu_dubao"])
      .write.mode("overwrite").format("parquet").saveAsTable("bao_cao.bc_dubao"))
print("   -> đã lưu bảng bao_cao.bc_dubao")

# ============================================================
# 2. KMeans — phân cụm cửa hàng theo hành vi (chuẩn hóa đặc trưng)
# ============================================================
from pyspark.ml.feature import VectorAssembler as VA2, StandardScaler
from pyspark.ml.clustering import KMeans

store_feat = erp.groupBy("store_id").agg(
    F.sum("revenue").alias("revenue"),
    F.avg("revenue").alias("avg_rev"),
    F.count("*").alias("n_txn"))
va = VA2(inputCols=["revenue", "avg_rev", "n_txn"], outputCol="raw")
scaler = StandardScaler(inputCol="raw", outputCol="features",
                        withStd=True, withMean=True)
km_data = scaler.fit(va.transform(store_feat)).transform(va.transform(store_feat))
km = KMeans(k=3, seed=42).fit(km_data)

phancum = (km.transform(km_data)
           .select("store_id", "revenue", "n_txn", "prediction"))
print("== Phân cụm cửa hàng (đã chuẩn hóa) ==")
phancum.orderBy("prediction", F.col("revenue").desc()).show(50)
print("Số cửa hàng mỗi cụm:")
phancum.groupBy("prediction").count().orderBy("prediction").show()
# Lưu kết quả phân cụm vào Hive
phancum.write.mode("overwrite").format("parquet").saveAsTable("bao_cao.bc_phancum_cuahang")
print("   -> đã lưu bảng bao_cao.bc_phancum_cuahang")

# ============================================================
# 3. Tương quan KHUYẾN MÃI vs LỢI NHUẬN (gộp POS + ERP qua txn_id)
# ============================================================
joined = spark.sql("""
    SELECT p.promotion, (e.revenue - e.cost) AS profit
    FROM sales_report p
    JOIN sales_report e ON p.txn_id = e.txn_id
    WHERE p.source = 'pos' AND e.source = 'erp' AND e.cost IS NOT NULL
""")
corr_val = round(joined.stat.corr("promotion", "profit"), 4)
print("Tương quan khuyến mãi vs lợi nhuận:", corr_val)
# Lưu tương quan vào Hive
(spark.createDataFrame([("khuyen_mai_vs_loi_nhuan", float(corr_val))],
                       ["chi_tieu", "gia_tri"])
      .write.mode("overwrite").format("parquet").saveAsTable("bao_cao.bc_tuongquan"))
print("   -> đã lưu bảng bao_cao.bc_tuongquan")

spark.stop()
