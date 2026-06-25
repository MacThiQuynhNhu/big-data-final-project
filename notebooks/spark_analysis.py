"""
Phân tích bằng Spark SQL + MLlib.
Dán từng phần vào một notebook trong Jupyter (http://localhost:8888, token: bigdata),
hoặc chạy trực tiếp trong container jupyter-spark.

Có 2 chế độ đọc dữ liệu:
  A) Đọc thẳng file CSV đã sinh  -> để test phần phân tích trước khi dựng NiFi/Kafka.
  B) Đọc từ Kafka topic sales-report-clean -> luồng end-to-end thật.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("bao-cao-kinh-doanh")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)

# Đường dẫn dữ liệu linh hoạt theo môi trường:
#   - Jupyter single-node : DATA = /home/jovyan/data  (mặc định)
#   - Cụm Spark (PA A)    : đặt DATA_DIR=/data
#   - HDFS (PA B)         : đặt DATA_DIR=hdfs://namenode:9000/input
DATA = os.environ.get("DATA_DIR", "/home/jovyan/data")

# =========================================================
# CHẾ ĐỘ A — đọc thẳng CSV (test nhanh)
# =========================================================
df = (
    spark.read.option("header", True).option("inferSchema", True)
    .csv(f"{DATA}/erp_sales.csv")
    .withColumn("txn_date", F.to_date("txn_date"))
    .na.drop(subset=["cost"])                       # loại bản ghi thiếu chi phí
    .withColumn("profit", F.col("revenue") - F.col("cost"))
    .withColumn("month", F.month("txn_date"))
    .withColumn("quarter", F.quarter("txn_date"))
)

# =========================================================
# CHẾ ĐỘ B — đọc từ Kafka (bỏ comment khi NiFi đã đẩy dữ liệu)
# =========================================================
# raw = (spark.read.format("kafka")
#        .option("kafka.bootstrap.servers", "kafka:9092")
#        .option("subscribe", "sales-report-clean")
#        .option("startingOffsets", "earliest")
#        .load())
# from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
# schema = (StructType()
#           .add("txn_id", StringType()).add("store_id", StringType())
#           .add("region", StringType()).add("revenue", DoubleType())
#           .add("cost", DoubleType()).add("txn_date", StringType()))
# df = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
#       .select("d.*")
#       .withColumn("txn_date", F.to_date("txn_date"))
#       .withColumn("profit", F.col("revenue") - F.col("cost"))
#       .withColumn("month", F.month("txn_date"))
#       .withColumn("quarter", F.quarter("txn_date")))

df.createOrReplaceTempView("sales")

# =========================================================
# 1. SPARK SQL — tổng hợp doanh thu/chi phí/lợi nhuận
# =========================================================
print("== Doanh thu - chi phí - lợi nhuận theo cửa hàng & tháng ==")
spark.sql("""
    SELECT store_id, month,
           SUM(revenue) AS doanh_thu,
           SUM(cost)    AS chi_phi,
           SUM(profit)  AS loi_nhuan
    FROM sales
    GROUP BY store_id, month
    ORDER BY store_id, month
""").show(20)

print("== Top sản phẩm bán chạy theo khu vực ==")
spark.sql("""
    SELECT region, store_id,
           SUM(revenue) AS doanh_thu
    FROM sales
    GROUP BY region, store_id
    ORDER BY doanh_thu DESC
""").show(10)

# =========================================================
# 2. MLlib — dự báo doanh thu tháng tới (Linear Regression)
#    Dùng chỉ số thời gian liên tục t = (năm - năm_đầu)*12 + tháng
#    để dự báo ĐÚNG tháng kế tiếp sau mốc dữ liệu cuối.
# =========================================================
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

dfm = df.withColumn("year", F.year("txn_date"))
min_year = dfm.agg(F.min("year")).head()[0]
monthly = (dfm.groupBy("year", "month")
           .agg(F.sum("revenue").alias("revenue"))
           .withColumn("t", (F.col("year") - F.lit(min_year)) * 12 + F.col("month"))
           .orderBy("t"))

assembler = VectorAssembler(inputCols=["t"], outputCol="features")
train = assembler.transform(monthly).select("features", "revenue")
model = LinearRegression(labelCol="revenue").fit(train)

next_t = monthly.agg(F.max("t")).head()[0] + 1
pred = model.predict(assembler.transform(
    spark.createDataFrame([(next_t,)], ["t"])).head().features)
print("Hệ số:", model.coefficients, "| Chặn:", round(model.intercept, 1))
print(f"Dự báo doanh thu tháng kế tiếp (t={next_t}):", round(pred, 0))

# =========================================================
# 3. MLlib — phân cụm cửa hàng theo hành vi (KMeans)
#    CHUẨN HÓA đặc trưng (StandardScaler) trước khi phân cụm,
#    nếu không revenue áp đảo -> cụm bị lệch.
# =========================================================
from pyspark.ml.feature import VectorAssembler as VA2, StandardScaler
from pyspark.ml.clustering import KMeans

store_feat = df.groupBy("store_id").agg(
    F.sum("revenue").alias("revenue"),
    F.avg("revenue").alias("avg_rev"),
    F.count("*").alias("n_txn"))
va = VA2(inputCols=["revenue", "avg_rev", "n_txn"], outputCol="raw")
scaler = StandardScaler(inputCol="raw", outputCol="features",
                        withStd=True, withMean=True)
km_data = scaler.fit(va.transform(store_feat)).transform(va.transform(store_feat))
km = KMeans(k=3, seed=42).fit(km_data)
print("== Phân cụm cửa hàng (đã chuẩn hóa) ==")
(km.transform(km_data)
   .select("store_id", "revenue", "n_txn", "prediction")
   .orderBy("prediction", F.col("revenue").desc())
   .show(50))
print("Số cửa hàng mỗi cụm:")
km.transform(km_data).groupBy("prediction").count().orderBy("prediction").show()

# =========================================================
# 4. MLlib — tương quan khuyến mãi vs doanh số
# =========================================================
# (cần cột promotion -> dùng chế độ đọc đầy đủ POS; minh họa với corr())
print("Tương quan revenue vs cost:",
      df.stat.corr("revenue", "cost"))

spark.stop()
