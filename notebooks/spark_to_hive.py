"""
NHÁNH BATCH: HDFS -> HIVE. Đọc dữ liệu sạch (NiFi đã PutHDFS) từ /clean,
tạo bảng Hive PHÂN VÙNG theo nguồn/tháng. KHÔNG dùng Kafka (Kafka cho nhánh streaming).

Chạy (chỉ cần HDFS + YARN):
  cd ~/big-data-final-project
  spark-submit --master yarn --deploy-mode client notebooks/spark_to_hive.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("nap-hive-tu-hdfs")
         .config("spark.sql.warehouse.dir", "hdfs://master:9000/user/hive/warehouse")
         .enableHiveSupport()
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

HDFS = "hdfs://master:9000/clean"

# Đọc dữ liệu sạch từ HDFS (chống trùng nếu có nhiều file do flow chạy nhiều lần)
pos = (spark.read.option("multiline", True).json(f"{HDFS}/pos")
       .dropDuplicates(["txn_id"]))
erp = (spark.read.option("multiline", True).json(f"{HDFS}/erp")
       .dropDuplicates(["txn_id"]))
kho = (spark.read.option("multiline", True).json(f"{HDFS}/kho")
       .dropDuplicates(["store_id", "product_id"]))

# Chuẩn hóa POS & ERP về schema chung rồi gộp
pos_n = pos.select(
    "txn_id", "store_id", "product_id",
    F.lit(None).cast("string").alias("region"),
    F.col("revenue").cast("double").alias("revenue"),
    F.lit(None).cast("double").alias("cost"),
    F.col("promotion").cast("int").alias("promotion"),
    F.to_date("txn_date").alias("txn_date"),
    F.lit("pos").alias("source"))

erp_n = erp.select(
    "txn_id", "store_id",
    F.lit(None).cast("string").alias("product_id"),
    "region",
    F.col("revenue").cast("double").alias("revenue"),
    F.col("cost").cast("double").alias("cost"),
    F.lit(None).cast("int").alias("promotion"),
    F.to_date("txn_date").alias("txn_date"),
    F.lit("erp").alias("source"))

sales = pos_n.unionByName(erp_n).withColumn("thang", F.month("txn_date"))

# Ghi vào Hive phân vùng theo (nguồn, tháng)
spark.sql("CREATE DATABASE IF NOT EXISTS bao_cao")

(sales.write.mode("overwrite").format("parquet")
      .partitionBy("source", "thang")
      .saveAsTable("bao_cao.sales_report"))

(kho.select("store_id", "product_id",
            F.col("stock_qty").cast("int"),
            F.col("reorder_level").cast("int"),
            F.lit("kho").alias("source"))
    .write.mode("overwrite").format("parquet")
    .partitionBy("source")
    .saveAsTable("bao_cao.inventory"))

print("== Phân vùng sales_report (nguồn/tháng) ==")
spark.sql("SHOW PARTITIONS bao_cao.sales_report").show(30, truncate=False)
print("== Số bản ghi mỗi nguồn ==")
spark.sql("SELECT source, COUNT(*) AS so FROM bao_cao.sales_report GROUP BY source").show()

spark.stop()
