"""
Đúng flow thầy: NiFi → KAFKA → Spark đọc Kafka → HIVE → Spark SQL.
Giao dịch (POS+ERP) lấy TỪ KAFKA topic sales-report-clean; tồn kho lấy từ HDFS.

Chạy (local mode vì Kafka ở localhost trên master):
  # bật Kafka trước
  ~/kafka_2.13-3.7.1/bin/kafka-server-start.sh -daemon ~/kafka_2.13-3.7.1/config/kraft/server.properties
  cd ~/big-data-final-project
  spark-submit --master local[2] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 \
    notebooks/spark_kafka_to_hive.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StringType, DoubleType, LongType)

spark = (SparkSession.builder
         .appName("kafka-to-hive")
         .config("spark.sql.warehouse.dir", "hdfs://master:9000/user/hive/warehouse")
         .enableHiveSupport()
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# ---------- Đọc GIAO DỊCH từ KAFKA ----------
raw = (spark.read.format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", "sales-report-clean")
       .option("startingOffsets", "earliest")
       .load())

# Schema gộp (POS và ERP chung topic, trường nào không có -> null)
schema = (StructType()
          .add("source", StringType()).add("import_time", StringType())
          .add("txn_id", StringType()).add("store_id", StringType())
          .add("product_id", StringType()).add("region", StringType())
          .add("qty", LongType()).add("revenue", DoubleType())
          .add("cost", DoubleType()).add("promotion", LongType())
          .add("txn_date", StringType()))

sales = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
         .select("d.*")
         .where(F.col("txn_id").isNotNull())
         .dropDuplicates(["source", "txn_id"])      # topic có thể trùng do chạy nhiều lần
         .withColumn("txn_date", F.to_date("txn_date"))
         .withColumn("thang", F.month("txn_date")))

print("Số giao dịch đọc TỪ KAFKA (sau khử trùng):", sales.count())
sales.groupBy("source").count().show()

# ---------- Ghi vào HIVE phân vùng theo nguồn/tháng ----------
spark.sql("CREATE DATABASE IF NOT EXISTS bao_cao")

(sales.select("txn_id", "store_id", "product_id", "region",
              "revenue", "cost", "promotion", "txn_date", "source", "thang")
      .write.mode("overwrite").format("parquet")
      .partitionBy("source", "thang")
      .saveAsTable("bao_cao.sales_report"))

# ---------- Tồn kho: master data, lấy từ HDFS ----------
kho = (spark.read.option("multiline", True)
       .json("hdfs://master:9000/clean/kho")
       .dropDuplicates(["store_id", "product_id"]))

(kho.select("store_id", "product_id",
            F.col("stock_qty").cast("int"),
            F.col("reorder_level").cast("int"),
            F.lit("kho").alias("source"))
    .write.mode("overwrite").format("parquet")
    .partitionBy("source")
    .saveAsTable("bao_cao.inventory"))

# ---------- Kiểm chứng ----------
print("== Phân vùng sales_report (nguồn lấy từ Kafka) ==")
spark.sql("SHOW PARTITIONS bao_cao.sales_report").show(30, truncate=False)
print("== Số bản ghi mỗi nguồn trong Hive ==")
spark.sql("SELECT source, COUNT(*) AS so FROM bao_cao.sales_report GROUP BY source").show()

spark.stop()
