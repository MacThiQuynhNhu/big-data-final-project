"""
NHÁNH TỐC ĐỘ (streaming) — đúng vai real-time: cảnh báo giao dịch lỗ nặng NGAY.
Spark Structured Streaming đọc Kafka LIÊN TỤC, phát hiện đơn lỗ -> in cảnh báo tức thì.

Chạy (Kafka phải đang chạy):
  cd ~/big-data-final-project
  spark-submit --master local[2] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 \
    notebooks/spark_stream_alert.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StringType, DoubleType, LongType)

spark = SparkSession.builder.appName("canh-bao-realtime").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# readStream = đọc LIÊN TỤC; startingOffsets=latest = chỉ bắt message MỚI (real-time)
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", "sales-report-clean")
       .option("startingOffsets", "latest")
       .load())

schema = (StructType()
          .add("source", StringType()).add("import_time", StringType())
          .add("txn_id", StringType()).add("store_id", StringType())
          .add("product_id", StringType()).add("region", StringType())
          .add("qty", LongType()).add("revenue", DoubleType())
          .add("cost", DoubleType()).add("promotion", LongType())
          .add("txn_date", StringType()))

txn = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
       .select("d.*"))

# LUẬT CẢNH BÁO: giao dịch ERP lỗ nặng (profit < -100)
NGUONG = -100
alerts = (txn
          .where((F.col("source") == "erp") & F.col("cost").isNotNull())
          .withColumn("profit", F.col("revenue") - F.col("cost"))
          .where(F.col("profit") < NGUONG)
          .withColumn("CANH_BAO", F.lit("LO NANG"))
          .select("CANH_BAO", "txn_id", "store_id", "region",
                  "revenue", "cost", "profit"))

# In cảnh báo ra console ngay khi có giao dịch khớp
query = (alerts.writeStream
         .outputMode("append")
         .format("console")
         .option("truncate", False)
         # checkpoint: chịu lỗi, restart tiếp tục đúng vị trí đọc Kafka
         .option("checkpointLocation", "file:///home/hduser/chk_alert")
         .start())

print(">>> Đang lắng nghe Kafka... bơm 1 giao dịch lỗ vào topic để thấy cảnh báo <<<")
query.awaitTermination()
