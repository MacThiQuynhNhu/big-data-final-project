"""
Spark Structured Streaming → PostgreSQL (cho Grafana đọc).
Đọc Kafka liên tục, mỗi micro-batch:
  - ghi thống kê (doanh thu/lợi nhuận/số giao dịch theo thời gian) -> bảng rt_thongke
  - ghi cảnh báo giao dịch lỗ nặng -> bảng rt_canhbao

Chạy (cần Kafka + Postgres chạy, có producer bắn dữ liệu):
  spark-submit --master local[2] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 \
    --jars /home/hduser/postgresql-42.7.3.jar \
    notebooks/spark_stream_dashboard.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StringType, DoubleType, LongType)

spark = SparkSession.builder.appName("stream-dashboard").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

PG_URL = "jdbc:postgresql://localhost:5432/erp"
PG_PROPS = {"user": "erp", "password": "erp123", "driver": "org.postgresql.Driver"}

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", "sales-report-clean")
       .option("startingOffsets", "latest")
       .load())

schema = (StructType()
          .add("source", StringType()).add("txn_id", StringType())
          .add("store_id", StringType()).add("region", StringType())
          .add("product_id", StringType()).add("revenue", DoubleType())
          .add("cost", DoubleType()).add("promotion", LongType())
          .add("txn_date", StringType()))

txn = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
       .select("d.*")
       .where(F.col("cost").isNotNull())
       .withColumn("profit", F.col("revenue") - F.col("cost")))


def xu_ly_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    # 1. Thống kê batch -> rt_thongke (1 dòng/micro-batch, có mốc thời gian)
    stats = (batch_df.groupBy().agg(
        F.round(F.sum("revenue"), 0).alias("doanh_thu"),
        F.round(F.sum("profit"), 0).alias("loi_nhuan"),
        F.count("*").alias("so_gd"))
        .withColumn("thoi_diem", F.current_timestamp()))
    stats.write.jdbc(PG_URL, "rt_thongke", mode="append", properties=PG_PROPS)

    # 2. Cảnh báo lỗ nặng -> rt_canhbao
    alerts = (batch_df.where(F.col("profit") < -100)
              .withColumn("thoi_diem", F.current_timestamp())
              .select("thoi_diem", "txn_id", "store_id", "region",
                      "revenue", "cost", "profit"))
    if not alerts.rdd.isEmpty():
        alerts.write.jdbc(PG_URL, "rt_canhbao", mode="append", properties=PG_PROPS)
    print(f"batch {batch_id}: đã ghi thống kê + {alerts.count()} cảnh báo")


query = (txn.writeStream
         .foreachBatch(xu_ly_batch)
         .outputMode("append")
         .trigger(processingTime="5 seconds")     # mỗi 5 giây 1 điểm dữ liệu
         .start())

print(">>> Streaming → PostgreSQL đang chạy (Ctrl+C để dừng)")
query.awaitTermination()
