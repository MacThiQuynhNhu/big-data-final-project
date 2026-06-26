"""
NHÁNH BATCH (thống nhất): đọc GIAO DỊCH từ /lake/transactions
— nơi NiFi ConsumeKafka đã đổ Kafka xuống HDFS (JSON Lines).
→ Batch ăn CÙNG một dòng Kafka với streaming = kiến trúc thống nhất.

Tồn kho (master data) vẫn nạp từ /clean/kho.

Chạy (cần HDFS + YARN):
  spark-submit --master yarn --deploy-mode client notebooks/spark_to_hive.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("nap-hive-tu-kafka-landing")
         .config("spark.sql.warehouse.dir", "hdfs://master:9000/user/hive/warehouse")
         .enableHiveSupport()
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

HDFS = "hdfs://master:9000"

# ---------- Giao dịch: đọc từ HDFS landing (Kafka → ConsumeKafka → HDFS) ----------
# Định dạng JSON Lines (mỗi dòng 1 giao dịch), bản ghi đã có sẵn cột 'source'
tx = spark.read.json(f"{HDFS}/lake/transactions")
tx = tx.dropDuplicates(["source", "txn_id"])

# POS và ERP khác trường nhau -> thiếu cột nào thì thêm null cho đồng nhất
for col_name, col_type in [("txn_id", "string"), ("store_id", "string"),
                           ("product_id", "string"), ("region", "string"),
                           ("revenue", "double"), ("cost", "double"),
                           ("promotion", "int"), ("txn_date", "string"),
                           ("source", "string")]:
    if col_name not in tx.columns:
        tx = tx.withColumn(col_name, F.lit(None).cast(col_type))

sales = (tx.select("txn_id", "store_id", "product_id", "region",
                   F.col("revenue").cast("double"),
                   F.col("cost").cast("double"),
                   F.col("promotion").cast("int"),
                   F.to_date("txn_date").alias("txn_date"),
                   "source")
         .where(F.col("source").isNotNull() & F.col("txn_id").isNotNull())
         .withColumn("thang", F.month("txn_date")))

spark.sql("CREATE DATABASE IF NOT EXISTS bao_cao")
(sales.write.mode("overwrite").format("parquet")
      .partitionBy("source", "thang")
      .saveAsTable("bao_cao.sales_report"))

# ---------- Tồn kho: master data, vẫn nạp từ /clean/kho ----------
kho = (spark.read.option("multiline", True).json(f"{HDFS}/clean/kho")
       .dropDuplicates(["store_id", "product_id"]))
(kho.select("store_id", "product_id",
            F.col("stock_qty").cast("int"),
            F.col("reorder_level").cast("int"),
            F.lit("kho").alias("source"))
    .write.mode("overwrite").format("parquet")
    .partitionBy("source")
    .saveAsTable("bao_cao.inventory"))

print("== Phân vùng sales_report (nguồn: Kafka landing /lake/transactions) ==")
spark.sql("SHOW PARTITIONS bao_cao.sales_report").show(30, truncate=False)
print("== Số bản ghi mỗi nguồn ==")
spark.sql("SELECT source, COUNT(*) AS so FROM bao_cao.sales_report GROUP BY source").show()

spark.stop()
