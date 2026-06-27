"""
Đẩy các bảng báo cáo (mart bc_*) từ HIVE sang PostgreSQL để Grafana trực quan hóa.
Grafana đọc Postgres dễ; đọc Hive khó -> copy marts sang Postgres làm "serving layer".

CHẠY LOCAL MODE (KHÔNG yarn): driver+executor ở master -> localhost:5432 kết nối được.
Cần: HDFS (đọc parquet) + MySQL metastore + PostgreSQL đang chạy.

  spark-submit --master local[2] --jars /home/hduser/postgresql-42.7.3.jar \
    notebooks/spark_marts_to_pg.py
"""
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("marts-hive-to-postgres")
         .config("spark.sql.warehouse.dir", "hdfs://master:9000/user/hive/warehouse")
         .enableHiveSupport()
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

PG_URL = "jdbc:postgresql://localhost:5432/erp"
PG_PROPS = {"user": "erp", "password": "erp123", "driver": "org.postgresql.Driver"}

# Lấy tất cả mart bc_* trong database bao_cao + vài dim hữu ích cho dashboard
tables = [r.tableName for r in spark.sql("SHOW TABLES IN bao_cao").collect()
          if r.tableName.startswith("bc_") or r.tableName in ("inventory", "dim_khachhang")]

print(f">>> Đẩy {len(tables)} bảng sang PostgreSQL...")
for t in tables:
    df = spark.table(f"bao_cao.{t}")
    (df.write.mode("overwrite").jdbc(PG_URL, t, properties=PG_PROPS))
    print(f"   -> {t} ({df.count()} dòng)")

print(">>> Xong. Grafana đọc các bảng này từ PostgreSQL (db erp).")
spark.stop()
