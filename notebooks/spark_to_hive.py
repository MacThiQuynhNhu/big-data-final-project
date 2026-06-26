"""
NHÁNH BATCH (thống nhất): đọc GIAO DỊCH từ /lake/transactions
— nơi NiFi ConsumeKafka đã đổ Kafka xuống HDFS (JSON Lines).
→ Batch ăn CÙNG một dòng Kafka với streaming = kiến trúc thống nhất.

Tồn kho tính từ SỰ KIỆN chuyển động (/lake/inventory), KHÔNG dùng Excel tĩnh.

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

# 3 nguồn bán hàng (POS/ERP/ECOMMERCE) khác trường nhau -> thiếu cột nào thì thêm null
for col_name, col_type in [("txn_id", "string"), ("invoice_id", "string"),
                           ("store_id", "string"), ("product_id", "string"),
                           ("region", "string"), ("qty", "int"),
                           ("revenue", "double"), ("cost", "double"),
                           ("promotion", "int"), ("kenh", "string"),
                           ("txn_date", "string"), ("customer_id", "string"),
                           ("device", "string"), ("payment_method", "string"),
                           ("source", "string")]:
    if col_name not in tx.columns:
        tx = tx.withColumn(col_name, F.lit(None).cast(col_type))

# kenh (channel): ERP đã có sẵn (offline/online cho cả 2 kênh); POS/ecom suy ra để dự phòng
sales = (tx.select("txn_id", "invoice_id", "store_id", "product_id", "region",
                   F.col("qty").cast("int"),
                   F.col("revenue").cast("double"),
                   F.col("cost").cast("double"),
                   F.col("promotion").cast("int"), "kenh",
                   "customer_id", "device", "payment_method",
                   F.to_date("txn_date").alias("txn_date"),
                   "source")
         .where(F.col("source").isNotNull() & F.col("txn_id").isNotNull())
         .withColumn("kenh",
                     F.coalesce(F.col("kenh"),
                                F.when(F.col("source") == "ecommerce", F.lit("online"))
                                 .when(F.col("source") == "pos", F.lit("offline"))))
         .withColumn("thang", F.month("txn_date")))

spark.sql("CREATE DATABASE IF NOT EXISTS bao_cao")
(sales.write.mode("overwrite").format("parquet")
      .partitionBy("source", "thang")
      .saveAsTable("bao_cao.sales_report"))

# ---------- Danh mục sản phẩm + giá vốn (dim) đọc từ Postgres qua JDBC ----------
PG_URL = "jdbc:postgresql://localhost:5432/erp"
PG_PROPS = {"user": "erp", "password": "erp123", "driver": "org.postgresql.Driver"}
sp = spark.read.jdbc(PG_URL, "san_pham", properties=PG_PROPS)   # product_id, ten_sp, unit_cost, reorder_level
sp.write.mode("overwrite").format("parquet").saveAsTable("bao_cao.dim_sanpham")

# ---------- Tồn kho TỔNG: tính từ SỰ KIỆN chuyển động (event-sourcing, kho trung tâm) ----------
# Movements (nhap qty>0 / xuat qty<0) đổ về /lake/inventory qua Kafka inventory-events.
# Tồn hiện tại = SUM(qty) theo product_id (1 kho tổng, không tách chi nhánh).
# Join giá vốn -> reorder_level riêng từng sản phẩm + GIÁ TRỊ TỒN (link tài chính).
mov = spark.read.json(f"{HDFS}/lake/inventory")
ton = (mov.select("product_id", F.col("qty").cast("int"))
          .groupBy("product_id").agg(F.sum("qty").alias("stock_qty")))
inv = (ton.join(sp, "product_id", "left")
          .withColumn("gia_tri_ton", F.round(F.col("stock_qty") * F.col("unit_cost"), 0))
          .withColumn("source", F.lit("kho"))
          .select("product_id", "ten_sp", "stock_qty",
                  "reorder_level", "unit_cost", "gia_tri_ton", "source"))
(inv.write.mode("overwrite").format("parquet")
    .partitionBy("source")
    .saveAsTable("bao_cao.inventory"))

# ---------- SNAPSHOT TỒN KHO theo NGÀY (periodic snapshot fact — APPEND, có lịch sử) ----------
# inventory ở trên = tồn HIỆN TẠI (ghi đè). Tồn kho là TRẠNG THÁI nên muốn theo dõi theo thời
# gian phải CHỤP ẢNH mỗi ngày: thêm cột ngay_chot, ghi ĐÚNG partition của ngày (chạy lại trong
# ngày không nhân đôi; các ngày trước giữ nguyên) -> cuộn được ngày -> tuần -> tháng.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
snap = (inv.withColumn("ngay_chot", F.current_date())
        .select("product_id", "ten_sp", "stock_qty", "reorder_level",
                "unit_cost", "gia_tri_ton", "ngay_chot"))
if "snapshot_tonkho" not in [t.name for t in spark.catalog.listTables("bao_cao")]:
    (snap.where("1=0").write.format("parquet")
         .partitionBy("ngay_chot").saveAsTable("bao_cao.snapshot_tonkho"))   # tạo lần đầu
snap.write.mode("overwrite").insertInto("bao_cao.snapshot_tonkho")           # ghi đè partition ngày

# ---------- Khách hàng (CRM) -> dim_khachhang (để phân tích theo phân khúc) ----------
crm = (spark.read.option("multiline", True).json(f"{HDFS}/clean/crm")
       .dropDuplicates(["customer_id"]))
(crm.select("customer_id", "customer_name", "segment", "region")
    .write.mode("overwrite").format("parquet").saveAsTable("bao_cao.dim_khachhang"))

print("== Phân vùng sales_report (nguồn: Kafka landing /lake/transactions) ==")
spark.sql("SHOW PARTITIONS bao_cao.sales_report").show(30, truncate=False)
print("== Số bản ghi mỗi nguồn ==")
spark.sql("SELECT source, COUNT(*) AS so FROM bao_cao.sales_report GROUP BY source").show()
print("== Số bản ghi theo kênh (online/offline) ==")
spark.sql("SELECT kenh, COUNT(*) AS so FROM bao_cao.sales_report "
          "WHERE kenh IS NOT NULL GROUP BY kenh").show()

spark.stop()
