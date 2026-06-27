#!/usr/bin/env bash
# Chạy LẠI toàn bộ luồng batch + đẩy kết quả sang PostgreSQL cho Grafana.
# Thiết kế để chạy ĐỊNH KỲ bằng cron, SONG SONG với streaming/NiFi -> RAM nhỏ + khóa chống chồng.
#
# Tiền đề: HDFS + YARN + PostgreSQL đang chạy.
# Dùng tay:  bash run_batch.sh      | Cron: */15 * * * * cd <project> && bash run_batch.sh >> ~/batch_cron.log 2>&1
# KHÔNG set -e: kiểm soát lỗi TƯỜNG MINH từng bước -> 1 báo cáo/MLlib lỗi KHÔNG chặn phân tích kỳ + đẩy Grafana.
cd "$(dirname "$0")"

# Môi trường Hadoop/Spark — set TƯỜNG MINH để CRON chạy được (cron không có sẵn các biến này)
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
export YARN_CONF_DIR=/usr/local/hadoop/etc/hadoop
export SPARK_HOME=/usr/local/spark
export PATH="$PATH:/home/hduser/.local/bin:$SPARK_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin"

PG_JAR=/home/hduser/postgresql-42.7.3.jar
# RAM NHỎ để chạy chung streaming + NiFi, và vừa slave 2GB (executor 640m + overhead ~1GB)
RES="--driver-memory 512m --executor-memory 640m --num-executors 2 --executor-cores 1"

# Khóa: nếu lần batch trước CHƯA xong thì BỎ lần này (tránh 2 batch chồng nhau)
exec 9>/tmp/run_batch.lock
flock -n 9 || { echo "[$(date '+%F %T')] Batch trước chưa xong -> bỏ lần này."; exit 0; }

FAILED=""
echo "=== [$(date '+%F %T')] BẮT ĐẦU batch ==="

# [1/5] CRITICAL — nạp Hive. Hỏng -> dừng cả batch (mọi bước sau phụ thuộc sales_report).
# LƯU Ý: spark_to_hive đọc TOÀN BỘ /lake/transactions mỗi lần (full rebuild, có dropDuplicates -> idempotent).
#   Đủ nhanh cho demo. Nếu chạy DÀI NGÀY khiến 1 lần batch > 15' thì archive bớt file /lake cũ (xem README/plan).
echo "--- [1/5] Nạp Hive (sales_report, inventory, dim, snapshot) ---"
spark-submit --master yarn $RES notebooks/spark_to_hive.py || {
  echo "[$(date '+%F %T')] FATAL: spark_to_hive lỗi -> hủy batch." >&2; exit 1; }

# [2/5] CRITICAL — phân tích kỳ (ngày->tuần/tháng->quý/năm, chỉ kỳ ĐÃ ĐÓNG). Ưu tiên cao nhất -> chạy SỚM.
echo "--- [2/5] Incremental: ngày->tuần/tháng->quý/năm (chỉ kỳ mới đã đóng) ---"
spark-submit --master yarn $RES notebooks/spark_incremental.py || {
  echo "[$(date '+%F %T')] ERROR: spark_incremental lỗi." >&2; FAILED="$FAILED incremental"; }

# [3/5] BEST-EFFORT — báo cáo snapshot bc_*. Lỗi KHÔNG chặn các bước sau.
echo "--- [3/5] Báo cáo SQL (bc_*) ---"
spark-submit --master yarn $RES notebooks/spark_report_hive.py || {
  echo "[$(date '+%F %T')] WARN: spark_report_hive lỗi (bỏ qua)." >&2; FAILED="$FAILED report"; }

# [4/5] BEST-EFFORT — MLlib (mong manh khi ít data). Lỗi KHÔNG chặn.
echo "--- [4/5] MLlib: dự báo + phân cụm + kế hoạch nhập ---"
spark-submit --master yarn $RES notebooks/spark_analysis.py || {
  echo "[$(date '+%F %T')] WARN: spark_analysis lỗi (bỏ qua)." >&2; FAILED="$FAILED analysis"; }

# [5/5] CRITICAL — đẩy marts (bc_* + agg_*) sang Postgres cho Grafana. LUÔN chạy dù bước phụ lỗi.
echo "--- [5/5] Đẩy marts -> PostgreSQL (Grafana) ---"
spark-submit --master local[1] --driver-memory 512m --jars "$PG_JAR" notebooks/spark_marts_to_pg.py || {
  echo "[$(date '+%F %T')] ERROR: spark_marts_to_pg lỗi." >&2; FAILED="$FAILED marts"; }

if [ -n "$FAILED" ]; then
  echo "=== [$(date '+%F %T')] XONG (có lỗi:$FAILED) ==="; exit 1
fi
echo "=== [$(date '+%F %T')] XONG. Grafana batch đã có số mới. ==="
