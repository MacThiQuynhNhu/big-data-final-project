#!/usr/bin/env bash
# Chạy LẠI toàn bộ luồng batch + đẩy kết quả sang PostgreSQL cho Grafana.
# Thiết kế để chạy ĐỊNH KỲ bằng cron, SONG SONG với streaming/NiFi -> RAM nhỏ + khóa chống chồng.
#
# Tiền đề: HDFS + YARN + PostgreSQL đang chạy.
# Dùng tay:  bash run_batch.sh      | Cron: */15 * * * * cd <project> && bash run_batch.sh >> ~/batch_cron.log 2>&1
set -e
cd "$(dirname "$0")"
PG_JAR=/home/hduser/postgresql-42.7.3.jar
# RAM NHỎ để chạy chung với streaming + NiFi (đừng tranh hết RAM của VM)
RES="--driver-memory 512m --executor-memory 1g --num-executors 2 --executor-cores 1"

# Khóa: nếu lần batch trước CHƯA xong thì BỎ lần này (tránh 2 batch chồng nhau)
exec 9>/tmp/run_batch.lock
flock -n 9 || { echo "[$(date '+%F %T')] Batch trước chưa xong -> bỏ lần này."; exit 0; }

echo "=== [$(date '+%F %T')] BẮT ĐẦU batch ==="
echo "--- [1/4] Nạp Hive (sales_report, inventory, dim, snapshot) ---"
spark-submit --master yarn $RES notebooks/spark_to_hive.py

echo "--- [2/4] Báo cáo SQL (bc_*) ---"
spark-submit --master yarn $RES notebooks/spark_report_hive.py

echo "--- [3/4] MLlib: dự báo + phân cụm + kế hoạch nhập ---"
spark-submit --master yarn $RES notebooks/spark_analysis.py

echo "--- [4/4] Đẩy marts -> PostgreSQL (Grafana) ---"
spark-submit --master local[1] --driver-memory 512m --jars "$PG_JAR" notebooks/spark_marts_to_pg.py

echo "=== [$(date '+%F %T')] XONG. Grafana batch đã có số mới. ==="
