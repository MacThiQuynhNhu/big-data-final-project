#!/usr/bin/env bash
# Chạy LẠI toàn bộ luồng batch + đẩy kết quả sang PostgreSQL cho Grafana.
# Mỗi lần dữ liệu mới đã land xuống /lake (feeder+NiFi đã chạy), chạy script này
# để phân tích lại (gồm dữ liệu/tháng mới) -> marts cập nhật -> Grafana tự đổi số.
#
# Tiền đề: HDFS + YARN + PostgreSQL đang chạy (batch KHÔNG cần Kafka/NiFi).
# Dùng:  bash run_batch.sh
set -e
cd "$(dirname "$0")"
PG_JAR=/home/hduser/postgresql-42.7.3.jar

echo "=== [1/4] Nạp Hive từ /lake (sales_report, inventory, dim, snapshot) ==="
spark-submit --master yarn notebooks/spark_to_hive.py

echo "=== [2/4] Báo cáo SQL (bc_*) ==="
spark-submit --master yarn notebooks/spark_report_hive.py

echo "=== [3/4] MLlib: dự báo + phân cụm + kế hoạch nhập hàng ==="
spark-submit --master yarn notebooks/spark_analysis.py

echo "=== [4/4] Đẩy marts Hive -> PostgreSQL (cho Grafana) ==="
spark-submit --master local[2] --jars "$PG_JAR" notebooks/spark_marts_to_pg.py

echo ""
echo "=== XONG. Grafana batch dashboard đã có số mới nhất. ==="
