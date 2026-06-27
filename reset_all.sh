#!/usr/bin/env bash
# RESET TOÀN BỘ -> chạy lại từ đầu, data SẠCH từ 2026-07-01.
# >> TRƯỚC KHI CHẠY: vào NiFi UI -> Stop TẤT CẢ processor. <<
# Tiền đề: HDFS + Kafka + PostgreSQL + MySQL metastore đang chạy.
set -e
cd "$(dirname "$0")"
KAFKA=~/kafka_2.13-3.7.1

echo ">> [1] Dừng feeder + streaming + CRM API"
pkill -f source_feeder.py 2>/dev/null || true
pkill -f spark_stream_dashboard.py 2>/dev/null || true
pkill -f "http.server 8000" 2>/dev/null || true
sleep 2

echo ">> [2] Xóa data nguồn Postgres (GIỮ san_pham) + mốc mô phỏng + CRM cũ"
PGPASSWORD=erp123 psql -h localhost -U erp -d erp -c \
  "TRUNCATE sales, ecommerce_orders, kho_chuyendong RESTART IDENTITY;"
PGPASSWORD=erp123 psql -h localhost -U erp -d erp -c \
  "TRUNCATE rt_thongke, rt_canhbao;" 2>/dev/null || true
rm -f data/.sim_anchor data/crm_customers.json

echo ">> [3] Tạo lại Kafka topic"
for t in sales-report-clean inventory-events; do
  $KAFKA/bin/kafka-topics.sh --delete --topic $t --bootstrap-server localhost:9092 2>/dev/null || true
done
sleep 5
for t in sales-report-clean inventory-events; do
  $KAFKA/bin/kafka-topics.sh --create --topic $t --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
done

echo ">> [4] Xóa HDFS /lake + /clean"
hdfs dfs -rm -r -f /lake /clean
hdfs dfs -mkdir -p /lake/transactions /lake/inventory /clean/crm
hdfs dfs -chmod -R 777 /lake /clean

echo ">> [5] Xóa toàn bộ báo cáo Hive (database bao_cao) -> build lại sạch"
spark-sql --master local[1] -e "DROP DATABASE IF EXISTS bao_cao CASCADE;" 2>/dev/null \
  || echo "   (spark-sql lỗi -> thử: hive -e \"DROP DATABASE IF EXISTS bao_cao CASCADE;\")"

echo ">> [6] Xóa checkpoint streaming"
rm -rf ~/chk_dashboard ~/chk_alert

echo ">> [7] Bật lại CRM API + feeder (ngày bắt đầu 2026-07-01)"
( cd data && nohup python3 -m http.server 8000 >~/crm_api.log 2>&1 & )
nohup python3 -u data_generator/source_feeder.py >~/feeder.log 2>&1 &

echo ""
echo "=== RESET XONG — data nguồn bắt đầu lại từ 2026-07-01 ==="
echo "TIẾP THEO trong NiFi UI:"
echo "  1) Clear state 3 QueryDatabaseTable (ERP, Ecom, Kho)"
echo "  2) Start TẤT CẢ processor"
echo "  3) InvokeHTTP (CRM) -> Run Once"
echo "Rồi: đợi ~5 phút cho data land -> chạy 'bash run_batch.sh'"
echo "Streaming: nohup spark-submit --master local[1] --driver-memory 768m \\"
echo "   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 \\"
echo "   --jars /home/hduser/postgresql-42.7.3.jar notebooks/spark_stream_dashboard.py >~/streaming.log 2>&1 &"
