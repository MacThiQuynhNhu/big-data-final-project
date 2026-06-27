#!/usr/bin/env bash
# Bật TẤT CẢ service để hệ thống chạy như thật: streaming live + batch định kỳ (cron).
# >> BẬT 2 SLAVE trong VMware TRƯỚC khi chạy script này. <<
# Dùng: bash start_all.sh   (rồi vào NiFi UI Start tất cả processor)
set -e
cd "$(dirname "$0")"
KAFKA=~/kafka_2.13-3.7.1
NIFI=~/nifi-1.28.1
PG_JAR=/home/hduser/postgresql-42.7.3.jar

echo ">> [1] HDFS + YARN"
start-dfs.sh
start-yarn.sh
hdfs dfsadmin -report | grep "Live datanodes" || true

echo ">> [2] PostgreSQL + Grafana"
sudo systemctl start postgresql grafana-server

echo ">> [3] Kafka (heap 512m)"
export KAFKA_HEAP_OPTS="-Xms512m -Xmx512m"
$KAFKA/bin/kafka-server-start.sh -daemon $KAFKA/config/kraft/server.properties
sleep 15

echo ">> [4] NiFi (đợi ~2 phút, RỒI VÀO UI START PROCESSOR)"
$NIFI/bin/nifi.sh start

echo ">> [5] CRM API (cổng 8000)"
( cd data && nohup python3 -m http.server 8000 >~/crm_api.log 2>&1 & )

echo ">> [6] Feeder (sinh dữ liệu live)"
nohup python3 -u data_generator/source_feeder.py >~/feeder.log 2>&1 &

echo ">> [7] Spark Streaming (nền, nhẹ) -> rt_* cho Grafana real-time"
nohup spark-submit --master local[1] --driver-memory 768m \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 \
  --jars $PG_JAR notebooks/spark_stream_dashboard.py >~/streaming.log 2>&1 &

echo ""
echo "=== ĐÃ BẬT XONG ==="
echo " - Vào NiFi UI (https://192.168.79.131:8443/nifi) -> Start TẤT CẢ processor."
echo " - Batch định kỳ: cài cron (xem README) -> tự chạy run_batch.sh mỗi 15 phút."
echo " - Grafana: http://192.168.79.131:3000 (dashboard real-time + batch)."
