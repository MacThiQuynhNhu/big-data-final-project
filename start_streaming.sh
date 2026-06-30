#!/usr/bin/env bash
# Bật Spark Streaming -> rt_* cho Grafana real-time
cd "$(dirname "$0")"

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
export SPARK_HOME=/usr/local/spark
export PATH="$PATH:$SPARK_HOME/bin:$HADOOP_HOME/bin"
PG_JAR=/home/hduser/postgresql-42.7.3.jar

echo "=== Starting Spark Streaming Dashboard ==="
nohup spark-submit --master local[1] --driver-memory 768m \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 \
  --jars "$PG_JAR" notebooks/spark_stream_dashboard.py >~/streaming.log 2>&1 &

sleep 3
ps aux | grep -v grep | grep spark_stream_dashboard && echo ">>> Streaming da chay!" || echo ">>> LOI: Streaming khong chay!"
