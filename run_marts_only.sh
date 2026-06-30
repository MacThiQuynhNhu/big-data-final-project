#!/usr/bin/env bash
# Chạy riêng spark_marts_to_pg để đẩy Hive -> PostgreSQL (step 5/5)
cd "$(dirname "$0")"

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
export YARN_CONF_DIR=/usr/local/hadoop/etc/hadoop
export SPARK_HOME=/usr/local/spark
export PATH="$PATH:/home/hduser/.local/bin:$SPARK_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin"
PG_JAR=/home/hduser/postgresql-42.7.3.jar

echo "=== [5/5] Day marts (bc_* + agg_*) -> PostgreSQL (Grafana) ==="
spark-submit --master local[1] --driver-memory 512m --jars "$PG_JAR" notebooks/spark_marts_to_pg.py
status=$?
if [ $status -eq 0 ]; then
  echo "=== MARTS XONG ==="
else
  echo "=== MARTS LOI (exit=$status) ==="
fi
