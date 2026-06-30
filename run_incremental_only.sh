#!/usr/bin/env bash
# Chạy riêng spark_incremental.py (idempotency test)
cd "$(dirname "$0")"

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
export YARN_CONF_DIR=/usr/local/hadoop/etc/hadoop
export SPARK_HOME=/usr/local/spark
export PATH="$PATH:/home/hduser/.local/bin:$SPARK_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin"

echo "=== [2/5] Incremental (idempotency check) ==="
spark-submit --master yarn --driver-memory 512m --executor-memory 640m --num-executors 2 --executor-cores 1 notebooks/spark_incremental.py 2>&1 | grep -E 'Incremental|ky moi|tich luy|ky '
echo "EXIT: $?"
