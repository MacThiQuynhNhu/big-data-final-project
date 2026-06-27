#!/usr/bin/env bash
# Giảm RAM để cụm 6+3+3 chạy mượt (chạy 1 lần trên MASTER, rồi restart NiFi).
# Các service khác đã được set nhỏ sẵn trong start_all.sh / run_batch.sh.
set -e
NIFI=~/nifi-1.28.1/conf/bootstrap.conf

echo ">> Giảm NiFi heap -> Xms 512m / Xmx 1024m (lớn nhất, giảm lag nhiều nhất)"
if [ -f "$NIFI" ]; then
  sed -i 's/^java\.arg\.2=-Xms.*/java.arg.2=-Xms512m/' "$NIFI"
  sed -i 's/^java\.arg\.3=-Xmx.*/java.arg.3=-Xmx1024m/' "$NIFI"
  grep -E "^java\.arg\.[23]=" "$NIFI"
else
  echo "   (không thấy $NIFI — sửa tay java.arg.3=-Xmx1024m)"
fi

cat <<'EOF'

>> Đã set sẵn trong script khác (không cần làm gì thêm):
   - Kafka heap 512m            (start_all.sh)
   - Spark Streaming local[1] 768m  (start_all.sh)
   - Batch executor 640m / driver 512m  (run_batch.sh)

>> ÁP DỤNG NiFi:  ~/nifi-1.28.1/bin/nifi.sh restart

>> Nếu VẪN chật RAM, thêm heap Hadoop nhỏ (rồi restart cụm):
   # MASTER: thêm vào hadoop-env.sh / yarn-env.sh
   export HDFS_NAMENODE_OPTS="-Xmx1g"
   export YARN_RESOURCEMANAGER_OPTS="-Xmx768m"
   # MỖI SLAVE: thêm vào hadoop-env.sh / yarn-env.sh
   export HDFS_DATANODE_OPTS="-Xmx512m"
   export YARN_NODEMANAGER_OPTS="-Xmx512m"
EOF
