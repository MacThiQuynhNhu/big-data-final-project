# SETUP — Dựng cụm + cài đặt thành phần

Hướng dẫn dựng môi trường từ đầu: cụm Hadoop 3 node + Spark + NiFi + Kafka + Hive + PostgreSQL
+ Grafana. Làm **một lần**. Cụm thực tế của đồ án dùng các giá trị dưới đây.

```
              master  (NameNode, ResourceManager, NiFi, Kafka, PostgreSQL, Grafana, Hive)
             /      \
        slave1      slave2   (DataNode, NodeManager mỗi máy)
```

| Hạng mục | Giá trị thật |
|---|---|
| OS | Ubuntu Server 22.04 |
| User | **`hduser`** |
| Java | OpenJDK **8** (có thể có cả 11; batch ép java-8 qua `JAVA_HOME`) |
| Hadoop | **3.3.0** tại `/usr/local/hadoop` |
| Spark | **3.1.1** (prebuilt for Hadoop 3) tại `/usr/local/spark` |
| RAM | master 6GB · slave1 3GB · slave2 3GB |
| Mạng | hostname `master/slave1/slave2` qua `/etc/hosts`; master IP host-only **192.168.79.131** (cho Web UI) |

> Mẹo: cài 1 máy **base** cho chuẩn → clone ra 3 máy → chỉ chỉnh hostname + IP. Tránh cài Ubuntu 3 lần.

---

## 1. Máy base (làm 1 lần, trước khi clone)

```bash
sudo apt update && sudo apt install -y openjdk-8-jdk
# /etc/hosts (cả 3 node giống nhau) — thay IP theo dải mạng của bạn
#   <ip-master> master
#   <ip-slave1> slave1
#   <ip-slave2> slave2

# SSH không mật khẩu (clone giữ nguyên khóa → 3 máy ssh nhau không cần pass)
ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys
printf "Host *\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n" > ~/.ssh/config && chmod 600 ~/.ssh/config
```

### Tải Hadoop + Spark vào `/usr/local`
```bash
cd /tmp
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.0/hadoop-3.3.0.tar.gz
wget https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz
sudo tar -xzf hadoop-3.3.0.tar.gz -C /usr/local && sudo mv /usr/local/hadoop-3.3.0 /usr/local/hadoop
sudo tar -xzf spark-3.1.1-bin-hadoop3.2.tgz -C /usr/local && sudo mv /usr/local/spark-3.1.1-bin-hadoop3.2 /usr/local/spark
sudo chown -R hduser:hduser /usr/local/hadoop /usr/local/spark
```

### Biến môi trường — thêm vào `~/.bashrc`
```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin
```
```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh
```
> ⚠️ `~/.bashrc` chỉ nạp cho shell tương tác. **Cron/SSH không tương tác KHÔNG nạp** các biến này
> → vì vậy `run_batch.sh` tự `export` lại HADOOP_CONF_DIR/SPARK_HOME/PATH ở đầu file.

### Cấu hình Hadoop (`/usr/local/hadoop/etc/hadoop/`)
**core-site.xml**
```xml
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://master:9000</value></property>
</configuration>
```
**hdfs-site.xml**
```xml
<configuration>
  <property><name>dfs.replication</name><value>2</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file:///app/hadoop/hdfs/namenode</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file:///app/hadoop/hdfs/datanode</value></property>
</configuration>
```
**yarn-site.xml** (RAM cho slave 3GB)
```xml
<configuration>
  <property><name>yarn.resourcemanager.hostname</name><value>master</value></property>
  <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
  <property><name>yarn.nodemanager.resource.memory-mb</name><value>2560</value></property>
  <property><name>yarn.scheduler.maximum-allocation-mb</name><value>2560</value></property>
  <property><name>yarn.scheduler.minimum-allocation-mb</name><value>256</value></property>
  <property><name>yarn.nodemanager.vmem-check-enabled</name><value>false</value></property>
</configuration>
```
**mapred-site.xml**: `mapreduce.framework.name = yarn` (+ các `*.env = HADOOP_MAPRED_HOME=/usr/local/hadoop`).
**workers**: hai dòng `slave1` / `slave2` (xóa `localhost`).

### Spark on YARN — `$SPARK_HOME/conf/`
```bash
cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh
cat >> $SPARK_HOME/conf/spark-env.sh <<'EOF'
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
export YARN_CONF_DIR=/usr/local/hadoop/etc/hadoop
EOF
```
RAM nhỏ: batch chạy `--executor-memory 640m --driver-memory 512m --num-executors 2` (đặt trong `run_batch.sh`, không để mặc định 1g).

---

## 2. Clone → master / slave1 / slave2
Full clone máy base ra 3 máy. Mỗi máy chỉnh **hostname** (`hostnamectl set-hostname ...`) + **IP tĩnh**
(netplan) đúng với `/etc/hosts`. Kiểm: `ssh slave1 hostname` không hỏi mật khẩu.

## 3. Khởi động cụm (trên master)
```bash
hdfs namenode -format        # CHỈ lần đầu
start-dfs.sh                 # NameNode (master) + DataNode (slaves)
start-yarn.sh                # ResourceManager + NodeManager
jps                          # master: NameNode, ResourceManager, SecondaryNameNode
hdfs dfsadmin -report | grep "Live datanodes"   # phải (2)
```

---

## 4. Cài các thành phần (trên master)

### PostgreSQL (nguồn ERP/Ecom/Kho + serving cho Grafana)
```bash
sudo apt install -y postgresql
sudo -u postgres psql -c "CREATE USER erp WITH PASSWORD 'erp123' SUPERUSER;"
sudo -u postgres psql -c "CREATE DATABASE erp OWNER erp;"
# cho phép kết nối md5 từ localhost (pg_hba.conf) rồi: sudo systemctl restart postgresql
PGPASSWORD=erp123 psql -h localhost -U erp -d erp -f ~/big-data-final-project/data_generator/setup_db.sql
```

### Hive metastore trên MySQL (kho dữ liệu bền)
- Cài MySQL, tạo DB `hivedb` + user, copy `mysql-connector-*.jar` vào `$SPARK_HOME/jars/`.
- Đặt `hive-site.xml` (trỏ metastore JDBC MySQL) vào `$SPARK_HOME/conf/` → Spark dùng chung
  metastore này (database `bao_cao`, warehouse `hdfs://master:9000/user/hive/warehouse`).
> Spark dùng Hive support qua metastore MySQL; KHÔNG dùng Derby cục bộ (tránh hỏng khi chạy chồng).

### Kafka 3.7.1 (KRaft — không cần ZooKeeper)
```bash
# giải nén kafka_2.13-3.7.1 vào ~ ; cấu hình log.dirs = ~/kafka-data (tránh /tmp mất sau reboot)
~/kafka_2.13-3.7.1/bin/kafka-storage.sh format -t <uuid> -c ~/kafka_2.13-3.7.1/config/kraft/server.properties
~/kafka_2.13-3.7.1/bin/kafka-server-start.sh -daemon ~/kafka_2.13-3.7.1/config/kraft/server.properties
# 2 topic:
for t in sales-report-clean inventory-events; do
  ~/kafka_2.13-3.7.1/bin/kafka-topics.sh --create --topic $t --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
done
```

### NiFi 1.28.1
- Giải nén `nifi-1.28.1` vào `~`; flow lưu ở `~/nifi-1.28.1/conf` (tắt/bật không mất).
- Import flow từ `thu-thap-da-nguon.xml` hoặc dựng tay theo [NIFI_FLOWS.md](NIFI_FLOWS.md).
- `~/nifi-1.28.1/bin/nifi.sh start` → UI `https://192.168.79.131:8443/nifi` (~2 phút mới lên).
- RAM nhỏ: giảm heap NiFi bằng `tune_ram.sh`.

### Grafana
```bash
sudo apt-get install -y software-properties-common wget
wget -q -O - https://apt.grafana.com/gpg.key | gpg --dearmor | sudo tee /etc/apt/keyrings/grafana.gpg >/dev/null
echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://apt.grafana.com stable main" | sudo tee /etc/apt/sources.list.d/grafana.list
sudo apt-get update && sudo apt-get install -y grafana && sudo systemctl enable --now grafana-server
```
→ `http://192.168.79.131:3000` (admin/admin). Thêm data source PostgreSQL (host `localhost:5432`,
db `erp`, user `erp`/`erp123`, SSL disable). Tạo 2 dashboard: xem [RESULTS.md](RESULTS.md).

---

## 5. Tắt / bật lại cụm
```bash
stop-yarn.sh && stop-dfs.sh      # tắt; tắt VM: poweroff slave trước, master sau
start-dfs.sh && start-yarn.sh    # bật lại (KHÔNG format nữa)
```

## Lỗi thường gặp
- **DataNode = 0 / write HDFS lỗi** → bật slave + `start-dfs.sh`; nếu format nhiều lần: xóa
  `dataNode` dir trên slave rồi format lại master 1 lần.
- **`No space left on device`** → đĩa slave đầy bởi scratch YARN/Spark: dọn
  `/tmp/hadoop-hduser/nm-local-dir/usercache/hduser/filecache/*` (thư viện Spark upload mỗi job).
  Khắc phục gốc: đặt `spark.yarn.archive` (đẩy spark libs lên HDFS 1 lần).
- **YARN job treo ACCEPTED** → thiếu RAM: giảm `--executor-memory`/`--num-executors`.
- **`JAVA_HOME is not set`** → kiểm dòng JAVA_HOME trong `hadoop-env.sh` + `run_batch.sh`.
- **Kafka mất metadata sau reboot** → `log.dirs` đang ở `/tmp`; chuyển sang `~/kafka-data`.
