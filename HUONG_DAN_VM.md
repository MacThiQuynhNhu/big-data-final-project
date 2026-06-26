# Hướng dẫn dựng cụm Hadoop + Spark 3 node trên VMware

Mục tiêu: 1 master + 2 slave, chạy HDFS + YARN + Spark on YARN.
Chiến lược: **cài 1 máy "gốc" (base) cho chuẩn → nhân bản (clone) ra 3 máy** →
chỉ chỉnh hostname/IP từng máy. Tránh cài Ubuntu 3 lần.

```
              master  (NameNode, ResourceManager)
             /      \
        slave1      slave2   (DataNode, NodeManager mỗi máy)
```

## Tài nguyên (máy 16GB RAM)

| VM | RAM | CPU | Đĩa | Vai trò |
|----|-----|-----|-----|---------|
| master | 4 GB | 2 | 25 GB | NameNode + ResourceManager |
| slave1 | 3 GB | 2 | 25 GB | DataNode + NodeManager |
| slave2 | 3 GB | 2 | 25 GB | DataNode + NodeManager |

Tổng 10GB cho VM, chừa 6GB cho Windows. **Đóng Chrome/app nặng khi chạy cụm.**

## Phiên bản dùng (đã kiểm chứng tương thích)

- Ubuntu Server 22.04 LTS (bản Server không GUI cho nhẹ)
- Java 8 (OpenJDK 8)
- Hadoop 3.3.6
- Spark 3.5.1 (bản prebuilt for Hadoop 3)

---

# PHẦN 1 — Chuẩn bị (tải về trên Windows)

1. **Ubuntu Server 22.04 ISO**: ubuntu.com/download/server (~2GB).
2. **VMware Workstation Player** (miễn phí) hoặc Pro.
3. (Tải sau, trên máy ảo) Hadoop & Spark — xem Phần 3.

---

# PHẦN 2 — Tạo máy "base" và cài Ubuntu

1. VMware → **Create a New Virtual Machine** → chọn ISO Ubuntu Server.
2. Đặt RAM 3GB, 2 CPU, đĩa 25GB. Tên máy ảo: `base`.
3. Card mạng: để **NAT** (mặc định — VMnet8). Cụm sẽ nói chuyện qua mạng NAT này.
4. Cài Ubuntu Server: chọn ngôn ngữ English, tạo user tên **`hadoop`** (mật khẩu tùy bạn,
   nhớ kỹ). Khi hỏi, **tích chọn "Install OpenSSH server"**.
5. Cài xong, đăng nhập bằng user `hadoop`.

### Xác định dải mạng NAT (để đặt IP tĩnh)
Trên VMware: **Edit → Virtual Network Editor → chọn VMnet8 (NAT)** → ghi lại
*Subnet IP* (ví dụ `192.168.220.0`) và bấm **NAT Settings** xem *Gateway IP*
(thường `192.168.220.2`). Thay số `220` trong hướng dẫn bằng số thật của bạn.

Ta sẽ gán:
```
master = 192.168.220.10
slave1 = 192.168.220.11
slave2 = 192.168.220.12
gateway= 192.168.220.2
```

---

# PHẦN 3 — Cấu hình máy base (làm 1 lần, trước khi clone)

Đăng nhập máy base, chạy lần lượt:

### 3.1 Cập nhật + cài Java 8
```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y openjdk-8-jdk
java -version          # phải ra 1.8.x
```

### 3.2 File hosts (cả 3 node sẽ giống nhau)
```bash
sudo nano /etc/hosts
```
Thêm các dòng (xóa dòng 127.0.1.1 cũ nếu có):
```
192.168.220.10 master
192.168.220.11 slave1
192.168.220.12 slave2
```

### 3.3 Tạo khóa SSH không mật khẩu (mẹo: làm TRƯỚC khi clone)
```bash
ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
# tắt hỏi xác nhận fingerprint
printf "Host *\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n" > ~/.ssh/config
chmod 600 ~/.ssh/config
```
Vì clone giữ nguyên khóa này, sau khi clone cả 3 máy SSH lẫn nhau không cần mật khẩu.

### 3.4 Tải Hadoop & Spark, đặt vào /opt
```bash
cd /tmp
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
sudo tar -xzf hadoop-3.3.6.tar.gz -C /opt && sudo mv /opt/hadoop-3.3.6 /opt/hadoop
sudo tar -xzf spark-3.5.1-bin-hadoop3.tgz -C /opt && sudo mv /opt/spark-3.5.1-bin-hadoop3 /opt/spark
sudo chown -R hadoop:hadoop /opt/hadoop /opt/spark
```

### 3.5 Biến môi trường
```bash
nano ~/.bashrc
```
Thêm vào cuối:
```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export SPARK_HOME=/opt/spark
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH
```
```bash
source ~/.bashrc
```

### 3.6 Chỉ JAVA_HOME cho Hadoop
```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh
```

### 3.7 Các file cấu hình Hadoop
Tất cả nằm trong `/opt/hadoop/etc/hadoop/`.

**core-site.xml** — `nano $HADOOP_HOME/etc/hadoop/core-site.xml`:
```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://master:9000</value>
  </property>
</configuration>
```

**hdfs-site.xml**:
```xml
<configuration>
  <property><name>dfs.replication</name><value>2</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file:///opt/hadoop/data/nameNode</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file:///opt/hadoop/data/dataNode</value></property>
</configuration>
```

**yarn-site.xml** (đã chỉnh RAM cho slave 3GB):
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

**mapred-site.xml**:
```xml
<configuration>
  <property><name>mapreduce.framework.name</name><value>yarn</value></property>
  <property><name>yarn.app.mapreduce.am.env</name><value>HADOOP_MAPRED_HOME=/opt/hadoop</value></property>
  <property><name>mapreduce.map.env</name><value>HADOOP_MAPRED_HOME=/opt/hadoop</value></property>
  <property><name>mapreduce.reduce.env</name><value>HADOOP_MAPRED_HOME=/opt/hadoop</value></property>
</configuration>
```

**workers** — `nano $HADOOP_HOME/etc/hadoop/workers`, xóa `localhost`, để:
```
slave1
slave2
```

### 3.8 Cấu hình Spark on YARN
```bash
cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh
cat >> $SPARK_HOME/conf/spark-env.sh <<'EOF'
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export YARN_CONF_DIR=/opt/hadoop/etc/hadoop
EOF

cp $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf
cat >> $SPARK_HOME/conf/spark-defaults.conf <<'EOF'
spark.master            yarn
spark.submit.deployMode client
spark.driver.memory     1g
spark.executor.memory   1g
spark.executor.instances 2
EOF
```

**Tắt máy base**: `sudo poweroff`. Giờ base đã sẵn sàng để nhân bản.

---

# PHẦN 4 — Nhân bản thành master, slave1, slave2

Với mỗi máy: VMware → chuột phải `base` → **Manage → Clone → Create a full clone**.
Tạo 3 clone tên: `master`, `slave1`, `slave2`.

> Full clone (không phải linked clone) để mỗi máy độc lập.

Sau đó **bật từng máy** và chỉnh 2 thứ: **hostname** và **IP tĩnh**.

### Trên máy master:
```bash
sudo hostnamectl set-hostname master
sudo nano /etc/netplan/00-installer-config.yaml
```
Sửa thành (interface thường là `ens33`):
```yaml
network:
  version: 2
  ethernets:
    ens33:
      dhcp4: no
      addresses: [192.168.220.10/24]
      routes:
        - to: default
          via: 192.168.220.2
      nameservers:
        addresses: [192.168.220.2, 8.8.8.8]
```
```bash
sudo netplan apply
sudo reboot
```

### Trên slave1: y hệt nhưng
- `sudo hostnamectl set-hostname slave1`
- địa chỉ `192.168.220.11/24`

### Trên slave2:
- `sudo hostnamectl set-hostname slave2`
- địa chỉ `192.168.220.12/24`

### Kiểm tra mạng (từ master)
```bash
ping -c2 slave1
ping -c2 slave2
ssh slave1 hostname     # phải in "slave1", KHÔNG hỏi mật khẩu
ssh slave2 hostname
```
Nếu SSH không hỏi mật khẩu và ping thông → mạng + SSH OK.

---

# PHẦN 5 — Khởi động cụm (chạy trên master)

### 5.1 Format HDFS (CHỈ làm 1 lần, lần đầu)
```bash
hdfs namenode -format
```

### 5.2 Khởi động
```bash
start-dfs.sh      # bật NameNode (master) + DataNode (slaves)
start-yarn.sh     # bật ResourceManager (master) + NodeManager (slaves)
```

### 5.3 Kiểm chứng bằng jps (đây là bằng chứng để demo)
```bash
jps               # trên master: NameNode, SecondaryNameNode, ResourceManager
ssh slave1 jps    # trên slave: DataNode, NodeManager
ssh slave2 jps
```

### 5.4 Web UI (mở trình duyệt trên Windows)
- **HDFS**: http://192.168.220.10:9870 → tab *Datanodes* thấy slave1, slave2
- **YARN**: http://192.168.220.10:8088 → *Nodes* thấy 2 NodeManager

---

# PHẦN 6 — Đưa dữ liệu vào HDFS và chạy phân tích

### 6.1 Copy dữ liệu từ Windows sang master
Mở PowerShell tại thư mục project (Windows có sẵn lệnh scp):
```powershell
scp -r data hadoop@192.168.220.10:/home/hadoop/
scp notebooks/spark_analysis.py hadoop@192.168.220.10:/home/hadoop/
```

### 6.2 Dữ liệu đến từ đâu?
KHÔNG còn nạp file CSV tĩnh. Dữ liệu do `data_generator/source_feeder.py` sinh LIVE cho
cả 5 nguồn → NiFi → Kafka → HDFS `/lake` → Hive. Các job Spark (`spark_to_hive.py`,
`spark_report_hive.py`, `spark_analysis.py`) đều đọc từ **Hive** (`bao_cao`), không đọc CSV.

### 6.3 Chạy luồng batch trên YARN
Theo `HUONG_DAN_CHAY_BATCH.md` (đầy đủ thứ tự bật service + thu thập + nạp Hive + báo cáo):
```bash
PGPASSWORD=erp123 psql -h localhost -U erp -d erp -f data_generator/setup_db.sql   # bảng nguồn (1 lần)
python3 data_generator/source_feeder.py &               # đổ dữ liệu live
# ... NiFi thu thập → /lake ... rồi:
spark-submit --master yarn --deploy-mode client notebooks/spark_to_hive.py
spark-submit --master yarn --deploy-mode client notebooks/spark_analysis.py
```

Theo dõi job tại YARN UI (8088) → thấy application chạy phân tán trên 2 node.

---

# PHẦN 7 — NiFi & Kafka chạy ở đâu?

Cụm 3 VM đã khá nặng. Hai cách:
1. **Đơn giản (khuyên dùng):** chạy NiFi + Kafka **trên master** (cài thêm) hoặc
   chạy bằng Docker **trên Windows host**, rồi để chúng đẩy dữ liệu vào HDFS/cụm.
2. NiFi đẩy file đã làm sạch → `hdfs dfs -put` vào HDFS → Spark đọc.

Với đồ án, cách gọn nhất: NiFi/Kafka demo phần *thu thập–làm sạch* (có thể chạy
Docker trên Windows như `docker-compose.yml`), còn cụm VM lo phần *lưu trữ + xử lý
phân tán*. Khi báo cáo, nêu rõ ranh giới này.

---

# PHẦN 8 — Tắt/bật lại cụm

```bash
# Tắt (trên master)
stop-yarn.sh && stop-dfs.sh
# Lần sau bật lại KHÔNG format nữa, chỉ:
start-dfs.sh && start-yarn.sh
```
Tắt VM: `sudo poweroff` từng máy (slave trước, master sau).

---

# Lỗi thường gặp

- **DataNode không lên / không thấy ở UI 9870:** thường do format HDFS nhiều lần →
  xóa `/opt/hadoop/data` trên cả slave (`rm -rf /opt/hadoop/data/*`) rồi format lại
  trên master 1 lần, start lại.
- **SSH hỏi mật khẩu:** kiểm tra `~/.ssh/authorized_keys` có pubkey, quyền
  `chmod 700 ~/.ssh; chmod 600 ~/.ssh/*`.
- **IP máy bị đổi / ping không thông:** kiểm tra netplan đúng subnet VMnet8, gateway `.2`.
- **YARN job treo ACCEPTED mãi:** thiếu RAM — giảm `spark.executor.memory` còn `512m`
  hoặc `spark.executor.instances 1`.
- **`JAVA_HOME is not set`:** kiểm tra dòng JAVA_HOME trong `hadoop-env.sh`.
- **Sau clone 2 slave cùng IP:** chưa sửa netplan/hostname trên từng máy — làm lại Phần 4.
