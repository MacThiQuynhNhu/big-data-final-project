# Hướng dẫn chạy Luồng 1 — Batch (báo cáo)

Luồng batch theo kiến trúc Lambda (thống nhất qua Kafka):
```
NiFi (5 nguồn) → Kafka → ConsumeKafka → HDFS /lake → Hive (phân vùng) → Spark SQL + MLlib → báo cáo
```
> Cả speed lẫn batch ăn CÙNG một dòng Kafka: streaming đọc trực tiếp topic, batch đọc bản
> đã đổ xuống HDFS `/lake` (qua ConsumeKafka → PutHDFS). Đây là điểm "thống nhất" của kiến trúc.

Tất cả lệnh chạy trên **master** (`hduser@master`), trừ phần NiFi làm trên Web UI.

---

## Bước 1 — Bật service cho giai đoạn thu thập
Bật 2 VM slave trong VMware trước, rồi trên master:
```bash
start-dfs.sh
hdfs dfsadmin -report | grep "Live datanodes"        # phải (2)

~/kafka_2.13-3.7.1/bin/kafka-server-start.sh -daemon ~/kafka_2.13-3.7.1/config/kraft/server.properties
sleep 5

sudo systemctl start postgresql                       # nguồn ERP + E-commerce + Kho

cd ~/big-data-final-project/data
nohup python3 -m http.server 8000 >~/crm_api.log 2>&1 &   # API cho CRM

~/nifi-1.28.1/bin/nifi.sh start                       # đợi ~2 phút

# Tạo bảng nguồn (1 lần) + bật feeder sinh giao dịch LIVE
PGPASSWORD=erp123 psql -h localhost -U erp -d erp -f ~/big-data-final-project/data_generator/setup_db.sql
nohup python3 ~/big-data-final-project/data_generator/source_feeder.py >~/feeder.log 2>&1 &
```

## Bước 2 — Xóa sạch dữ liệu cũ (chạy lại từ đầu)
```bash
hdfs dfs -rm -r -f /lake /clean /user/hive/warehouse/bao_cao.db

cd ~/kafka_2.13-3.7.1
for t in sales-report-clean inventory-events; do
  bin/kafka-topics.sh --delete --topic $t --bootstrap-server localhost:9092
  sleep 2
  bin/kafka-topics.sh --create --topic $t --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
done

# (chỉ khi dùng Derby cục bộ — bỏ qua nếu đã chuyển metastore sang MySQL)
# rm -rf ~/big-data-final-project/metastore_db ~/big-data-final-project/derby.log
```

## Bước 3 — Thu thập LIVE (NiFi 5 nguồn)
Vào NiFi UI `https://192.168.79.131:8443/nifi` → **Start tất cả processor** (Ctrl+A → ▶).
Feeder (đã bật ở Bước 1) tự đổ giao dịch liên tục:
- **POS** (offline): máy bán hàng chi nhánh POST giao dịch lên `http://localhost:9998/pos`
  → NiFi **ListenHTTP** (Listening Port `9998`, Base Path `pos`) nhận liên tục → ReplaceText
  làm sạch (`Doanh_Thu`/`Ngay`) → PublishKafka.
- **ERP** (offline) + **E-commerce** (online) + **Kho** (WMS): feeder insert vào Postgres
  (`sales`, `ecommerce_orders`, `kho_chuyendong`) → QueryDatabaseTable đọc incremental.
- **CRM**: InvokeHTTP gọi API định kỳ → `/clean/crm` (dimension).

NiFi đẩy giao dịch → Kafka `sales-report-clean`, chuyển động kho → `inventory-events`;
ConsumeKafka đổ cả hai xuống HDFS `/lake/transactions` và `/lake/inventory`.

**Sau ~1–2 phút** (đã đủ dữ liệu): vào NiFi **Stop tất cả processor** để chốt một mẻ batch.
*(Có thể dừng feeder: `pkill -f source_feeder.py`.)*

## Bước 4 — Kiểm tra thu thập
```bash
hdfs dfs -ls /lake/transactions /lake/inventory /clean/crm
~/kafka_2.13-3.7.1/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic sales-report-clean
```
✅ Đậu khi: `/lake/transactions` và `/lake/inventory` đều có file; Kafka offset > 0 và tăng dần.

## Bước 5 — Nạp Hive từ HDFS
Chuyển sang giai đoạn phân tích: tắt NiFi/Kafka/API/feeder/Postgres nhường RAM, bật YARN.
(`san_pham` ghi cố định trong code — batch KHÔNG cần Postgres/JDBC.)
```bash
~/nifi-1.28.1/bin/nifi.sh stop
pkill -f "http.server 8000"; pkill -f source_feeder.py
~/kafka_2.13-3.7.1/bin/kafka-server-stop.sh          # batch không cần Kafka

start-yarn.sh
cd ~/big-data-final-project
spark-submit --master yarn notebooks/spark_to_hive.py 2>&1 | tee ~/ketqua_hive.txt
```
✅ Đậu khi: `SHOW PARTITIONS` ra `source=pos/...`, `source=erp/...`, `source=ecommerce/...`;
phần cuối in số bản ghi theo kênh (online/offline) và tồn kho tính từ chuyển động.
> Tồn kho = SUM(qty) sự kiện chuyển động ở `/lake/inventory` (không còn dùng Excel tĩnh).

## Bước 6 — Báo cáo + MLlib
```bash
spark-submit --master yarn notebooks/spark_report_hive.py 2>&1 | tee ~/ketqua_baocao.txt
spark-submit --master yarn notebooks/spark_analysis.py 2>&1 | tee ~/ketqua_mllib.txt
```
> Cả hai đọc từ Hive (`bao_cao`). `spark_report_hive` = báo cáo SQL, `spark_analysis` = MLlib.
✅ Đậu khi: các báo cáo ra số (doanh thu/chi phí/lợi nhuận theo cửa hàng, vùng; top sản phẩm;
lợi nhuận theo sản phẩm; **doanh thu theo kênh online/offline**; **đơn online theo thiết bị/
thanh toán**; cảnh báo tồn kho tổng; **giá trị tồn kho theo giá vốn**) + MLlib dự báo/phân cụm.

---

## Quản lý RAM theo giai đoạn (master 6GB)

| Giai đoạn | Bật | Tắt |
|---|---|---|
| Thu thập (B1–4) | HDFS, Kafka, Postgres, NiFi, API, feeder | YARN |
| Batch: Hive + báo cáo + MLlib (B5–6) | HDFS, YARN, PostgreSQL | Kafka, NiFi, API, feeder |

## Lỗi thường gặp
- `metastore_db already booted` → chạy 2 lần chồng nhau; xóa `metastore_db` rồi chạy lại.
- DataNode = 0 / write HDFS lỗi → bật slave + `start-dfs.sh`, kiểm `hdfs dfsadmin -report`.
- POS không có dữ liệu → feeder chưa chạy / NiFi `ListenHTTP` chưa Start (feeder báo "push lỗi").
- QueryDatabaseTable/InvokeHTTP đẻ nhiều file → chưa Stop sau khi fetch; dùng `dropDuplicates` đã chống trùng phía Spark.
