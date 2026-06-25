# Hướng dẫn chạy Luồng 1 — Batch (báo cáo)

Luồng batch theo kiến trúc Lambda:
```
NiFi (4 nguồn) → HDFS → Hive (phân vùng) → Spark SQL + Spark MLlib → báo cáo
```
> Kafka KHÔNG nằm trong luồng batch — Kafka dành cho nhánh streaming (xem `spark_stream_alert.py`).
> Trong lúc thu thập, NiFi vẫn đẩy giao dịch vào Kafka để dành cho streaming; còn batch đọc từ HDFS.

Tất cả lệnh chạy trên **master** (`hduser@master`), trừ phần NiFi làm trên Web UI.

---

## Bước 1 — Bật service cho giai đoạn thu thập
Bật 2 VM slave trong VMware trước, rồi trên master:
```bash
start-dfs.sh
hdfs dfsadmin -report | grep "Live datanodes"        # phải (2)

~/kafka_2.13-3.7.1/bin/kafka-server-start.sh -daemon ~/kafka_2.13-3.7.1/config/kraft/server.properties
sleep 5

sudo systemctl start postgresql                       # nguồn ERP

cd ~/big-data-final-project/data
nohup python3 -m http.server 8000 >~/crm_api.log 2>&1 &   # API cho CRM

~/nifi-1.28.1/bin/nifi.sh start                       # đợi ~2 phút
```

## Bước 2 — Xóa sạch dữ liệu cũ (chạy lại từ đầu)
```bash
hdfs dfs -rm -r -f /clean /user/hive/warehouse/bao_cao.db

cd ~/kafka_2.13-3.7.1
bin/kafka-topics.sh --delete --topic sales-report-clean --bootstrap-server localhost:9092
sleep 3
bin/kafka-topics.sh --create --topic sales-report-clean --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

rm -rf ~/big-data-final-project/metastore_db ~/big-data-final-project/derby.log
rm -f ~/nifi_input/*
```
> Giữ nguyên `/input/erp_sales.csv` (MLlib dùng). Kiểm: `hdfs dfs -ls /input`
> — thiếu thì `hdfs dfs -put ~/big-data-final-project/data/erp_sales.csv /input/`.

## Bước 3 — Thu thập lại (NiFi 4 nguồn)
Vào NiFi UI `https://192.168.79.131:8443/nifi` → **Start tất cả processor** (Ctrl+A → ▶).
Kích hoạt nguồn file bằng cách copy vào thư mục NiFi canh:
```bash
cp ~/big-data-final-project/data/pos_transactions.json ~/nifi_input/
cp ~/big-data-final-project/data/inventory.xlsx ~/nifi_input/
```
- POS, Kho: GetFile đọc file vừa copy.
- ERP (QueryDatabaseTable) + CRM (InvokeHTTP): tự chạy theo lịch.

**Sau ~1 phút** (mỗi nguồn fetch 1 lần): **Stop `QueryDatabaseTable` và `InvokeHTTP`** trong NiFi
→ tránh fetch lặp đẻ nhiều file trùng.

## Bước 4 — Kiểm tra thu thập
```bash
hdfs dfs -ls /clean/pos /clean/erp /clean/kho /clean/crm
~/kafka_2.13-3.7.1/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic sales-report-clean
```
✅ Đậu khi: 4 thư mục `/clean/*` đều có file; Kafka offset ≈ 19788 (pos 9994 + erp 9794).

## Bước 5 — Nạp Hive từ HDFS
Chuyển sang giai đoạn phân tích: tắt NiFi/Kafka/API nhường RAM, bật YARN.
```bash
~/nifi-1.28.1/bin/nifi.sh stop
pkill -f "http.server 8000"
~/kafka_2.13-3.7.1/bin/kafka-server-stop.sh          # batch không cần Kafka

start-yarn.sh
cd ~/big-data-final-project
spark-submit --master yarn notebooks/spark_to_hive.py 2>&1 | tee ~/ketqua_hive.txt
```
✅ Đậu khi: `SHOW PARTITIONS` ra `source=pos/...`, `source=erp/...`.
> Lưu ý: erp = 9994 (HDFS giữ đủ; RouteOnAttribute chỉ lọc nhánh Kafka). Báo cáo lọc `cost IS NOT NULL` nên vẫn đúng.

## Bước 6 — Báo cáo + MLlib
```bash
spark-submit --master yarn notebooks/spark_report_hive.py 2>&1 | tee ~/ketqua_baocao.txt
DATA_DIR=hdfs://master:9000/input spark-submit --master yarn notebooks/spark_analysis.py 2>&1 | tee ~/ketqua_mllib.txt
```
✅ Đậu khi: 5 báo cáo ra số (doanh thu/chi phí/lợi nhuận, vùng, top sản phẩm, tồn kho, gộp đa nguồn)
+ MLlib ra dự báo doanh thu, phân cụm cửa hàng.

---

## Quản lý RAM theo giai đoạn (master 6GB)

| Giai đoạn | Bật | Tắt |
|---|---|---|
| Thu thập (B1–4) | HDFS, Kafka, Postgres, NiFi, API | YARN |
| Batch: Hive + báo cáo + MLlib (B5–6) | HDFS, YARN | Kafka, NiFi, API |

## Lỗi thường gặp
- `metastore_db already booted` → chạy 2 lần chồng nhau; xóa `metastore_db` rồi chạy lại.
- DataNode = 0 / write HDFS lỗi → bật slave + `start-dfs.sh`, kiểm `hdfs dfsadmin -report`.
- NiFi `Out=0` ở GetFile → file chưa có trong `~/nifi_input` (copy lại).
- QueryDatabaseTable/InvokeHTTP đẻ nhiều file → chưa Stop sau khi fetch; dùng `dropDuplicates` đã chống trùng phía Spark.
