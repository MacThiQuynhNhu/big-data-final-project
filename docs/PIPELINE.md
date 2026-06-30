# PIPELINE — Vận hành (batch + streaming)

Hai cách vận hành: **(A) tự động bằng cron** (giống hệ thống thật) và **(B) chạy demo thủ công
một mẻ** (để trình diễn/nộp bài). Cộng phần **(C) streaming** real-time.

```
NiFi (5 nguồn) → Kafka ─┬─→ ConsumeKafka → HDFS /lake → [run_batch.sh] → Hive → PostgreSQL → Grafana
                        └─→ spark_stream_dashboard.py → rt_* → PostgreSQL → Grafana
```

---

## A. Vận hành tự động (cron) — cách cụm thực sự chạy

### Bật toàn bộ stack
```bash
cd ~/big-data-final-project && bash start_all.sh
```
`start_all.sh` bật: HDFS, YARN, PostgreSQL, Grafana, Kafka, NiFi, CRM API, feeder, streaming.
Sau đó vào **NiFi UI → Start tất cả processor** (feeder → Kafka).

### Cài cron chạy batch mỗi 15 phút
```bash
crontab -e
# thêm dòng:
*/15 * * * * bash -lc 'cd /home/hduser/big-data-final-project && bash run_batch.sh' >> /home/hduser/batch_cron.log 2>&1
```
`run_batch.sh` chạy 5 bước, có **flock** chống 2 mẻ chồng nhau, và tự `export` môi trường
Hadoop (vì cron không nạp `~/.bashrc`):

| Bước | Job | Mức độ | Khi lỗi |
|---|---|---|---|
| [1/5] | `spark_to_hive.py` | **CRITICAL** | Hủy cả batch (mọi bước sau phụ thuộc `sales_report`) |
| [2/5] | `spark_incremental.py` | **CRITICAL** | Ghi lỗi nhưng tiếp tục — phân tích kỳ ưu tiên cao |
| [3/5] | `spark_report_hive.py` | best-effort | Bỏ qua, không chặn bước sau |
| [4/5] | `spark_analysis.py` (MLlib) | best-effort | Bỏ qua, chạy lại lần sau |
| [5/5] | `spark_marts_to_pg.py` | **CRITICAL** | Luôn chạy — đẩy `bc_*`/`agg_*` sang PostgreSQL |

Theo dõi:
```bash
tail -f ~/batch_cron.log            # mỗi mẻ kết thúc bằng dòng "XONG"
grep -c XONG ~/batch_cron.log       # số mẻ thành công (tăng dần mỗi 15')
bash check_status.sh                # nhanh: agg counts + lake file + lock
```

### Cuộn bậc thang (cascade) — vì sao chỉ chạy 1 lần/kỳ
`spark_incremental.py`: `cur = MAX(txn_date)`; chỉ tính kỳ **đã đóng** (`ky < mốc-của-cur`) **và**
chưa xử lý (`ky > MAX(ky) trong bảng đích` — watermark), rồi **APPEND**. Cuộn:
raw→`agg_ngay`, `agg_ngay`→`agg_tuan`/`agg_thang`, `agg_thang`→`agg_quy`/`agg_nam`.
→ Mỗi kỳ tính đúng một lần khi đóng; chạy lại 100 lần kết quả không đổi (idempotent).

### Ingest tăng dần (incremental) — vì sao batch không chậm dần
`spark_to_hive.py` chỉ đọc file MỚI trong `/lake/transactions`, **APPEND** vào `sales_report`,
rồi `hdfs mv` file đã xử lý sang `transactions_archive`. Dù chạy bao lâu, mỗi mẻ chỉ xử lý
lượng nhỏ phát sinh.

---

## B. Chạy demo thủ công (một mẻ)
Khi muốn trình diễn theo "giai đoạn" để tiết kiệm RAM (thu thập ↔ phân tích tách nhau):

```bash
# 1) Thu thập: bật HDFS, Kafka, Postgres, NiFi, feeder ~1-2 phút cho data land xuống /lake
#    (NiFi UI: Start processor; kiểm hdfs dfs -ls /lake/transactions có file)
# 2) Chạy batch:
bash run_batch.sh
# hoặc từng bước riêng để debug:
bash run_incremental_only.sh     # chỉ bước [2] (test idempotency)
bash run_marts_only.sh           # chỉ bước [5] (đẩy lại sang Grafana)
```

---

## C. Streaming real-time (speed layer)
`spark_stream_dashboard.py` đọc Kafka `sales-report-clean`, mỗi micro-batch (~vài giây) ghi:
- `rt_thongke` — doanh thu/lợi nhuận/số giao dịch
- `rt_canhbao` — giao dịch lỗ (profit < ngưỡng)

Bật riêng (nếu chết giữa chừng):
```bash
bash start_streaming.sh
# hoặc:
nohup spark-submit --master local[1] --driver-memory 768m \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 \
  --jars /home/hduser/postgresql-42.7.3.jar notebooks/spark_stream_dashboard.py >~/streaming.log 2>&1 &
```
> ⚠️ **`rt_*` ghi theo GIỜ THẬT** (lúc xử lý), khác `txn_date` nghiệp vụ của batch. Trên Grafana
> dashboard streaming dùng range tương đối **"Last 15 minutes"** để thấy data chạy real-time.
> (Batch dashboard dùng ngày nghiệp vụ 2026-07 → ...). Đây là bản chất speed layer vs batch layer.

---

## Quản lý RAM (master 6GB)
| Tình huống | Khuyến nghị |
|---|---|
| Chạy tất cả (cron + streaming + NiFi + Kafka) | Được, nhưng sát RAM — `tune_ram.sh` giảm heap NiFi |
| Backfill nặng / nhiều file `/lake` tồn đọng | Tạm tắt streaming + feeder cho batch có chỗ |
| Demo phân tích | Có thể tắt Kafka/NiFi/feeder, chỉ giữ HDFS+YARN+Postgres |

## Lỗi thường gặp
- **Cron lỗi `HADOOP_CONF_DIR must be set`** → `run_batch.sh` trên VM chưa có khối `export` env
  (cron không nạp `~/.bashrc`). Đảm bảo `grep -c HADOOP_CONF_DIR run_batch.sh` > 0.
- **`spark_to_hive` lỗi `0 datanode(s)` / `No space left`** → đĩa slave đầy (xem SETUP §Lỗi).
- **Grafana không thấy số mới (batch)** → marts chưa đẩy: `bash run_marts_only.sh`.
- **POS không có data** → feeder chưa chạy hoặc NiFi ListenHTTP chưa Start (feeder báo "push lỗi").
- **agg_* trùng kỳ sau reset** → HDFS warehouse còn data cũ; `reset_all.sh` đã xóa
  `/user/hive/warehouse/bao_cao.db` để tránh (vì `agg_*` ghi append).
