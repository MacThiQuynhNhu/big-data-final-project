"""
Mô phỏng giao dịch LIVE đổ về CÁC NGUỒN (để NiFi ingest liên tục → Kafka).
Mỗi vòng sinh 1 lô giao dịch, ghi vào CẢ HAI nguồn (cùng txn_id để join được):
  - POS  -> file JSON nhỏ trong thư mục NiFi canh   (NiFi GetFile)
  - ERP  -> insert dòng vào PostgreSQL bảng sales    (NiFi QueryDatabaseTable incremental)

Cài: pip3 install psycopg2-binary
Chạy: python3 source_feeder.py        (Ctrl+C để dừng)
"""
import json
import os
import random
import time
from datetime import datetime

import psycopg2

POS_DIR = "/home/hduser/nifi_input"
os.makedirs(POS_DIR, exist_ok=True)

STORES = [("California", "West"), ("New York", "East"), ("Texas", "Central"),
          ("Washington", "West"), ("Pennsylvania", "East"), ("Florida", "South"),
          ("Illinois", "Central"), ("Ohio", "East"), ("Virginia", "South"),
          ("Arizona", "West"), ("Georgia", "South"), ("Michigan", "Central")]
PRODUCTS = ["TEC-CO-10004722", "OFF-BI-10003527", "FUR-CH-10002024",
            "OFF-BI-10001359", "TEC-MA-10001127", "OFF-SU-10000151",
            "FUR-BO-10001798", "OFF-PA-10001970", "TEC-AC-10002049"]

conn = psycopg2.connect(host="localhost", dbname="erp", user="erp", password="erp123")
conn.autocommit = True
cur = conn.cursor()

print(">>> Đang đổ giao dịch về POS (file) + ERP (database)... (Ctrl+C để dừng)")
i = 0
try:
    while True:
        n = random.randint(5, 15)                      # 1 lô vài giao dịch
        pos_batch = []
        ts = datetime.now().strftime("%Y%m%d%H%M%S%f")
        today = datetime.now().strftime("%Y-%m-%d")
        for _ in range(n):
            txn_id = f"LIVE{i:09d}"
            store, region = random.choice(STORES)
            revenue = round(random.uniform(50, 5000), 0)
            if random.random() < 0.15:
                cost = round(revenue * random.uniform(1.1, 1.8), 0)   # lỗ
            else:
                cost = round(revenue * random.uniform(0.55, 0.9), 0)  # lãi
            # POS: tên cột "bẩn" (Doanh_Thu/Ngay) để NiFi ReplaceText làm sạch
            pos_batch.append({
                "txn_id": txn_id, "store_id": store,
                "product_id": random.choice(PRODUCTS),
                "qty": random.randint(1, 10), "Doanh_Thu": revenue,
                "promotion": random.randint(0, 1), "Ngay": today})
            # ERP: insert vào Postgres (cùng txn_id, có cost/region)
            cur.execute(
                "INSERT INTO sales (txn_id, store_id, region, revenue, cost, txn_date) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (txn_id, store, region, revenue, cost, today))
            i += 1
        # ghi 1 file POS cho cả lô
        with open(os.path.join(POS_DIR, f"pos_{ts}.json"), "w", encoding="utf-8") as f:
            json.dump(pos_batch, f, ensure_ascii=False)
        print(f"  đổ {n} giao dịch (tổng {i}) → POS file + ERP rows")
        time.sleep(random.uniform(3, 6))               # vài giây/lô
except KeyboardInterrupt:
    cur.close()
    conn.close()
    print(f"\nDừng. Tổng đã đổ: {i} giao dịch.")
