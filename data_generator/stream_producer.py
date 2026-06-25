"""
Producer mô phỏng POS/ERP hoạt động THẬT: sinh giao dịch liên tục bắn vào Kafka.
Đa số có lãi, ~15% lỗ (để dashboard có cảnh báo).

Cài: pip3 install kafka-python
Chạy:  python3 stream_producer.py          (Ctrl+C để dừng)
"""
import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

STORES = [
    ("California", "West"), ("New York", "East"), ("Texas", "Central"),
    ("Washington", "West"), ("Pennsylvania", "East"), ("Florida", "South"),
    ("Illinois", "Central"), ("Ohio", "East"), ("Michigan", "Central"),
    ("Virginia", "South"), ("Arizona", "West"), ("Georgia", "South"),
]
PRODUCTS = ["TEC-CO-10004722", "OFF-BI-10003527", "FUR-CH-10002024",
            "OFF-BI-10001359", "TEC-MA-10001127", "OFF-SU-10000151",
            "FUR-BO-10001798", "OFF-PA-10001970", "TEC-AC-10002049"]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"))

print(">>> Đang bắn giao dịch vào Kafka topic sales-report-clean... (Ctrl+C để dừng)")
i = 0
try:
    while True:
        store, region = random.choice(STORES)
        revenue = round(random.uniform(50, 5000), 0)
        # đa số có lãi (cost 60-90% doanh thu); ~15% lỗ (cost > doanh thu)
        if random.random() < 0.15:
            cost = round(revenue * random.uniform(1.1, 1.8), 0)      # lỗ
        else:
            cost = round(revenue * random.uniform(0.55, 0.9), 0)     # lãi
        msg = {
            "source": "erp",
            "txn_id": f"RT{i:08d}",
            "store_id": store,
            "region": region,
            "product_id": random.choice(PRODUCTS),
            "revenue": revenue,
            "cost": cost,
            "promotion": random.randint(0, 1),
            "txn_date": datetime.now().strftime("%Y-%m-%d"),
        }
        producer.send("sales-report-clean", msg)
        i += 1
        if i % 20 == 0:
            print(f"  đã gửi {i} giao dịch...")
        time.sleep(random.uniform(0.2, 0.8))     # vài giao dịch mỗi giây
except KeyboardInterrupt:
    producer.flush()
    print(f"\nDừng. Tổng đã gửi: {i} giao dịch.")
