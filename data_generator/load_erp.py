"""
Nạp file erp_sales.csv vào PostgreSQL để đóng vai "hệ thống ERP".
NiFi sẽ dùng QueryDatabaseTable đọc từ bảng này.

Chạy SAU khi docker-compose đã chạy:  python load_erp.py
"""
import os
import sys

import pandas as pd
from sqlalchemy import create_engine

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

CSV = os.path.join(os.path.dirname(__file__), "..", "data", "erp_sales.csv")

# Khớp với thông tin trong docker-compose.yml
engine = create_engine("postgresql+psycopg2://erp:erp123@localhost:5432/erp")

df = pd.read_csv(CSV)
df.to_sql("sales", engine, if_exists="replace", index=False)
print(f"Đã nạp {len(df)} dòng vào bảng erp.sales (PostgreSQL).")
