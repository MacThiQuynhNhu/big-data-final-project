"""
Đọc dataset THẬT từ Kaggle (Superstore), map về schema chuẩn, rồi tách ra
4 nguồn / 4 định dạng để mô phỏng hệ thống đa nguồn.

Chuẩn bị:
  1) Tải "Sample - Superstore.csv" từ Kaggle, đặt vào: data/raw/superstore.csv
     (hoặc: kaggle datasets download -d vivek468/superstore-dataset-final -p data/raw --unzip)
  2) python prepare_from_kaggle.py                 # dùng nguyên dữ liệu thật
     python prepare_from_kaggle.py --target 200000 # gen thêm cho đủ volume

Cột Superstore: Order ID, Order Date, Region, State, City, Segment,
                Customer ID, Customer Name, Category, Sub-Category,
                Product ID, Product Name, Sales, Quantity, Discount, Profit
"""
import argparse
import json
import os
import random
import sys

import numpy as np
import pandas as pd

try:                                    # cho phép in tiếng Việt trên console Windows
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

random.seed(42)
np.random.seed(42)

HERE = os.path.dirname(__file__)
RAW = os.path.join(HERE, "..", "data", "raw", "superstore.csv")
OUT = os.path.join(HERE, "..", "data")


def load_real():
    # Superstore thường mã hóa latin-1
    for enc in ("utf-8", "latin-1"):
        try:
            df = pd.read_csv(RAW, encoding=enc)
            break
        except (UnicodeDecodeError, FileNotFoundError) as e:
            if isinstance(e, FileNotFoundError):
                raise SystemExit(
                    f"Chưa thấy {RAW}. Tải Superstore từ Kaggle và đặt vào đó.")
            continue

    df.columns = [c.strip() for c in df.columns]
    # Map về schema chuẩn của dự án
    out = pd.DataFrame({
        "txn_id": df["Order ID"].astype(str) + "_" + df.index.astype(str),
        "store_id": df["State"],                       # mỗi bang = 1 chi nhánh
        "region": df["Region"],
        "product_id": df["Product ID"],
        "product_name": df["Product Name"],
        "qty": df["Quantity"],
        "revenue": df["Sales"].round(0),
        "cost": (df["Sales"] - df["Profit"]).round(0),  # cost = doanh thu - lợi nhuận
        "promotion": (df["Discount"] > 0).astype(int),  # có giảm giá = khuyến mãi
        "txn_date": pd.to_datetime(df["Order Date"]).dt.date.astype(str),
        "customer_id": df["Customer ID"],
        "customer_name": df["Customer Name"],
        "segment": df["Segment"],
    })
    return out


def augment(df, target):
    """Gen thêm dòng bằng cách lấy mẫu lại từ dữ liệu thật + nhiễu nhẹ,
    giữ nguyên phân phối sản phẩm/vùng/khách hàng thật."""
    if target <= len(df):
        return df
    n_extra = target - len(df)
    extra = df.sample(n=n_extra, replace=True, random_state=1).copy()
    # nhiễu nhẹ trên qty và revenue để không trùng lặp y hệt
    jitter = np.random.uniform(0.85, 1.15, size=n_extra)
    extra["qty"] = np.maximum(1, (extra["qty"] * jitter).round()).astype(int)
    extra["revenue"] = (extra["revenue"] * jitter).round(0)
    extra["cost"] = (extra["cost"] * jitter).round(0)
    extra["txn_id"] = ["G{:07d}".format(i) for i in range(n_extra)]
    return pd.concat([df, extra], ignore_index=True)


def split_sources(df):
    os.makedirs(OUT, exist_ok=True)

    # POS -> JSON (tên cột "lộn xộn" để NiFi làm sạch)
    pos = df[["txn_id", "store_id", "product_id", "qty", "revenue",
              "promotion", "txn_date", "customer_id"]].rename(
        columns={"revenue": "Doanh_Thu", "txn_date": "Ngay"})
    with open(os.path.join(OUT, "pos_transactions.json"), "w", encoding="utf-8") as f:
        json.dump(pos.to_dict(orient="records"), f, ensure_ascii=False)

    # ERP -> CSV (chèn ít giá trị thiếu để minh họa làm sạch)
    erp = df[["txn_id", "store_id", "region", "revenue", "cost", "txn_date"]].copy()
    miss = erp.sample(frac=0.02, random_state=1).index
    erp.loc[miss, "cost"] = None
    erp.to_csv(os.path.join(OUT, "erp_sales.csv"), index=False, encoding="utf-8")

    # CRM -> JSON (khách hàng thật từ Superstore)
    crm = (df[["customer_id", "customer_name", "segment", "region"]]
           .drop_duplicates("customer_id"))
    with open(os.path.join(OUT, "crm_customers.json"), "w", encoding="utf-8") as f:
        json.dump(crm.to_dict(orient="records"), f, ensure_ascii=False)

    # Kho hàng -> Excel (tồn kho gen thêm: dataset không có)
    pairs = df[["store_id", "product_id"]].drop_duplicates()
    pairs = pairs.copy()
    pairs["stock_qty"] = np.random.randint(0, 500, size=len(pairs))
    pairs["reorder_level"] = np.random.choice([20, 50, 100], size=len(pairs))
    pairs.to_excel(os.path.join(OUT, "inventory.xlsx"), index=False)
    pairs.to_csv(os.path.join(OUT, "inventory.csv"), index=False, encoding="utf-8")

    return len(pos), len(crm), len(pairs)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", type=int, default=0,
                    help="tổng số dòng mong muốn (gen thêm nếu lớn hơn dữ liệu thật)")
    args = ap.parse_args()

    df = load_real()
    print(f"Dữ liệu thật: {len(df)} dòng.")
    if args.target:
        df = augment(df, args.target)
        print(f"Sau khi gen thêm: {len(df)} dòng.")
    n_pos, n_crm, n_inv = split_sources(df)
    print("Đã tạo 4 nguồn trong data/:")
    print(f"  - pos_transactions.json  ({n_pos} giao dịch)")
    print(f"  - erp_sales.csv          (ERP)")
    print(f"  - crm_customers.json     ({n_crm} khách hàng thật)")
    print(f"  - inventory.xlsx         ({n_inv} dòng tồn kho)")
