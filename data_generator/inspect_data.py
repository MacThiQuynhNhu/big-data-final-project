"""
Soi nhanh 4 nguồn dữ liệu đã tạo: vài dòng mẫu + thống kê tổng quan.
Chạy:  python inspect_data.py
"""
import json
import os
import sys

import pandas as pd

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)

D = os.path.join(os.path.dirname(__file__), "..", "data")


def line(t=""):
    print("\n" + "=" * 70)
    if t:
        print(t)
        print("=" * 70)


# ---------- POS (JSON) ----------
pos = pd.read_json(os.path.join(D, "pos_transactions.json"))
line("POS · pos_transactions.json (JSON)")
print("Số dòng:", len(pos), "| Cột:", list(pos.columns))
print(pos.head(3).to_string(index=False))

# ---------- ERP (CSV) ----------
erp = pd.read_csv(os.path.join(D, "erp_sales.csv"))
line("ERP · erp_sales.csv (CSV)")
print("Số dòng:", len(erp), "| Cột:", list(erp.columns))
print(erp.head(3).to_string(index=False))
print("Số dòng thiếu cost (cần loại ở NiFi):", int(erp["cost"].isna().sum()))

# ---------- CRM (JSON) ----------
crm = pd.read_json(os.path.join(D, "crm_customers.json"))
line("CRM · crm_customers.json (JSON)")
print("Số khách:", len(crm), "| Cột:", list(crm.columns))
print(crm.head(3).to_string(index=False))
print("Phân khúc:", crm["segment"].value_counts().to_dict())

# ---------- Kho hàng (Excel) ----------
inv = pd.read_excel(os.path.join(D, "inventory.xlsx"))
line("Kho hàng · inventory.xlsx (Excel)")
print("Số dòng:", len(inv), "| Cột:", list(inv.columns))
print(inv.head(3).to_string(index=False))

# ---------- Thống kê tổng quan (dùng ERP làm trục giao dịch) ----------
line("THỐNG KÊ TỔNG QUAN")
erp["txn_date"] = pd.to_datetime(erp["txn_date"])
erp["profit"] = erp["revenue"] - erp["cost"]
print("Khoảng thời gian:", erp["txn_date"].min().date(), "→", erp["txn_date"].max().date())
print("Số cửa hàng (store_id/bang):", erp["store_id"].nunique())
print("Số vùng (region):", erp["region"].nunique(), "->", sorted(erp["region"].dropna().unique()))
print("Số sản phẩm (POS):", pos["product_id"].nunique())
print("Số khách hàng:", crm["customer_id"].nunique())
print("Tỉ lệ giao dịch có khuyến mãi:", f"{pos['promotion'].mean()*100:.1f}%")
print("\nTổng doanh thu : {:,.0f}".format(erp["revenue"].sum()))
print("Tổng chi phí   : {:,.0f}".format(erp["cost"].sum()))
print("Tổng lợi nhuận : {:,.0f}".format(erp["profit"].sum()))
print("Số giao dịch LỖ (profit<0): {} ({:.1f}%)".format(
    int((erp["profit"] < 0).sum()), (erp["profit"] < 0).mean() * 100))
print("\nPhân phối doanh thu mỗi dòng:")
print(erp["revenue"].describe()[["min", "25%", "50%", "75%", "max"]].round(1).to_string())

line("TOP 5 CỬA HÀNG THEO DOANH THU")
top = (erp.groupby("store_id")["revenue"].sum().sort_values(ascending=False).head(5))
print(top.apply(lambda x: f"{x:,.0f}").to_string())

line("DOANH THU THEO QUÝ")
erp["quarter"] = erp["txn_date"].dt.to_period("Q").astype(str)
print(erp.groupby("quarter")["revenue"].sum().apply(lambda x: f"{x:,.0f}").to_string())
