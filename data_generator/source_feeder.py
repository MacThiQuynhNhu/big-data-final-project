"""
Mô phỏng dữ liệu LIVE đổ về CẢ 5 NGUỒN (để NiFi ingest liên tục). Không nguồn nào tĩnh.

  KÊNH OFFLINE (cửa hàng vật lý):
  - POS  -> MỖI MÁY BÁN HÀNG PUSH 1 HÓA ĐƠN (invoice gồm nhiều DÒNG sản phẩm) qua API HTTP
           lên trung tâm (NiFi ListenHTTP). Đúng thực tế: POS đám mây đẩy hóa đơn real-time.
  - ERP  -> insert TỪNG DÒNG hóa đơn vào PostgreSQL bảng sales (NiFi QueryDatabaseTable).
           revenue = qty*giá bán ; cost = qty*unit_cost (COGS lấy từ san_pham).

  KÊNH ONLINE (thương mại điện tử - website + app):
  - ECOM -> insert đơn online vào ecommerce_orders (có cost = COGS để tính lãi theo kênh).

  KHO TỔNG (1 kho trung tâm, dùng chung 2 kênh):
  - WMS  -> SỰ KIỆN chuyển động vào kho_chuyendong; cost = |qty|*unit_cost (LINK tài chính).
           Bán = xuat (COGS) ; restock = nhap (tiền mua). Tồn = SUM(qty) theo sản phẩm.

  KHÁCH HÀNG:
  - CRM  -> ghi danh sách khách ra crm_customers.json (API HTTP phục vụ); có khách mới dần.

  GIÁ VỐN: bảng san_pham (product_id, unit_cost, reorder_level) -> nối KHO <-> TÀI CHÍNH.

Tạo bảng trước: PGPASSWORD=erp123 psql -h localhost -U erp -d erp -f setup_db.sql
Cài: pip3 install psycopg2-binary
Chạy: python3 source_feeder.py        (Ctrl+C để dừng)
"""
import json
import os
import random
import time
import urllib.request
from datetime import datetime, timedelta

import psycopg2

POS_API = "http://localhost:9998/pos"   # NiFi ListenHTTP nhận POST từ máy bán hàng chi nhánh
CRM_FILE = "/home/hduser/big-data-final-project/data/crm_customers.json"   # API phục vụ file này
os.makedirs(os.path.dirname(CRM_FILE), exist_ok=True)

STORES = [("California", "West"), ("New York", "East"), ("Texas", "Central"),
          ("Washington", "West"), ("Pennsylvania", "East"), ("Florida", "South"),
          ("Illinois", "Central"), ("Ohio", "East"), ("Virginia", "South"),
          ("Arizona", "West"), ("Georgia", "South"), ("Michigan", "Central")]
SEGMENTS = ["Consumer", "Corporate", "Home Office"]
DEVICES = ["web", "app"]
PAYMENTS = ["card", "ewallet", "cod"]

conn = psycopg2.connect(host="localhost", dbname="erp", user="erp", password="erp123")
conn.autocommit = True
cur = conn.cursor()

# ----- GIÁ VỐN: nạp danh mục sản phẩm (cần chạy setup_db.sql trước) -----
cur.execute("SELECT product_id, unit_cost, reorder_level FROM san_pham")
COST, REORDER = {}, {}
for _pid, _uc, _rl in cur.fetchall():
    COST[_pid] = float(_uc); REORDER[_pid] = int(_rl)
if not COST:
    raise SystemExit("Bảng san_pham trống. Chạy: PGPASSWORD=erp123 psql -h localhost -U erp -d erp -f setup_db.sql")
PRODUCTS = list(COST.keys())


def sale_price(unit_cost):
    """Giá bán = giá vốn * markup. ~15% trường hợp markup<1 (bán lỗ)."""
    markup = random.uniform(0.7, 0.95) if random.random() < 0.15 else random.uniform(1.1, 2.0)
    return round(unit_cost * markup, 0)


def biz_date():
    """Ngày giao dịch rải ~9 tháng gần nhất (lệch về gần đây) -> batch có lịch sử nhiều
    tháng + xu hướng tăng cho dự báo. (Streaming dùng giờ xử lý nên không bị ảnh hưởng.)"""
    return (datetime.now() - timedelta(days=int(random.triangular(0, 270, 0)))).strftime("%Y-%m-%d")


# ----- CRM: danh sách khách hàng (dimension động, feeder sở hữu) -----
def new_customer(n):
    _store, region = random.choice(STORES)
    return {"customer_id": f"CUST{n:04d}", "customer_name": f"Customer {n}",
            "segment": random.choice(SEGMENTS), "region": region}

customers = [new_customer(k) for k in range(1, 51)]
next_cust = 51

def write_crm():
    with open(CRM_FILE, "w", encoding="utf-8") as f:
        json.dump(customers, f, ensure_ascii=False)

write_crm()

# ----- POS: máy bán hàng chi nhánh PUSH hóa đơn lên API trung tâm (NiFi ListenHTTP) -----
def post_pos(invoice):
    data = json.dumps(invoice, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        POS_API, data=data,
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        urllib.request.urlopen(req, timeout=3)
        return True
    except Exception:
        return False                     # NiFi ListenHTTP chưa bật -> bỏ qua hóa đơn này


def xuat_kho(product, qty):
    """Bán -> XUẤT kho tổng (qty âm); cost = COGS = qty*unit_cost (link tài chính)."""
    cur.execute(
        "INSERT INTO kho_chuyendong (product_id, loai, qty, cost) VALUES (%s,'xuat',%s,%s)",
        (product, -qty, round(qty * COST[product], 0)))


# Seed TỒN KHO BAN ĐẦU vào KHO TỔNG (1 lần): tồn đầu = 2× ngưỡng (THẤP) để cầu sớm vượt cung.
cur.execute("SELECT COUNT(*) FROM kho_chuyendong WHERE loai = 'nhap_dau'")
if cur.fetchone()[0] == 0:
    for _p in PRODUCTS:
        q = REORDER[_p] * 2
        cur.execute(
            "INSERT INTO kho_chuyendong (product_id, loai, qty, cost) VALUES (%s,'nhap_dau',%s,%s)",
            (_p, q, round(q * COST[_p], 0)))
    print("  (đã seed tồn kho ban đầu cho kho tổng)")

print(">>> Đổ dữ liệu LIVE: POS(api) + ERP(db) + ECOM(db) + KHO TỔNG(db) + CRM(api)... (Ctrl+C để dừng)")
i = 0     # đếm DÒNG bán offline (line items)
j = 0     # đếm đơn online
inv = 0   # đếm hóa đơn offline
try:
    while True:
        # ===== KÊNH OFFLINE: POS (HÓA ĐƠN qua API) + ERP (dòng vào db) =====
        # Mỗi tick: vài CHI NHÁNH, MỖI CHI NHÁNH 1 HÓA ĐƠN gồm nhiều dòng sản phẩm.
        n = 0
        pos_fail = 0
        active_stores = random.sample(STORES, random.randint(2, 5))
        for store, region in active_stores:
            invoice_id = f"INV{inv:09d}"; inv += 1
            inv_date = biz_date()                            # ngày của cả hóa đơn
            lines = []
            for _ in range(random.randint(1, 4)):             # giỏ hàng 1-4 món
                txn_id = f"L{i:09d}"                          # id dòng duy nhất (POS = ERP)
                product = random.choice(PRODUCTS)
                qty = random.randint(1, 5)
                price = sale_price(COST[product])
                revenue = round(price * qty, 0)
                cogs = round(COST[product] * qty, 0)
                promo = random.randint(0, 1)
                # POS: tên cột "bẩn" (Doanh_Thu/Ngay) để NiFi làm sạch khi flatten
                lines.append({"txn_id": txn_id, "product_id": product, "qty": qty,
                              "Doanh_Thu": revenue, "promotion": promo})
                # ERP: 1 dòng hóa đơn = 1 bản ghi tài chính (revenue + COGS), kenh=offline
                cur.execute(
                    "INSERT INTO sales (txn_id, invoice_id, product_id, store_id, region, qty, "
                    "revenue, cost, kenh, txn_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'offline',%s)",
                    (txn_id, invoice_id, product, store, region, qty, revenue, cogs, inv_date))
                xuat_kho(product, qty)                        # bán -> xuất kho tổng
                i += 1
                n += 1
            invoice = {"invoice_id": invoice_id, "store_id": store,
                       "region": region, "Ngay": inv_date, "lines": lines}
            if not post_pos(invoice):                         # máy bán hàng push API
                pos_fail += 1

        # ===== KÊNH ONLINE: ECOMMERCE (vận hành) + ERP (tài chính, kenh=online) =====
        m = random.randint(2, 8)
        for _ in range(m):
            order_id = f"WEB{j:09d}"
            product = random.choice(PRODUCTS)
            qty = random.randint(1, 5)
            price = sale_price(COST[product])
            revenue = round(price * qty, 0)
            cogs = round(COST[product] * qty, 0)
            _store, region = random.choice(STORES)            # vùng giao hàng
            od = biz_date()
            # E-commerce (VẬN HÀNH online): KHÔNG có cost (tài chính ở ERP, giống POS)
            cur.execute(
                "INSERT INTO ecommerce_orders "
                "(order_id, customer_id, region, product_id, qty, revenue, "
                " device, payment_method, order_date) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (order_id, random.choice(customers)["customer_id"], region, product, qty,
                 revenue, random.choice(DEVICES), random.choice(PAYMENTS), od))
            # ERP (TÀI CHÍNH): đơn online cũng ghi nhận tài chính — link qua txn_id=order_id, kenh=online
            cur.execute(
                "INSERT INTO sales (txn_id, invoice_id, product_id, store_id, region, qty, "
                "revenue, cost, kenh, txn_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'online',%s)",
                (order_id, order_id, product, None, region, qty, revenue, cogs, od))
            xuat_kho(product, qty)                            # bán online -> xuất kho tổng
            j += 1

        # ===== KHO TỔNG: NHẬP BÙ PHẢN ỨNG — chỉ nhập SP đang DƯỚI ngưỡng (có độ trễ) =====
        # 30% cơ hội/tick -> có ĐỘ TRỄ nên tại thời điểm chụp luôn có vài SP dưới ngưỡng
        # -> cảnh báo tồn + kế hoạch nhập có số. Tồn được "ghìm" thấp quanh ngưỡng (không phình).
        cur.execute("SELECT product_id, COALESCE(SUM(qty),0) FROM kho_chuyendong GROUP BY product_id")
        ton = {r[0]: r[1] for r in cur.fetchall()}
        for product in PRODUCTS:
            if ton.get(product, 0) < REORDER[product] and random.random() < 0.3:
                q = REORDER[product] * 2                  # nhập 1 lô = 2× ngưỡng
                cur.execute(
                    "INSERT INTO kho_chuyendong (product_id, loai, qty, cost) VALUES (%s,'nhap',%s,%s)",
                    (product, q, round(q * COST[product], 0)))

        # ===== CRM: thỉnh thoảng có KHÁCH MỚI đăng ký =====
        new_cnt = 0
        if random.random() < 0.3:
            customers.append(new_customer(next_cust)); next_cust += 1
            new_cnt = 1
            write_crm()

        warn = f"  [!] {pos_fail} hóa đơn push lỗi (NiFi ListenHTTP chưa bật?)" if pos_fail else ""
        print(f"  offline {n} dòng / {len(active_stores)} hóa đơn (tổng {inv} HĐ) | "
              f"online {m} (tổng {j}) | khách {len(customers)}"
              + (" (+1 mới)" if new_cnt else "") + warn)
        time.sleep(random.uniform(3, 6))
except KeyboardInterrupt:
    cur.close()
    conn.close()
    print(f"\nDừng. Offline {i} dòng / {inv} hóa đơn | Online {j} | Khách {len(customers)}.")
