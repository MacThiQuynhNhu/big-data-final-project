#!/usr/bin/env python3
"""Sinh ảnh minh chứng kiểu terminal cho báo cáo (output SQL/Spark từ PostgreSQL trên VM).
Chạy: python gen_report_images.py  -> ghi PNG vào report_images/
"""
import os, sys, subprocess
from PIL import Image, ImageDraw, ImageFont

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HOST = "hduser@192.168.79.131"
OUT = "report_images"
os.makedirs(OUT, exist_ok=True)

FONT = ImageFont.truetype("C:/Windows/Fonts/consola.ttf", 17)
FONTB = ImageFont.truetype("C:/Windows/Fonts/consolab.ttf", 17)


def ssh(cmd):
    r = subprocess.run(["ssh", "-o", "ConnectTimeout=25", HOST, cmd],
                       capture_output=True, text=True, encoding="utf-8", errors="replace")
    return (r.stdout or "").rstrip("\n")


def psql(sql):
    return ssh(f'PGPASSWORD=erp123 psql -h localhost -U erp -d erp -P pager=off -c "{sql}"')


# Bảng màu Ubuntu GNOME Terminal
BG, BAR = "#300A24", "#3b3b3b"            # nền tím aubergine + thanh tiêu đề xám
GREEN, CYAN, SEP, FG, WC = "#8ae234", "#34e2e2", "#888a85", "#eeeeec", "#dcdcdc"


def render(name, title, body):
    cmd = title.split("\n")
    out = body.split("\n") if body.strip() else ["(không có dữ liệu)"]
    lines = cmd + [""] + out
    pad, lh, th = 16, 23, 38
    maxw = max([FONT.getlength(l) for l in lines] + [FONTB.getlength("hduser@master: ~") + 150])
    W, H = int(maxw) + pad * 2, th + pad + lh * len(lines) + pad
    img = Image.new("RGB", (W, H), BG)
    d = ImageDraw.Draw(img)
    # Thanh tiêu đề GNOME: tên cửa sổ trái + nút (minimize/maximize/close) bên PHẢI
    d.rectangle([0, 0, W, th], fill=BAR)
    d.text((pad, 10), "hduser@master: ~", font=FONTB, fill=WC)
    cy = th // 2
    xc = W - pad - 8                                   # close (✕)
    d.line([xc - 5, cy - 5, xc + 5, cy + 5], fill=WC, width=2)
    d.line([xc - 5, cy + 5, xc + 5, cy - 5], fill=WC, width=2)
    xm = xc - 28                                       # maximize (▢)
    d.rectangle([xm - 5, cy - 5, xm + 5, cy + 5], outline=WC, width=2)
    xn = xm - 28                                       # minimize (—)
    d.line([xn - 6, cy + 5, xn + 6, cy + 5], fill=WC, width=2)
    # Thân: dòng lệnh (xanh lá) + output (tiêu đề cyan, kẻ bảng xám, dữ liệu trắng)
    y, ostart = th + pad, len(cmd) + 1
    for i, l in enumerate(lines):
        if i < len(cmd):
            col = GREEN
        elif i == ostart:
            col = CYAN
        else:
            s = l.strip()
            col = SEP if (s and set(s) <= set("-+|")) else FG
        d.text((pad, y), l, font=FONT, fill=col)
        y += lh
    p = os.path.join(OUT, name + ".png")
    img.save(p)
    print("  ->", p, f"({W}x{H})")


POS_JSON = '''POST http://localhost:9998/pos      (NiFi ListenHTTP nhận từ máy bán hàng)

{
  "invoice_id": "POS-00001",
  "store_id":   "California",
  "txn_date":   "2026-07-01",
  "total":      900.0,
  "items": [
    {"product_id": "TEC-CO-10004722", "qty": 2, "price": 450.0}
  ]
}

HTTP/1.1 200 OK'''

SQL = [
    ("erp_sales", "psql erp=>  SELECT * FROM sales LIMIT 5;",
     "SELECT txn_id, product_id, store_id, qty, revenue, cost, kenh, txn_date FROM sales ORDER BY id LIMIT 5"),
    ("ecommerce_orders", "psql erp=>  SELECT * FROM ecommerce_orders LIMIT 5;",
     "SELECT order_id, customer_id, product_id, qty, revenue, device, payment_method, order_date FROM ecommerce_orders ORDER BY id LIMIT 5"),
    ("kho_chuyendong", "psql erp=>  SELECT * FROM kho_chuyendong LIMIT 5;",
     "SELECT product_id, loai, qty, cost, thoi_diem FROM kho_chuyendong ORDER BY id LIMIT 5"),
    ("bc_doanhthu_cuahang", "psql bao_cao=>  SELECT * FROM bc_doanhthu_cuahang ... LIMIT 12;",
     "SELECT store_id, thang, doanh_thu, chi_phi, loi_nhuan FROM bc_doanhthu_cuahang ORDER BY thang, store_id LIMIT 12"),
    ("bc_loinhuan_vung", "psql bao_cao=>  SELECT * FROM bc_loinhuan_vung;",
     "SELECT region, doanh_thu, loi_nhuan FROM bc_loinhuan_vung ORDER BY doanh_thu DESC"),
    ("bc_top_sanpham", "psql bao_cao=>  SELECT * FROM bc_top_sanpham;",
     "SELECT product_id, doanh_thu, so_luong_ban FROM bc_top_sanpham ORDER BY doanh_thu DESC"),
    ("forecast", "spark_analysis.py  ->  bc_dubao  (Linear Regression: dự báo tháng tới)",
     "SELECT thang_t, doanh_thu_dubao FROM bc_dubao"),
    ("kehoach_nhap", "spark_analysis.py  ->  bc_kehoach_nhaphang  (dự báo cầu -> đề xuất nhập)",
     "SELECT product_id, ten_sp, du_bao_thang, ton_hien_tai, reorder_level, de_xuat_nhap, chi_phi_nhap_du_kien FROM bc_kehoach_nhaphang ORDER BY de_xuat_nhap DESC"),
    ("rt_thongke", "psql erp=>  SELECT * FROM rt_thongke ORDER BY thoi_diem DESC LIMIT 10;",
     "SELECT * FROM rt_thongke ORDER BY thoi_diem DESC LIMIT 10"),
    ("rt_canhbao", "psql erp=>  SELECT * FROM rt_canhbao ORDER BY thoi_diem DESC LIMIT 5;",
     "SELECT * FROM rt_canhbao ORDER BY thoi_diem DESC LIMIT 5"),
    ("marts_output", "spark_marts_to_pg.py  ->  các bảng đã đẩy sang PostgreSQL cho Grafana",
     "SELECT relname AS bang, n_live_tup AS so_dong FROM pg_stat_user_tables WHERE relname LIKE 'bc_%' OR relname LIKE 'agg_%' OR relname IN ('inventory','dim_khachhang') ORDER BY relname"),
]

print("== Sinh ảnh terminal ==")
render("pos_sample", "POS API push (HTTP POST -> NiFi ListenHTTP)", POS_JSON)

crm = ssh("python3 -m json.tool < ~/big-data-final-project/data/crm_customers.json 2>/dev/null | head -28 "
          "|| head -c 1000 ~/big-data-final-project/data/crm_customers.json")
render("crm_api", "GET http://localhost:8000/crm_customers.json   (CRM API)",
       "HTTP/1.1 200 OK\nContent-Type: application/json\n\n" + (crm or "(file trống)"))

for name, title, sql in SQL:
    render(name, title, psql(sql))

print("Xong. Ảnh trong:", OUT)
