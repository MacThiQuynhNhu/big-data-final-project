-- ============================================================
-- Tạo các bảng NGUỒN + DIMENSION cho feeder (PostgreSQL, db "erp", user "erp")
-- Chạy: PGPASSWORD=erp123 psql -h localhost -U erp -d erp -f setup_db.sql
--
-- ⚠️ Nếu ĐÃ có bảng cũ (schema khác) -> CREATE IF NOT EXISTS sẽ BỎ QUA, thiếu cột mới.
--    Khi đổi schema, XÓA bảng cũ trước (dữ liệu mô phỏng, feeder sinh lại):
--    PGPASSWORD=erp123 psql -h localhost -U erp -d erp \
--      -c "DROP TABLE IF EXISTS sales, ecommerce_orders, kho_chuyendong, san_pham CASCADE;"
-- ============================================================

-- DIMENSION: DANH MỤC SẢN PHẨM + GIÁ VỐN (nối KHO <-> TÀI CHÍNH)
--   unit_cost = giá vốn 1 đơn vị; reorder_level = ngưỡng tái đặt riêng từng sản phẩm.
--   Mọi COGS (giá vốn hàng bán) và giá trị nhập kho đều tính từ unit_cost này.
CREATE TABLE IF NOT EXISTS san_pham (
    product_id     TEXT PRIMARY KEY,
    ten_sp         TEXT,
    unit_cost      DOUBLE PRECISION,
    reorder_level  INT
);
INSERT INTO san_pham (product_id, ten_sp, unit_cost, reorder_level) VALUES
    ('TEC-CO-10004722', 'Copier',        300, 30),
    ('OFF-BI-10003527', 'Binder',          8, 100),
    ('FUR-CH-10002024', 'Chair',         150, 20),
    ('OFF-BI-10001359', 'Binder Mini',     5, 100),
    ('TEC-MA-10001127', 'Machine',       200, 25),
    ('OFF-SU-10000151', 'Supplies',       10, 80),
    ('FUR-BO-10001798', 'Bookcase',      120, 15),
    ('OFF-PA-10001970', 'Paper',           6, 120),
    ('TEC-AC-10002049', 'Accessory',      40, 50)
ON CONFLICT (product_id) DO NOTHING;

-- ERP — hệ tài chính của CẢ CÔNG TY: ghi nhận tài chính cho CẢ HAI kênh (offline + online).
--   Mỗi dòng = 1 dòng sản phẩm (offline: dòng hóa đơn POS ; online: đơn e-commerce).
--   revenue = qty * giá bán ; cost = qty * unit_cost (COGS) ; kenh = offline | online.
--   POS  link qua txn_id (offline) ; E-commerce link qua txn_id (online).
--   NiFi QueryDatabaseTable đọc incremental theo id.
CREATE TABLE IF NOT EXISTS sales (
    id         SERIAL PRIMARY KEY,
    txn_id     TEXT,                 -- id dòng (duy nhất) — link với POS/E-commerce
    invoice_id TEXT,                 -- offline: gom dòng cùng hóa đơn ; online: = order_id
    product_id TEXT,
    store_id   TEXT,                 -- offline: cửa hàng ; online: NULL
    region     TEXT,
    qty        INT,
    revenue    DOUBLE PRECISION,
    cost       DOUBLE PRECISION,     -- COGS = qty * unit_cost
    kenh       TEXT,                 -- offline | online
    txn_date   DATE
);

-- ECOMMERCE — nền tảng bán ONLINE (VẬN HÀNH): đơn web/app. KHÔNG giữ cost (tài chính ở ERP,
--   giống POS không giữ cost). Link sang ERP qua txn_id (= order_id).
CREATE TABLE IF NOT EXISTS ecommerce_orders (
    id             SERIAL PRIMARY KEY,
    order_id       TEXT,
    customer_id    TEXT,
    region         TEXT,
    product_id     TEXT,
    qty            INT,
    revenue        DOUBLE PRECISION,  -- giá bán (vận hành) ; COGS nằm ở ERP
    device         TEXT,              -- web | app
    payment_method TEXT,              -- card | ewallet | cod
    order_date     DATE
);

-- WMS — KHO TỔNG (1 kho trung tâm cho cả công ty; KHÔNG tách theo chi nhánh).
--   SỰ KIỆN chuyển động (fact). cost = giá trị tiền của chuyển động = |qty| * unit_cost.
--     loai='nhap_dau' qty>0 : tồn ban đầu (feeder seed 1 lần)  -> tiền mua hàng
--     loai='nhap'     qty>0 : restock từ nhà cung cấp           -> tiền mua hàng
--     loai='xuat'     qty<0 : bán (offline + online)            -> COGS
--   Tồn hiện tại = SUM(qty) theo product_id ; giá trị tồn = tồn * unit_cost.
CREATE TABLE IF NOT EXISTS kho_chuyendong (
    id          SERIAL PRIMARY KEY,
    product_id  TEXT,
    loai        TEXT,
    qty         INT,
    cost        DOUBLE PRECISION,    -- tiền của chuyển động (link tài chính)
    thoi_diem   TIMESTAMPTZ DEFAULT now()
);

-- Index hỗ trợ đọc incremental của NiFi
CREATE INDEX IF NOT EXISTS idx_sales_id ON sales (id);
CREATE INDEX IF NOT EXISTS idx_ecom_id  ON ecommerce_orders (id);
CREATE INDEX IF NOT EXISTS idx_kho_id   ON kho_chuyendong (id);
