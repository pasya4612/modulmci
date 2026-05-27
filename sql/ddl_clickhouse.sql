-- ══════════════════════════════════════════════════════════════
--  ddl_clickhouse.sql
--  Orders Analytics Pipeline — DDL & Query Metabase
--  Database : analytics
--  Source   : http://96.9.212.102:8000/orders
--
--  Struktur data API (nested):
--    orders[] → products[] per order
--    Field order : order_id, user_id, order_number, order_dow,
--                  order_hour_of_day, days_since_prior_order, eval_set
--    Field produk: product_id, product_name, aisle_id, aisle,
--                  department_id, department, add_to_cart_order, reordered
-- ══════════════════════════════════════════════════════════════


-- ──────────────────────────────────────────────────────────────
--  BAGIAN 1 — DDL: Buat Database & 4 Tabel Analitik
-- ──────────────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS analytics;


-- Tabel 1: orders_by_category
--   Agregasi jumlah order dan reorder rate per department produk.
--   Sumber kolom: department (dari nested products[].department)
CREATE TABLE IF NOT EXISTS analytics.orders_by_category
(
    department      String   COMMENT 'Nama department / kategori produk',
    total_orders    Int64    COMMENT 'Jumlah order unik yang mengandung dept ini',
    total_items     Int64    COMMENT 'Total item dari dept ini di semua order',
    reordered_count Int64    COMMENT 'Jumlah item yang merupakan reorder (reordered=1)',
    reorder_rate    Float64  COMMENT 'Persentase item yang di-reorder (%)',
    updated_at      DateTime DEFAULT now() COMMENT 'Waktu pipeline terakhir dijalankan'
)
ENGINE = MergeTree()
ORDER BY total_orders
COMMENT 'Agregasi order & reorder rate per department produk';


-- Tabel 2: orders_by_status
--   Distribusi orders berdasarkan eval_set (prior / train / test).
--   eval_set adalah label Instacart yang menandai tipe split data.
CREATE TABLE IF NOT EXISTS analytics.orders_by_status
(
    eval_set     String   COMMENT 'Tipe split data Instacart: prior/train/test',
    total_orders Int64    COMMENT 'Jumlah order unik untuk eval_set ini',
    total_items  Int64    COMMENT 'Total item produk untuk eval_set ini',
    percentage   Float64  COMMENT 'Persentase dari total keseluruhan order (%)',
    updated_at   DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY total_orders
COMMENT 'Distribusi order berdasarkan eval_set (prior/train/test)';


-- Tabel 3: top_products
--   Top-10 produk yang paling sering muncul di order,
--   diurutkan berdasarkan frekuensi kemunculan (total_orders).
CREATE TABLE IF NOT EXISTS analytics.top_products
(
    product_id      Int64    COMMENT 'ID unik produk',
    product_name    String   COMMENT 'Nama produk',
    department      String   COMMENT 'Department / kategori produk',
    aisle           String   COMMENT 'Lorong / subkategori produk',
    total_orders    Int64    COMMENT 'Jumlah order unik yang mengandung produk ini',
    total_items     Int64    COMMENT 'Total kemunculan produk di semua order',
    reordered_count Int64    COMMENT 'Berapa kali produk ini di-reorder',
    reorder_rate    Float64  COMMENT 'Persentase kemunculan yang merupakan reorder (%)',
    updated_at      DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY total_orders
COMMENT 'Top-10 produk yang paling sering dipesan';


-- Tabel 4: orders_by_dow
--   Pola pemesanan berdasarkan hari dalam seminggu (Day of Week).
--   order_dow 0=Sabtu, 1=Minggu, 2=Senin, ..., 6=Jumat (konvensi Instacart).
CREATE TABLE IF NOT EXISTS analytics.orders_by_dow
(
    order_dow     Int64    COMMENT 'Nomor hari (0=Sabtu s/d 6=Jumat)',
    day_name      String   COMMENT 'Nama hari dalam bahasa Inggris',
    total_orders  Int64    COMMENT 'Jumlah order unik pada hari ini',
    total_items   Int64    COMMENT 'Total item yang dibeli pada hari ini',
    avg_cart_size Float64  COMMENT 'Rata-rata jumlah item per order (ukuran keranjang)',
    updated_at    DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY order_dow
COMMENT 'Distribusi dan pola order berdasarkan hari dalam seminggu';


-- ──────────────────────────────────────────────────────────────
--  BAGIAN 2 — DML Operasional
--  Pipeline Python melakukan DROP TABLE + CREATE + INSERT otomatis.
--  Command di bawah untuk reset manual jika diperlukan.
-- ──────────────────────────────────────────────────────────────

TRUNCATE TABLE analytics.orders_by_category;
TRUNCATE TABLE analytics.orders_by_status;
TRUNCATE TABLE analytics.top_products;
TRUNCATE TABLE analytics.orders_by_dow;


-- ──────────────────────────────────────────────────────────────
--  BAGIAN 3 — QUERY METABASE (10 Visualisasi)
--  Koneksi: host=clickhouse-server port=8123
--           user=admin password=rahasia database=analytics
--  Cara pakai: New → SQL query → paste query → Run → Visualize
-- ──────────────────────────────────────────────────────────────


-- ════════════════════════════════════════════════════════════
--  SCORECARD (4 Number Cards — taruh di baris paling atas)
-- ════════════════════════════════════════════════════════════

-- Query 1 — Scorecard: Total Orders
-- Visualisasi : Number / Scalar
-- Card title  : "Total Orders"
SELECT sum(total_orders) AS total_orders
FROM analytics.orders_by_category;


-- Query 2 — Scorecard: Total Items
-- Visualisasi : Number / Scalar
-- Card title  : "Total Items Dipesan"
SELECT sum(total_items) AS total_items
FROM analytics.orders_by_category;


-- Query 3 — Scorecard: Reorder Rate
-- Visualisasi : Number / Scalar
-- Card title  : "Overall Reorder Rate (%)"
SELECT
    round(
        sum(reordered_count) * 100.0 / nullIf(sum(total_items), 0),
        2
    ) AS reorder_rate_pct
FROM analytics.orders_by_category;


-- Query 4 — Scorecard: Total Department
-- Visualisasi : Number / Scalar
-- Card title  : "Total Department"
SELECT count(DISTINCT department) AS total_departments
FROM analytics.orders_by_category;


-- ════════════════════════════════════════════════════════════
--  CHART VISUALISASI
-- ════════════════════════════════════════════════════════════

-- Query 5 — Pesanan Tiap Departemen
-- Visualisasi : Row Chart (Bar Horizontal)
--   X-axis    : total_orders
--   Y-axis    : department
-- Card title  : "Pesanan Tiap Departemen"
SELECT
    department,
    total_orders,
    total_items,
    reorder_rate
FROM analytics.orders_by_category
ORDER BY total_orders DESC;


-- Query 6 — Top 10 Produk Terpopuler
-- Visualisasi : Row Chart (Bar Horizontal)
--   X-axis    : total_orders
--   Y-axis    : product_name
-- Card title  : "Top 10 Produk Terpopuler"
SELECT
    product_name,
    department,
    aisle,
    total_orders,
    total_items,
    reorder_rate
FROM analytics.top_products
ORDER BY total_orders DESC
LIMIT 10;


-- Query 7 — Pola Order per Hari
-- Visualisasi : Bar Chart (Vertical)
--   X-axis    : day_name
--   Y-axis    : total_orders
-- Card title  : "Pola Order per Hari"
SELECT
    day_name,
    order_dow,
    total_orders,
    total_items
FROM analytics.orders_by_dow
ORDER BY order_dow;


-- Query 8 — Distribusi per Hari (proporsi)
-- Visualisasi : Pie Chart
--   Dimension : day_name
--   Value     : total_orders
-- Card title  : "Distribusi Order per Hari"
SELECT
    day_name,
    total_orders,
    round(total_orders * 100.0 / sum(total_orders) OVER (), 1) AS percentage
FROM analytics.orders_by_dow
ORDER BY order_dow;


-- Query 9 — Distribusi per Order (Eval Set)
-- Visualisasi : Pie Chart
--   Dimension : eval_set
--   Value     : total_orders
-- Card title  : "Distribusi Tipe Order (Eval Set)"
SELECT
    eval_set,
    total_orders,
    total_items,
    percentage
FROM analytics.orders_by_status
ORDER BY total_orders DESC;


-- Query 10 — Reorder Rate per Departemen
-- Visualisasi : Row Chart (Bar Horizontal)
--   X-axis    : reorder_rate
--   Y-axis    : department
-- Card title  : "Reorder Rate per Departemen (%)"
SELECT
    department,
    reorder_rate,
    reordered_count,
    total_items
FROM analytics.orders_by_category
ORDER BY reorder_rate DESC;


-- ════════════════════════════════════════════════════════════
--  BONUS — Query Validasi (jalankan di ClickHouse langsung)
-- ════════════════════════════════════════════════════════════

-- Cek semua tabel ada dan berisi data
SHOW TABLES FROM analytics;
SELECT 'orders_by_category' AS tabel, count(*) AS rows FROM analytics.orders_by_category
UNION ALL
SELECT 'orders_by_status',            count(*) FROM analytics.orders_by_status
UNION ALL
SELECT 'top_products',                count(*) FROM analytics.top_products
UNION ALL
SELECT 'orders_by_dow',               count(*) FROM analytics.orders_by_dow;
