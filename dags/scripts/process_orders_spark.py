"""
process_orders_spark.py
Task 2 dari Orders Analytics Pipeline.

Membaca Parquet dari Data Lake (satu baris per produk per order),
lalu menghitung 4 tabel agregasi analitik di ClickHouse:

  1. orders_by_category  — order count & reorder rate per department
  2. orders_by_status    — distribusi eval_set (prior / train / test)
  3. top_products        — Top-10 produk paling sering dipesan
  4. orders_by_dow       — pola order per hari dalam seminggu
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from clickhouse_driver import Client
import os
import glob
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
#  Konfigurasi
# ─────────────────────────────────────────────────────────────
DATA_LAKE_PATH  = "/opt/airflow/data_lake/orders"
CLICKHOUSE_HOST = "clickhouse-server"
CLICKHOUSE_USER = "admin"
CLICKHOUSE_PASS = "rahasia"
CLICKHOUSE_DB   = "analytics"

# Mapping order_dow → nama hari (konvensi Instacart: 0=Sabtu, 1=Minggu)
DOW_NAMES = {
    0: "Saturday", 1: "Sunday", 2: "Monday", 3: "Tuesday",
    4: "Wednesday", 5: "Thursday", 6: "Friday",
}


# ─────────────────────────────────────────────────────────────
#  Helper
# ─────────────────────────────────────────────────────────────
def get_ch_client() -> Client:
    return Client(host=CLICKHOUSE_HOST, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASS)


def recreate_and_insert(client: Client, ddl: str, table: str, columns: list, rows: list) -> None:
    """DROP + CREATE tabel (schema bersih), lalu INSERT data baru."""
    full_table = f"{CLICKHOUSE_DB}.{table}"
    client.execute(f"DROP TABLE IF EXISTS {full_table}")
    client.execute(ddl)
    if rows:
        cols_str = ", ".join(columns)
        client.execute(f"INSERT INTO {full_table} ({cols_str}) VALUES", rows)
        log.info(f"  ✅ {full_table}: {len(rows)} baris berhasil di-insert.")
    else:
        log.warning(f"  ⚠️  {full_table}: tidak ada data untuk di-insert.")


# ─────────────────────────────────────────────────────────────
#  Fungsi Utama
# ─────────────────────────────────────────────────────────────
def run_spark_analytics() -> None:
    log.info("=" * 60)
    log.info("  Memulai Spark Analytics — Orders Pipeline")
    log.info("=" * 60)

    # ── 1. Inisialisasi Spark ────────────────────────────────
    spark = (
        SparkSession.builder
        .appName("Orders_Analytics_Pipeline")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── 2. Baca Data Lake ────────────────────────────────────
    parquet_path = f"file://{DATA_LAKE_PATH}/"
    log.info(f"Membaca Parquet dari: {parquet_path}")

    try:
        df_raw = spark.read.parquet(parquet_path)
    except Exception as exc:
        log.error(f"Tidak ada file Parquet yang dapat dibaca: {exc}")
        spark.stop()
        raise

    total_rows   = df_raw.count()
    total_orders = df_raw.select(F.countDistinct("order_id")).collect()[0][0]
    log.info(f"Total item produk dari Data Lake : {total_rows}")
    log.info(f"Total orders unik                : {total_orders}")

    # ── 3a. Agregasi: Order & Reorder per Department ─────────
    log.info("Menghitung agregasi per department (orders_by_category)...")
    df_category = (
        df_raw.groupBy("department")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.count("*").alias("total_items"),
            F.sum("reordered").alias("reordered_count"),
            F.round(
                F.sum("reordered").cast("double") / F.count("*") * 100, 2
            ).alias("reorder_rate"),
        )
        .orderBy(F.desc("total_orders"))
    ).toPandas()

    # ── 3b. Agregasi: Distribusi Eval Set ────────────────────
    log.info("Menghitung distribusi eval_set (orders_by_status)...")
    df_status = (
        df_raw.select("order_id", "eval_set").dropDuplicates(["order_id"])
        .groupBy("eval_set")
        .agg(F.count("order_id").alias("total_orders"))
        .withColumn(
            "percentage",
            F.round(F.col("total_orders").cast("double") / F.lit(float(total_orders)) * 100, 2),
        )
        .withColumn("total_items",
            F.col("total_orders")  # placeholder; di-isi via join di bawah
        )
        .orderBy(F.desc("total_orders"))
    ).toPandas()

    # Isi total_items dari df_raw.groupBy eval_set
    df_items_per_eval = (
        df_raw.groupBy("eval_set")
        .agg(F.count("*").alias("total_items"))
    ).toPandas()
    df_status = df_status.drop(columns=["total_items"]).merge(
        df_items_per_eval, on="eval_set", how="left"
    )
    df_status["total_items"] = df_status["total_items"].fillna(0).astype("int64")

    # ── 3c. Agregasi: Top-10 Produk ──────────────────────────
    log.info("Menghitung Top-10 produk (top_products)...")
    df_products = (
        df_raw.groupBy("product_id", "product_name", "department", "aisle")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.count("*").alias("total_items"),
            F.sum("reordered").alias("reordered_count"),
            F.round(
                F.sum("reordered").cast("double") / F.count("*") * 100, 2
            ).alias("reorder_rate"),
        )
        .orderBy(F.desc("total_orders"))
        .limit(10)
    ).toPandas()

    # ── 3d. Agregasi: Pola Order per Hari (DOW) ──────────────
    log.info("Menghitung pola order per hari (orders_by_dow)...")
    df_dow = (
        df_raw.groupBy("order_dow")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.count("*").alias("total_items"),
            F.round(
                F.count("*").cast("double") / F.countDistinct("order_id"), 2
            ).alias("avg_cart_size"),
        )
        .orderBy("order_dow")
    ).toPandas()
    df_dow["day_name"] = df_dow["order_dow"].map(DOW_NAMES).fillna("Unknown")

    spark.stop()
    log.info("Spark session dihentikan.")

    # ── 4. DDL & Load ke ClickHouse ──────────────────────────
    log.info("=" * 60)
    log.info("  Memuat hasil ke ClickHouse...")
    log.info("=" * 60)

    client = get_ch_client()
    client.execute(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")

    # --- Tabel 1: orders_by_category (per department) ---
    recreate_and_insert(
        client,
        ddl=f"""
            CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.orders_by_category
            (
                department      String   COMMENT 'Kategori / department produk',
                total_orders    Int64    COMMENT 'Jumlah order unik mengandung dept ini',
                total_items     Int64    COMMENT 'Total item dari dept ini',
                reordered_count Int64    COMMENT 'Jumlah item yang merupakan reorder',
                reorder_rate    Float64  COMMENT 'Persentase reorder (%)',
                updated_at      DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY total_orders
            COMMENT 'Agregasi order per department produk'
        """,
        table="orders_by_category",
        columns=["department", "total_orders", "total_items", "reordered_count", "reorder_rate"],
        rows=[
            (
                str(r["department"]),
                int(r["total_orders"]),
                int(r["total_items"]),
                int(r["reordered_count"]),
                float(r["reorder_rate"]),
            )
            for _, r in df_category.iterrows()
        ],
    )

    # --- Tabel 2: orders_by_status (distribusi eval_set) ---
    recreate_and_insert(
        client,
        ddl=f"""
            CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.orders_by_status
            (
                eval_set     String   COMMENT 'Tipe data: prior / train / test',
                total_orders Int64    COMMENT 'Jumlah order unik untuk tipe ini',
                total_items  Int64    COMMENT 'Total item produk untuk tipe ini',
                percentage   Float64  COMMENT 'Persentase dari total order (%)',
                updated_at   DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY total_orders
            COMMENT 'Distribusi order berdasarkan eval_set'
        """,
        table="orders_by_status",
        columns=["eval_set", "total_orders", "total_items", "percentage"],
        rows=[
            (
                str(r["eval_set"]),
                int(r["total_orders"]),
                int(r["total_items"]),
                float(r["percentage"]),
            )
            for _, r in df_status.iterrows()
        ],
    )

    # --- Tabel 3: top_products ---
    recreate_and_insert(
        client,
        ddl=f"""
            CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.top_products
            (
                product_id      Int64    COMMENT 'ID produk',
                product_name    String   COMMENT 'Nama produk',
                department      String   COMMENT 'Department / kategori produk',
                aisle           String   COMMENT 'Lorong / subkategori produk',
                total_orders    Int64    COMMENT 'Jumlah order unik yang mengandung produk ini',
                total_items     Int64    COMMENT 'Total kemunculan produk di semua order',
                reordered_count Int64    COMMENT 'Jumlah kemunculan sebagai reorder',
                reorder_rate    Float64  COMMENT 'Persentase reorder (%)',
                updated_at      DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY total_orders
            COMMENT 'Top-10 produk paling sering dipesan'
        """,
        table="top_products",
        columns=["product_id", "product_name", "department", "aisle",
                 "total_orders", "total_items", "reordered_count", "reorder_rate"],
        rows=[
            (
                int(r["product_id"]),
                str(r["product_name"]),
                str(r["department"]),
                str(r["aisle"]),
                int(r["total_orders"]),
                int(r["total_items"]),
                int(r["reordered_count"]),
                float(r["reorder_rate"]),
            )
            for _, r in df_products.iterrows()
        ],
    )

    # --- Tabel 4: orders_by_dow (pola per hari) ---
    recreate_and_insert(
        client,
        ddl=f"""
            CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.orders_by_dow
            (
                order_dow     Int64    COMMENT 'Hari dalam seminggu (0=Sabtu ... 6=Jumat)',
                day_name      String   COMMENT 'Nama hari dalam bahasa Inggris',
                total_orders  Int64    COMMENT 'Jumlah order unik pada hari ini',
                total_items   Int64    COMMENT 'Total item yang dibeli pada hari ini',
                avg_cart_size Float64  COMMENT 'Rata-rata jumlah item per order',
                updated_at    DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY order_dow
            COMMENT 'Distribusi order berdasarkan hari dalam seminggu'
        """,
        table="orders_by_dow",
        columns=["order_dow", "day_name", "total_orders", "total_items", "avg_cart_size"],
        rows=[
            (
                int(r["order_dow"]),
                str(r["day_name"]),
                int(r["total_orders"]),
                int(r["total_items"]),
                float(r["avg_cart_size"]),
            )
            for _, r in df_dow.iterrows()
        ],
    )

    # ── 5. Bersihkan Data Lake ───────────────────────────────
    log.info("Membersihkan file Parquet lama dari Data Lake...")
    for f in glob.glob(os.path.join(DATA_LAKE_PATH, "*.parquet")):
        try:
            os.remove(f)
            log.info(f"  Dihapus: {f}")
        except OSError as exc:
            log.warning(f"  Gagal menghapus {f}: {exc.strerror}")

    log.info("=" * 60)
    log.info("  ✅ Orders Analytics Pipeline Selesai!")
    log.info("=" * 60)


# ─────────────────────────────────────────────────────────────
#  Entry Point
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_spark_analytics()
