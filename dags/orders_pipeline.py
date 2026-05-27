"""
orders_pipeline.py
──────────────────
DAG utama untuk Orders Analytics Pipeline.

Alur:
  Task 1 (fetch_orders)        : Fetch data orders dari REST API → simpan Parquet ke Data Lake
  Task 2 (process_orders_spark): Baca Parquet via PySpark → agregasi analitik → load ke ClickHouse
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# ─────────────────────────────────────────────────────────────
#  Default Arguments
# ─────────────────────────────────────────────────────────────
default_args = {
    'owner'           : 'mci_engineer',
    'start_date'      : datetime(2024, 1, 1),
    'retries'         : 1,
    'retry_delay'     : timedelta(minutes=2),
    'email_on_failure': False,
    'email_on_retry'  : False,
}

# ─────────────────────────────────────────────────────────────
#  DAG Definition
# ─────────────────────────────────────────────────────────────
with DAG(
    dag_id='orders_analytics_pipeline',
    default_args=default_args,
    schedule_interval='*/15 * * * *',   # Micro-batch setiap 15 menit
    catchup=False,
    max_active_runs=1,
    tags=['orders', 'ecommerce', 'clickhouse', 'spark'],
    description='Micro-batching Orders API → Parquet (Data Lake) → Spark → ClickHouse → Metabase',
) as dag:

    # ── Task 1: Ingest dari Orders REST API ───────────────────
    fetch_orders = BashOperator(
        task_id='fetch_orders',
        bash_command='python /opt/airflow/dags/scripts/fetch_orders.py',
        doc_md="""
        ### fetch_orders
        Menarik seluruh data orders dari REST API endpoint
        `http://96.9.212.102:8000/orders`, mem-parsing field penting,
        lalu menyimpan hasilnya sebagai file Parquet bertimestamp
        ke folder `/opt/airflow/data_lake/orders/`.
        """,
    )

    # ── Task 2: Spark Processing → ClickHouse ─────────────────
    process_orders = BashOperator(
        task_id='process_orders_spark',
        bash_command='python /opt/airflow/dags/scripts/process_orders_spark.py',
        doc_md="""
        ### process_orders_spark
        Membaca seluruh file Parquet di Data Lake menggunakan PySpark,
        menghitung berbagai agregasi analitik:
          - Revenue & jumlah order per kategori produk
          - Distribusi status order (completed / pending / cancelled)
          - Top-10 produk terlaris berdasarkan revenue
          - Tren harian: jumlah order & total revenue per hari
        Kemudian meng-upsert (TRUNCATE + INSERT) hasil ke ClickHouse.
        File Parquet lama dihapus setelah diproses.
        """,
    )

    # ── Dependency ─────────────────────────────────────────────
    fetch_orders >> process_orders
