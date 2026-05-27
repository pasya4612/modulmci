# Orders Analytics Pipeline
### Micro-Batch Data Engineering: Apache Airflow · PySpark · ClickHouse · Metabase

---

> **Ringkasan Proyek**
> Pipeline ini secara otomatis menarik data orders dari REST API setiap 15 menit,
> mengekspansi nested products, memproses dengan PySpark untuk menghasilkan 4 tabel
> agregasi analitik, menyimpan hasilnya ke ClickHouse sebagai Data Warehouse, dan
> memvisualisasikannya melalui 10 Questions + 1 Dashboard di Metabase.
>
> Dataset Source: `http://96.9.212.102:8000/orders`

---

## Arsitektur Pipeline

```
[Orders REST API]  http://96.9.212.102:8000/orders
        |
        |  HTTP GET → nested JSON
        |  {orders: [{order_id, products: [{product_id,...}]}]}
        v
[Task 1: fetch_orders.py]
        |  - Ekspansi products[] → 1 baris per (order, produk)
        |  - Simpan sebagai Parquet bertimestamp
        v
[Data Lake: data_lake/orders/orders_YYYYMMDD_HHMMSS.parquet]
        |
        |  spark.read.parquet(...)
        v
[Task 2: process_orders_spark.py]
        |  - groupBy("department")  → orders_by_category
        |  - groupBy("eval_set")    → orders_by_status
        |  - groupBy("product_*")   → top_products
        |  - groupBy("order_dow")   → orders_by_dow
        |  - DROP + CREATE + INSERT ke ClickHouse
        v
[ClickHouse: database analytics — 4 tabel]
        |
        |  SQL queries via Metabase
        v
[Metabase: 10 Questions + Dashboard]
```

---

## Struktur Direktori & Fungsi Tiap File

```
orders-pipeline/
|
|-- docker-compose.yml          <- Orkestrasi semua service Docker
|-- Dockerfile                  <- Custom image Airflow + Java + PySpark
|-- requirements.txt            <- Dependensi Python pipeline
|
|-- dags/
|   |-- orders_pipeline.py      <- Definisi DAG Airflow (penjadwalan)
|   `-- scripts/
|       |-- fetch_orders.py     <- Task 1: Ingest API → Parquet
|       `-- process_orders_spark.py  <- Task 2: Spark Agregasi → ClickHouse
|
|-- data_lake/
|   `-- orders/                 <- Penyimpanan sementara file Parquet
|
|-- sql/
|   `-- ddl_clickhouse.sql      <- DDL schema + 10 Query Metabase
|
`-- metabase-plugins/           <- Direktori untuk plugin ClickHouse driver
```

---

## Penjelasan Fungsi Tiap File

### 1. `docker-compose.yml`

Mendefinisikan dan mengorkestrasi **5 service** yang berjalan bersama:

| Service | Image | Port | Fungsi |
|---|---|---|---|
| `postgres` | postgres:13 | - | Database metadata Airflow |
| `airflow-init` | custom | - | Inisialisasi DB + user admin (sekali jalan) |
| `airflow-webserver` | custom | 8080 | UI monitoring DAG & task |
| `airflow-scheduler` | custom | - | Engine penjadwalan DAG tiap 15 menit |
| `clickhouse-server` | clickhouse/clickhouse-server | 8123, 9000 | Data Warehouse analitik |
| `metabase` | metabase/metabase | 3000 | Visualisasi & dashboard |

Fitur penting yang dikonfigurasi:
- `x-airflow-common` anchor: reuse konfigurasi environment + volume untuk semua service Airflow
- `healthcheck` pada postgres dan ClickHouse: service lain baru start setelah service ini sehat
- `MB_PLUGINS_DIR=/plugins` + volume `./metabase-plugins`: tempat plugin ClickHouse driver untuk Metabase
- `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1`: mengaktifkan manajemen user ClickHouse

---

### 2. `Dockerfile`

Membangun **custom Docker image** di atas `apache/airflow:2.9.1-python3.11` dengan tambahan:

```
apache/airflow:2.9.1-python3.11
        |
        | apt-get install default-jre-headless
        | (Java Runtime — wajib untuk PySpark)
        |
        | ENV JAVA_HOME=/usr/lib/jvm/default-java
        | (agar Spark bisa menemukan instalasi Java)
        |
        | pip install -r requirements.txt
        | (library Python tambahan)
        v
Custom Airflow Image (Airflow + Java + PySpark + dependencies)
```

Tanpa Java, PySpark tidak bisa diinisialisasi karena Spark berjalan di atas JVM (Java Virtual Machine).

---

### 3. `requirements.txt`

Daftar library Python yang diinstal ke dalam custom Airflow image:

| Library | Versi | Fungsi |
|---|---|---|
| `pyspark` | 3.5.1 | Engine pemrosesan data terdistribusi untuk agregasi |
| `clickhouse-driver` | 0.2.7 | Koneksi native Python ke ClickHouse via TCP port 9000 |
| `pandas` | 2.2.1 | Konversi DataFrame Spark ke Python untuk insert ClickHouse |
| `requests` | 2.31.0 | HTTP client untuk memanggil Orders REST API |
| `pyarrow` | 15.0.2 | Engine baca/tulis format file Parquet |

---

### 4. `dags/orders_pipeline.py`

File **definisi DAG** (Directed Acyclic Graph) Airflow yang mengatur:

- **Penjadwalan**: `schedule_interval='*/15 * * * *'` → berjalan otomatis setiap 15 menit
- **Urutan task**: `fetch_orders >> process_orders` (Task 1 harus selesai sebelum Task 2)
- **Retry logic**: jika task gagal, dicoba ulang 1x dengan jeda 2 menit
- **Operator yang dipakai**: `BashOperator` — menjalankan script Python via perintah bash

```
DAG: orders_analytics_pipeline
  |
  Task 1: fetch_orders
  BashOperator → python /opt/airflow/dags/scripts/fetch_orders.py
  |
  v
  Task 2: process_orders_spark
  BashOperator → python /opt/airflow/dags/scripts/process_orders_spark.py
```

Parameter DAG:

| Parameter | Nilai | Keterangan |
|---|---|---|
| `schedule_interval` | `*/15 * * * *` | Micro-batch setiap 15 menit |
| `catchup` | `False` | Tidak menjalankan backfill historis |
| `max_active_runs` | `1` | Mencegah dua pipeline berjalan bersamaan |
| `retries` | `1` | Retry otomatis jika gagal |
| `retry_delay` | 2 menit | Jeda sebelum retry |

---

### 5. `dags/scripts/fetch_orders.py`

Task 1 pipeline. Bertanggung jawab untuk **ingest data dari API dan menyimpan ke Data Lake**.

#### Alur Kerja:
```
1. HTTP GET ke http://96.9.212.102:8000/orders
2. Parsing response JSON → ekstrak list orders[]
3. Untuk setiap order, ekspansi products[] → 1 baris per (order, produk)
4. Buat pandas DataFrame dari semua baris
5. Simpan sebagai Parquet ke data_lake/orders/orders_YYYYMMDD_HHMMSS.parquet
```

#### Fungsi & Algoritma yang Dipakai:

**`requests.get(url, headers, timeout)`**
Memanggil API dengan timeout 30 detik. Jika gagal (HTTP error, timeout), exception di-raise agar Airflow mencatat task sebagai FAILED dan melakukan retry.

**Ekspansi Nested Products (Loop)**
```python
for o in orders:          # iterasi tiap order
    for p in o["products"]:   # iterasi tiap produk dalam order
        rows.append({
            "order_id": o["order_id"],   # field dari order
            "department": p["department"] # field dari produk
            ...
        })
```
Teknik ini mengubah struktur hirarkis (1 order → N produk) menjadi tabel flat yang bisa disimpan ke Parquet. Hasilnya: **1 baris per kombinasi (order, produk)**.

**`pd.DataFrame(rows)`**
Membuat DataFrame dari list of dict. Setiap key dict menjadi nama kolom.

**Type Casting**
```python
pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")
```
Memastikan kolom numerik bertipe benar. `errors="coerce"` mengubah nilai yang tidak bisa dikonversi menjadi NaN, lalu `fillna(0)` menggantinya dengan 0. Kolom `days_since_prior_order` dibiarkan float dengan NaN (valid untuk order pertama user yang belum punya riwayat).

**`df.to_parquet(path, engine="pyarrow")`**
Menyimpan DataFrame ke format Parquet menggunakan engine PyArrow. Parquet dipilih karena:
- Format columnar: Spark hanya membaca kolom yang dibutuhkan (column pruning)
- Kompresi otomatis: ukuran file jauh lebih kecil dari CSV
- Menyimpan tipe data: tidak perlu parsing ulang saat dibaca

#### Schema Parquet Output (1 baris per produk per order):

| Kolom | Tipe | Sumber |
|---|---|---|
| `order_id` | Int64 | `orders[].order_id` |
| `user_id` | Int64 | `orders[].user_id` |
| `order_number` | Int64 | `orders[].order_number` |
| `order_dow` | Int64 | `orders[].order_dow` (0=Sabtu...6=Jumat) |
| `order_hour_of_day` | Int64 | `orders[].order_hour_of_day` |
| `days_since_prior_order` | Float64 | `orders[].days_since_prior_order` (nullable) |
| `eval_set` | String | `orders[].eval_set` (prior/train/test) |
| `product_id` | Int64 | `orders[].products[].product_id` |
| `product_name` | String | `orders[].products[].product_name` |
| `aisle_id` | Int64 | `orders[].products[].aisle_id` |
| `aisle` | String | `orders[].products[].aisle` |
| `department_id` | Int64 | `orders[].products[].department_id` |
| `department` | String | `orders[].products[].department` |
| `add_to_cart_order` | Int64 | `orders[].products[].add_to_cart_order` |
| `reordered` | Int64 | `orders[].products[].reordered` (0 atau 1) |

---

### 6. `dags/scripts/process_orders_spark.py`

Task 2 pipeline. Bertanggung jawab untuk **agregasi data dengan PySpark dan load ke ClickHouse**.

#### Alur Kerja:
```
1. Inisialisasi SparkSession
2. Baca semua file Parquet dari data_lake/orders/
3. Hitung 4 agregasi analitik
4. Konversi hasil ke pandas (toPandas())
5. Buat database + DROP + CREATE tabel di ClickHouse
6. INSERT data ke tiap tabel
7. Hapus file Parquet dari Data Lake
```

#### Fungsi & Algoritma yang Dipakai:

**SparkSession**
```python
SparkSession.builder
    .appName("Orders_Analytics_Pipeline")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
```
Menginisialisasi engine Spark lokal. `shuffle.partitions=4` diset kecil karena data tidak besar (menghindari overhead 200 partisi default Spark).

**`spark.read.parquet(path)`**
Membaca semua file `.parquet` dalam folder sekaligus menjadi satu DataFrame terpadu. Spark memanfaatkan schema Parquet untuk type inference otomatis.

**Agregasi 1 — `orders_by_category` (per department)**
```python
df_raw.groupBy("department").agg(
    F.countDistinct("order_id"),   # hitung order UNIK (bukan total baris)
    F.count("*"),                  # hitung total item
    F.sum("reordered"),            # jumlah item yang merupakan reorder
    F.round(F.sum("reordered") / F.count("*") * 100, 2)  # reorder rate %
)
```
- `countDistinct` dipakai (bukan `count`) karena 1 order bisa punya banyak produk dari department yang sama — kita ingin menghitung berapa ORDER, bukan berapa baris.
- `reorder_rate = reordered_count / total_items * 100` mengukur seberapa sering produk di department ini dibeli ulang.

**Agregasi 2 — `orders_by_status` (distribusi eval_set)**
```python
df_raw.select("order_id", "eval_set").dropDuplicates(["order_id"])
    .groupBy("eval_set")
    .agg(F.count("order_id"))
    .withColumn("percentage",
        F.col("total_orders") / F.lit(float(total_orders)) * 100
    )
```
- `.dropDuplicates(["order_id"])` dulu sebelum groupBy untuk menghitung order unik per eval_set (bukan baris produk).
- `percentage` dihitung dengan membagi total orders per eval_set terhadap grand total.

**Agregasi 3 — `top_products` (produk terpopuler)**
```python
df_raw.groupBy("product_id", "product_name", "department", "aisle")
    .agg(
        F.countDistinct("order_id"),  # berapa order mengandung produk ini
        F.count("*"),                 # total kemunculan
        F.sum("reordered"),
        F.round(F.sum("reordered") / F.count("*") * 100, 2)
    )
    .orderBy(F.desc("total_orders"))
    .limit(10)
```
- Diurutkan berdasarkan `total_orders` (bukan revenue karena tidak ada harga di data).
- `.limit(10)` dieksekusi di Spark (bukan di Python) untuk efisiensi — hanya 10 baris yang ditransfer ke driver.

**Agregasi 4 — `orders_by_dow` (pola per hari)**
```python
df_raw.groupBy("order_dow").agg(
    F.countDistinct("order_id"),
    F.count("*"),
    F.round(F.count("*") / F.countDistinct("order_id"), 2)  # avg cart size
)
```
- `avg_cart_size = total_items / total_orders` mengukur rata-rata berapa produk yang dibeli dalam satu order per hari.
- `DOW_NAMES` dict memetakan angka 0-6 ke nama hari (ditambahkan di pandas setelah `.toPandas()`).

**`.toPandas()`**
Mengumpulkan hasil agregasi Spark (yang mungkin tersebar di beberapa partisi) ke satu pandas DataFrame di driver node. Aman dilakukan setelah agregasi karena datanya sudah kecil (maks puluhan baris).

**`clickhouse_driver.Client`**
Koneksi native ke ClickHouse via port 9000 (TCP). Lebih cepat dari HTTP interface untuk bulk insert.

**Strategi `DROP TABLE + CREATE TABLE + INSERT`**
```python
client.execute("DROP TABLE IF EXISTS analytics.orders_by_category")
client.execute("CREATE TABLE IF NOT EXISTS analytics.orders_by_category (...)")
client.execute("INSERT INTO analytics.orders_by_category (...) VALUES", rows)
```
Pendekatan ini (bukan TRUNCATE+INSERT) memastikan schema ClickHouse selalu sinkron dengan versi terbaru script. Jika ada perubahan kolom, DROP+CREATE akan membuat ulang tabel dengan schema yang benar.

**Pembersihan Data Lake**
```python
for f in glob.glob(os.path.join(DATA_LAKE_PATH, "*.parquet")):
    os.remove(f)
```
File Parquet dihapus setelah diproses agar pada batch berikutnya tidak ada duplikasi data lama yang ikut terbaca Spark.

---

### 7. `sql/ddl_clickhouse.sql`

File referensi berisi dua bagian:

**Bagian 1 — DDL (Data Definition Language)**
Definisi schema 4 tabel analitik di ClickHouse. Semua tabel menggunakan:
- Engine `MergeTree()`: engine default ClickHouse untuk data analitik, sangat cepat untuk query agregasi
- `ORDER BY`: menentukan primary key fisik tabel (urutan penyimpanan di disk), dipilih kolom yang paling sering dipakai untuk filter/sort
- `DEFAULT now()` pada `updated_at`: otomatis terisi waktu insert jika tidak di-provide

**Bagian 2 — 10 Query Metabase**
Query siap pakai untuk 10 visualisasi di Metabase, mencakup 4 scorecard angka dan 6 chart.

---

## Schema ClickHouse — 4 Tabel Analitik

### Tabel 1: `analytics.orders_by_category`
| Kolom | Tipe | Keterangan |
|---|---|---|
| `department` | String | Nama department produk |
| `total_orders` | Int64 | Jumlah order unik |
| `total_items` | Int64 | Total item dipesan |
| `reordered_count` | Int64 | Jumlah item yang di-reorder |
| `reorder_rate` | Float64 | Persentase reorder (%) |
| `updated_at` | DateTime | Waktu pipeline terakhir |

### Tabel 2: `analytics.orders_by_status`
| Kolom | Tipe | Keterangan |
|---|---|---|
| `eval_set` | String | Tipe split: prior/train/test |
| `total_orders` | Int64 | Jumlah order unik |
| `total_items` | Int64 | Total item |
| `percentage` | Float64 | Persentase dari total (%) |
| `updated_at` | DateTime | Waktu pipeline terakhir |

### Tabel 3: `analytics.top_products`
| Kolom | Tipe | Keterangan |
|---|---|---|
| `product_id` | Int64 | ID produk |
| `product_name` | String | Nama produk |
| `department` | String | Department produk |
| `aisle` | String | Subkategori produk |
| `total_orders` | Int64 | Jumlah order mengandung produk ini |
| `total_items` | Int64 | Total kemunculan produk |
| `reordered_count` | Int64 | Berapa kali di-reorder |
| `reorder_rate` | Float64 | Persentase reorder (%) |
| `updated_at` | DateTime | Waktu pipeline terakhir |

### Tabel 4: `analytics.orders_by_dow`
| Kolom | Tipe | Keterangan |
|---|---|---|
| `order_dow` | Int64 | Nomor hari (0=Sabtu...6=Jumat) |
| `day_name` | String | Nama hari (Saturday...Friday) |
| `total_orders` | Int64 | Jumlah order unik |
| `total_items` | Int64 | Total item dibeli |
| `avg_cart_size` | Float64 | Rata-rata item per order |
| `updated_at` | DateTime | Waktu pipeline terakhir |

---

## Visualisasi & Questions Metabase (10 Query)

Semua query disimpan dalam Collection yang sama, lalu ditambahkan ke 1 Dashboard.

| # | Nama Question | Tabel | Visualisasi | X-axis | Y-axis |
|---|---|---|---|---|---|
| 1 | Scorecard - Total Orders | orders_by_category | Number | — | total_orders |
| 2 | Scorecard - Total Items | orders_by_category | Number | — | total_items |
| 3 | Scorecard - Reorder Rate | orders_by_category | Number | — | reorder_rate_pct |
| 4 | Scorecard - Total Department | orders_by_category | Number | — | total_departments |
| 5 | Pesanan Tiap Departemen | orders_by_category | Row Chart | department | total_orders |
| 6 | Top 10 Produk Terpopuler | top_products | Row Chart | product_name | total_orders |
| 7 | Pola Order per Hari | orders_by_dow | Bar Chart | day_name | total_orders |
| 8 | Distribusi per Hari | orders_by_dow | Pie Chart | day_name | total_orders |
| 9 | Distribusi per Order | orders_by_status | Pie Chart | eval_set | total_orders |
| 10 | Reorder Rate per Departemen | orders_by_category | Row Chart | department | reorder_rate |

### Layout Dashboard

```
+------------------+------------------+------------------+------------------+
| Total Orders     | Total Items      | Reorder Rate     | Total Department |
| [Number]         | [Number]         | [Number]         | [Number]         |
+------------------+------------------+------------------+------------------+
| Pesanan Tiap Departemen             | Top 10 Produk Terpopuler            |
| [Row Chart]                         | [Row Chart]                          |
+-------------------------------------+--------------------------------------+
| Pola Order per Hari                 | Distribusi per Hari                  |
| [Bar Chart]                         | [Pie Chart]                          |
+-------------------------------------+--------------------------------------+
| Distribusi per Order                | Reorder Rate per Departemen          |
| [Pie Chart]                         | [Row Chart]                          |
+------------------------------------------------------------------+--------+
```

---

## Cara Menjalankan

### Prasyarat
- Docker Desktop berjalan
- Port 8080 (Airflow), 3000 (Metabase), 8123 & 9000 (ClickHouse) tidak dipakai
- Koneksi jaringan ke `http://96.9.212.102:8000`

### Step 1 — Download Plugin Metabase (sekali saja)
```bash
mkdir -p metabase-plugins
curl -L -o metabase-plugins/clickhouse.metabase-driver.jar \
  https://github.com/ClickHouse/metabase-clickhouse-driver/releases/latest/download/clickhouse.metabase-driver.jar
```

### Step 2 — Build & Jalankan
```bash
docker compose build
docker compose up -d
```

### Step 3 — Cek Status
```bash
docker compose ps
```

### Step 4 — Akses UI

| Service | URL | User | Password |
|---|---|---|---|
| Airflow | http://localhost:8080 | admin | admin |
| Metabase | http://localhost:3000 | (setup wizard) | — |
| ClickHouse HTTP | http://localhost:8123/play | admin | rahasia |

### Step 5 — Validasi Data ClickHouse
```bash
docker exec -it orders-pipeline-clickhouse-server-1 \
  clickhouse-client --user admin --password rahasia
```
```sql
SHOW TABLES FROM analytics;
SELECT * FROM analytics.orders_by_category ORDER BY total_orders DESC;
SELECT * FROM analytics.top_products ORDER BY total_orders DESC;
SELECT * FROM analytics.orders_by_dow ORDER BY order_dow;
SELECT * FROM analytics.orders_by_status;
```

### Step 6 — Koneksi Metabase ke ClickHouse
```
Settings → Admin → Databases → Add Database → ClickHouse
  Host     : clickhouse-server
  Port     : 8123
  Database : analytics
  Username : admin
  Password : rahasia
```

---
