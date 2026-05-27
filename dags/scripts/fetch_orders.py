"""
fetch_orders.py
Task 1 dari Orders Analytics Pipeline.

API response structure (http://96.9.212.102:8000/orders):
  {
    "total_orders": 100,
    "orders": [
      {
        "order_id": ..., "user_id": ..., "order_number": ...,
        "order_dow": ..., "order_hour_of_day": ...,
        "days_since_prior_order": ..., "eval_set": "prior"|"train"|"test",
        "products": [
          {
            "product_id": ..., "product_name": ...,
            "aisle_id": ..., "aisle": ...,
            "department_id": ..., "department": ...,
            "add_to_cart_order": ..., "reordered": 0|1
          }, ...
        ]
      }, ...
    ]
  }

Output Parquet schema (satu baris per produk per order):
  order_id, user_id, order_number, order_dow, order_hour_of_day,
  days_since_prior_order, eval_set,
  product_id, product_name, aisle_id, aisle,
  department_id, department, add_to_cart_order, reordered
"""

import requests
import pandas as pd
import os
from datetime import datetime


# ─────────────────────────────────────────────────────────────
#  Konfigurasi
# ─────────────────────────────────────────────────────────────
ORDERS_API_URL = "http://96.9.212.102:8000/orders"
DATA_LAKE_PATH = "/opt/airflow/data_lake/orders"

HEADERS = {
    "Accept"    : "application/json",
    "User-Agent": "OrdersAnalyticsPipeline/1.0 MCI2026-ITS",
}


# ─────────────────────────────────────────────────────────────
#  Fungsi Utama
# ─────────────────────────────────────────────────────────────
def fetch_orders() -> None:
    print("=" * 60)
    print("  Membuka keran data: Orders REST API...")
    print(f"  Endpoint : {ORDERS_API_URL}")
    print("=" * 60)

    try:
        response = requests.get(ORDERS_API_URL, headers=HEADERS, timeout=30)
        response.raise_for_status()
        raw_data = response.json()

        # Ambil list orders dari response
        if isinstance(raw_data, list):
            orders = raw_data
        elif isinstance(raw_data, dict):
            orders = (
                raw_data.get("orders")
                or raw_data.get("data")
                or raw_data.get("results")
                or []
            )
        else:
            orders = []

        print(f"  → API mengembalikan {len(orders)} orders.")

        if not orders:
            raise ValueError("API mengembalikan data kosong — pipeline dihentikan.")

        # ── Ekspansi nested products → satu baris per (order, product) ──
        rows = []
        for o in orders:
            order_id    = int(o.get("order_id", 0) or 0)
            user_id     = int(o.get("user_id", 0) or 0)
            order_num   = int(o.get("order_number", 0) or 0)
            order_dow   = int(o.get("order_dow", 0) or 0)
            order_hour  = int(o.get("order_hour_of_day", 0) or 0)
            days_since  = o.get("days_since_prior_order")  # bisa None (order pertama)
            eval_set    = str(o.get("eval_set", "unknown"))

            products = o.get("products", [])
            if not products:
                continue

            for p in products:
                rows.append({
                    "order_id"              : order_id,
                    "user_id"               : user_id,
                    "order_number"          : order_num,
                    "order_dow"             : order_dow,
                    "order_hour_of_day"     : order_hour,
                    "days_since_prior_order": float(days_since) if days_since is not None else None,
                    "eval_set"              : eval_set,
                    "product_id"            : int(p.get("product_id", 0) or 0),
                    "product_name"          : str(p.get("product_name", "")),
                    "aisle_id"              : int(p.get("aisle_id", 0) or 0),
                    "aisle"                 : str(p.get("aisle", "")),
                    "department_id"         : int(p.get("department_id", 0) or 0),
                    "department"            : str(p.get("department", "")),
                    "add_to_cart_order"     : int(p.get("add_to_cart_order", 0) or 0),
                    "reordered"             : int(p.get("reordered", 0) or 0),
                })

        if not rows:
            raise ValueError("Tidak ada baris produk yang dapat di-parse dari orders.")

        df = pd.DataFrame(rows)

        # ── Type casting ─────────────────────────────────────────────
        int_cols = [
            "order_id", "user_id", "order_number", "order_dow",
            "order_hour_of_day", "product_id", "aisle_id",
            "department_id", "add_to_cart_order", "reordered",
        ]
        for col in int_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")

        # days_since_prior_order tetap float (boleh NaN untuk order pertama)
        df["days_since_prior_order"] = pd.to_numeric(
            df["days_since_prior_order"], errors="coerce"
        )

        # ── Simpan ke Data Lake ──────────────────────────────────────
        ts          = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(DATA_LAKE_PATH, f"orders_{ts}.parquet")
        os.makedirs(DATA_LAKE_PATH, exist_ok=True)
        df.to_parquet(output_path, index=False, engine="pyarrow")

        print(f"\n✅ Berhasil menyimpan {len(df)} baris ke:")
        print(f"   {output_path}")
        print(f"\n   Jumlah orders unik : {df['order_id'].nunique()}")
        print(f"   Jumlah item produk : {len(df)}")
        print(f"   Department unik    : {df['department'].nunique()}")
        print(f"   Distribusi eval_set: {df.drop_duplicates('order_id')['eval_set'].value_counts().to_dict()}")
        print(f"   Reorder rate       : {df['reordered'].mean() * 100:.1f}%")

    except requests.exceptions.Timeout:
        print("❌ Timeout saat menghubungi Orders API.")
        raise
    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP error: {e}")
        raise
    except Exception as e:
        print(f"❌ Gagal menarik data: {e}")
        raise


# ─────────────────────────────────────────────────────────────
#  Entry Point
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    fetch_orders()
