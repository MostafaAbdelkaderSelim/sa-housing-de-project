"""
ETL Pipeline — Saudi Arabia Housing Market
==========================================
Extract  : Reads raw CSV
Transform : Cleans, enriches, validates
Load      : Writes Parquet partitioned files + SQLite warehouse
"""

import os
import logging
import hashlib
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/etl_pipeline.log", mode="a"),
    ],
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
RAW_PATH       = Path("data/raw/Housing_Market_SA_1M.csv")
PROCESSED_PATH = Path("data/processed")
WAREHOUSE_PATH = Path("data/warehouse")

PROCESSED_PATH.mkdir(parents=True, exist_ok=True)
WAREHOUSE_PATH.mkdir(parents=True, exist_ok=True)
Path("logs").mkdir(exist_ok=True)


# ══════════════════════════════════════════════════════════════════════════════
# EXTRACT
# ══════════════════════════════════════════════════════════════════════════════
def extract(path: Path, chunksize: int = 200_000) -> pd.DataFrame:
    """Read CSV in chunks to handle large files gracefully."""
    log.info(f"Extracting from {path} ...")
    chunks = []
    for i, chunk in enumerate(pd.read_csv(path, chunksize=chunksize)):
        chunks.append(chunk)
        log.info(f"  Chunk {i+1}: {len(chunk):,} rows loaded")
    df = pd.concat(chunks, ignore_index=True)
    log.info(f"Extract complete — {len(df):,} rows, {df.shape[1]} cols")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# TRANSFORM
# ══════════════════════════════════════════════════════════════════════════════
def transform(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Starting transformations ...")
    original_len = len(df)

    # 1. Parse dates
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["year"]        = df["Date"].dt.year
    df["month"]       = df["Date"].dt.month
    df["quarter"]     = df["Date"].dt.quarter
    df["day_of_week"] = df["Date"].dt.dayofweek
    df["year_month"]  = df["Date"].dt.to_period("M").astype(str)
    log.info("  ✓ Date parsed and time features extracted")

    # 2. Drop nulls in critical columns
    critical = ["Transaction_ID", "Date", "City", "Property_Type", "Transaction_Value"]
    before = len(df)
    df = df.dropna(subset=critical)
    log.info(f"  ✓ Dropped {before - len(df):,} rows with null critical fields")

    # 3. Remove duplicates
    before = len(df)
    df = df.drop_duplicates(subset=["Transaction_ID"])
    log.info(f"  ✓ Removed {before - len(df):,} duplicate Transaction_IDs")

    # 4. Filter out negative/zero values
    df = df[df["Transaction_Value"] > 0]
    df = df[df["Area_sqm"] > 0]

    # 5. Cap outliers (IQR method) on Transaction_Value
    Q1 = df["Transaction_Value"].quantile(0.01)
    Q3 = df["Transaction_Value"].quantile(0.99)
    df["Transaction_Value"] = df["Transaction_Value"].clip(Q1, Q3)
    log.info("  ✓ Outliers capped at 1st–99th percentile")

    # 6. Derived metrics
    df["price_per_sqm"]     = (df["Transaction_Value"] / df["Area_sqm"]).round(2)
    df["value_band"] = pd.cut(
        df["Transaction_Value"],
        bins=[0, 500_000, 1_000_000, 2_000_000, 5_000_000, np.inf],
        labels=["<500K", "500K-1M", "1M-2M", "2M-5M", ">5M"],
    )
    df["demand_supply_ratio"] = (df["Demand_Score"] / df["Supply_Score"].replace(0, np.nan)).round(4)

    # 7. Standardise string columns
    for col in ["City", "District", "Property_Type", "Purpose"]:
        df[col] = df[col].str.strip().str.title()

    # 8. Boolean flag for high-demand zones
    df["is_high_demand"] = df["Demand_Score"] > df["Demand_Score"].median()

    # 9. Surrogate keys (hash-based)
    df["city_key"]     = df["City"].apply(lambda x: int(hashlib.md5(x.encode()).hexdigest(), 16) % 10**6)
    df["district_key"] = df["District"].apply(lambda x: int(hashlib.md5(x.encode()).hexdigest(), 16) % 10**6)

    log.info(f"Transform complete — {len(df):,} / {original_len:,} rows retained")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# LOAD
# ══════════════════════════════════════════════════════════════════════════════
def load(df: pd.DataFrame) -> None:
    """Write processed data as partitioned Parquet + SQLite warehouse."""

    # ── Parquet (partitioned by city + year) ──────────────────────────────────
    log.info("Loading to partitioned Parquet ...")
    for (city, year), group in df.groupby(["City", "year"]):
        city_clean = city.replace(" ", "_")
        part_dir = PROCESSED_PATH / f"city={city_clean}" / f"year={year}"
        part_dir.mkdir(parents=True, exist_ok=True)
        out = part_dir / "data.parquet"
        group.to_parquet(out, index=False, engine="pyarrow")
    log.info(f"  ✓ Parquet written to {PROCESSED_PATH}")

    # ── SQLite Warehouse ───────────────────────────────────────────────────────
    log.info("Loading to SQLite warehouse ...")
    import sqlite3
    conn = sqlite3.connect(WAREHOUSE_PATH / "housing_dw.sqlite")

    # Fact table
    fact_cols = [
        "Transaction_ID", "Date", "city_key", "district_key",
        "Property_Type", "Transaction_Value", "Area_sqm",
        "price_per_sqm", "Number_of_Units", "Contract_Duration",
        "Purpose", "Demand_Score", "Supply_Score", "Interest_Rate",
        "Infrastructure_Distance_KM", "Population_Migration_Inflow",
        "Vacancy_Rate", "demand_supply_ratio", "value_band",
        "is_high_demand", "year", "month", "quarter", "year_month",
    ]
    df[fact_cols].to_sql("fact_transactions", conn, if_exists="replace", index=False)

    # Dim: City
    dim_city = df[["city_key", "City"]].drop_duplicates().rename(columns={"City": "city_name"})
    dim_city.to_sql("dim_city", conn, if_exists="replace", index=False)

    # Dim: District
    dim_district = df[["district_key", "District", "City"]].drop_duplicates().rename(columns={"District": "district_name", "City": "city_name"})
    dim_district.to_sql("dim_district", conn, if_exists="replace", index=False)

    # Dim: Date
    dim_date = df[["Date", "year", "month", "quarter", "day_of_week", "year_month"]].drop_duplicates()
    dim_date.to_sql("dim_date", conn, if_exists="replace", index=False)

    conn.close()
    log.info(f"  ✓ SQLite warehouse saved to {WAREHOUSE_PATH / 'housing_dw.sqlite'}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def run_pipeline():
    start = datetime.now()
    log.info("=" * 60)
    log.info("SA Housing Market ETL Pipeline — START")
    log.info("=" * 60)

    df_raw       = extract(RAW_PATH)
    df_clean     = transform(df_raw)
    load(df_clean)

    elapsed = (datetime.now() - start).seconds
    log.info(f"Pipeline finished in {elapsed}s ✅")
    return df_clean


if __name__ == "__main__":
    run_pipeline()
