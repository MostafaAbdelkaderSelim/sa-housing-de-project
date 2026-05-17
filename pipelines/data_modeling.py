"""
Data Modeling — Star Schema Builder
====================================
Builds a full Star Schema from the cleaned dataset:
  Fact   : fact_transactions
  Dims   : dim_city | dim_district | dim_property_type | dim_date | dim_purpose
  Views  : mart_city_summary | mart_yearly_trend | mart_property_analysis
"""

import sqlite3
import logging
from pathlib import Path
import pandas as pd

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DB_PATH = Path("data/warehouse/housing_dw.sqlite")


# ══════════════════════════════════════════════════════════════════════════════
# DDL — Create Schema
# ══════════════════════════════════════════════════════════════════════════════
SCHEMA_DDL = """
-- ── Dimension: Date ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER PRIMARY KEY,
    full_date       TEXT,
    year            INTEGER,
    quarter         INTEGER,
    month           INTEGER,
    month_name      TEXT,
    day_of_week     INTEGER,
    day_name        TEXT,
    is_weekend      INTEGER,
    year_month      TEXT
);

-- ── Dimension: City ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_city (
    city_key        INTEGER PRIMARY KEY,
    city_name       TEXT NOT NULL,
    region          TEXT,
    is_major_city   INTEGER DEFAULT 1
);

-- ── Dimension: District ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_district (
    district_key    INTEGER PRIMARY KEY,
    district_name   TEXT NOT NULL,
    city_key        INTEGER,
    city_name       TEXT,
    FOREIGN KEY (city_key) REFERENCES dim_city(city_key)
);

-- ── Dimension: Property Type ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_property_type (
    property_type_key   INTEGER PRIMARY KEY AUTOINCREMENT,
    property_type_name  TEXT UNIQUE,
    category            TEXT,
    typical_area_min    REAL,
    typical_area_max    REAL
);

-- ── Dimension: Purpose ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_purpose (
    purpose_key     INTEGER PRIMARY KEY AUTOINCREMENT,
    purpose_name    TEXT UNIQUE,
    is_investment   INTEGER
);

-- ── Fact Table ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_sk              INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id              TEXT UNIQUE NOT NULL,
    date_key                    INTEGER,
    city_key                    INTEGER,
    district_key                INTEGER,
    property_type_key           INTEGER,
    purpose_key                 INTEGER,
    transaction_value           REAL,
    area_sqm                    REAL,
    price_per_sqm               REAL,
    number_of_units             INTEGER,
    contract_duration           INTEGER,
    demand_score                REAL,
    supply_score                REAL,
    demand_supply_ratio         REAL,
    interest_rate               REAL,
    infrastructure_distance_km  REAL,
    population_migration_inflow INTEGER,
    vacancy_rate                REAL,
    value_band                  TEXT,
    is_high_demand              INTEGER,
    year                        INTEGER,
    month                       INTEGER,
    quarter                     INTEGER,
    year_month                  TEXT,
    FOREIGN KEY (city_key)          REFERENCES dim_city(city_key),
    FOREIGN KEY (district_key)      REFERENCES dim_district(district_key),
    FOREIGN KEY (date_key)          REFERENCES dim_date(date_key)
);
"""

# ══════════════════════════════════════════════════════════════════════════════
# Mart Views
# ══════════════════════════════════════════════════════════════════════════════
MART_VIEWS = {
    "mart_city_summary": """
        CREATE VIEW IF NOT EXISTS mart_city_summary AS
        SELECT
            c.city_name,
            COUNT(f.transaction_sk)                         AS total_transactions,
            ROUND(SUM(f.transaction_value), 0)              AS total_value_sar,
            ROUND(AVG(f.transaction_value), 0)              AS avg_value_sar,
            ROUND(AVG(f.price_per_sqm), 2)                  AS avg_price_per_sqm,
            ROUND(AVG(f.area_sqm), 1)                       AS avg_area_sqm,
            ROUND(AVG(f.demand_score), 2)                   AS avg_demand_score,
            ROUND(AVG(f.vacancy_rate) * 100, 2)             AS avg_vacancy_pct,
            ROUND(AVG(f.interest_rate) * 100, 2)            AS avg_interest_rate_pct
        FROM fact_transactions f
        JOIN dim_city c ON f.city_key = c.city_key
        GROUP BY c.city_name
        ORDER BY total_transactions DESC
    """,
    "mart_yearly_trend": """
        CREATE VIEW IF NOT EXISTS mart_yearly_trend AS
        SELECT
            f.year,
            COUNT(f.transaction_sk)                         AS total_transactions,
            ROUND(SUM(f.transaction_value), 0)              AS total_value_sar,
            ROUND(AVG(f.transaction_value), 0)              AS avg_value_sar,
            ROUND(AVG(f.demand_score), 3)                   AS avg_demand,
            ROUND(AVG(f.supply_score), 3)                   AS avg_supply,
            ROUND(AVG(f.interest_rate) * 100, 2)            AS avg_interest_rate_pct,
            ROUND(AVG(f.vacancy_rate) * 100, 2)             AS avg_vacancy_pct
        FROM fact_transactions f
        GROUP BY f.year
        ORDER BY f.year
    """,
    "mart_property_analysis": """
        CREATE VIEW IF NOT EXISTS mart_property_analysis AS
        SELECT
            f.Property_Type                                 AS property_type,
            COUNT(f.transaction_sk)                         AS total_transactions,
            ROUND(AVG(f.transaction_value), 0)              AS avg_value_sar,
            ROUND(AVG(f.area_sqm), 1)                       AS avg_area_sqm,
            ROUND(AVG(f.price_per_sqm), 2)                  AS avg_price_per_sqm,
            ROUND(AVG(f.demand_score), 2)                   AS avg_demand_score,
            ROUND(SUM(f.transaction_value), 0)              AS total_market_value
        FROM fact_transactions f
        GROUP BY f.Property_Type
        ORDER BY total_transactions DESC
    """,
    "mart_district_heatmap": """
        CREATE VIEW IF NOT EXISTS mart_district_heatmap AS
        SELECT
            c.city_name,
            d.district_name,
            COUNT(f.transaction_sk)                         AS transactions,
            ROUND(AVG(f.transaction_value), 0)              AS avg_value,
            ROUND(AVG(f.demand_score), 2)                   AS avg_demand,
            ROUND(AVG(f.vacancy_rate) * 100, 2)             AS vacancy_pct,
            ROUND(AVG(f.price_per_sqm), 2)                  AS avg_price_sqm
        FROM fact_transactions f
        JOIN dim_city c     ON f.city_key     = c.city_key
        JOIN dim_district d ON f.district_key = d.district_key
        GROUP BY c.city_name, d.district_name
        ORDER BY transactions DESC
    """,
    "mart_value_band_distribution": """
        CREATE VIEW IF NOT EXISTS mart_value_band_distribution AS
        SELECT
            f.value_band,
            c.city_name,
            COUNT(f.transaction_sk)         AS count,
            ROUND(AVG(f.area_sqm), 1)       AS avg_area,
            ROUND(AVG(f.demand_score), 2)   AS avg_demand
        FROM fact_transactions f
        JOIN dim_city c ON f.city_key = c.city_key
        GROUP BY f.value_band, c.city_name
        ORDER BY f.value_band, count DESC
    """,
}


# ══════════════════════════════════════════════════════════════════════════════
# SCD Type 2 — Slowly Changing Dimension
# ══════════════════════════════════════════════════════════════════════════════
SCD2_DDL = """
CREATE TABLE IF NOT EXISTS scd2_city_metrics (
    scd_key             INTEGER PRIMARY KEY AUTOINCREMENT,
    city_key            INTEGER,
    city_name           TEXT,
    avg_price_per_sqm   REAL,
    avg_demand_score    REAL,
    avg_vacancy_rate    REAL,
    effective_date      TEXT,
    expiry_date         TEXT DEFAULT '9999-12-31',
    is_current          INTEGER DEFAULT 1,
    record_hash         TEXT
);
"""


def build_schema(conn: sqlite3.Connection) -> None:
    log.info("Creating Star Schema DDL ...")
    conn.executescript(SCHEMA_DDL)
    conn.executescript(SCD2_DDL)
    conn.commit()
    log.info("  ✓ Schema created")


def build_marts(conn: sqlite3.Connection) -> None:
    log.info("Building mart views ...")
    for name, ddl in MART_VIEWS.items():
        conn.execute(f"DROP VIEW IF EXISTS {name}")
        conn.execute(ddl)
        log.info(f"  ✓ {name}")
    conn.commit()


def populate_scd2(conn: sqlite3.Connection) -> None:
    """Simulate SCD2 by capturing yearly snapshots of city metrics."""
    log.info("Populating SCD2 city metrics ...")
    try:
        df = pd.read_sql("""
            SELECT city_key, year,
                   AVG(price_per_sqm)  AS avg_price_per_sqm,
                   AVG(demand_score)   AS avg_demand_score,
                   AVG(vacancy_rate)   AS avg_vacancy_rate
            FROM fact_transactions
            GROUP BY city_key, year
            ORDER BY city_key, year
        """, conn)

        dim_city = pd.read_sql("SELECT * FROM dim_city", conn)
        df = df.merge(dim_city, on="city_key", how="left")

        import hashlib
        rows = []
        for _, r in df.iterrows():
            record_str = f"{r['city_key']}_{r['year']}_{r['avg_price_per_sqm']:.2f}"
            rec_hash   = hashlib.md5(record_str.encode()).hexdigest()
            rows.append({
                "city_key":          int(r["city_key"]),
                "city_name":         r.get("city_name", ""),
                "avg_price_per_sqm": round(r["avg_price_per_sqm"], 2),
                "avg_demand_score":  round(r["avg_demand_score"], 4),
                "avg_vacancy_rate":  round(r["avg_vacancy_rate"], 4),
                "effective_date":    f"{int(r['year'])}-01-01",
                "expiry_date":       f"{int(r['year'])}-12-31",
                "is_current":        1 if r["year"] == df["year"].max() else 0,
                "record_hash":       rec_hash,
            })

        pd.DataFrame(rows).to_sql("scd2_city_metrics", conn, if_exists="replace", index=False)
        log.info(f"  ✓ SCD2 populated with {len(rows)} records")
    except Exception as e:
        log.warning(f"  SCD2 skipped (fact table may be empty): {e}")


def run_modeling():
    log.info("=" * 60)
    log.info("Data Modeling — Star Schema Build")
    log.info("=" * 60)
    conn = sqlite3.connect(DB_PATH)
    build_schema(conn)
    build_marts(conn)
    populate_scd2(conn)
    conn.close()
    log.info("Modeling complete ✅")


if __name__ == "__main__":
    run_modeling()
