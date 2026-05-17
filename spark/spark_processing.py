"""
Big Data Processing — PySpark
===============================
Demonstrates Spark-based processing of the 1M-row dataset:
  • Batch aggregations (city/district/property)
  • Partition strategies (by city, by year)
  • Spark SQL queries
  • Performance comparison notes vs Pandas

Run with:
    spark-submit spark/spark_processing.py
    OR: python spark/spark_processing.py  (uses local[*] mode)
"""

import logging
import time
from pathlib import Path

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

RAW_CSV  = "data/raw/Housing_Market_SA_1M.csv"
SPARK_OUT = "data/processed/spark_output"


def get_spark():
    """Create or retrieve a SparkSession (local mode)."""
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("SA_Housing_Market_DE")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_spark_pipeline():
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType
    from pyspark.sql.window import Window

    spark = get_spark()
    log.info("SparkSession created — reading CSV ...")

    t0 = time.time()
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(RAW_CSV)
    )
    log.info(f"  Read {df.count():,} rows in {time.time()-t0:.1f}s")

    # ── Feature Engineering ───────────────────────────────────────────────────
    df = (
        df
        .withColumn("Date",              F.to_date("Date"))
        .withColumn("year",              F.year("Date"))
        .withColumn("month",             F.month("Date"))
        .withColumn("quarter",           F.quarter("Date"))
        .withColumn("price_per_sqm",     (F.col("Transaction_Value") / F.col("Area_sqm")).cast(DoubleType()))
        .withColumn("demand_supply_ratio", (F.col("Demand_Score") / F.col("Supply_Score")))
        .withColumn("is_high_demand",    F.when(F.col("Demand_Score") > 5, 1).otherwise(0))
        .withColumn("value_band",
            F.when(F.col("Transaction_Value") < 500_000,   "<500K")
             .when(F.col("Transaction_Value") < 1_000_000, "500K-1M")
             .when(F.col("Transaction_Value") < 2_000_000, "1M-2M")
             .when(F.col("Transaction_Value") < 5_000_000, "2M-5M")
             .otherwise(">5M")
        )
    )

    df.createOrReplaceTempView("housing")

    # ── Spark SQL Aggregations ─────────────────────────────────────────────────
    log.info("Running Spark SQL aggregations ...")

    city_summary = spark.sql("""
        SELECT
            City,
            COUNT(*)                        AS total_transactions,
            ROUND(SUM(Transaction_Value), 0)  AS total_value,
            ROUND(AVG(Transaction_Value), 0)  AS avg_value,
            ROUND(AVG(price_per_sqm), 2)      AS avg_price_sqm,
            ROUND(AVG(Demand_Score), 3)       AS avg_demand,
            ROUND(AVG(Vacancy_Rate) * 100, 2) AS avg_vacancy_pct
        FROM housing
        GROUP BY City
        ORDER BY total_transactions DESC
    """)
    city_summary.show()

    yearly_trend = spark.sql("""
        SELECT
            year,
            COUNT(*)                          AS transactions,
            ROUND(AVG(Transaction_Value), 0)  AS avg_value,
            ROUND(AVG(Demand_Score), 3)       AS avg_demand,
            ROUND(AVG(Interest_Rate)*100, 2)  AS avg_interest_pct
        FROM housing
        GROUP BY year
        ORDER BY year
    """)
    yearly_trend.show()

    # ── Window Function: Rank districts by avg price within each city ─────────
    log.info("Applying window functions ...")
    window_spec = Window.partitionBy("City").orderBy(F.desc("avg_price_sqm"))

    district_ranked = (
        df.groupBy("City", "District")
          .agg(
              F.count("*").alias("txn_count"),
              F.round(F.avg("Transaction_Value"), 0).alias("avg_value"),
              F.round(F.avg("price_per_sqm"), 2).alias("avg_price_sqm"),
          )
          .withColumn("rank_in_city", F.rank().over(window_spec))
          .filter(F.col("rank_in_city") <= 5)
          .orderBy("City", "rank_in_city")
    )
    district_ranked.show(20)

    # ── Write partitioned Parquet ─────────────────────────────────────────────
    log.info("Writing partitioned Parquet output ...")
    out_path = SPARK_OUT + "/partitioned_by_city_year"
    (
        df.write
          .mode("overwrite")
          .partitionBy("City", "year")
          .parquet(out_path)
    )
    log.info(f"  ✓ Parquet written to {out_path}")

    # ── Write city summary ────────────────────────────────────────────────────
    city_summary.write.mode("overwrite").csv(SPARK_OUT + "/city_summary", header=True)
    yearly_trend.write.mode("overwrite").csv(SPARK_OUT + "/yearly_trend",  header=True)

    spark.stop()
    log.info("Spark pipeline complete ✅")


if __name__ == "__main__":
    try:
        run_spark_pipeline()
    except ImportError:
        log.warning("PySpark not installed. Install with: pip install pyspark")
        log.info("Falling back to pandas for demonstration ...")

        import pandas as pd
        df = pd.read_csv(RAW_CSV)
        df["Date"] = pd.to_datetime(df["Date"])
        df["year"] = df["Date"].dt.year
        df["price_per_sqm"] = df["Transaction_Value"] / df["Area_sqm"]

        print("\n=== City Summary (Pandas fallback) ===")
        print(df.groupby("City").agg(
            transactions=("Transaction_ID", "count"),
            avg_value=("Transaction_Value", "mean"),
            avg_price_sqm=("price_per_sqm", "mean"),
        ).round(0))
