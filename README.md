# 🏘️ Saudi Arabia Housing Market — Data Engineering Portfolio Project

> **End-to-end Data Engineering pipeline** on a 1,000,000-row real-estate dataset covering 5 Saudi cities (2022–2025).

[![CI Pipeline](https://github.com/YOUR_USERNAME/sa-housing-de/actions/workflows/pipeline.yml/badge.svg)](https://github.com/YOUR_USERNAME/sa-housing-de/actions)
[![dbt](https://img.shields.io/badge/dbt-1.8-orange)](https://www.getdbt.com/)
[![Airflow](https://img.shields.io/badge/Airflow-2.9-blue)](https://airflow.apache.org/)
[![Spark](https://img.shields.io/badge/Spark-3.5-yellow)](https://spark.apache.org/)
[![Terraform](https://img.shields.io/badge/Terraform-1.5-purple)](https://www.terraform.io/)

---

## 📐 Architecture Overview

```
Raw CSV (1M rows)
      │
      ▼
┌─────────────────────────────────────────────────────────────┐
│                      AIRFLOW DAG                            │
│                                                             │
│  [Data Quality Gate]                                        │
│         │                                                   │
│  [ETL Pipeline]  ──→  Partitioned Parquet + SQLite DW      │
│         │                                                   │
│  [Data Modeling]  ──→  Star Schema + SCD2 + Mart Views     │
│         │                    │                              │
│  [PySpark]          [dbt staging → intermediate → marts]   │
│                               │                            │
│                     [Anomaly Detection]                     │
└─────────────────────────────────────────────────────────────┘
                               │
                    [HTML Dashboard (Plotly)]
                               │
                    [GitHub Pages Deployment]
```

---

## 🗂️ Project Structure

```
sa-housing-de/
│
├── data/
│   ├── raw/                      ← Original CSV (1M rows)
│   ├── processed/                ← Partitioned Parquet (by city/year)
│   └── warehouse/                ← SQLite Data Warehouse
│
├── pipelines/
│   ├── etl_pipeline.py           ← Extract → Transform → Load
│   └── data_modeling.py          ← Star Schema + SCD2 + Mart Views
│
├── spark/
│   └── spark_processing.py       ← PySpark aggregations + window functions
│
├── dbt_project/
│   ├── models/
│   │   ├── staging/              ← stg_transactions.sql
│   │   ├── intermediate/         ← int_transactions_enriched.sql
│   │   └── marts/                ← mart_city_performance.sql
│   ├── tests/                    ← Custom data tests
│   └── dbt_project.yml
│
├── data_quality/
│   ├── quality_checks.py         ← 25+ data expectations
│   └── reports/                  ← JSON quality reports
│
├── orchestration/
│   └── airflow_dag.py            ← Full DAG with 10 tasks
│
├── infrastructure/
│   └── main.tf                   ← Terraform: S3 + Glue + Redshift Serverless
│
├── dashboard/
│   └── generate_dashboard.py     ← Self-contained HTML dashboard (Plotly)
│
└── .github/workflows/
    └── pipeline.yml              ← CI/CD: lint → quality → ETL → dbt → deploy
```

---

## 📊 Dataset

| Field | Description |
|---|---|
| Transaction_ID | Unique identifier (e.g. `RIY-12345678`) |
| Date | Transaction date (2022-01-01 → 2025-09-30) |
| City | Riyadh, Jeddah, Dammam, Khobar, Jubail |
| District | Neighbourhood within the city |
| Property_Type | Apartment, Villa, Duplex, Studio, Commercial Unit, Land |
| Transaction_Value | Price in SAR |
| Area_sqm | Property area in square metres |
| Demand_Score | Market demand index (0–10) |
| Supply_Score | Market supply index (0–10) |
| Vacancy_Rate | % of vacant properties in the area |
| Interest_Rate | Applicable mortgage rate |
| Infrastructure_Distance_KM | Distance to nearest major infrastructure |
| Population_Migration_Inflow | Migration flow into the area |

**Stats:** 1,000,000 rows · 16 columns · 0 nulls · ~5 cities · ~45 districts

---

## 🚀 Quick Start

### 1. Clone & install
```bash
git clone https://github.com/YOUR_USERNAME/sa-housing-de.git
cd sa-housing-de
pip install -r requirements.txt
```

### 2. Place your data
```bash
cp Housing_Market_SA_1M.csv data/raw/
```

### 3. Run the full pipeline
```bash
# Step 1 — Data Quality
python data_quality/quality_checks.py

# Step 2 — ETL
python pipelines/etl_pipeline.py

# Step 3 — Star Schema
python pipelines/data_modeling.py

# Step 4 — dbt
cd dbt_project && dbt run && dbt test

# Step 5 — Spark (optional, requires PySpark)
spark-submit spark/spark_processing.py

# Step 6 — Dashboard
python dashboard/generate_dashboard.py
# Open: dashboard/sa_housing_dashboard.html
```

---

## 🧱 Component Details

### 1. ETL Pipeline (`pipelines/etl_pipeline.py`)
- Chunk-based CSV reading (handles files larger than RAM)
- Date parsing + time feature extraction (year, month, quarter)
- Outlier capping (1st–99th percentile IQR)
- Derived features: `price_per_sqm`, `demand_supply_ratio`, `value_band`
- Partitioned Parquet output: `city=Riyadh/year=2023/data.parquet`
- SQLite warehouse with Fact + 3 Dimension tables

### 2. Data Modeling (`pipelines/data_modeling.py`)
- **Star Schema**: `fact_transactions` + `dim_city` + `dim_district` + `dim_date` + `dim_property_type`
- **5 Mart Views**: city summary, yearly trend, property analysis, district heatmap, value band distribution
- **SCD Type 2**: yearly snapshots of city price metrics with `effective_date` / `expiry_date`

### 3. PySpark (`spark/spark_processing.py`)
- Local `[*]` mode or submit to cluster
- Spark SQL aggregations on full 1M dataset
- Window functions: rank districts by avg price within each city
- Partitioned Parquet output (city + year)
- Adaptive Query Execution (AQE) enabled

### 4. Data Quality (`data_quality/quality_checks.py`)
- 25+ custom expectations across 5 dimensions: Completeness, Uniqueness, Validity, Consistency, Timeliness
- Severity levels: `critical` / `warning` / `info`
- Quality score (0–100%)
- JSON report per run stored in `data_quality/reports/`

### 5. dbt (`dbt_project/`)
| Layer | Model | Description |
|---|---|---|
| Staging | `stg_transactions` | Type casting, null filtering, naming |
| Intermediate | `int_transactions_enriched` | City benchmarks, risk score, investment score |
| Mart | `mart_city_performance` | City KPIs with yearly ranking |

### 6. Orchestration (`orchestration/airflow_dag.py`)
- 10-task DAG, daily at 06:00 UTC
- Dependency chain: Quality → ETL → Modeling + Spark → dbt → Anomaly Detection
- Retry logic, timeout, email alerts on failure

### 7. Infrastructure (`infrastructure/main.tf`)
- **S3**: Bronze / Silver / Gold data lake buckets with versioning + lifecycle policies
- **AWS Glue**: Crawler + ETL Job (Spark on managed infrastructure)
- **Redshift Serverless**: 8 RPU auto-scaling warehouse
- **IAM**: Least-privilege roles

### 8. Dashboard (`dashboard/generate_dashboard.py`)
- Self-contained HTML file, no server needed
- Plotly charts: city transactions, avg values, yearly trend, property type pie
- Dark theme, responsive layout
- Deployed automatically to GitHub Pages via CI

---

## 🔄 CI/CD Pipeline

```
Push to main
     │
     ├── Lint (flake8 + black)
     ├── Unit tests (pytest + coverage)
     ├── Data quality gate
     ├── ETL run
     ├── dbt run + test
     └── Dashboard build → GitHub Pages
```

---

## 🛠️ Tech Stack

| Category | Technology |
|---|---|
| Language | Python 3.11 |
| ETL | Pandas, PyArrow |
| Big Data | PySpark 3.5 |
| Transformation | dbt-core 1.8 |
| Orchestration | Apache Airflow 2.9 |
| Data Quality | Custom framework (GE-inspired) |
| Warehouse | SQLite (local) / Redshift Serverless (cloud) |
| Storage | Parquet (partitioned) / S3 (cloud) |
| IaC | Terraform 1.5 |
| Visualization | Plotly |
| CI/CD | GitHub Actions |
| Cloud | AWS (S3, Glue, Redshift, IAM) |

---

## 👤 Author

Built as a **Data Engineering portfolio project** showcasing production-grade DE skills.

*Dataset: Saudi Arabia Housing Market (synthetic, 1M rows, 2022–2025)*
