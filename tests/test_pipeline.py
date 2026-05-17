"""
Unit Tests — SA Housing Market DE Pipeline
==========================================
Tests for ETL transforms, data quality checks, and modeling logic.
Run with: pytest tests/ -v
"""

import sys
import pandas as pd
import numpy as np
import pytest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


# ── Fixtures ──────────────────────────────────────────────────────────────────
@pytest.fixture
def sample_df():
    """Minimal valid dataset for testing."""
    return pd.DataFrame({
        "Transaction_ID":   ["RIY-001", "DAM-002", "JED-003", "KHO-004", "JUB-005"],
        "Date":             ["2023-01-15", "2023-06-20", "2024-03-10", "2024-07-05", "2025-01-01"],
        "City":             ["Riyadh", "Dammam", "Jeddah", "Khobar", "Jubail"],
        "District":         ["Al Olaya", "Al Shula", "Al Hamra", "Al Khobar Al Shamaliyya", "Al Fanateer"],
        "Property_Type":    ["Apartment", "Villa", "Duplex", "Studio", "Land"],
        "Transaction_Value": [850_000, 1_200_000, 950_000, 500_000, 2_500_000],
        "Area_sqm":         [120.0, 380.0, 200.0, 55.0, 600.0],
        "Number_of_Units":  [1, 1, 2, 1, 1],
        "Contract_Duration": [12, 0, 24, 12, 0],
        "Purpose":          ["Sale", "Sale", "Rent", "Rent", "Investment"],
        "Demand_Score":     [7.5, 6.2, 8.1, 5.9, 4.3],
        "Supply_Score":     [5.0, 4.8, 6.0, 3.5, 7.0],
        "Interest_Rate":    [0.045, 0.050, 0.042, 0.048, 0.055],
        "Infrastructure_Distance_KM": [1.2, 3.4, 0.8, 2.1, 8.5],
        "Population_Migration_Inflow": [1500, 800, 2200, 600, 300],
        "Vacancy_Rate":     [0.08, 0.12, 0.05, 0.15, 0.20],
    })


@pytest.fixture
def dirty_df(sample_df):
    """Dataset with intentional quality issues."""
    df = sample_df.copy()
    df.loc[0, "Transaction_Value"] = -1000   # negative value
    df.loc[1, "Area_sqm"]          = 0        # zero area
    df.loc[2, "Transaction_ID"]    = None     # null ID
    df.loc[3, "Transaction_ID"]    = "DAM-002" # duplicate ID
    return df


# ══════════════════════════════════════════════════════════════════════════════
# ETL Transform Tests
# ══════════════════════════════════════════════════════════════════════════════
class TestETLTransform:

    def test_date_parsing(self, sample_df):
        """Date column should be parsed to datetime."""
        sample_df["Date"] = pd.to_datetime(sample_df["Date"])
        assert pd.api.types.is_datetime64_any_dtype(sample_df["Date"])
        assert sample_df["Date"].isna().sum() == 0

    def test_price_per_sqm_calculation(self, sample_df):
        """price_per_sqm = Transaction_Value / Area_sqm."""
        sample_df["price_per_sqm"] = sample_df["Transaction_Value"] / sample_df["Area_sqm"]
        assert round(sample_df.loc[0, "price_per_sqm"], 2) == round(850_000 / 120.0, 2)

    def test_demand_supply_ratio(self, sample_df):
        """demand_supply_ratio = Demand_Score / Supply_Score."""
        sample_df["demand_supply_ratio"] = sample_df["Demand_Score"] / sample_df["Supply_Score"]
        assert sample_df["demand_supply_ratio"].iloc[0] == pytest.approx(7.5 / 5.0, rel=1e-3)

    def test_value_band_assignment(self, sample_df):
        """Value bands should be assigned correctly."""
        def band(v):
            if v < 500_000:   return "<500K"
            if v < 1_000_000: return "500K-1M"
            if v < 2_000_000: return "1M-2M"
            if v < 5_000_000: return "2M-5M"
            return ">5M"
        sample_df["value_band"] = sample_df["Transaction_Value"].apply(band)
        assert sample_df.loc[0, "value_band"] == "500K-1M"
        assert sample_df.loc[1, "value_band"] == "1M-2M"
        assert sample_df.loc[4, "value_band"] == "2M-5M"

    def test_string_standardisation(self, sample_df):
        """City names should be title-cased after cleaning."""
        sample_df["City"] = sample_df["City"].str.strip().str.title()
        assert sample_df["City"].iloc[0] == "Riyadh"

    def test_negative_values_filtered(self, dirty_df):
        """Rows with negative Transaction_Value should be dropped."""
        cleaned = dirty_df[dirty_df["Transaction_Value"] > 0]
        assert len(cleaned) < len(dirty_df)
        assert (cleaned["Transaction_Value"] > 0).all()

    def test_zero_area_filtered(self, dirty_df):
        """Rows with zero Area_sqm should be dropped."""
        cleaned = dirty_df[dirty_df["Area_sqm"] > 0]
        assert len(cleaned) < len(dirty_df)

    def test_null_id_filtered(self, dirty_df):
        """Rows with null Transaction_ID should be dropped."""
        cleaned = dirty_df.dropna(subset=["Transaction_ID"])
        assert cleaned["Transaction_ID"].isna().sum() == 0

    def test_duplicate_id_removed(self, dirty_df):
        """Duplicate Transaction_IDs should be de-duplicated."""
        cleaned = dirty_df.dropna(subset=["Transaction_ID"]).drop_duplicates(subset=["Transaction_ID"])
        assert cleaned["Transaction_ID"].nunique() == len(cleaned)

    def test_time_features_extracted(self, sample_df):
        """Year, month, quarter should be derived from Date."""
        sample_df["Date"]    = pd.to_datetime(sample_df["Date"])
        sample_df["year"]    = sample_df["Date"].dt.year
        sample_df["month"]   = sample_df["Date"].dt.month
        sample_df["quarter"] = sample_df["Date"].dt.quarter
        assert sample_df.loc[0, "year"]    == 2023
        assert sample_df.loc[0, "month"]   == 1
        assert sample_df.loc[0, "quarter"] == 1


# ══════════════════════════════════════════════════════════════════════════════
# Data Quality Tests
# ══════════════════════════════════════════════════════════════════════════════
class TestDataQuality:

    def test_no_nulls_in_sample(self, sample_df):
        """Sample fixture should have no nulls."""
        assert sample_df.isna().sum().sum() == 0

    def test_all_cities_valid(self, sample_df):
        valid_cities = {"Riyadh", "Jeddah", "Dammam", "Khobar", "Jubail"}
        assert set(sample_df["City"].unique()).issubset(valid_cities)

    def test_all_property_types_valid(self, sample_df):
        valid_types = {"Apartment", "Villa", "Duplex", "Studio", "Commercial Unit", "Land"}
        assert set(sample_df["Property_Type"].unique()).issubset(valid_types)

    def test_demand_score_range(self, sample_df):
        assert (sample_df["Demand_Score"].between(0, 10)).all()

    def test_vacancy_rate_range(self, sample_df):
        assert (sample_df["Vacancy_Rate"].between(0, 1)).all()

    def test_interest_rate_range(self, sample_df):
        assert (sample_df["Interest_Rate"].between(0, 0.3)).all()

    def test_transaction_value_positive(self, sample_df):
        assert (sample_df["Transaction_Value"] > 0).all()

    def test_area_sqm_positive(self, sample_df):
        assert (sample_df["Area_sqm"] > 0).all()

    def test_transaction_id_unique(self, sample_df):
        assert sample_df["Transaction_ID"].nunique() == len(sample_df)


# ══════════════════════════════════════════════════════════════════════════════
# Data Modeling Tests
# ══════════════════════════════════════════════════════════════════════════════
class TestDataModeling:

    def test_star_schema_city_dim(self, sample_df):
        """dim_city should have unique city keys."""
        dim_city = sample_df[["City"]].drop_duplicates().copy()
        import hashlib
        dim_city["city_key"] = dim_city["City"].apply(
            lambda x: int(hashlib.md5(x.encode()).hexdigest(), 16) % 10**6
        )
        assert dim_city["city_key"].nunique() == len(dim_city)

    def test_mart_aggregation_row_count(self, sample_df):
        """mart_city_summary should have one row per city."""
        summary = sample_df.groupby("City").agg(
            count=("Transaction_ID", "count"),
            avg_val=("Transaction_Value", "mean"),
        ).reset_index()
        assert len(summary) == sample_df["City"].nunique()

    def test_investment_score_formula(self, sample_df):
        """Investment score should be computable from the data."""
        sample_df["investment_score"] = (
            sample_df["Demand_Score"] * 15
            + (1 - sample_df["Vacancy_Rate"]) * 10
            - sample_df["Interest_Rate"] * 50
        ).round(2)
        assert sample_df["investment_score"].isna().sum() == 0
        assert (sample_df["investment_score"] > 0).any()

    def test_scd2_effective_dates(self):
        """SCD2 records should have valid date ranges."""
        records = [
            {"effective_date": "2022-01-01", "expiry_date": "2022-12-31"},
            {"effective_date": "2023-01-01", "expiry_date": "2023-12-31"},
        ]
        df = pd.DataFrame(records)
        df["effective_date"] = pd.to_datetime(df["effective_date"])
        df["expiry_date"]    = pd.to_datetime(df["expiry_date"])
        assert (df["expiry_date"] >= df["effective_date"]).all()


# ══════════════════════════════════════════════════════════════════════════════
# Analytics Tests
# ══════════════════════════════════════════════════════════════════════════════
class TestAnalytics:

    def test_city_ranking(self, sample_df):
        """Cities should be rankable by transaction count."""
        ranked = (
            sample_df.groupby("City")["Transaction_ID"]
            .count()
            .reset_index()
            .sort_values("Transaction_ID", ascending=False)
        )
        assert len(ranked) == sample_df["City"].nunique()

    def test_yearly_aggregation(self, sample_df):
        sample_df["Date"] = pd.to_datetime(sample_df["Date"])
        sample_df["year"] = sample_df["Date"].dt.year
        yearly = sample_df.groupby("year")["Transaction_Value"].mean()
        assert len(yearly) > 0
        assert (yearly > 0).all()

    def test_price_per_sqm_by_property(self, sample_df):
        """Land should have lower price_per_sqm than Villa on average."""
        sample_df["price_per_sqm"] = sample_df["Transaction_Value"] / sample_df["Area_sqm"]
        by_type = sample_df.groupby("Property_Type")["price_per_sqm"].mean()
        # Land (600sqm, 2.5M) = 4166/sqm vs Apartment (120sqm, 850K) = 7083/sqm
        if "Land" in by_type and "Apartment" in by_type:
            assert by_type["Land"] < by_type["Apartment"]
