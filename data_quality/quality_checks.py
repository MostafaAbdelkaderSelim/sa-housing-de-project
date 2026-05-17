"""
Data Quality Framework
=======================
Custom expectations engine inspired by Great Expectations.
Validates the housing dataset across multiple dimensions:
  ✓ Completeness   — no nulls in required fields
  ✓ Uniqueness     — Transaction_ID must be unique
  ✓ Validity       — values within expected ranges
  ✓ Consistency    — referential and logical checks
  ✓ Timeliness     — dates within expected window

Outputs a JSON report + human-readable summary.
"""

import json
import logging
import sqlite3
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, List

import pandas as pd
import numpy as np

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

REPORT_DIR = Path("data_quality/reports")
REPORT_DIR.mkdir(parents=True, exist_ok=True)


# ══════════════════════════════════════════════════════════════════════════════
# Core Data Classes
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class ExpectationResult:
    name: str
    column: str
    passed: bool
    severity: str          # "critical" | "warning" | "info"
    observed_value: Any
    expected: str
    details: str = ""


@dataclass
class QualityReport:
    run_id: str
    run_timestamp: str
    dataset: str
    total_rows: int
    results: List[ExpectationResult] = field(default_factory=list)

    @property
    def passed(self):   return [r for r in self.results if r.passed]
    @property
    def failed(self):   return [r for r in self.results if not r.passed]
    @property
    def critical_failures(self): return [r for r in self.failed if r.severity == "critical"]

    def score(self) -> float:
        if not self.results: return 0.0
        return round(len(self.passed) / len(self.results) * 100, 2)


# ══════════════════════════════════════════════════════════════════════════════
# Expectation Helpers
# ══════════════════════════════════════════════════════════════════════════════
def expect_no_nulls(df: pd.DataFrame, col: str, severity="critical") -> ExpectationResult:
    null_count = df[col].isna().sum()
    return ExpectationResult(
        name="expect_column_no_nulls",
        column=col,
        passed=null_count == 0,
        severity=severity,
        observed_value=int(null_count),
        expected="0 nulls",
        details=f"{null_count:,} null values found" if null_count else "All values present",
    )


def expect_unique(df: pd.DataFrame, col: str, severity="critical") -> ExpectationResult:
    dup_count = df.duplicated(subset=[col]).sum()
    return ExpectationResult(
        name="expect_column_unique",
        column=col,
        passed=dup_count == 0,
        severity=severity,
        observed_value=int(dup_count),
        expected="0 duplicates",
        details=f"{dup_count:,} duplicates found" if dup_count else "All values unique",
    )


def expect_values_in_set(df: pd.DataFrame, col: str, valid_set: set, severity="warning") -> ExpectationResult:
    invalid = df[~df[col].isin(valid_set)][col].unique()
    passed  = len(invalid) == 0
    return ExpectationResult(
        name="expect_values_in_set",
        column=col,
        passed=passed,
        severity=severity,
        observed_value=list(invalid)[:10],
        expected=f"Values in {valid_set}",
        details="" if passed else f"Invalid values: {list(invalid)[:5]}",
    )


def expect_value_between(df: pd.DataFrame, col: str, min_val: float, max_val: float, severity="warning") -> ExpectationResult:
    out_of_range = ((df[col] < min_val) | (df[col] > max_val)).sum()
    pct = round(out_of_range / len(df) * 100, 2)
    return ExpectationResult(
        name="expect_column_values_to_be_between",
        column=col,
        passed=out_of_range == 0,
        severity=severity,
        observed_value=f"{out_of_range:,} ({pct}%)",
        expected=f"[{min_val}, {max_val}]",
        details=f"Min={df[col].min():.2f}, Max={df[col].max():.2f}, Mean={df[col].mean():.2f}",
    )


def expect_date_range(df: pd.DataFrame, col: str, min_date: str, max_date: str, severity="warning") -> ExpectationResult:
    dates = pd.to_datetime(df[col], errors="coerce")
    out   = ((dates < min_date) | (dates > max_date)).sum()
    return ExpectationResult(
        name="expect_date_in_range",
        column=col,
        passed=out == 0,
        severity=severity,
        observed_value=f"{out:,} out-of-range",
        expected=f"{min_date} → {max_date}",
        details=f"Date range in data: {dates.min().date()} → {dates.max().date()}",
    )


def expect_no_negative(df: pd.DataFrame, col: str, severity="critical") -> ExpectationResult:
    neg = (df[col] < 0).sum()
    return ExpectationResult(
        name="expect_no_negative_values",
        column=col,
        passed=neg == 0,
        severity=severity,
        observed_value=int(neg),
        expected="No negative values",
        details=f"{neg:,} negative values found" if neg else "All values non-negative",
    )


def expect_row_count(df: pd.DataFrame, expected_count: int, tolerance: float = 0.05, severity="critical") -> ExpectationResult:
    actual  = len(df)
    pct_diff = abs(actual - expected_count) / expected_count
    passed  = pct_diff <= tolerance
    return ExpectationResult(
        name="expect_row_count",
        column="*",
        passed=passed,
        severity=severity,
        observed_value=f"{actual:,}",
        expected=f"{expected_count:,} ± {tolerance*100:.0f}%",
        details=f"Difference: {pct_diff*100:.2f}%",
    )


def expect_column_mean_between(df: pd.DataFrame, col: str, min_mean: float, max_mean: float, severity="info") -> ExpectationResult:
    mean_val = df[col].mean()
    passed   = min_mean <= mean_val <= max_mean
    return ExpectationResult(
        name="expect_column_mean_between",
        column=col,
        passed=passed,
        severity=severity,
        observed_value=round(mean_val, 4),
        expected=f"mean in [{min_mean}, {max_mean}]",
    )


# ══════════════════════════════════════════════════════════════════════════════
# Expectation Suite
# ══════════════════════════════════════════════════════════════════════════════
def run_suite(df: pd.DataFrame) -> QualityReport:
    run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    report = QualityReport(
        run_id=run_id,
        run_timestamp=datetime.now().isoformat(),
        dataset="Housing_Market_SA_1M",
        total_rows=len(df),
    )

    checks = [
        # Completeness
        expect_no_nulls(df, "Transaction_ID"),
        expect_no_nulls(df, "Date"),
        expect_no_nulls(df, "City"),
        expect_no_nulls(df, "District"),
        expect_no_nulls(df, "Property_Type"),
        expect_no_nulls(df, "Transaction_Value"),
        expect_no_nulls(df, "Area_sqm"),

        # Uniqueness
        expect_unique(df, "Transaction_ID"),

        # Validity — categorical
        expect_values_in_set(df, "City",
            {"Riyadh", "Jeddah", "Dammam", "Khobar", "Jubail"}),
        expect_values_in_set(df, "Property_Type",
            {"Apartment", "Villa", "Duplex", "Studio", "Commercial Unit", "Land"}),
        expect_values_in_set(df, "Purpose",
            {"Sale", "Rent", "Investment"}),

        # Validity — numeric ranges
        expect_value_between(df, "Transaction_Value", 50_000, 10_000_000),
        expect_value_between(df, "Area_sqm",          10, 5_000),
        expect_value_between(df, "Demand_Score",       0, 10),
        expect_value_between(df, "Supply_Score",       0, 10),
        expect_value_between(df, "Vacancy_Rate",       0, 1),
        expect_value_between(df, "Interest_Rate",      0, 0.3),

        # No negatives
        expect_no_negative(df, "Transaction_Value"),
        expect_no_negative(df, "Area_sqm"),
        expect_no_negative(df, "Number_of_Units"),

        # Date range
        expect_date_range(df, "Date", "2020-01-01", "2026-12-31"),

        # Row count
        expect_row_count(df, 1_000_000, tolerance=0.01),

        # Statistical
        expect_column_mean_between(df, "Transaction_Value", 800_000, 1_500_000),
        expect_column_mean_between(df, "Demand_Score", 3, 8),
        expect_column_mean_between(df, "Vacancy_Rate", 0.05, 0.30),
    ]

    report.results = checks
    return report


# ══════════════════════════════════════════════════════════════════════════════
# Reporting
# ══════════════════════════════════════════════════════════════════════════════
def print_report(report: QualityReport) -> None:
    print("\n" + "=" * 65)
    print(f"  DATA QUALITY REPORT — {report.run_timestamp[:19]}")
    print("=" * 65)
    print(f"  Dataset   : {report.dataset}")
    print(f"  Total Rows: {report.total_rows:,}")
    print(f"  Quality Score: {report.score():.1f}%  "
          f"({len(report.passed)} passed / {len(report.failed)} failed)")
    print("-" * 65)

    for r in report.results:
        icon = "✅" if r.passed else ("🔴" if r.severity == "critical" else "⚠️ ")
        col  = f"[{r.column}]".ljust(35)
        print(f"  {icon}  {col}  {r.name}")
        if not r.passed:
            print(f"       → Expected: {r.expected}")
            print(f"       → Observed: {r.observed_value}")
            if r.details:
                print(f"       → Details:  {r.details}")

    print("-" * 65)
    if report.critical_failures:
        print(f"  🔴 CRITICAL FAILURES: {len(report.critical_failures)}")
        for f_ in report.critical_failures:
            print(f"     • {f_.column}: {f_.details}")
    else:
        print("  ✅ No critical failures detected")
    print("=" * 65 + "\n")


def save_report(report: QualityReport) -> Path:
    results_data = []
    for r in report.results:
        results_data.append({
            "name": r.name,
            "column": r.column,
            "passed": bool(r.passed),
            "severity": r.severity,
            "observed_value": str(r.observed_value),
            "expected": str(r.expected),
            "details": r.details,
        })

    payload = {
        "run_id": report.run_id,
        "run_timestamp": report.run_timestamp,
        "dataset": report.dataset,
        "total_rows": report.total_rows,
        "quality_score": report.score(),
        "summary": {
            "total_checks": len(report.results),
            "passed": len(report.passed),
            "failed": len(report.failed),
            "critical_failures": len(report.critical_failures),
        },
        "results": results_data,
    }

    out = REPORT_DIR / f"{report.run_id}.json"
    out.write_text(json.dumps(payload, indent=2))
    log.info(f"Report saved to {out}")
    return out


def run_quality_checks(csv_path: str = "data/raw/Housing_Market_SA_1M.csv"):
    log.info("Loading dataset for quality checks ...")
    df = pd.read_csv(csv_path)
    log.info(f"  Loaded {len(df):,} rows")

    report = run_suite(df)
    print_report(report)
    save_report(report)

    if report.critical_failures:
        log.error(f"Quality gate FAILED — {len(report.critical_failures)} critical issue(s)")
        return False
    log.info(f"Quality gate PASSED — Score: {report.score():.1f}%")
    return True


if __name__ == "__main__":
    run_quality_checks()
