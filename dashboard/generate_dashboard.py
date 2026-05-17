"""
Analytics Dashboard — SA Housing Market
=========================================
Generates a full HTML dashboard from the warehouse data.
No Streamlit/Dash dependency — pure Python + Plotly → HTML file.

Usage:
    python dashboard/generate_dashboard.py
    → opens dashboard/sa_housing_dashboard.html
"""

import sqlite3
import json
import logging
from pathlib import Path
import pandas as pd

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DB_PATH  = Path("data/warehouse/housing_dw.sqlite")
OUT_PATH = Path("dashboard/sa_housing_dashboard.html")
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)


def load_data() -> dict:
    """Load aggregated data from SQLite warehouse."""
    conn = sqlite3.connect(DB_PATH)
    data = {}

    try:
        data["city_summary"] = pd.read_sql("SELECT * FROM mart_city_summary", conn)
    except Exception:
        pass

    try:
        data["yearly_trend"] = pd.read_sql("SELECT * FROM mart_yearly_trend", conn)
    except Exception:
        pass

    try:
        data["property"]     = pd.read_sql("SELECT * FROM mart_property_analysis", conn)
    except Exception:
        pass

    # Fallback: raw aggregations from fact table
    if not data:
        df = pd.read_sql("SELECT * FROM fact_transactions LIMIT 50000", conn)
        data["city_summary"] = df.groupby("City").agg(
            total_transactions=("transaction_id","count"),
            avg_value_sar=("Transaction_Value","mean"),
        ).reset_index()

    conn.close()
    return data


def build_html(data: dict) -> str:
    """Build a self-contained HTML dashboard."""
    city_df  = data.get("city_summary",  pd.DataFrame())
    trend_df = data.get("yearly_trend",  pd.DataFrame())
    prop_df  = data.get("property",      pd.DataFrame())

    def to_json(df):
        return df.to_json(orient="records") if not df.empty else "[]"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>SA Housing Market — DE Dashboard</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; }}
  header {{
    background: linear-gradient(135deg, #1e3a5f, #2563eb);
    padding: 28px 40px; border-bottom: 2px solid #334155;
  }}
  header h1 {{ font-size: 1.8rem; font-weight: 700; letter-spacing: 1px; }}
  header p  {{ font-size: .9rem; color: #93c5fd; margin-top: 4px; }}
  .kpi-row {{
    display: flex; gap: 16px; padding: 28px 40px 0;
    flex-wrap: wrap;
  }}
  .kpi {{
    flex: 1; min-width: 180px;
    background: #1e293b; border: 1px solid #334155;
    border-radius: 12px; padding: 20px;
    text-align: center;
  }}
  .kpi .label {{ font-size: .75rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 1px; }}
  .kpi .value {{ font-size: 1.6rem; font-weight: 700; color: #60a5fa; margin-top: 6px; }}
  .charts {{
    display: grid; grid-template-columns: 1fr 1fr;
    gap: 20px; padding: 28px 40px;
  }}
  .chart-card {{
    background: #1e293b; border: 1px solid #334155;
    border-radius: 12px; padding: 20px;
  }}
  .chart-card h3 {{ font-size: .85rem; color: #94a3b8; margin-bottom: 12px; text-transform: uppercase; }}
  .chart-card.full {{ grid-column: 1 / -1; }}
  footer {{
    text-align: center; padding: 20px; color: #475569; font-size: .8rem;
    border-top: 1px solid #1e293b; margin-top: 10px;
  }}
  @media (max-width: 768px) {{
    .charts {{ grid-template-columns: 1fr; }}
    .chart-card.full {{ grid-column: 1; }}
  }}
</style>
</head>
<body>

<header>
  <h1>🏘️ Saudi Arabia Housing Market</h1>
  <p>Data Engineering Pipeline Dashboard — Built with Python + Plotly</p>
</header>

<div class="kpi-row" id="kpis">
  <div class="kpi"><div class="label">Total Transactions</div><div class="value" id="kpi-txn">—</div></div>
  <div class="kpi"><div class="label">Total Market Value</div><div class="value" id="kpi-val">—</div></div>
  <div class="kpi"><div class="label">Avg Transaction (SAR)</div><div class="value" id="kpi-avg">—</div></div>
  <div class="kpi"><div class="label">Avg Demand Score</div><div class="value" id="kpi-demand">—</div></div>
  <div class="kpi"><div class="label">Avg Vacancy Rate</div><div class="value" id="kpi-vacancy">—</div></div>
</div>

<div class="charts">

  <div class="chart-card">
    <h3>Transactions by City</h3>
    <div id="chart-city-txn" style="height:300px"></div>
  </div>

  <div class="chart-card">
    <h3>Avg Transaction Value by City (SAR)</h3>
    <div id="chart-city-val" style="height:300px"></div>
  </div>

  <div class="chart-card full">
    <h3>Yearly Market Trend</h3>
    <div id="chart-trend" style="height:320px"></div>
  </div>

  <div class="chart-card">
    <h3>Property Type Distribution</h3>
    <div id="chart-prop-pie" style="height:300px"></div>
  </div>

  <div class="chart-card">
    <h3>Avg Price per SQM by Property Type</h3>
    <div id="chart-prop-price" style="height:300px"></div>
  </div>

</div>

<footer>SA Housing Market DE Portfolio Project · Built with Python, dbt, Airflow, Spark, Terraform</footer>

<script>
const CITY  = {to_json(city_df)};
const TREND = {to_json(trend_df)};
const PROP  = {to_json(prop_df)};

const PLOTLY_LAYOUT = {{
  paper_bgcolor: 'transparent',
  plot_bgcolor:  'transparent',
  font: {{ color: '#e2e8f0', family: 'Segoe UI' }},
  margin: {{ t: 10, l: 50, r: 20, b: 50 }},
  xaxis: {{ gridcolor: '#1e3a5f' }},
  yaxis: {{ gridcolor: '#1e3a5f' }},
}};

function fmt(n) {{ return n ? (n/1e6).toFixed(1) + 'M' : '—'; }}
function fmtN(n) {{ return n ? n.toLocaleString() : '—'; }}

// KPIs
if (CITY.length) {{
  const total = CITY.reduce((a,b) => a + (b.total_transactions||0), 0);
  const totalVal = CITY.reduce((a,b) => a + (b.total_value_sar||0), 0);
  const avgVal   = totalVal / total;
  const avgDem   = CITY.reduce((a,b) => a + (b.avg_demand_score||0), 0) / CITY.length;
  const avgVac   = CITY.reduce((a,b) => a + (b.avg_vacancy_pct||0), 0) / CITY.length;
  document.getElementById('kpi-txn').textContent     = fmtN(total);
  document.getElementById('kpi-val').textContent     = 'SAR ' + fmt(totalVal);
  document.getElementById('kpi-avg').textContent     = fmtN(Math.round(avgVal));
  document.getElementById('kpi-demand').textContent  = avgDem.toFixed(2);
  document.getElementById('kpi-vacancy').textContent = avgVac.toFixed(1) + '%';
}}

// City — Transactions
if (CITY.length) {{
  Plotly.newPlot('chart-city-txn', [{{
    x: CITY.map(d => d.city_name),
    y: CITY.map(d => d.total_transactions),
    type: 'bar',
    marker: {{ color: ['#3b82f6','#8b5cf6','#06b6d4','#f59e0b','#10b981'] }},
  }}], PLOTLY_LAYOUT, {{responsive:true}});

  Plotly.newPlot('chart-city-val', [{{
    x: CITY.map(d => d.city_name),
    y: CITY.map(d => d.avg_value_sar),
    type: 'bar',
    marker: {{ color: '#60a5fa' }},
  }}], PLOTLY_LAYOUT, {{responsive:true}});
}}

// Yearly Trend
if (TREND.length) {{
  Plotly.newPlot('chart-trend', [
    {{
      x: TREND.map(d=>d.year), y: TREND.map(d=>d.avg_value_sar),
      name: 'Avg Value (SAR)', type: 'scatter', mode: 'lines+markers',
      line: {{ color: '#60a5fa', width: 3 }}, yaxis: 'y'
    }},
    {{
      x: TREND.map(d=>d.year), y: TREND.map(d=>d.total_transactions),
      name: 'Transactions', type: 'bar',
      marker: {{ color: '#8b5cf680' }}, yaxis: 'y2'
    }},
  ], {{
    ...PLOTLY_LAYOUT,
    legend: {{ bgcolor: 'transparent' }},
    yaxis:  {{ title: 'Avg Value', gridcolor: '#1e3a5f' }},
    yaxis2: {{ title: 'Transactions', overlaying: 'y', side: 'right', gridcolor: '#1e3a5f' }},
  }}, {{responsive:true}});
}}

// Property Type
if (PROP.length) {{
  Plotly.newPlot('chart-prop-pie', [{{
    labels: PROP.map(d=>d.property_type),
    values: PROP.map(d=>d.total_transactions),
    type: 'pie',
    hole: 0.4,
    marker: {{ colors: ['#3b82f6','#8b5cf6','#06b6d4','#f59e0b','#10b981','#ef4444'] }},
    textinfo: 'label+percent',
  }}], {{ ...PLOTLY_LAYOUT, showlegend: false }}, {{responsive:true}});

  Plotly.newPlot('chart-prop-price', [{{
    x: PROP.map(d=>d.property_type),
    y: PROP.map(d=>d.avg_price_per_sqm||d.avg_value_sar),
    type: 'bar',
    marker: {{ color: '#8b5cf6' }},
  }}], PLOTLY_LAYOUT, {{responsive:true}});
}}
</script>
</body>
</html>"""
    return html


def run():
    log.info("Loading warehouse data ...")
    data = load_data()
    log.info("Building dashboard HTML ...")
    html = build_html(data)
    OUT_PATH.write_text(html, encoding="utf-8")
    log.info(f"Dashboard saved → {OUT_PATH}")


if __name__ == "__main__":
    run()
