# deep-dive-lakehouse

End-to-end local NYC taxi analytics demo with:

1. **Parquet data lake (Bronze/Silver/Gold)** with partitioning + compression  
2. **DuckDB star schema** (`fact_trips` + dimensions)  
3. **Streamlit dashboard** answering operations questions

## What this answers

- Which pickup zones are most profitable right now vs this time last year?
- What hours have the worst tip rates, and how does that change by borough?
- How did ride demand drop after a rainstorm or holiday?
- Which vendor earns more per mile?

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/build_lakehouse.py
streamlit run app/dashboard.py
```

## Deliverables in this repo

- `scripts/build_lakehouse.py`  
  - Generates reproducible synthetic NYC taxi trips for 2023-2024
  - Builds Bronze and Silver Parquet layers partitioned by `pickup_year/pickup_month/pickup_day`
  - Builds Gold Parquet marts for zone profitability and vendor efficiency
  - Builds DuckDB warehouse at `data/warehouse/lakehouse.duckdb`
  - Prints a partition-pruning metric for a YoY-style query
- `sql/star_schema.sql` — star schema definition (`fact_trips`, `dim_zone`, `dim_vendor`, `dim_date`)
- `sql/analytics_queries.sql` — SQL used by the dashboard/business questions
- `app/dashboard.py` — Streamlit dashboard with 4 operations-focused charts

## Example measurable scan output

Running the build script prints a partition scan metric similar to:

```text
YoY-style filter scans 0.27% of day partitions (2/731).
```

This shows why date partitioning helps year-over-year analytics stay fast.

## Star schema (logical)

- `fact_trips` links to:
  - `dim_zone` (pickup/dropoff lookup for borough and display names)
  - `dim_vendor` (vendor identity)
  - `dim_date` (calendar + holiday/rainstorm flags)