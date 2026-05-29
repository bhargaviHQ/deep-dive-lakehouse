from __future__ import annotations

from pathlib import Path

import duckdb
import plotly.express as px
import streamlit as st

WAREHOUSE_PATH = Path(__file__).resolve().parents[1] / "data" / "warehouse" / "lakehouse.duckdb"

st.set_page_config(page_title="NYC Taxi Lakehouse Dashboard", layout="wide")
st.title("NYC Taxi Operations Dashboard")

if not WAREHOUSE_PATH.exists():
    st.error("Warehouse not found. Run `python scripts/build_lakehouse.py` first.")
    st.stop()

con = duckdb.connect(WAREHOUSE_PATH.as_posix(), read_only=True)

latest_period = con.execute(
    """
    SELECT MAX(pickup_datetime) AS latest_ts
    FROM fact_trips
    """
).fetchone()[0]

st.caption(f"Latest trip timestamp in dataset: {latest_period}")

q_zone_profit = con.execute(
    """
    WITH latest AS (
      SELECT DATE(MAX(pickup_datetime)) AS d, HOUR(MAX(pickup_datetime)) AS h FROM fact_trips
    )
    SELECT
      z.zone_name,
      SUM(CASE WHEN DATE(f.pickup_datetime) = latest.d AND f.hour_of_day = latest.h THEN f.total_amount END) AS revenue_now,
      SUM(CASE WHEN DATE(f.pickup_datetime) = latest.d - INTERVAL 1 YEAR AND f.hour_of_day = latest.h THEN f.total_amount END) AS revenue_last_year
    FROM fact_trips f
    JOIN dim_zone z ON z.zone_id = f.pickup_zone_id
    CROSS JOIN latest
    GROUP BY z.zone_name
    HAVING COALESCE(revenue_now, 0) > 0 OR COALESCE(revenue_last_year, 0) > 0
    ORDER BY revenue_now DESC NULLS LAST
    LIMIT 10
    """
).df()

q_tip_rate = con.execute(
    """
    SELECT
      z.borough,
      f.hour_of_day,
      ROUND(AVG(f.tip_rate) * 100, 2) AS tip_rate_pct
    FROM fact_trips f
    JOIN dim_zone z ON z.zone_id = f.pickup_zone_id
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
).df()

q_event_impact = con.execute(
    """
    SELECT
      d.date_value,
      CASE
        WHEN d.is_holiday THEN 'holiday'
        WHEN d.is_rainstorm THEN 'rainstorm'
        ELSE 'normal'
      END AS event_type,
      COUNT(*) AS trips
    FROM fact_trips f
    JOIN dim_date d ON d.date_id = f.date_id
    GROUP BY 1, 2
    """
).df()

q_vendor_eff = con.execute(
    """
    SELECT
      v.vendor_name,
      ROUND(SUM(f.total_amount) / NULLIF(SUM(f.trip_distance_miles), 0), 2) AS revenue_per_mile,
      ROUND(AVG(f.trip_distance_miles), 2) AS avg_miles
    FROM fact_trips f
    JOIN dim_vendor v ON v.vendor_id = f.vendor_id
    GROUP BY 1
    ORDER BY revenue_per_mile DESC
    """
).df()

left, right = st.columns(2)
with left:
    st.subheader("Most profitable pickup zones now vs same hour last year")
    st.plotly_chart(
        px.bar(
            q_zone_profit.melt(
                id_vars=["zone_name"],
                value_vars=["revenue_now", "revenue_last_year"],
                var_name="period",
                value_name="revenue",
            ),
            x="zone_name",
            y="revenue",
            color="period",
            barmode="group",
        ),
        use_container_width=True,
    )

with right:
    st.subheader("Worst tip rates by hour and borough")
    st.plotly_chart(
        px.line(q_tip_rate, x="hour_of_day", y="tip_rate_pct", color="borough"),
        use_container_width=True,
    )

left, right = st.columns(2)
with left:
    st.subheader("Demand drop after rainstorm or holiday")
    avg_event = (
        q_event_impact.groupby("event_type", as_index=False)["trips"].mean().sort_values("trips", ascending=False)
    )
    st.plotly_chart(px.bar(avg_event, x="event_type", y="trips"), use_container_width=True)

with right:
    st.subheader("Which vendor earns more per mile?")
    st.plotly_chart(px.bar(q_vendor_eff, x="vendor_name", y="revenue_per_mile"), use_container_width=True)

con.close()
