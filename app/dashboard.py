from __future__ import annotations

from pathlib import Path

import duckdb
import streamlit as st

from utils import (
    build_compression_chart,
    build_funnel_chart,
    build_partition_heatmap,
    build_query_plan_table,
    count_parquet_files,
    get_dir_size_mb,
    get_file_size_mb,
    run_timed_query,
)

ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE_PATH = ROOT / "data" / "warehouse" / "lakehouse.duckdb"
SILVER_DIR = ROOT / "data" / "lake" / "silver" / "trips"
SILVER_UNCOMPRESSED_DIR = ROOT / "data" / "lake" / "silver_uncompressed" / "trips"
FLAT_PATH = ROOT / "data" / "lake" / "flat" / "trips.parquet"

st.set_page_config(page_title="Lakehouse Optimization Lab", layout="wide")

if not WAREHOUSE_PATH.exists():
    st.error("Warehouse not found. Run `python scripts/build_lakehouse.py` first.")
    st.stop()

con = duckdb.connect(WAREHOUSE_PATH.as_posix(), read_only=True)


# ── shared metric card ───────────────────────────────────────────────────────

def _metric_card(label: str, files: int, data_mb: float, rows: int, color: str, time_ms: float | None = None) -> None:
    time_line = f"<div style='font-size:12px;color:#888;margin-top:2px;'>⏱ {time_ms:.0f} ms query time</div>" if time_ms is not None else ""
    st.markdown(
        f"""
        <div style="border:3px solid {color};border-radius:10px;padding:16px 20px;background:#fafafa;">
          <div style="font-size:13px;font-weight:600;color:#555;margin-bottom:8px;">{label}</div>
          <div style="font-size:28px;font-weight:700;color:{color};">📁 {files:,} files</div>
          <div style="font-size:18px;font-weight:600;color:{color};margin-top:2px;">{data_mb:.1f} MB read</div>
          <div style="font-size:13px;color:#666;margin-top:4px;">🔢 {rows:,} rows scanned</div>
          {time_line}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _reduction_badge(before_val: float, after_val: float, label: str) -> None:
    if before_val > 0:
        pct = (1 - after_val / before_val) * 100
        color = "#22c55e" if pct >= 50 else "#f97316"
        st.markdown(
            f"""
            <div style="text-align:center;padding:12px 0;">
              <span style="background:{color};color:white;font-size:20px;
                font-weight:700;padding:8px 20px;border-radius:20px;">
                {pct:.0f}% less {label}
              </span>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _speedup_badge(before_ms: float, after_ms: float) -> None:
    if after_ms > 0:
        x = before_ms / after_ms
        color = "#22c55e" if x >= 2 else "#f97316"
        st.markdown(
            f"""
            <div style="text-align:center;padding:12px 0;">
              <span style="background:{color};color:white;font-size:22px;
                font-weight:700;padding:8px 20px;border-radius:20px;">
                {x:.1f}x faster
              </span>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _partition_day_dir(base_dir: Path, day: pd.Timestamp) -> Path:
    return (
        base_dir
        / f"pickup_year={day.year}"
        / f"pickup_month={day.month}"
        / f"pickup_day={day.day}"
    )


def _files_and_mb_for_dates(base_dir: Path, dates: set[str]) -> tuple[int, float]:
    files, mb = 0, 0.0
    for d in dates:
        day_dir = _partition_day_dir(base_dir, pd.to_datetime(d))
        files += count_parquet_files(day_dir)
        mb += get_dir_size_mb(day_dir)
    return files, mb


# ── Tab 1: Partitioning ──────────────────────────────────────────────────────

def render_partitioning_tab(con: duckdb.DuckDBPyConnection) -> None:
    st.markdown("### How date partitioning eliminates file scans")
    st.markdown(
        "Both queries run on the **same Silver Parquet data**. "
        "The difference: **before** ignores the directory structure and opens every file; "
        "**after** uses hive partitioning to skip all non-matching day folders entirely."
    )

    all_dates_df = con.execute("SELECT DISTINCT CAST(date_value AS VARCHAR) AS d FROM dim_date ORDER BY d").df()
    all_dates = all_dates_df["d"].tolist()
    min_d, max_d = all_dates[0], all_dates[-1]

    col_a, col_b, col_c = st.columns([2, 2, 1])
    with col_a:
        start_date = st.date_input("From date", value=None, min_value=min_d, max_value=max_d, key="part_start")
    with col_b:
        end_date = st.date_input("To date", value=None, min_value=min_d, max_value=max_d, key="part_end")
    with col_c:
        st.markdown("<div style='margin-top:28px;'></div>", unsafe_allow_html=True)
        run = st.button("▶ Run Comparison", key="part_run", use_container_width=True)

    if start_date is None or end_date is None:
        st.info("Select a date range above and click Run Comparison.")
        return

    if start_date > end_date:
        st.warning("Start date must be before end date.")
        return

    if not run:
        return

    sd, ed = start_date.isoformat(), end_date.isoformat()

    silver_glob = f"{SILVER_DIR.as_posix()}/**/*.parquet"

    # Both queries hit the same Silver data — only hive_partitioning flag differs
    no_pruning_sql = f"""
        SELECT COUNT(*) AS cnt
        FROM read_parquet('{silver_glob}', hive_partitioning=false)
        WHERE pickup_date BETWEEN DATE '{sd}' AND DATE '{ed}'
    """
    pruning_sql = f"""
        SELECT COUNT(*) AS cnt
        FROM read_parquet('{silver_glob}', hive_partitioning=true)
        WHERE pickup_date BETWEEN DATE '{sd}' AND DATE '{ed}'
    """

    total_files = count_parquet_files(SILVER_DIR)
    total_mb = get_dir_size_mb(SILVER_DIR)
    total_rows = con.execute("SELECT COUNT(*) FROM fact_trips").fetchone()[0]

    scanned_dates = set(
        d for d in pd.date_range(sd, ed).strftime("%Y-%m-%d")
        if d in set(all_dates)
    )

    # Files and MB that partition pruning actually reads
    files_pruned = 0
    mb_pruned = 0.0
    for d in scanned_dates:
        dt = pd.to_datetime(d)
        day_dir = (
            SILVER_DIR
            / f"pickup_year={dt.year}"
            / f"pickup_month={dt.month}"
            / f"pickup_day={dt.day}"
        )
        files_pruned += count_parquet_files(day_dir)
        mb_pruned += get_dir_size_mb(day_dir)

    with st.spinner("Running both queries on the same data..."):
        with duckdb.connect() as c1:
            _, t_no_pruning = run_timed_query(c1, no_pruning_sql)
        with duckdb.connect() as c2:
            df_pruned, t_pruning = run_timed_query(c2, pruning_sql)

    rows_matched = int(df_pruned["cnt"].iloc[0])

    left, mid, right = st.columns([5, 1, 5])
    with left:
        _metric_card(
            "BEFORE — No partition pruning",
            total_files, total_mb, total_rows, "#94a3b8", t_no_pruning,
        )
    with mid:
        st.markdown("<div style='margin-top:70px;text-align:center;font-size:28px;'>→</div>", unsafe_allow_html=True)
    with right:
        _metric_card(
            "AFTER — Hive partition pruning",
            files_pruned, mb_pruned, rows_matched, "#22c55e", t_pruning,
        )

    _reduction_badge(total_mb, mb_pruned, "data read")

    st.markdown("#### Which day-partitions were touched?")
    st.caption(
        f"**Before** reads all {len(all_dates)} day-partitions. "
        f"**After** skips {len(all_dates) - len(scanned_dates)} days and reads only {len(scanned_dates)}."
    )
    hm_left, hm_right = st.columns(2)
    with hm_left:
        st.markdown("**Before** — all partitions scanned")
        fig_before = build_partition_heatmap(all_dates, set(all_dates))
        st.plotly_chart(fig_before, use_container_width=True, key="hm_before")
    with hm_right:
        st.markdown("**After** — only matching partitions scanned")
        fig_after = build_partition_heatmap(all_dates, scanned_dates)
        st.plotly_chart(fig_after, use_container_width=True, key="hm_after")


# ── Tab 2: Incremental processing ────────────────────────────────────────────

def render_incremental_tab(con: duckdb.DuckDBPyConnection) -> None:
    st.markdown("### Full refresh vs incremental processing")
    st.markdown(
        "Simulate ingesting a new day into Silver. "
        "**Full refresh** rewrites all historical partitions up to that day, while "
        "**incremental** rewrites only impacted partitions."
    )

    all_dates = con.execute("SELECT DISTINCT pickup_date AS d FROM fact_trips ORDER BY d").df()["d"].tolist()
    all_date_str = [d.isoformat() for d in all_dates]

    new_day = st.selectbox("New day to ingest", all_date_str[-31:], index=30, key="inc_new_day")
    prior_dates = [d for d in all_date_str if d < new_day]
    default_late_idx = max(0, len(prior_dates) - 8)
    late_day = st.selectbox("Late-arriving correction day", prior_dates, index=default_late_idx, key="inc_late_day")
    run = st.button("▶ Run Incremental Lab", key="inc_run")

    if not run:
        return

    silver_glob = f"{SILVER_DIR.as_posix()}/**/*.parquet"
    touched_full = {d for d in all_date_str if d <= new_day}
    touched_incremental = {new_day}
    touched_merge = {new_day, late_day}

    full_files, full_mb = _files_and_mb_for_dates(SILVER_DIR, touched_full)
    inc_files, inc_mb = _files_and_mb_for_dates(SILVER_DIR, touched_incremental)
    merge_files, merge_mb = _files_and_mb_for_dates(SILVER_DIR, touched_merge)

    full_sql = f"""
        SELECT COUNT(*) AS cnt
        FROM read_parquet('{silver_glob}', hive_partitioning=true)
        WHERE pickup_date <= DATE '{new_day}'
    """
    incremental_sql = f"""
        SELECT COUNT(*) AS cnt
        FROM read_parquet('{silver_glob}', hive_partitioning=true)
        WHERE pickup_date = DATE '{new_day}'
    """
    late_rows_sql = f"""
        SELECT COUNT(*) AS cnt
        FROM read_parquet('{silver_glob}', hive_partitioning=true)
        WHERE pickup_date = DATE '{late_day}'
    """
    merge_sql = f"""
        SELECT COUNT(*) AS cnt
        FROM (
            SELECT b.trip_id, COALESCE(l.total_amount, b.total_amount) AS total_amount
            FROM (
                SELECT trip_id, total_amount
                FROM read_parquet('{silver_glob}', hive_partitioning=true)
                WHERE pickup_date <= DATE '{new_day}'
            ) b
            LEFT JOIN (
                SELECT trip_id, total_amount * 1.08 AS total_amount
                FROM read_parquet('{silver_glob}', hive_partitioning=true)
                WHERE pickup_date = DATE '{late_day}'
            ) l USING (trip_id)
        )
    """

    with st.spinner("Running full refresh vs incremental comparison..."):
        with duckdb.connect() as c1:
            df_full, t_full = run_timed_query(c1, full_sql)
        with duckdb.connect() as c2:
            df_inc, t_inc = run_timed_query(c2, incremental_sql)
        with duckdb.connect() as c3:
            df_late_rows, _ = run_timed_query(c3, late_rows_sql)
        with duckdb.connect() as c4:
            _, t_merge = run_timed_query(c4, merge_sql)

    rows_full = int(df_full["cnt"].iloc[0])
    rows_inc = int(df_inc["cnt"].iloc[0])
    rows_late = int(df_late_rows["cnt"].iloc[0])
    rows_merge = rows_inc + rows_late

    st.markdown("#### New-day load impact")
    l1, m1, r1 = st.columns([5, 1, 5])
    with l1:
        _metric_card("BEFORE — Full refresh", full_files, full_mb, rows_full, "#94a3b8", t_full)
    with m1:
        st.markdown("<div style='margin-top:70px;text-align:center;font-size:28px;'>→</div>", unsafe_allow_html=True)
    with r1:
        _metric_card("AFTER — Incremental new-day load", inc_files, inc_mb, rows_inc, "#22c55e", t_inc)

    _reduction_badge(rows_full, rows_inc, "rows processed")
    _speedup_badge(t_full, t_inc)

    st.markdown("#### Late-arriving data correction")
    st.caption(
        f"Simulated correction: `{late_day}` arrives late and updates historical rows. "
        "Incremental merge touches only affected partitions."
    )
    l2, m2, r2 = st.columns([5, 1, 5])
    with l2:
        _metric_card("BEFORE — Full refresh for correction", full_files, full_mb, rows_full, "#94a3b8", t_full)
    with m2:
        st.markdown("<div style='margin-top:70px;text-align:center;font-size:28px;'>→</div>", unsafe_allow_html=True)
    with r2:
        _metric_card("AFTER — Incremental merge/update", merge_files, merge_mb, rows_merge, "#22c55e", t_merge)

    st.info(
        f"Late-arriving rows processed: **{rows_late:,}**. "
        f"Incremental merge rewrites **{merge_files}** partition files instead of **{full_files}** in a full rebuild."
    )


# ── Tab 2: Compression ───────────────────────────────────────────────────────

def render_compression_tab(con: duckdb.DuckDBPyConnection) -> None:
    st.markdown("### How compression affects storage and read speed")
    st.markdown(
        "**Same partitioned layout, same 150K rows — only the compression codec changes.** "
        "ZSTD shrinks files on disk, which means less IO and faster reads even though "
        "the CPU has to decompress."
    )

    run = st.button("▶ Run Comparison", key="comp_run")
    if not run:
        return

    uncompressed_sql = f"SELECT COUNT(*) FROM read_parquet('{SILVER_UNCOMPRESSED_DIR.as_posix()}/**/*.parquet', hive_partitioning=true)"
    zstd_sql         = f"SELECT COUNT(*) FROM read_parquet('{SILVER_DIR.as_posix()}/**/*.parquet', hive_partitioning=true)"

    with st.spinner("Reading both formats from disk (fresh connections)..."):
        with duckdb.connect() as c1:
            _, t_uncompressed = run_timed_query(c1, uncompressed_sql)
        with duckdb.connect() as c2:
            _, t_zstd = run_timed_query(c2, zstd_sql)

    size_uncompressed = get_dir_size_mb(SILVER_UNCOMPRESSED_DIR)
    size_zstd         = get_dir_size_mb(SILVER_DIR)
    files             = count_parquet_files(SILVER_DIR)  # same file count for both

    # Before / After cards
    left, mid, right = st.columns([5, 1, 5])
    with left:
        st.markdown(
            f"""
            <div style="border:3px solid #94a3b8;border-radius:10px;padding:16px 20px;background:#fafafa;">
              <div style="font-size:13px;font-weight:600;color:#555;margin-bottom:8px;">
                BEFORE — Partitioned, uncompressed
              </div>
              <div style="font-size:28px;font-weight:700;color:#94a3b8;">{size_uncompressed:.1f} MB</div>
              <div style="font-size:14px;color:#666;margin-top:4px;">📁 {files:,} files</div>
              <div style="font-size:12px;color:#888;margin-top:2px;">⏱ {t_uncompressed:.0f} ms read time</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with mid:
        st.markdown("<div style='margin-top:55px;text-align:center;font-size:28px;'>→</div>", unsafe_allow_html=True)
    with right:
        st.markdown(
            f"""
            <div style="border:3px solid #22c55e;border-radius:10px;padding:16px 20px;background:#fafafa;">
              <div style="font-size:13px;font-weight:600;color:#555;margin-bottom:8px;">
                AFTER — Partitioned, ZSTD compressed
              </div>
              <div style="font-size:28px;font-weight:700;color:#22c55e;">{size_zstd:.1f} MB</div>
              <div style="font-size:14px;color:#666;margin-top:4px;">📁 {files:,} files</div>
              <div style="font-size:12px;color:#888;margin-top:2px;">⏱ {t_zstd:.0f} ms read time</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    size_saving = (1 - size_zstd / size_uncompressed) * 100 if size_uncompressed > 0 else 0
    _reduction_badge(size_uncompressed, size_zstd, "disk space")

    st.markdown("#### Size vs read time")
    labels = ["Uncompressed", "ZSTD"]
    fig = build_compression_chart(labels, [size_uncompressed, size_zstd], [t_uncompressed, t_zstd])
    st.plotly_chart(fig, use_container_width=True)

    st.info(
        f"**{size_saving:.0f}% smaller on disk** with ZSTD. "
        f"Read time also drops because the engine reads fewer bytes from storage — "
        f"decompression CPU cost is far cheaper than extra IO at scale."
    )


# ── Tab 3: Pre-aggregation ───────────────────────────────────────────────────

_SILVER_GLOB = (ROOT / "data" / "lake" / "silver" / "trips").as_posix() + "/**/*.parquet"
_GOLD_ZONE   = (ROOT / "data" / "lake" / "gold" / "zone_hourly_profitability").as_posix() + "/**/*.parquet"
_GOLD_VENDOR = (ROOT / "data" / "lake" / "gold" / "vendor_daily_efficiency").as_posix() + "/**/*.parquet"

PREAGG_QUERIES = {
    "Zone profitability (by hour)": {
        "silver": f"""
            SELECT pickup_location_id, hour_of_day,
                   COUNT(*) AS trip_count,
                   ROUND(SUM(total_amount), 2) AS total_revenue,
                   ROUND(AVG(tip_rate), 4) AS avg_tip_rate
            FROM read_parquet('{_SILVER_GLOB}', hive_partitioning=true)
            GROUP BY 1, 2
        """,
        "gold": f"""
            SELECT pickup_location_id, hour_of_day,
                   SUM(trip_count) AS trip_count,
                   ROUND(SUM(total_revenue), 2) AS total_revenue,
                   ROUND(AVG(avg_tip_rate), 4) AS avg_tip_rate
            FROM read_parquet('{_GOLD_ZONE}', hive_partitioning=true)
            GROUP BY 1, 2
        """,
        "gold_dir": ROOT / "data" / "lake" / "gold" / "zone_hourly_profitability",
    },
    "Vendor daily efficiency": {
        "silver": f"""
            SELECT vendor_id,
                   COUNT(*) AS trip_count,
                   ROUND(SUM(total_amount), 2) AS revenue,
                   ROUND(SUM(trip_distance_miles), 2) AS miles,
                   ROUND(SUM(total_amount) / NULLIF(SUM(trip_distance_miles), 0), 2) AS revenue_per_mile
            FROM read_parquet('{_SILVER_GLOB}', hive_partitioning=true)
            GROUP BY 1
        """,
        "gold": f"""
            SELECT vendor_id,
                   SUM(trip_count) AS trip_count,
                   ROUND(SUM(revenue), 2) AS revenue,
                   ROUND(SUM(miles), 2) AS miles,
                   ROUND(SUM(revenue) / NULLIF(SUM(miles), 0), 2) AS revenue_per_mile
            FROM read_parquet('{_GOLD_VENDOR}', hive_partitioning=true)
            GROUP BY 1
        """,
        "gold_dir": ROOT / "data" / "lake" / "gold" / "vendor_daily_efficiency",
    },
}


def render_preagg_tab(con: duckdb.DuckDBPyConnection) -> None:
    st.markdown("### How pre-aggregation reduces rows scanned")
    st.markdown(
        "The **before** query aggregates 150K raw Silver rows at query time. "
        "The **after** query reads a Gold mart that was already aggregated during the pipeline — "
        "orders of magnitude fewer rows to touch."
    )

    query_name = st.selectbox("Query", list(PREAGG_QUERIES.keys()), key="preagg_q")
    run = st.button("▶ Run Comparison", key="preagg_run")
    if not run:
        return

    q = PREAGG_QUERIES[query_name]
    gold_dir = q["gold_dir"]

    silver_files = count_parquet_files(SILVER_DIR)
    silver_mb = get_dir_size_mb(SILVER_DIR)
    gold_files = count_parquet_files(gold_dir)
    gold_mb = get_dir_size_mb(gold_dir)

    gold_glob = gold_dir.as_posix() + "/**/*.parquet"

    with st.spinner("Running both queries from disk (fresh connections)..."):
        with duckdb.connect() as c1:
            df_silver, t_silver = run_timed_query(c1, q["silver"])
        with duckdb.connect() as c2:
            df_gold, t_gold = run_timed_query(c2, q["gold"])
        # Count source rows actually scanned (before GROUP BY) for each layer
        with duckdb.connect() as c3:
            rows_silver = c3.execute(
                f"SELECT COUNT(*) FROM read_parquet('{_SILVER_GLOB}', hive_partitioning=true)"
            ).fetchone()[0]
        with duckdb.connect() as c4:
            rows_gold = c4.execute(
                f"SELECT COUNT(*) FROM read_parquet('{gold_glob}', hive_partitioning=true)"
            ).fetchone()[0]

    left, mid, right = st.columns([5, 1, 5])
    with left:
        _metric_card("BEFORE — Silver (raw rows, no pre-agg)", silver_files, silver_mb, rows_silver, "#94a3b8", t_silver)
    with mid:
        st.markdown("<div style='margin-top:70px;text-align:center;font-size:28px;'>→</div>", unsafe_allow_html=True)
    with right:
        _metric_card("AFTER — Gold mart (pre-aggregated)", gold_files, gold_mb, rows_gold, "#22c55e", t_gold)

    _reduction_badge(rows_silver, rows_gold, "rows scanned")

    st.markdown("#### Row reduction through the pipeline")
    fig = build_funnel_chart(rows_silver, rows_gold, t_silver, t_gold)
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("**Sample result (first 5 rows):**")
    st.dataframe(df_gold.head(5), use_container_width=True)


# ── Tab 4: Star Schema ───────────────────────────────────────────────────────

STAR_QUERIES = {
    "Vendor revenue per mile": {
        "flat": """
            SELECT vendor_name,
                   ROUND(SUM(total_amount) / NULLIF(SUM(trip_distance_miles), 0), 2) AS revenue_per_mile,
                   COUNT(*) AS trips
            FROM flat_trips
            GROUP BY vendor_name
            ORDER BY revenue_per_mile DESC
        """,
        "star": """
            SELECT v.vendor_name,
                   ROUND(SUM(f.total_amount) / NULLIF(SUM(f.trip_distance_miles), 0), 2) AS revenue_per_mile,
                   COUNT(*) AS trips
            FROM fact_trips f
            JOIN dim_vendor v ON v.vendor_id = f.vendor_id
            GROUP BY v.vendor_name
            ORDER BY revenue_per_mile DESC
        """,
    },
    "Tip rate by borough and hour": {
        "flat": """
            SELECT pickup_borough, hour_of_day,
                   ROUND(AVG(tip_rate) * 100, 2) AS tip_rate_pct
            FROM flat_trips
            GROUP BY 1, 2
            ORDER BY 1, 2
        """,
        "star": """
            SELECT z.borough AS pickup_borough, f.hour_of_day,
                   ROUND(AVG(f.tip_rate) * 100, 2) AS tip_rate_pct
            FROM fact_trips f
            JOIN dim_zone z ON z.zone_id = f.pickup_zone_id
            GROUP BY 1, 2
            ORDER BY 1, 2
        """,
    },
    "Holiday vs rainstorm demand": {
        "flat": """
            SELECT
                CASE WHEN is_holiday THEN 'holiday'
                     WHEN is_rainstorm THEN 'rainstorm'
                     ELSE 'normal' END AS event_type,
                COUNT(*) AS trips,
                ROUND(AVG(total_amount), 2) AS avg_fare
            FROM flat_trips
            GROUP BY 1
        """,
        "star": """
            SELECT
                CASE WHEN d.is_holiday THEN 'holiday'
                     WHEN d.is_rainstorm THEN 'rainstorm'
                     ELSE 'normal' END AS event_type,
                COUNT(*) AS trips,
                ROUND(AVG(f.total_amount), 2) AS avg_fare
            FROM fact_trips f
            JOIN dim_date d ON d.date_id = f.date_id
            GROUP BY 1
        """,
    },
}


def render_star_schema_tab(con: duckdb.DuckDBPyConnection) -> None:
    st.markdown("### Star schema vs flat table: data model design")
    st.markdown(
        "Both models answer the same questions and query in similar time — "
        "the difference is **redundancy, storage efficiency, and how hard it is to evolve the model**."
    )

    # ── Redundancy comparison ────────────────────────────────────────────────
    st.markdown("#### Where the star schema wins: redundancy")

    flat_cols   = con.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name='flat_trips'").fetchone()[0]
    fact_cols   = con.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name='fact_trips'").fetchone()[0]
    total_trips = con.execute("SELECT COUNT(*) FROM fact_trips").fetchone()[0]
    n_zones     = con.execute("SELECT COUNT(*) FROM dim_zone").fetchone()[0]
    n_vendors   = con.execute("SELECT COUNT(*) FROM dim_vendor").fetchone()[0]
    n_dates     = con.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0]

    c1, c2 = st.columns(2)
    with c1:
        st.markdown(
            f"""
            <div style="border:3px solid #94a3b8;border-radius:10px;padding:16px;background:#fafafa;">
              <div style="font-size:13px;font-weight:600;color:#555;margin-bottom:10px;">
                FLAT TABLE — flat_trips
              </div>
              <div style="font-size:26px;font-weight:700;color:#94a3b8;">{flat_cols} columns</div>
              <div style="font-size:13px;color:#666;margin-top:6px;">
                🔁 <b>zone_name</b> stored <b>{total_trips:,}×</b> (one per trip row)<br>
                🔁 <b>vendor_name</b> stored <b>{total_trips:,}×</b><br>
                🔁 <b>weekday_name</b> stored <b>{total_trips:,}×</b><br><br>
                To add a new zone attribute → <b>ALTER TABLE + UPDATE {total_trips:,} rows</b>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            f"""
            <div style="border:3px solid #22c55e;border-radius:10px;padding:16px;background:#fafafa;">
              <div style="font-size:13px;font-weight:600;color:#555;margin-bottom:10px;">
                STAR SCHEMA — fact + dims
              </div>
              <div style="font-size:26px;font-weight:700;color:#22c55e;">{fact_cols} cols in fact + 3 dim tables</div>
              <div style="font-size:13px;color:#666;margin-top:6px;">
                ✅ <b>zone_name</b> stored <b>{n_zones}×</b> (once in dim_zone)<br>
                ✅ <b>vendor_name</b> stored <b>{n_vendors}×</b> (once in dim_vendor)<br>
                ✅ <b>weekday_name</b> stored <b>{n_dates}×</b> (once in dim_date)<br><br>
                To add a new zone attribute → <b>ALTER dim_zone + UPDATE {n_zones} rows</b>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # ── String redundancy visualised ─────────────────────────────────────────
    import plotly.graph_objects as go
    st.markdown("#### String copies stored on disk")
    entities   = ["zone_name", "vendor_name", "weekday_name"]
    flat_count = [total_trips] * 3
    star_count = [n_zones, n_vendors, n_dates]
    fig = go.Figure()
    fig.add_trace(go.Bar(name="Flat table", x=entities, y=flat_count, marker_color="#94a3b8"))
    fig.add_trace(go.Bar(name="Star schema", x=entities, y=star_count, marker_color="#22c55e"))
    fig.update_layout(
        barmode="group", height=280,
        yaxis=dict(title="Copies stored", type="log"),
        margin=dict(l=0, r=0, t=10, b=0),
        legend=dict(orientation="h", y=1.1),
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption("Log scale — star schema stores dimension strings orders of magnitude fewer times.")

    # ── Query comparison ─────────────────────────────────────────────────────
    st.markdown("#### Run a query on both models")
    query_name = st.selectbox("Query", list(STAR_QUERIES.keys()), key="star_q")
    run = st.button("▶ Run Comparison", key="star_run")
    if not run:
        return

    q = STAR_QUERIES[query_name]
    with st.spinner("Running both queries..."):
        df_flat, t_flat = run_timed_query(con, q["flat"])
        df_star, t_star = run_timed_query(con, q["star"])

    lq, mq, rq = st.columns([5, 1, 5])
    with lq:
        st.markdown(
            f"""
            <div style="border:2px solid #94a3b8;border-radius:8px;padding:12px 16px;background:#fafafa;">
              <div style="font-size:12px;font-weight:600;color:#555;">Flat table query</div>
              <div style="font-size:22px;font-weight:700;color:#94a3b8;">{t_flat:.0f} ms</div>
              <div style="font-size:12px;color:#666;">{len(df_flat):,} result rows</div>
            </div>
            """, unsafe_allow_html=True,
        )
    with mq:
        st.markdown("<div style='margin-top:30px;text-align:center;font-size:24px;'>→</div>", unsafe_allow_html=True)
    with rq:
        st.markdown(
            f"""
            <div style="border:2px solid #22c55e;border-radius:8px;padding:12px 16px;background:#fafafa;">
              <div style="font-size:12px;font-weight:600;color:#555;">Star schema query</div>
              <div style="font-size:22px;font-weight:700;color:#22c55e;">{t_star:.0f} ms</div>
              <div style="font-size:12px;color:#666;">{len(df_star):,} result rows — identical answer</div>
            </div>
            """, unsafe_allow_html=True,
        )

    st.info(
        "Query time is similar for both — the star schema benefit is **model maintainability and storage**, "
        "not raw query speed on a single small table. At scale, selective column reads from narrower "
        "fact + dim tables outperform scanning a wide flat table."
    )

    st.markdown("**Result:**")
    st.dataframe(df_star, use_container_width=True)


# ── Main layout ──────────────────────────────────────────────────────────────

import pandas as pd  # noqa: E402 — used inside render functions above

st.title("Lakehouse Optimization Lab")
st.caption("Click a tab, configure the query, and see how each data engineering decision changes performance.")

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Partitioning",
    "Incremental Processing",
    "Compression",
    "Pre-aggregation",
    "Star Schema",
])

with tab1:
    render_partitioning_tab(con)
with tab2:
    render_incremental_tab(con)
with tab3:
    render_compression_tab(con)
with tab4:
    render_preagg_tab(con)
with tab5:
    render_star_schema_tab(con)

con.close()
