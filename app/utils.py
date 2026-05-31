from __future__ import annotations

import time
from pathlib import Path

import duckdb
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px


def run_timed_query(con: duckdb.DuckDBPyConnection, sql: str) -> tuple[pd.DataFrame, float]:
    t0 = time.perf_counter()
    df = con.execute(sql).df()
    elapsed_ms = (time.perf_counter() - t0) * 1000
    return df, elapsed_ms


def count_parquet_files(path: Path) -> int:
    return len(list(path.rglob("*.parquet"))) if path.exists() else 0


def get_dir_size_mb(path: Path) -> float:
    if not path.exists():
        return 0.0
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) / (1024 * 1024)


def get_file_size_mb(path: Path) -> float:
    return path.stat().st_size / (1024 * 1024) if path.exists() else 0.0


def build_partition_heatmap(all_dates: list[str], scanned_dates: set[str]) -> go.Figure:
    """731-day grid: orange = scanned, light grey = skipped, white = empty cell."""
    dates = pd.to_datetime(sorted(all_dates))
    n = len(dates)
    cols = 52
    rows = (n + cols - 1) // cols

    z, text, customdata = [], [], []
    for r in range(rows):
        z_row, t_row, c_row = [], [], []
        for c in range(cols):
            idx = r * cols + c
            if idx < n:
                d = dates[idx].strftime("%Y-%m-%d")
                scanned = d in scanned_dates
                z_row.append(1 if scanned else 0)
                t_row.append(d)
                c_row.append("SCANNED" if scanned else "skipped")
            else:
                z_row.append(None)  # empty padding cells
                t_row.append("")
                c_row.append("")
        z.append(z_row)
        text.append(t_row)
        customdata.append(c_row)

    # zmin=0 (skipped=grey), zmax=1 (scanned=orange); None cells render transparent
    fig = go.Figure(go.Heatmap(
        z=z,
        text=text,
        customdata=customdata,
        hovertemplate="%{text}<br>%{customdata}<extra></extra>",
        colorscale=[[0, "#e8e8e8"], [0.001, "#e8e8e8"], [0.999, "#f97316"], [1, "#f97316"]],
        zmin=0, zmax=1,
        showscale=False,
        xgap=2, ygap=2,
    ))
    fig.update_layout(
        height=220,
        margin=dict(l=0, r=0, t=10, b=0),
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        paper_bgcolor="white",
        plot_bgcolor="white",
    )
    return fig


def build_compression_chart(labels: list[str], sizes_mb: list[float], times_ms: list[float]) -> go.Figure:
    fig = go.Figure()
    colors = ["#94a3b8", "#60a5fa", "#22c55e"]
    fig.add_trace(go.Bar(
        name="File size (MB)",
        x=labels, y=sizes_mb,
        marker_color=colors,
        opacity=0.8,
        yaxis="y",
    ))
    fig.add_trace(go.Scatter(
        name="Read time (ms)",
        x=labels, y=times_ms,
        mode="lines+markers",
        marker=dict(size=10, color="#f97316"),
        line=dict(color="#f97316", width=2),
        yaxis="y2",
    ))
    fig.update_layout(
        yaxis=dict(title="Size (MB)", showgrid=True),
        yaxis2=dict(title="Read time (ms)", overlaying="y", side="right", showgrid=False),
        legend=dict(orientation="h", y=1.1),
        margin=dict(l=0, r=0, t=30, b=0),
        height=300,
        barmode="group",
    )
    return fig


def build_funnel_chart(before_rows: int, after_rows: int, before_ms: float, after_ms: float) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Funnel(
        name="Rows scanned",
        y=["Silver (raw)", "Gold (pre-agg)"],
        x=[before_rows, after_rows],
        textinfo="value+percent initial",
        marker=dict(color=["#94a3b8", "#22c55e"]),
    ))
    fig.update_layout(
        height=280,
        margin=dict(l=0, r=0, t=10, b=0),
    )
    return fig


def build_query_plan_table(explain_df: pd.DataFrame) -> pd.DataFrame:
    """Clean up DuckDB EXPLAIN ANALYZE output into a readable table."""
    if explain_df.empty:
        return explain_df
    col = explain_df.columns[0]
    lines = explain_df[col].tolist()
    rows = []
    for line in lines:
        line = str(line).strip()
        if line:
            rows.append({"Step": line})
    return pd.DataFrame(rows)
