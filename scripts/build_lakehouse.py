from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW_PATH = ROOT / "data" / "raw" / "generated" / "nyc_taxi_trips.csv"
BRONZE_DIR = ROOT / "data" / "lake" / "bronze" / "trips"
SILVER_DIR = ROOT / "data" / "lake" / "silver" / "trips"
GOLD_DIR = ROOT / "data" / "lake" / "gold"
WAREHOUSE_PATH = ROOT / "data" / "warehouse" / "lakehouse.duckdb"

ZONES = [
    (4, "Alphabet City", "Manhattan"),
    (12, "Upper East Side North", "Manhattan"),
    (24, "Upper East Side South", "Manhattan"),
    (45, "Midtown Center", "Manhattan"),
    (68, "East Chelsea", "Manhattan"),
    (87, "Financial District North", "Manhattan"),
    (132, "JFK Airport", "Queens"),
    (138, "LaGuardia Airport", "Queens"),
    (161, "Midwood", "Brooklyn"),
    (186, "Mott Haven", "Bronx"),
    (230, "Times Sq/Theatre District", "Manhattan"),
]

VENDORS = [(1, "Creative Mobile"), (2, "Curb Mobility")]
HOLIDAYS = {"2023-01-01", "2023-12-25", "2024-01-01", "2024-12-25"}
RAINSTORMS = {"2023-04-30", "2023-09-29", "2024-04-03", "2024-09-20"}


def _reset_dirs() -> None:
    for target in [BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
        if target.exists():
            shutil.rmtree(target)
    for target in [RAW_PATH.parent, WAREHOUSE_PATH.parent]:
        target.mkdir(parents=True, exist_ok=True)
    (ROOT / "data" / "lake").mkdir(parents=True, exist_ok=True)
    BRONZE_DIR.parent.mkdir(parents=True, exist_ok=True)
    SILVER_DIR.parent.mkdir(parents=True, exist_ok=True)
    GOLD_DIR.mkdir(parents=True, exist_ok=True)


def _generate_raw(rows: int = 150_000, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    start = np.datetime64("2023-01-01T00:00:00")
    end = np.datetime64("2024-12-31T23:59:59")
    seconds = int((end - start) / np.timedelta64(1, "s"))
    pickup_datetime = start + rng.integers(0, seconds, rows).astype("timedelta64[s]")
    pickup_datetime = pd.to_datetime(pickup_datetime)

    trip_minutes = rng.integers(5, 52, rows)
    dropoff_datetime = pickup_datetime + pd.to_timedelta(trip_minutes, unit="m")

    zone_ids = np.array([item[0] for item in ZONES])
    pickup_location_id = rng.choice(zone_ids, size=rows)
    dropoff_location_id = rng.choice(zone_ids, size=rows)

    trip_distance = np.round(rng.gamma(shape=2.3, scale=1.7, size=rows) + 0.3, 2)
    vendor_id = rng.choice([1, 2], size=rows, p=[0.57, 0.43])
    passenger_count = rng.integers(1, 5, rows)

    hours = pickup_datetime.hour.to_numpy()
    surge = np.where((hours >= 7) & (hours <= 9), 1.35, 1.0)
    surge = np.where((hours >= 16) & (hours <= 19), 1.28, surge)
    fare_amount = 3 + (trip_distance * 2.35) + (trip_minutes * 0.24)
    fare_amount = np.round(fare_amount * surge, 2)
    fare_amount = np.round(fare_amount * np.where(vendor_id == 1, 1.04, 0.96), 2)

    event_type = []
    for ts in pickup_datetime:
        d = ts.date().isoformat()
        if d in HOLIDAYS:
            event_type.append("holiday")
        elif d in RAINSTORMS:
            event_type.append("rainstorm")
        else:
            event_type.append("normal")
    event_type = np.array(event_type)

    tip_base = np.where((hours >= 1) & (hours <= 5), 0.11, 0.18)
    tip_base = np.where((hours >= 20) & (hours <= 23), tip_base - 0.02, tip_base)
    tip_base = np.where(event_type == "rainstorm", tip_base - 0.03, tip_base)
    tip_rate = np.clip(rng.normal(loc=tip_base, scale=0.04), 0.02, 0.35)
    tip_amount = np.round(fare_amount * tip_rate, 2)
    total_amount = np.round(fare_amount + tip_amount + 1.5, 2)

    raw_df = pd.DataFrame(
        {
            "trip_id": np.arange(1, rows + 1),
            "vendor_id": vendor_id,
            "pickup_datetime": pickup_datetime,
            "dropoff_datetime": dropoff_datetime,
            "pickup_location_id": pickup_location_id,
            "dropoff_location_id": dropoff_location_id,
            "passenger_count": passenger_count,
            "trip_distance_miles": trip_distance,
            "fare_amount": fare_amount,
            "tip_amount": tip_amount,
            "total_amount": total_amount,
            "event_type": event_type,
        }
    )
    raw_df.to_csv(RAW_PATH, index=False)
    return raw_df


def _build_lake(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        COPY (
            SELECT *,
                   DATE(pickup_datetime) AS pickup_date,
                   YEAR(pickup_datetime) AS pickup_year,
                   MONTH(pickup_datetime) AS pickup_month,
                   DAY(pickup_datetime) AS pickup_day
            FROM read_csv_auto('{RAW_PATH.as_posix()}')
        )
        TO '{BRONZE_DIR.as_posix()}'
        (FORMAT PARQUET, PARTITION_BY (pickup_year, pickup_month, pickup_day), COMPRESSION ZSTD)
        """
    )

    con.execute(
        f"""
        COPY (
            SELECT
                trip_id,
                vendor_id,
                CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
                CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
                pickup_location_id,
                dropoff_location_id,
                passenger_count,
                trip_distance_miles,
                fare_amount,
                tip_amount,
                total_amount,
                ROUND(tip_amount / NULLIF(fare_amount, 0), 4) AS tip_rate,
                event_type,
                DATE(pickup_datetime) AS pickup_date,
                HOUR(pickup_datetime) AS hour_of_day,
                YEAR(pickup_datetime) AS pickup_year,
                MONTH(pickup_datetime) AS pickup_month,
                DAY(pickup_datetime) AS pickup_day
            FROM read_parquet('{BRONZE_DIR.as_posix()}/**/*.parquet', hive_partitioning=true)
            WHERE total_amount > 0
        )
        TO '{SILVER_DIR.as_posix()}'
        (FORMAT PARQUET, PARTITION_BY (pickup_year, pickup_month, pickup_day), COMPRESSION ZSTD)
        """
    )

    con.execute(
        f"""
        COPY (
            SELECT
                pickup_date,
                pickup_location_id,
                hour_of_day,
                COUNT(*) AS trip_count,
                ROUND(SUM(total_amount), 2) AS total_revenue,
                ROUND(AVG(tip_rate), 4) AS avg_tip_rate
            FROM read_parquet('{SILVER_DIR.as_posix()}/**/*.parquet', hive_partitioning=true)
            GROUP BY 1, 2, 3
        )
        TO '{(GOLD_DIR / "zone_hourly_profitability").as_posix()}'
        (FORMAT PARQUET, PARTITION_BY (pickup_date), COMPRESSION ZSTD)
        """
    )

    con.execute(
        f"""
        COPY (
            SELECT
                pickup_date,
                vendor_id,
                COUNT(*) AS trip_count,
                ROUND(SUM(total_amount), 2) AS revenue,
                ROUND(SUM(trip_distance_miles), 2) AS miles,
                ROUND(SUM(total_amount) / NULLIF(SUM(trip_distance_miles), 0), 2) AS revenue_per_mile
            FROM read_parquet('{SILVER_DIR.as_posix()}/**/*.parquet', hive_partitioning=true)
            GROUP BY 1, 2
        )
        TO '{(GOLD_DIR / "vendor_daily_efficiency").as_posix()}'
        (FORMAT PARQUET, PARTITION_BY (pickup_date), COMPRESSION ZSTD)
        """
    )


def _build_star_schema(con: duckdb.DuckDBPyConnection) -> None:
    zones_df = pd.DataFrame(ZONES, columns=["zone_id", "zone_name", "borough"])
    vendors_df = pd.DataFrame(VENDORS, columns=["vendor_id", "vendor_name"])
    con.register("stg_zone_df", zones_df)
    con.register("stg_vendor_df", vendors_df)

    con.execute("DROP TABLE IF EXISTS fact_trips")
    con.execute("DROP TABLE IF EXISTS dim_date")
    con.execute("DROP TABLE IF EXISTS dim_zone")
    con.execute("DROP TABLE IF EXISTS dim_vendor")

    con.execute(
        """
        CREATE TABLE dim_zone AS
        SELECT zone_id, zone_name, borough
        FROM stg_zone_df
        """
    )

    con.execute(
        """
        CREATE TABLE dim_vendor AS
        SELECT vendor_id, vendor_name
        FROM stg_vendor_df
        """
    )

    con.execute(
        f"""
        CREATE TABLE dim_date AS
        SELECT
            CAST(STRFTIME(pickup_date, '%Y%m%d') AS INTEGER) AS date_id,
            pickup_date AS date_value,
            YEAR(pickup_date) AS year_num,
            MONTH(pickup_date) AS month_num,
            DAY(pickup_date) AS day_num,
            DAYNAME(pickup_date) AS weekday_name,
            CASE WHEN STRFTIME(pickup_date, '%w') IN ('0', '6') THEN TRUE ELSE FALSE END AS is_weekend,
            CASE WHEN event_type = 'holiday' THEN TRUE ELSE FALSE END AS is_holiday,
            CASE WHEN event_type = 'rainstorm' THEN TRUE ELSE FALSE END AS is_rainstorm
        FROM (
            SELECT DISTINCT pickup_date, event_type
            FROM read_parquet('{SILVER_DIR.as_posix()}/**/*.parquet', hive_partitioning=true)
        )
        """
    )

    con.execute(
        f"""
        CREATE TABLE fact_trips AS
        SELECT
            trip_id,
            CAST(STRFTIME(pickup_date, '%Y%m%d') AS INTEGER) AS date_id,
            vendor_id,
            pickup_location_id AS pickup_zone_id,
            dropoff_location_id AS dropoff_zone_id,
            pickup_datetime,
            dropoff_datetime,
            hour_of_day,
            trip_distance_miles,
            fare_amount,
            tip_amount,
            total_amount,
            tip_rate,
            event_type
        FROM read_parquet('{SILVER_DIR.as_posix()}/**/*.parquet', hive_partitioning=true)
        """
    )


def _print_partition_scan_stats(con: duckdb.DuckDBPyConnection) -> None:
    total_days = con.execute(
        f"""
        SELECT COUNT(DISTINCT pickup_date)
        FROM read_parquet('{SILVER_DIR.as_posix()}/**/*.parquet', hive_partitioning=true)
        """
    ).fetchone()[0]
    scanned_days = con.execute(
        f"""
        SELECT COUNT(DISTINCT pickup_date)
        FROM read_parquet('{SILVER_DIR.as_posix()}/**/*.parquet', hive_partitioning=true)
        WHERE pickup_date BETWEEN DATE '2024-12-24' AND DATE '2024-12-25'
        """
    ).fetchone()[0]
    pct = (scanned_days / total_days) * 100 if total_days > 0 else 0
    print(f"YoY-style filter scans {pct:.2f}% of day partitions ({scanned_days}/{total_days}).")


def main() -> None:
    _reset_dirs()
    _generate_raw()

    with duckdb.connect() as con:
        _build_lake(con)
        _print_partition_scan_stats(con)

    with duckdb.connect(WAREHOUSE_PATH.as_posix()) as con:
        _build_star_schema(con)

    print(f"Raw CSV: {RAW_PATH}")
    print(f"Parquet lake ready under: {ROOT / 'data' / 'lake'}")
    print(f"Star schema warehouse: {WAREHOUSE_PATH}")


if __name__ == "__main__":
    main()
