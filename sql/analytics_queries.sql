-- 1) Most profitable pickup zones right now vs same hour last year
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
ORDER BY revenue_now DESC NULLS LAST;

-- 2) Worst tip rates by hour and borough
SELECT z.borough, f.hour_of_day, ROUND(AVG(f.tip_rate) * 100, 2) AS tip_rate_pct
FROM fact_trips f
JOIN dim_zone z ON z.zone_id = f.pickup_zone_id
GROUP BY z.borough, f.hour_of_day
ORDER BY tip_rate_pct ASC;

-- 3) Demand drop after rainstorm or holiday
WITH daily AS (
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
)
SELECT event_type, ROUND(AVG(trips), 2) AS avg_daily_trips
FROM daily
GROUP BY 1
ORDER BY avg_daily_trips DESC;

-- 4) Vendor earnings per mile
SELECT
  v.vendor_name,
  ROUND(SUM(f.total_amount) / NULLIF(SUM(f.trip_distance_miles), 0), 2) AS revenue_per_mile
FROM fact_trips f
JOIN dim_vendor v ON v.vendor_id = f.vendor_id
GROUP BY v.vendor_name
ORDER BY revenue_per_mile DESC;
