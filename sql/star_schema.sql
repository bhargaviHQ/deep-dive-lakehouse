CREATE TABLE dim_zone (
  zone_id INTEGER PRIMARY KEY,
  zone_name VARCHAR,
  borough VARCHAR
);

CREATE TABLE dim_vendor (
  vendor_id INTEGER PRIMARY KEY,
  vendor_name VARCHAR
);

CREATE TABLE dim_date (
  date_id INTEGER PRIMARY KEY,
  date_value DATE,
  year_num INTEGER,
  month_num INTEGER,
  day_num INTEGER,
  weekday_name VARCHAR,
  is_weekend BOOLEAN,
  is_holiday BOOLEAN,
  is_rainstorm BOOLEAN
);

CREATE TABLE fact_trips (
  trip_id BIGINT PRIMARY KEY,
  date_id INTEGER,
  vendor_id INTEGER,
  pickup_zone_id INTEGER,
  dropoff_zone_id INTEGER,
  pickup_datetime TIMESTAMP,
  dropoff_datetime TIMESTAMP,
  hour_of_day INTEGER,
  trip_distance_miles DOUBLE,
  fare_amount DOUBLE,
  tip_amount DOUBLE,
  total_amount DOUBLE,
  tip_rate DOUBLE,
  event_type VARCHAR
);
