{{ config(materialized='table') }}

SELECT
    date_trunc('hour', pickup_datetime) AS pickup_hour,
    COUNT(*) AS trip_count,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(SUM(total_amount), 2) AS total_revenue
FROM {{ ref('stg_taxi_trips') }}
GROUP BY date_trunc('hour', pickup_datetime)
ORDER BY pickup_hour
