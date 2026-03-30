{{ config(materialized='table') }}

SELECT
    date_trunc('day', pickup_datetime) AS trip_date,
    COUNT(*) AS trip_count,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(trip_distance), 2) AS avg_distance,
    ROUND(AVG(CASE WHEN fare_amount > 0 THEN tip_amount / fare_amount * 100 ELSE 0 END), 2) AS avg_tip_pct
FROM {{ ref('stg_taxi_trips') }}
GROUP BY date_trunc('day', pickup_datetime)
ORDER BY trip_date
