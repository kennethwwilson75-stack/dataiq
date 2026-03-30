{{ config(materialized='table') }}

SELECT
    pt.payment_type_name,
    date_trunc('day', t.pickup_datetime) AS trip_date,
    COUNT(*) AS trip_count,
    ROUND(SUM(t.total_amount), 2) AS total_revenue,
    ROUND(AVG(t.fare_amount), 2) AS avg_fare
FROM {{ ref('stg_taxi_trips') }} t
JOIN {{ ref('stg_payment_types') }} pt
    ON t.payment_type = pt.payment_type
GROUP BY pt.payment_type_name, date_trunc('day', t.pickup_datetime)
ORDER BY trip_date, payment_type_name
