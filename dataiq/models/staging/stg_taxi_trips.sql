{{ config(materialized='table') }}

SELECT
    "VendorID" AS vendor_id,
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    payment_type,
    "PULocationID" AS pickup_location_id,
    "DOLocationID" AS dropoff_location_id,
    ROUND(
        EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60.0,
        2
    ) AS trip_duration_minutes
FROM {{ ref('raw_taxi_trips') }}
WHERE fare_amount > 0
  AND trip_distance > 0
  AND passenger_count > 0
