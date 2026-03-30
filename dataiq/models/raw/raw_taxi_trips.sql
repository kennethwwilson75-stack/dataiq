{{ config(materialized='table') }}

SELECT *
FROM read_parquet('../data/raw/yellow_tripdata_2024_01.parquet')
WHERE tpep_pickup_datetime >= '2024-01-01'
  AND tpep_pickup_datetime < '2024-02-01'
