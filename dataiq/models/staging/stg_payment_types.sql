{{ config(materialized='table') }}

SELECT 1 AS payment_type, 'Credit Card' AS payment_type_name
UNION ALL SELECT 2, 'Cash'
UNION ALL SELECT 3, 'No Charge'
UNION ALL SELECT 4, 'Dispute'
UNION ALL SELECT 5, 'Unknown'
