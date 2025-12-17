{{ config(materialized='table') }}

SELECT
    date AS pickup_date,
    borough AS pickup_borough,
    total_revenue,
    avg_precipitation AS avg_precip,
    CASE
        WHEN avg_precipitation > 0.5 THEN 'HIGH'
        WHEN avg_precipitation > 0.1 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS precipitation_severity
FROM {{ source('pipeline_output', 'daily_aggregations') }}