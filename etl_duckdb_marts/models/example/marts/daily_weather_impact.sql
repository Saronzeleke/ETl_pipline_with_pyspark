{{ config(materialized='table') }}

SELECT
    pickup_date,
    pickup_borough,
    total_revenue,
    avg_precip,
    CASE
        WHEN avg_precip > 5.0 THEN 'HIGH'
        WHEN avg_precip > 1.0 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS precipitation_severity
FROM {{ source('pipeline_output', 'daily_aggregations') }}