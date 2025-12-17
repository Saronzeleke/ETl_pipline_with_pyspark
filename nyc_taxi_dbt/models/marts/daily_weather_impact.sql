{{ config(materialized='table') }}

SELECT
    pickup_date,
    pickup_borough,
    daily_revenue AS total_revenue,
    avg_precip,
    CASE
        WHEN avg_precip > 0.5 THEN 'HIGH'
        WHEN avg_precip > 0.1 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS precipitation_severity
FROM {{ source('pipeline_output', 'daily_aggregations') }}