SELECT
    pickup_date,
    PULocationID AS location_id,
    trip_count,
    avg_fare,
    daily_revenue
FROM daily_aggregations
WHERE trip_count > 0
ORDER BY pickup_date DESC