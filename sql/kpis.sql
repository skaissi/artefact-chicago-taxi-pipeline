SELECT trip_date, trip_count, total_revenue_usd
FROM daily_revenue
ORDER BY trip_date;

SELECT trip_date, trip_count, avg_duration_min, avg_trip_miles
FROM daily_trip_metrics
ORDER BY trip_date;

SELECT rank, pickup_zone, trip_count, total_revenue_usd
FROM top_pickup_zones
ORDER BY rank;
