

select
    pickup_date,
    pickup_hour,
    count(*) as total_trips,
    round(sum(total_amount), 2) as total_revenue,
    round(avg(fare_amount), 2) as avg_fare_amount,
    round(avg(tip_amount), 2) as avg_tip_amount,
    round(avg(trip_distance), 2) as avg_trip_distance,
    round(avg(trip_duration_min), 2) as avg_trip_duration_min,
    round(avg(fare_per_mile), 2) as avg_fare_per_mile,
    round(avg(tip_rate), 4) as avg_tip_rate
from TAXIOPS_DB.PUBLIC_STAGING.stg_taxi_trips_silver
group by pickup_date, pickup_hour
order by pickup_date, pickup_hour