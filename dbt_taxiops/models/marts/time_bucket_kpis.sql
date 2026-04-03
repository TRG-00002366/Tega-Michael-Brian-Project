{{ config(materialized='table') }}

select
    pickup_date,
    trip_time_bucket,

    count(*) as total_trips,
    round(sum(total_amount), 2) as total_revenue,
    round(avg(fare_amount), 2) as avg_fare_amount,
    round(avg(tip_rate), 4) as avg_tip_rate,
    round(avg(trip_duration_min), 2) as avg_trip_duration_min

from {{ ref('stg_taxi_trips_silver') }}
group by pickup_date, trip_time_bucket
order by pickup_date, trip_time_bucket