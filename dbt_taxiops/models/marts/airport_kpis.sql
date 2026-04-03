{{ config(materialized='table') }}

select
    pickup_date,
    is_airport_trip,

    count(*) as total_trips,
    round(sum(total_amount), 2) as total_revenue,
    round(avg(fare_amount), 2) as avg_fare_amount,
    round(avg(trip_distance), 2) as avg_trip_distance,
    round(avg(trip_duration_min), 2) as avg_trip_duration_min

from {{ ref('stg_taxi_trips_silver') }}
group by pickup_date, is_airport_trip
order by pickup_date, is_airport_trip