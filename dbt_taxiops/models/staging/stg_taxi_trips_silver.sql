{{ config(materialized='view') }}

select
    vendor_id,
    payment_type,
    pickup_ts,
    dropoff_ts,
    total_amount,
    fare_amount,
    tip_amount,
    trip_distance,
    passenger_count,
    pickup_location_id,
    dropoff_location_id,

    cast(pickup_ts as date) as pickup_date,
    date_part('hour', pickup_ts) as pickup_hour,

    round(datediff('second', pickup_ts, dropoff_ts) / 60.0, 2) as trip_duration_min,

    case
        when fare_amount > 0 then tip_amount / fare_amount
        else 0.0
    end as tip_rate,

    case
        when trip_distance > 0 then fare_amount / trip_distance
        else 0.0
    end as fare_per_mile

from {{ source('raw', 'taxi_trips_silver') }}