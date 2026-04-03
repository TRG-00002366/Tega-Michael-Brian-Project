

with typed_trips as (

    select
        event_id,
        schema_version,

        vendor_id,
        payment_type,
        ratecode_id,
        store_and_fwd_flag,

        pickup_datetime,
        dropoff_datetime,

        try_to_timestamp_ntz(pickup_ts) as pickup_ts,
        try_to_timestamp_ntz(dropoff_ts) as dropoff_ts,
        try_to_timestamp_ntz(event_ts) as event_ts,

        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        total_amount,
        extra,
        mta_tax,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        airport_fee,

        pickup_location_id,
        dropoff_location_id

    from TAXIOPS_DB.RAW.taxi_trips_silver

)

select
    event_id,
    schema_version,

    vendor_id,
    payment_type,
    ratecode_id,
    store_and_fwd_flag,

    pickup_datetime,
    dropoff_datetime,
    pickup_ts,
    dropoff_ts,
    event_ts,

    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    extra,
    mta_tax,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,

    pickup_location_id,
    dropoff_location_id,

    cast(pickup_ts as date) as pickup_date,
    date_part('hour', pickup_ts) as pickup_hour,

    round(datediff('second', pickup_ts, dropoff_ts) / 60.0, 2) as trip_duration_min,

    case
        when fare_amount > 0 then round(tip_amount / fare_amount, 4)
        else 0.0
    end as tip_rate,

    case
        when trip_distance > 0 then round(fare_amount / trip_distance, 2)
        else 0.0
    end as fare_per_mile,

    case
        when pickup_location_id in (132, 138) or dropoff_location_id in (132, 138) then true
        else false
    end as is_airport_trip,

    case
        when date_part('hour', pickup_ts) between 6 and 10 then 'morning'
        when date_part('hour', pickup_ts) between 11 and 15 then 'midday'
        when date_part('hour', pickup_ts) between 16 and 20 then 'evening'
        else 'overnight'
    end as trip_time_bucket

from typed_trips
where pickup_ts is not null
  and dropoff_ts is not null
  and event_ts is not null
  and total_amount >= 0
  and fare_amount >= 0
  and trip_distance > 0
  and dropoff_ts > pickup_ts
  and cast(pickup_ts as date) between '2025-01-01' and '2030-12-31'