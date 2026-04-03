

select
    s.pickup_date,
    s.payment_type,
    d.payment_type_desc,
    count(*) as total_trips,
    round(sum(s.total_amount), 2) as total_revenue,
    round(avg(s.fare_amount), 2) as avg_fare_amount,
    round(avg(s.tip_amount), 2) as avg_tip_amount,
    round(avg(s.tip_rate), 4) as avg_tip_rate,
    round(avg(s.trip_distance), 2) as avg_trip_distance
from TAXIOPS_DB.PUBLIC_STAGING.stg_taxi_trips_silver s
left join TAXIOPS_DB.PUBLIC.dim_payment_type d
    on s.payment_type = d.payment_type
group by s.pickup_date, s.payment_type, d.payment_type_desc
order by s.pickup_date, s.payment_type