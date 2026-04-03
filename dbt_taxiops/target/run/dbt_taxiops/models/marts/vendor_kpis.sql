
  
    

        create or replace transient table TAXIOPS_DB.PUBLIC_GOLD.vendor_kpis
         as
        (

select
    s.pickup_date,
    s.vendor_id,
    d.vendor_name,
    count(*) as total_trips,
    round(sum(s.total_amount), 2) as total_revenue,
    round(avg(s.trip_distance), 2) as avg_trip_distance,
    round(avg(s.fare_amount), 2) as avg_fare_amount,
    round(avg(s.fare_per_mile), 2) as avg_fare_per_mile,
    round(avg(s.trip_duration_min), 2) as avg_trip_duration_min,
    round(max(s.total_amount), 2) as max_trip_total
from TAXIOPS_DB.PUBLIC_STAGING.stg_taxi_trips_silver s
left join TAXIOPS_DB.PUBLIC.dim_vendor d
    on s.vendor_id = d.vendor_id
group by s.pickup_date, s.vendor_id, d.vendor_name
order by s.pickup_date, s.vendor_id
        );
      
  