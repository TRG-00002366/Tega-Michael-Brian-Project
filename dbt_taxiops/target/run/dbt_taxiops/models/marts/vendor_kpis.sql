
  
    

        create or replace transient table TAXIOPS_DB.PUBLIC_GOLD.vendor_kpis
         as
        (

select
    pickup_date,
    vendor_id,
    count(*) as total_trips,
    round(sum(total_amount), 2) as total_revenue,
    round(avg(trip_distance), 2) as avg_trip_distance,
    round(avg(fare_per_mile), 2) as avg_fare_per_mile,
    round(max(total_amount), 2) as max_trip_total
from TAXIOPS_DB.PUBLIC_STAGING.stg_taxi_trips_silver
group by pickup_date, vendor_id
order by pickup_date, vendor_id
        );
      
  