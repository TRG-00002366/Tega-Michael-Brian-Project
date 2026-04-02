
  
    

        create or replace transient table TAXIOPS_DB.PUBLIC_GOLD.payment_kpis
         as
        (

select
    pickup_date,
    payment_type,
    count(*) as total_trips,
    round(sum(total_amount), 2) as total_revenue,
    round(avg(fare_amount), 2) as avg_fare_amount,
    round(avg(tip_rate), 4) as avg_tip_rate
from TAXIOPS_DB.PUBLIC_STAGING.stg_taxi_trips_silver
group by pickup_date, payment_type
order by pickup_date, payment_type
        );
      
  