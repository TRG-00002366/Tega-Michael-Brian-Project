

select distinct
    pickup_date as date_key,
    extract(year from pickup_date) as year,
    extract(quarter from pickup_date) as quarter,
    extract(month from pickup_date) as month_num,
    to_char(pickup_date, 'Mon') as month_name,
    extract(day from pickup_date) as day_of_month,
    extract(dayofweek from pickup_date) as day_of_week_num,
    trim(to_char(pickup_date, 'Day')) as day_name,
    case
        when extract(dayofweek from pickup_date) in (1, 7) then true
        else false
    end as is_weekend
from TAXIOPS_DB.PUBLIC_STAGING.stg_taxi_trips_silver
where pickup_date is not null