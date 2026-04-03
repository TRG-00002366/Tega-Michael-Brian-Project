

select distinct
    vendor_id,
    case
        when vendor_id = '1' then 'Creative Mobile Technologies'
        when vendor_id = '2' then 'VeriFone'
        else 'Unknown Vendor'
    end as vendor_name
from TAXIOPS_DB.PUBLIC_STAGING.stg_taxi_trips_silver
where vendor_id is not null