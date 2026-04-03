{{ config(materialized='table') }}

select distinct
    payment_type,
    case
        when payment_type = '1' then 'Credit Card'
        when payment_type = '2' then 'Cash'
        when payment_type = '3' then 'No Charge'
        when payment_type = '4' then 'Dispute'
        when payment_type = '5' then 'Unknown'
        when payment_type = '6' then 'Voided Trip'
        else 'Other'
    end as payment_type_desc
from {{ ref('stg_taxi_trips_silver') }}
where payment_type is not null