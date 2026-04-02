{{ config(
    schema='analytics',
    materialized='table' 
) }}

select
    store_id,
    store_name,
    store_open_date,
    region,
    state,
    city,
    etl_loaded_at
from {{ ref('stg_stores') }}
