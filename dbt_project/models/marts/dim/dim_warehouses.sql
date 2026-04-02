{{ config(
    schema='analytics',
    materialized='table' 
) }}

select
    warehouse_id,
    state as warehouse_state,
    city as warehouse_city,
    etl_loaded_at
from {{ ref('stg_warehouses') }}
