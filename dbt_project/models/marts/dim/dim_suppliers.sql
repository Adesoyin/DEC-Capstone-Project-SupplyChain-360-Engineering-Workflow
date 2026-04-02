{{ config(
    schema='analytics',
    materialized='table' 
) }}

select
    supplier_id,
    supplier_name,
    category as supplier_category,
    country,
    etl_loaded_at
from {{ ref('stg_suppliers') }}
