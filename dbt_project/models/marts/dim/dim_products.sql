{{ config(
    schema='analytics',
    materialized='table' 
) }}


select
    p.product_id,
    p.product_name,
    p.brand,
    p.category,
    p.unit_price,
    s.supplier_id,
    s.supplier_name, 
    s.etl_loaded_at
from {{ ref('stg_products') }} p
left join {{ ref('stg_suppliers') }} s 
    on p.supplier_id = s.supplier_id
