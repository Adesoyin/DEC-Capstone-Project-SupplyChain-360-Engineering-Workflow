{{ config(
    schema='analytics',
    materialized='table' 
) }}

SELECT
    p.product_id,
    p.product_name,
    w.warehouse_id,
    w.warehouse_city,
    w.warehouse_state,

    i.snapshot_date,
    i.inventory_business_date,
    i.quantity_available,
    i.reorder_threshold,
    i.stock_status,

    i.is_stockout,
    i.is_below_reorder_level,
    i.stock_buffer,
    i.stock_coverage_ratio

FROM {{ ref('fact_inventory') }} i
LEFT JOIN {{ ref('dim_products') }} p
    ON i.product_id = p.product_id
LEFT JOIN {{ ref('dim_warehouses') }} w
    ON i.warehouse_id = w.warehouse_id

