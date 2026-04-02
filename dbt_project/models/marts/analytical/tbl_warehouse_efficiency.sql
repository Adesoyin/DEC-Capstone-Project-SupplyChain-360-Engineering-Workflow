{{ config(
    schema='analytics',
    materialized='table' 
) }}

SELECT
    w.warehouse_id,
    w.warehouse_city,
    w.warehouse_state,

    COUNT(DISTINCT f.shipment_id) AS total_shipments,
    AVG(f.delivery_delay_days) AS avg_delivery_delay,
    SUM(f.quantity_shipped) AS total_volume,

    AVG(i.stock_coverage_ratio) AS avg_stock_coverage

FROM {{ ref('fact_shipments') }} f
LEFT JOIN {{ ref('fact_inventory') }} i
    ON f.warehouse_id = i.warehouse_id
LEFT JOIN {{ ref('dim_warehouses') }} w
    ON f.warehouse_id = w.warehouse_id

GROUP BY 1,2,3
