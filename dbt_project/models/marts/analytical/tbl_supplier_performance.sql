{{ config(
    schema='analytics',
    materialized='table' 
) }}

SELECT
    s.supplier_id,
    s.supplier_name,

    COUNT(f.shipment_id) AS total_shipments,
    AVG(f.delivery_delay_days) AS avg_delivery_delay,
    SUM(f.quantity_shipped) AS total_quantity_shipped,

    SUM(CASE WHEN f.delivery_delay_days > 0 THEN 1 ELSE 0 END) AS late_deliveries

FROM {{ ref('fact_shipments') }} f
JOIN {{ ref('dim_products') }} p
    ON f.product_id = p.product_id
JOIN {{ ref('dim_suppliers') }} s
    ON p.supplier_id = s.supplier_id

GROUP BY 1,2