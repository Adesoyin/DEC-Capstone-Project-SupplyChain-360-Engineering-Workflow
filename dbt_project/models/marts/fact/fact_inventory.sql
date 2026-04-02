{{ config(
    schema='analytics',
    materialized='incremental' 
) }}

WITH inventory AS (
    SELECT *
    FROM {{ ref('stg_warehouse_inventory') }}
)

SELECT
    product_id,
    warehouse_id,
    snapshot_date,
    inventory_business_date,
    quantity_available,
    reorder_threshold,

    CASE 
        WHEN quantity_available = 0 THEN 'Out of Stock'
        WHEN quantity_available <= reorder_threshold THEN 'Low Stock'
        ELSE 'Healthy'
    END AS stock_status,
    -- stockout flag
    CASE 
        WHEN quantity_available = 0 THEN 1 
        ELSE 0 
    END AS is_stockout,

    -- low stock / reorder risk
    CASE 
        WHEN quantity_available <= reorder_threshold THEN 1
        ELSE 0
    END AS is_below_reorder_level,

    -- inventory buffer meaning how far is the risk
    (quantity_available - reorder_threshold) AS stock_buffer,

    -- stock coverage ratio
    CASE 
        WHEN reorder_threshold = 0 THEN NULL
        ELSE quantity_available * 1.0 / reorder_threshold
    END AS stock_coverage_ratio,

    dec_source_ingestion_date,
    source_file,
    source_modified_date,
    etl_loaded_at

FROM inventory

{% if is_incremental() %}
  where etl_loaded_at >= (select max(snapshot_date) from {{ this }})
{% endif %}
