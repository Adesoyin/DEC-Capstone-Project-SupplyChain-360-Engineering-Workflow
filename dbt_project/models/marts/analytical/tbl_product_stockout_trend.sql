{{ config(
    schema='analytics',
    materialized='table' 
) }}


WITH cte AS (

    SELECT *
    FROM {{ ref('fact_inventory') }}
)
SELECT
    p.product_name,
    i.snapshot_date,
    SUM(i.is_stockout) AS stockout_count
FROM cte i
JOIN {{ ref('dim_products') }} p USING (product_id)
GROUP BY 1,2
ORDER BY stockout_count DESC