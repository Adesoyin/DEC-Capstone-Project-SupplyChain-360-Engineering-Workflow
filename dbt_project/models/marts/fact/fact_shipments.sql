{{ config(
    schema='analytics',
    materialized='incremental',
    unique_key='shipment_id'
) }}

select
    shipment_id,
    warehouse_id,
    store_id,
    product_id,
    carrier,
    quantity_shipped,
    shipment_date,
    shipment_business_date,
    actual_delivery_date,
    expected_delivery_date,
    DATEDIFF(day, expected_delivery_date, actual_delivery_date) AS delivery_delay_days,
    etl_loaded_at
from {{ ref('stg_shipment_delivery') }}

{% if is_incremental() %}
  where shipment_date >= (select max(shipment_date) from {{ this }})
{% endif %}
