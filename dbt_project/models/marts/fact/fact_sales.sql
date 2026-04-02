{{ config(
    schema='analytics',
    materialized='incremental',
    unique_key='transaction_id'
) }}

{% if is_incremental() %}
with last_loaded as (
    select max(transaction_timestamp) as max_ts
    from {{ this }}
)
{% endif %}

select
    transaction_id,
    store_id,
    product_id,
    unit_price,
    quantity_sold,
    sale_amount,
    discount_pct,
    --transaction_timestamp,
    transaction_date,
    cast(transaction_timestamp as datetime) as sale_datetime
    etl_loaded_at
from {{ ref('stg_transaction_sales') }}

{% if is_incremental() %}
where transaction_timestamp >= (select max_ts from last_loaded)
{% endif %}
