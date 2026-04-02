select  
    TRANSACTION_ID,
    STORE_ID,
    PRODUCT_ID,
    UNIT_PRICE,
    QUANTITY_SOLD,
    SALE_AMOUNT,
    DISCOUNT_PCT,
    TRANSACTION_TIMESTAMP,
    cast(BUSINESS_DATE as date) as transaction_date,
    -- SOURCE_FILE,
    CAST(INGESTION_TIMESTAMP AS DATE) AS dec_source_ingested_time,
    _AIRBYTE_EXTRACTED_AT as airbyteloaded_modified_date,
    --DATEADD(hour, -3, CURRENT_TIMESTAMP()) AS etl_loaded_at
    {{ dbt.dateadd(datepart='hour', interval=-4, from_date_or_timestamp='current_timestamp()') }} as etl_loaded_at
from {{ source('raw', 'sales') }}