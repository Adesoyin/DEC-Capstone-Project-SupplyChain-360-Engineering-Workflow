
select 
    PRODUCT_ID,
    WAREHOUSE_ID,
    QUANTITY_AVAILABLE,
    REORDER_THRESHOLD,
    CAST(SNAPSHOT_DATE AS DATE) AS snapshot_date,
    CAST(BUSINESS_DATE AS DATE) AS inventory_business_date,
    CAST(INGESTED_AT AS DATETIME) as dec_source_ingestion_date,
    SOURCE_FILE,
    _AIRBYTE_EXTRACTED_AT as source_modified_date,
    -- DATEADD(hour, -3, CURRENT_TIMESTAMP()) AS etl_loaded_at
    {{ dbt.dateadd(datepart='hour', interval=-4, from_date_or_timestamp='current_timestamp()') }} as etl_loaded_at

from {{ source('raw', 'inventory') }}