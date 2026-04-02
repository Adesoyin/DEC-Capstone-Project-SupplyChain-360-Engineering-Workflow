select 
    WAREHOUSE_ID,
    "STATE",
    CITY,
    SOURCE_FILE,
    CAST(INGESTED_AT AS DATE) as dec_source_ingestion_date,    
    _AIRBYTE_EXTRACTED_AT as airbyteloaded_modified_date,
    --DATEADD(hour, -3, CURRENT_TIMESTAMP()) AS etl_loaded_at
    {{ dbt.dateadd(datepart='hour', interval=-4, from_date_or_timestamp='current_timestamp()') }} as etl_loaded_at

from {{ source('raw', 'warehouses') }}