with cte as (
    select * 
    from  {{ source('raw', 'products') }} )

select  
    trim(BRAND) as BRAND,
    trim(CATEGORY) as CATEGORY,
    trim(PRODUCT_ID) as PRODUCT_ID,
    UNIT_PRICE,
    trim(SUPPLIER_ID) as SUPPLIER_ID,
    trim(PRODUCT_NAME) as PRODUCT_NAME,
    SOURCE_FILE,
    CAST(INGESTED_AT as DATETIME) AS dec_source_ingestion_date,    
    _AIRBYTE_EXTRACTED_AT as airbyteloaded_modified_date,
    --DATEADD(hour, -3, CURRENT_TIMESTAMP()) AS etl_loaded_at
    {{ dbt.dateadd(datepart='hour', interval=-4, from_date_or_timestamp='current_timestamp()') }} as etl_loaded_at
from cte