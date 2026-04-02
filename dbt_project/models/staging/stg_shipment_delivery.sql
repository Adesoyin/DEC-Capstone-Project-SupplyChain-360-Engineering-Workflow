with cte as (
    select * 
    from  {{ source('raw', 'shipments') }} )

select 
    SHIPMENT_ID,
    trim(WAREHOUSE_ID) as WAREHOUSE_ID,
    trim(STORE_ID) as STORE_ID,
    trim(PRODUCT_ID) as PRODUCT_ID,
    CARRIER,
    QUANTITY_SHIPPED,
    
    CAST(SHIPMENT_DATE AS DATE) AS SHIPMENT_DATE,
    CAST(ACTUAL_DELIVERY_DATE AS DATE) AS ACTUAL_DELIVERY_DATE,
    CAST(EXPECTED_DELIVERY_DATE AS DATE) AS EXPECTED_DELIVERY_DATE,
    CAST(BUSINESS_DATE AS DATE) AS shipment_business_date,
    CAST(INGESTED_AT AS DATETIME) as dec_source_ingestion_date,
    SOURCE_FILE,
    _AIRBYTE_EXTRACTED_AT as source_modified_date,
    --DATEADD(hour, -3, CURRENT_TIMESTAMP()) AS etl_loaded_at
    {{ dbt.dateadd(datepart='hour', interval=-4, from_date_or_timestamp='current_timestamp()') }} as etl_loaded_at

from cte