{{ config(
    schema='analytics',
    materialized='table' 
) }}

WITH dates AS (

    SELECT DISTINCT
        CAST(transaction_timestamp AS DATE) AS transaction_date
    FROM {{ ref('stg_transaction_sales') }}

)

SELECT
    transaction_date,
    EXTRACT(YEAR FROM transaction_date) AS year,
    EXTRACT(MONTH FROM transaction_date) AS month,
    EXTRACT(DAY FROM transaction_date) AS day,
    EXTRACT(DOW FROM transaction_date) AS day_of_week
FROM dates