{{ config(materialized='view') }}

SELECT
    symbol,
    company_name,
    sector,
    GETDATE() as loaded_at
FROM {{ source('raw', 'dim_metadata') }}