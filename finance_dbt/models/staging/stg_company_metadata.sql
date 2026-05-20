{{ config(materialized='view') }}

SELECT
    symbol,
    company_name,
    INITCAP(LOWER(sector)) AS sector,
    GETDATE() as loaded_at
FROM {{ source('raw', 'dim_metadata') }}