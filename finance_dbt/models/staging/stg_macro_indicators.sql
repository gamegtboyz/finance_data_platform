{{ config(materialized='view') }}

SELECT
    series_id,
    date,
    value,
    GETDATE() as loaded_at
FROM {{ source('raw', 'macros') }}