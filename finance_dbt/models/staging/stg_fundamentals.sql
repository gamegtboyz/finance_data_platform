{{ config(materialized='view') }}

SELECT
    symbol,
    period_end_date,
    form_type,
    metric,
    value,
    unit,
    filed_date,
    GETDATE() AS loaded_at
FROM {{ source('raw', 'fundamentals') }}