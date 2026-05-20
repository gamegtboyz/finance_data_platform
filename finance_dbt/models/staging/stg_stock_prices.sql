{{ config(materialized='view') }}

SELECT
    symbol,
    date,
    open_price,
    high,
    low,
    close_price,
    volume,
    GETDATE() AS loaded_at
FROM {{ source('raw', 'stock_prices') }}