-- Fails if volume is less than zero on trade day.
SELECT
    symbol,
    date,
    volume
FROM {{ ref('stg_stock_prices') }}
WHERE volume < 0