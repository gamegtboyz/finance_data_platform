-- Fails if any OHLC show negative prices.
SELECT
    symbol,
    date,
    open_price,
    high,
    low,
    close_price
FROM {{ ref('stg_stock_prices') }}
WHERE
    open_price < 0
    OR high < 0
    OR low < 0
    OR close_price < 0
