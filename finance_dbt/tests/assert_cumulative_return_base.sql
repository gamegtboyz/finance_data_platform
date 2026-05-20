-- Fails if cumulative return is NULL where close_price and first_close are both present.
SELECT
    symbol_date,
    symbol,
    date,
    close_price,
    cumulative_return
FROM {{ ref('mart_daily_stock_performance') }}
WHERE cumulative_return IS NULL
    AND close_price IS NOT NULL