-- Fails if intraday high is less than intradya low for any stock on any trade day.
SELECT
    symbol,
    date,
    high,
    low
FROM {{ ref('stg_stock_prices') }}
WHERE high < low