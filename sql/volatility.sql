WITH daily_returns AS (
    SELECT
        s.symbol,
        s.date,
        (s.close_price / LAG(s.close_price) OVER (PARTITION BY s.symbol ORDER BY s.date ASC)) - 1 AS daily_return
    FROM stock_prices s
)

SELECT
    symbol,
    date,
    STDDEV(
        daily_return
    ) OVER (PARTITION BY symbol ORDER BY date ASC ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) AS volatility
FROM daily_returns
ORDER BY symbol, date ASC;