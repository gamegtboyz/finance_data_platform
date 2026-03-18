SELECT
    s.symbol,
    s.date,
    s.close,
    LAG(s.close) OVER (
        PARTITION BY s.symbol
        ORDER BY s.date ASC
        ) AS prev_close,
    ROUND (
        (s.close / LAG(s.close) OVER ( PARTITION BY s.symbol ORDER BY s.date ASC)) - 1, 4
    ) AS daily_return
    FROM stock_prices s
ORDER BY s.symbol, s.date ASC;