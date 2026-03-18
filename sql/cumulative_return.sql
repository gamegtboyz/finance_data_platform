SELECT
    s.symbol,
    s.date,
    s.close,
    FIRST_VALUE(s.close) OVER (PARTITION BY s.symbol ORDER BY s.date ASC) AS first_close,
    ROUND (
        (s.close / FIRST_VALUE(s.close) OVER (PARTITION BY s.symbol ORDER BY s.date ASC)) - 1, 4
    ) AS cumulative_return
FROM stock_prices s
ORDER By s.symbol, s.date ASC;