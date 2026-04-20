SELECT
    s.symbol,
    s.date,
    s.close_price,
    FIRST_VALUE(s.close_price) OVER (PARTITION BY s.symbol ORDER BY s.date ASC) AS first_close,
    ROUND (
        (s.close_price / FIRST_VALUE(s.close_price) OVER (PARTITION BY s.symbol ORDER BY s.date ASC)) - 1, 4
    ) AS cumulative_return
FROM stock_prices s
ORDER By s.symbol, s.date ASC;