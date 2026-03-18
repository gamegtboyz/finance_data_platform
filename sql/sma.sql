SELECT
    s.symbol,
    s.date,
    s.close,
    AVG(s.close) OVER (
        PARTITION BY s.symbol
        ORDER BY s.date ASC
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS sma5,
    AVG(s.close) OVER (
        PARTITION BY s.symbol
        ORDER BY s.date ASC
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS sma20
FROM stock_prices s
ORDER BY s.symbol, s.date ASC;