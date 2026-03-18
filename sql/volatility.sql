SELECT
    s.symbol,
    s.date,
    STDDEV(
        (s.close / LAG(s.close) OVER (PARTITION BY s.symbol ORDER BY s.date ASC)) - 1
    ) OVER (PARTITION BY s.symbol ORDER BY s.date ASC ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) AS volatility
FROM stock_prices s
ORDER BY s.symbol, s.date ASC;