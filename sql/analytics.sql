-- please run this script with: psql -U finance_user -d finance_db -f sql/analytics.sql

-- 1. calculate SMA5 and SMA20
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

-- 2. Daily returns
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

-- 3. Volatility (standard deviation of daily returns) over the last 21 trading days
SELECT
    s.symbol,
    s.date,
    STDDEV(
        (s.close / LAG(s.close) OVER (PARTITION BY s.symbol ORDER BY s.date ASC)) - 1
    ) OVER (PARTITION BY s.symbol ORDER BY s.date ASC ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) AS volatility
FROM stock_prices s
ORDER BY s.symbol, s.date ASC;

-- 4. Cumulative returs from each symbol's first loaded date
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