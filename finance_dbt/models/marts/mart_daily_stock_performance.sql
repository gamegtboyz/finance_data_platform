{{ 
    config(
        materialized='incremental',
        unique_key='symbol_date',
        dist='symbol',
        sort='date'
    )
}}

SELECT
    symbol || '_' || CAST(date as VARCHAR) AS symbol_date,      -- surrogate key
    symbol,
    date,
    company_name,
    sector,
    close_price,
    daily_return,
    --SMA
    AVG(close_price) OVER (
        PARTITION BY symbol ORDER BY date
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS sma_5,
    AVG(close_price) OVER (
        PARTITION BY symbol ORDER BY date
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS sma_20,
    -- Volatility (21 trading day rolling stdev of daily returns)
    STDDEV(daily_return) OVER (
        PARTITION BY symbol ORDER BY date
        ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
    ) AS volatility_21,
    -- Cumulattive return
    ROUND((close_price / first_close) - 1, 6) AS cumulative_return
FROM {{ ref('int_stock_prices_enriched') }}

{% if is_incremental() %}
WHERE date > (SELECT MAX(date) from {{ this }})
{% endif %}