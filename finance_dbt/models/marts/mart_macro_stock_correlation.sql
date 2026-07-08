{{ config(materialized='table', dist='symbol', sort='date') }}

WITH macro_pivoted AS (
    SELECT
        date,
        MAX(CASE WHEN series_id = 'FEDFUNDS'    THEN value END) AS fed_funds_rate,
        MAX(CASE WHEN series_id = 'CPIAUCSL'    THEN value END) AS cpi,
        MAX(CASE WHEN series_id = 'T10Y2Y'      THEN value END) AS yield_curve_spread,
        MAX(CASE WHEN series_id = 'UNRATE'      THEN value END) AS unemployment_rate
    FROM {{ ref('stg_macro_indicators') }}
    GROUP BY date
)

SELECT
    sp.symbol,
    sp.date,
    sp.sector,
    sp.daily_return,
    sp.close_price,
    m.fed_funds_rate,
    m.cpi,
    m.yield_curve_spread,
    m.unemployment_rate
FROM {{ ref('mart_daily_stock_performance') }} sp
LEFT JOIN macro_pivoted m ON sp.date = m.date