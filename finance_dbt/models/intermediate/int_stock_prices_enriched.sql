{{ config(materialized='table') }}

SELECT
    sp.symbol,
    sp.date,
    sp.open_price,
    sp.high,
    sp.low,
    sp.close_price,
    sp.volume,
    m.company_name,
    m.sector,
    d.day,
    d.month,
    d.year,
    d.quarter,
    d.day_of_week,
    d.week_of_year,
    -- daily log return
    ROUND(
        (sp.close_price / LAG(sp.close_price) OVER (PARTITION BY sp.symbol ORDER BY sp.date)) - 1, 6
    ) AS daily_return,
    -- first close for cumulative return base
    FIRST_VALUE(sp.close_price) OVER (
        PARTITION BY sp.symbol 
        ORDER BY sp.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS first_close
FROM {{ ref('stg_stock_prices') }} AS sp
LEFT JOIN {{ ref('stg_company_metadata') }} AS m ON sp.symbol = m.symbol
LEFT JOIN {{ source('raw', 'dim_date') }} AS d ON sp.date = d.date