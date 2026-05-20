{{ config(materialized='table') }}

SELECT
    sector,
    date,
    SUM(volume) AS total_volume,
    AVG(daily_return) AS avg_daily_return
FROM {{ ref('int_stock_prices_enriched') }}
GROUP BY sector, date
ORDER BY sector, date