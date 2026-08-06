{{ config(materialized='table', dist="symbol", sort='date') }}

WITH daily_sentiment AS (
    SELECT
        symbol,
        date,
        AVG(sentiment_score) AS avg_sentiment_score,
        COUNT(*) AS headline_count
    FROM {{ ref('stg_news_sentiment')}}
    GROUP BY symbol, date
),

returns_with_lead AS (
    SELECT
        symbol,
        date,
        sector,
        daily_return,
        close_price,
        LEAD(daily_return) OVER (PARTITION BY symbol ORDER BY date) as next_day_return
    FROM {{ ref('mart_daily_stock_performance')}}
)

SELECT
    d.symbol,
    d.date,
    d.avg_sentiment_score,
    d.headline_count,
    r.sector,
    r.daily_return,
    r.close_price,
    r.next_day_return
FROM daily_sentiment d
LEFT JOIN  returns_with_lead r on d.symbol = r.symbol AND d.date = r.date
