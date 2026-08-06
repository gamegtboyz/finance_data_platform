{{ config(materialized='view') }}

SELECT
    symbol,
    date,
    headline,
    sentiment_score,
    source,
    url,
    GETDATE() as loaded_at
FROM {{ source('raw', 'news_sentiment')}}