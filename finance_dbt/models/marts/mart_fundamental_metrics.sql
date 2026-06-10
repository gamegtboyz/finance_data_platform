{{ config(materialized='table', dist='symbol', sort='period_end_date') }}

WITH pivoted AS (
    SELECT
        symbol,
        period_end_date,
        form_type,
        MAX(CASE WHEN metric = 'revenue' THEN value END) AS revenue,
        MAX(CASE WHEN metric = 'net_income' THEN value END ) AS net_income,
        MAX(CASE WHEN metric = 'eps_diluted' THEN value END) AS eps_diluted,
        MAX(CASE WHEN metric = 'long_term_debt' THEN value END) AS long_term_debt,
        MAX(CASE WHEN metric = 'total_assets' THEN value END) AS total_assets
    FROM {{ ref('stg_fundamentals') }}
    GROUP BY symbol, period_end_date, form_type
),

with_ratios AS (
    SELECT
        p.*,
        m.company_name,
        m.sector,
        -- Revenue YoY growth (annual filings only)
        ROUND(
            revenue / NULLIF(
                LAG(revenue) OVER (PARTITION BY symbol, form_type ORDER BY period_end_date), 0
            ) - 1, 4
        ) AS revenue_yoy_growth,
        -- D/A ratio
        ROUND(
            long_term_debt / NULLIF(total_assets, 0), 4
        ) AS debt_to_assets
    FROM pivoted p
    LEFT JOIN {{ source('raw', 'dim_metadata') }} m ON p.symbol = m.symbol
)

SELECT * FROM with_ratios