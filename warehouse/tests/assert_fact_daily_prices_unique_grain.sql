-- Returns rows if any (composite_figi, price_date) appears more than once
SELECT
    composite_figi,
    price_date,
    COUNT(*) AS row_count
FROM {{ ref('fact_daily_prices') }}
GROUP BY composite_figi, price_date
HAVING COUNT(*) > 1