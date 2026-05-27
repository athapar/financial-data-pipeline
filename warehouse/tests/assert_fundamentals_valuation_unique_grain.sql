-- Returns rows if any composite_figi appears more than once in valuation mart
SELECT
    composite_figi,
    COUNT(*) AS row_count
FROM {{ ref('mart_fundamentals_valuation') }}
GROUP BY composite_figi
HAVING COUNT(*) > 1
