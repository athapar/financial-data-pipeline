-- Returns rows where TTM aggregation includes fewer than 4 quarters
-- These would produce misleading annualized metrics
SELECT
    composite_figi,
    ticker,
    quarters_included
FROM {{ ref('int_fundamentals_ttm') }}
WHERE quarters_included < 4
