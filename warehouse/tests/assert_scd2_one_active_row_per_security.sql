-- Returns rows if any composite_figi has more than one active SCD2 record
SELECT
    composite_figi,
    COUNT(*) AS active_row_count
FROM {{ ref('int_security_master_scd2') }}
WHERE dbt_valid_to IS NULL
GROUP BY composite_figi
HAVING COUNT(*) > 1
