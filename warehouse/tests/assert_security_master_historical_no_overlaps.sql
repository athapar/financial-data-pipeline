-- Returns rows if any FIGI has overlapping validity windows in the historical SCD2
SELECT
    a.composite_figi,
    a.ticker as ticker_a,
    a.valid_from as from_a,
    a.valid_to as to_a,
    b.ticker as ticker_b,
    b.valid_from as from_b,
    b.valid_to as to_b
FROM {{ ref('int_security_master_historical') }} a
JOIN {{ ref('int_security_master_historical') }} b
    ON a.composite_figi = b.composite_figi
    AND a.valid_from < b.valid_from
    AND (a.valid_to IS NULL OR a.valid_to > b.valid_from)
    AND a.source != b.source
