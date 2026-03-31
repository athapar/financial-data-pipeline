WITH splits_with_figi AS 
    (SELECT s.ticker as ticker, 
        m.composite_figi as composite_figi,
        CAST(s.execution_date AS DATE) AS split_date,
        CAST(s.split_to as FLOAT64)/CAST(s.split_from AS FLOAT64) as split_ratio 
    FROM {{ source('raw', 'splits') }} s LEFT JOIN {{ ref('int_security_master_scd2') }} m 
    ON m.ticker = s.ticker 
    AND m.dbt_valid_to is null),

price_dates AS
    (
        SELECT DISTINCT m.composite_figi as composite_figi,
                        DATE(b.t) as price_date
        FROM {{ source('raw', 'daily_bars') }} b
        LEFT JOIN {{ ref('int_security_master_scd2') }} m
        
        ON b.symbol = m.ticker
        AND m.dbt_valid_to IS NULL 
    )

SELECT COALESCE(EXP(SUM(LN(split_ratio))), 1.0) AS adjustment_factor,
        pd.composite_figi,
        pd.price_date
FROM price_dates pd
LEFT JOIN splits_with_figi sf
ON pd.composite_figi = sf.composite_figi
AND sf.split_date > pd.price_date
GROUP BY pd.composite_figi, pd.price_date