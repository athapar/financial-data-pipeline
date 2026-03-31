{% snapshot int_security_master_scd2 %}

{{
    config(
        target_schema='marketdatapipeline',
        unique_key='composite_figi',
        strategy='check',
        check_cols=['ticker', 'name', 'primary_exchange', 'active'],
        invalidate_hard_deletes=True
    )
}}

SELECT
    composite_figi,
    ticker,
    name,
    primary_exchange,
    active,
    currency_name
FROM {{ source('raw', 'tickers') }}
WHERE composite_figi IS NOT NULL

{% endsnapshot %}
