with dividends_with_figi as (
    select
        m.composite_figi,
        d.ticker,
        d.ex_dividend_date,
        d.cash_amount,
        d.frequency
    from {{ ref('stg_dividends') }} d
    inner join {{ ref('int_security_master_scd2') }} m
        on d.ticker = m.ticker
        and m.dbt_valid_to is null
    where d.dividend_type = 'CD'
),

trailing_12m as (
    select
        d1.composite_figi,
        d1.ticker,
        d1.ex_dividend_date,
        d1.cash_amount,
        (
            select sum(d2.cash_amount)
            from dividends_with_figi d2
            where d2.composite_figi = d1.composite_figi
              and d2.ex_dividend_date > date_sub(d1.ex_dividend_date, interval 365 day)
              and d2.ex_dividend_date <= d1.ex_dividend_date
        ) as ttm_dividends_per_share
    from dividends_with_figi d1
),

with_price as (
    select
        t.composite_figi,
        t.ticker,
        t.ex_dividend_date,
        t.cash_amount,
        t.ttm_dividends_per_share,
        p.close_price,
        safe_divide(t.ttm_dividends_per_share, p.close_price) as ttm_dividend_yield
    from trailing_12m t
    inner join {{ ref('fact_daily_prices') }} p
        on t.composite_figi = p.composite_figi
        and t.ex_dividend_date = p.price_date
)

select * from with_price
