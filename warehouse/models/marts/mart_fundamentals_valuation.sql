with latest_prices as (
    select
        composite_figi,
        ticker,
        close_price,
        price_date,
        row_number() over (
            partition by composite_figi
            order by price_date desc
        ) as rn
    from {{ ref('fact_daily_prices') }}
),

current_price as (
    select composite_figi, ticker, close_price, price_date
    from latest_prices
    where rn = 1
),

company as (
    select
        co.composite_figi,
        co.market_cap,
        co.shares_outstanding
    from {{ ref('stg_company_overview') }} co
    where co.composite_figi is not null
)

select
    ttm.composite_figi,
    ttm.ticker,
    p.close_price,
    p.price_date as price_as_of,
    ttm.period_end as financials_as_of,
    ttm.filing_date,
    ttm.quarters_included,

    co.market_cap,
    co.shares_outstanding,

    -- Valuation ratios
    safe_divide(p.close_price, ttm.ttm_diluted_eps) as pe_ratio,
    safe_divide(co.market_cap, ttm.book_value) as pb_ratio,
    safe_divide(co.market_cap, ttm.ttm_revenue) as ps_ratio,
    safe_divide(
        co.market_cap + ttm.total_liabilities - ttm.current_assets,
        ttm.ttm_operating_income
    ) as ev_ebit,
    safe_divide(co.market_cap, ttm.ttm_free_cash_flow) as price_to_fcf,

    -- Profitability
    ttm.gross_margin,
    ttm.operating_margin,
    ttm.net_margin,
    safe_divide(ttm.ttm_net_income, ttm.book_value) as roe,
    safe_divide(ttm.ttm_net_income, ttm.total_assets) as roa,

    -- Financial health
    ttm.current_ratio,
    ttm.debt_to_equity,

    -- Absolute fundamentals
    ttm.ttm_revenue,
    ttm.ttm_net_income,
    ttm.ttm_operating_income,
    ttm.ttm_free_cash_flow,
    ttm.book_value,
    ttm.total_assets,
    ttm.total_liabilities

from {{ ref('int_fundamentals_ttm') }} ttm
inner join current_price p
    on ttm.composite_figi = p.composite_figi
left join company co
    on ttm.composite_figi = co.composite_figi
where ttm.quarters_included = 4
