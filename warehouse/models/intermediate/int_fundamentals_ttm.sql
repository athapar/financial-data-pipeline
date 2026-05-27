with financials_with_figi as (
    select
        m.composite_figi,
        f.*
    from {{ ref('stg_quarterly_financials') }} f
    inner join {{ ref('int_security_master_historical') }} m
        on f.ticker = m.ticker
        and m.valid_to is null
),

ranked as (
    select
        *,
        row_number() over (
            partition by composite_figi
            order by period_end desc
        ) as quarter_rank
    from financials_with_figi
),

-- Sum the 4 most recent quarters for flow metrics (income stmt + cash flow)
-- Use most recent quarter for point-in-time balance sheet
ttm as (
    select
        composite_figi,
        min(ticker) as ticker,
        max(case when quarter_rank = 1 then fiscal_year end) as latest_fiscal_year,
        max(case when quarter_rank = 1 then fiscal_period end) as latest_fiscal_period,
        max(case when quarter_rank = 1 then period_end end) as period_end,
        max(case when quarter_rank = 1 then filing_date end) as filing_date,

        sum(revenues) as ttm_revenue,
        sum(net_income) as ttm_net_income,
        sum(gross_profit) as ttm_gross_profit,
        sum(operating_income) as ttm_operating_income,
        sum(diluted_eps) as ttm_diluted_eps,

        sum(operating_cash_flow) as ttm_operating_cash_flow,
        sum(investing_cash_flow) as ttm_investing_cash_flow,

        -- Balance sheet: point-in-time from most recent quarter
        max(case when quarter_rank = 1 then total_assets end) as total_assets,
        max(case when quarter_rank = 1 then total_liabilities end) as total_liabilities,
        max(case when quarter_rank = 1 then total_equity end) as book_value,
        max(case when quarter_rank = 1 then current_assets end) as current_assets,
        max(case when quarter_rank = 1 then current_liabilities end) as current_liabilities,

        count(*) as quarters_included
    from ranked
    where quarter_rank <= 4
    group by composite_figi
),

with_derived as (
    select
        *,
        ttm_operating_cash_flow + ttm_investing_cash_flow as ttm_free_cash_flow,
        safe_divide(ttm_gross_profit, ttm_revenue) as gross_margin,
        safe_divide(ttm_operating_income, ttm_revenue) as operating_margin,
        safe_divide(ttm_net_income, ttm_revenue) as net_margin,
        safe_divide(current_assets, current_liabilities) as current_ratio,
        safe_divide(total_liabilities, book_value) as debt_to_equity
    from ttm
)

select * from with_derived
