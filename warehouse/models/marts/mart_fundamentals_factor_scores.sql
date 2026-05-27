with valuation as (
    select * from {{ ref('mart_fundamentals_valuation') }}
),

percentiles as (
    select
        *,
        -- Value: low P/E and low P/B are "value" stocks
        percent_rank() over (order by pe_ratio asc) as value_pe_pctile,
        percent_rank() over (order by pb_ratio asc) as value_pb_pctile,
        percent_rank() over (order by ps_ratio asc) as value_ps_pctile,

        -- Growth: high revenue growth and high margins are "growth" stocks
        percent_rank() over (order by operating_margin desc) as growth_margin_pctile,
        percent_rank() over (order by roe desc) as growth_roe_pctile,

        -- Quality: high ROE, low leverage, strong cash flow
        percent_rank() over (order by roe desc) as quality_roe_pctile,
        percent_rank() over (order by debt_to_equity asc) as quality_leverage_pctile,
        percent_rank() over (order by safe_divide(ttm_free_cash_flow, ttm_net_income) desc) as quality_fcf_pctile
    from valuation
    where pe_ratio > 0
)

select
    composite_figi,
    ticker,

    -- Composite factor scores (0-1, higher = stronger signal)
    (value_pe_pctile + value_pb_pctile + value_ps_pctile) / 3.0 as value_score,
    (growth_margin_pctile + growth_roe_pctile) / 2.0 as growth_score,
    (quality_roe_pctile + quality_leverage_pctile + quality_fcf_pctile) / 3.0 as quality_score,

    -- Classification based on dominant factor
    case
        when (value_pe_pctile + value_pb_pctile + value_ps_pctile) / 3.0 >= 0.7 then 'VALUE'
        when (growth_margin_pctile + growth_roe_pctile) / 2.0 >= 0.7 then 'GROWTH'
        when (quality_roe_pctile + quality_leverage_pctile + quality_fcf_pctile) / 3.0 >= 0.7 then 'QUALITY'
        else 'BLEND'
    end as factor_classification,

    -- Underlying metrics for transparency
    pe_ratio,
    pb_ratio,
    operating_margin,
    roe,
    debt_to_equity,
    safe_divide(ttm_free_cash_flow, ttm_net_income) as fcf_conversion

from percentiles
