with ranked as (
    select
        symbol,
        t,
        c as close_price,
        v as volume,
        row_number() over (
            partition by symbol, t
            order by t desc
        ) as rn
    from {{ ref('stg_daily_bars') }}
)
select
    symbol,
    t,
    close_price,
    volume
from ranked
where rn = 1