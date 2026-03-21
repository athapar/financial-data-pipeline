select
    symbol,
    t,
    close_price,
    close_price / lag(close_price) over (partition by symbol order by t) - 1 as daily_return
from {{ ref('fact_daily_prices') }}