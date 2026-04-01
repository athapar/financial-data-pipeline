with returns AS (
    select
        ticker,
        composite_figi,
        price_date,
        close_price,
        close_price / lag(close_price) over (partition by composite_figi order by price_date) - 1 as daily_return
from {{ ref('fact_daily_prices') }}
)

select
    ticker,
    composite_figi,
    price_date,
    close_price,
    daily_return,
    stddev(daily_return) OVER (
                                PARTITION BY composite_figi
                                ORDER BY price_date
                                ROWS BETWEEN 19 PRECEDING and CURRENT ROW
    ) AS volatility_20d,
    stddev(daily_return) OVER (
                                PARTITION BY composite_figi
                                ORDER BY price_date
                                ROWS BETWEEN 59 PRECEDING and CURRENT ROW
    ) AS volatility_60d
from returns