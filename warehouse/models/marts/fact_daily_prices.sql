with 
    -- Cast bars data stored to staging table
    bars as (  
        SELECT  
            Date(t) as price_date,
            symbol,
            o,
            h,
            l,
            c,
            v,
            vw,
            n

        FROM {{ ref('stg_daily_bars') }}
    ),
    
    -- Join bars to SCD2 security master and get composite_figi
    bars_with_security as (
        SELECT
            m.composite_figi, 
            b.*

        FROM bars b INNER JOIN {{ ref('int_security_master_historical') }} m
        ON b.symbol = m.ticker AND m.valid_to is null
    ),
    
    -- Adjust historic prices using adjustment factors calculated from splits data
    bars_with_factors as (
        SELECT
            b.composite_figi as composite_figi,
            b.symbol as ticker,
            b.price_date as price_date,
            b.o / s.adjustment_factor AS open_price,
            b.h / s.adjustment_factor AS high_price,
            b.l / s.adjustment_factor AS low_price,
            b.c / s.adjustment_factor AS close_price,
            b.vw / s.adjustment_factor AS vwap,
            b.v AS volume,        -- not a price, no adjustment
            b.n AS trade_count    -- not a price, no adjustment

        FROM bars_with_security b INNER JOIN {{ ref('int_split_adjustment_factors') }} s
        ON b.composite_figi = s.composite_figi and b.price_date = s.price_date

    ),

    -- dedupe data
    ranked as (
    select
        composite_figi,
        ticker,
        price_date,
        open_price,     -- already adjusted in bars_with_factors
        high_price,     -- already adjusted in bars_with_factors
        low_price,      -- already adjusted in bars_with_factors
        close_price,    -- already adjusted in bars_with_factors
        vwap,           -- already adjusted in bars_with_factors
        volume,       
        trade_count,    
        row_number() over (
            partition by composite_figi, price_date
            order by price_date desc
        ) as rn
    from bars_with_factors
)

select
    composite_figi,
    ticker,
    price_date,
    open_price,
    high_price,
    low_price,
    close_price, 
    vwap,
    volume,
    trade_count
from ranked
where rn = 1