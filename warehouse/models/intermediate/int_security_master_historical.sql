with seed as (
    select
        composite_figi,
        ticker,
        name,
        cast(valid_from as date) as valid_from,
        safe_cast(valid_to as date) as valid_to
    from {{ ref('ticker_history_seed') }}
),

snapshot_current as (
    select
        composite_figi,
        ticker,
        name,
        cast(dbt_valid_from as date) as valid_from,
        cast(dbt_valid_to as date) as valid_to
    from {{ ref('int_security_master_scd2') }}
),

-- Seed provides history before the snapshot started.
-- Snapshot provides forward-looking change detection.
-- For a given FIGI, use seed rows that ended BEFORE the snapshot's first row,
-- then use snapshot rows from that point forward.
snapshot_starts as (
    select
        composite_figi,
        min(valid_from) as first_snapshot_date
    from snapshot_current
    group by composite_figi
),

historical_rows as (
    select
        s.composite_figi,
        s.ticker,
        s.name,
        s.valid_from,
        -- Close the seed row at the snapshot start if it overlaps
        case
            when s.valid_to is null and ss.first_snapshot_date is not null
                then ss.first_snapshot_date
            else s.valid_to
        end as valid_to,
        'seed' as source
    from seed s
    left join snapshot_starts ss
        on s.composite_figi = ss.composite_figi
    where s.valid_to is not null
       or ss.first_snapshot_date is null
),

forward_rows as (
    select
        composite_figi,
        ticker,
        name,
        valid_from,
        valid_to,
        'snapshot' as source
    from snapshot_current
),

combined as (
    select * from historical_rows
    union all
    select * from forward_rows
)

select
    composite_figi,
    ticker,
    name,
    valid_from,
    valid_to,
    source
from combined
