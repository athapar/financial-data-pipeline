with seed as (
    select
        composite_figi,
        ticker,
        name,
        cast(valid_from as date) as valid_from,
        safe_cast(valid_to as date) as valid_to
    from {{ ref('ticker_history_seed') }}
),

-- Pull snapshot scd2 table, cast rows
snapshot_scd2 as (
    select
        composite_figi,
        ticker,
        name,
        cast(dbt_valid_from as date) as valid_from,
        cast(dbt_valid_to as date) as valid_to
    from {{ ref('int_security_master_scd2') }}
),

-- Find date of first snapshot for each composite_figi
snapshot_starts as (
    select
        composite_figi,
        min(valid_from) as first_snapshot_date
    from snapshot_scd2
    group by composite_figi
),


-- Query keeps historical rows with closed valid_to dates (PIT joins)
-- Cuts off rows with valid_to that are unbounded or after first dbt snapshot (all other rows excluded)
seed_trimmed as (
    select seed.composite_figi, seed.ticker, seed.name, seed.valid_from,
    case
        when scd2.first_snapshot_date is null then seed.valid_to -- no shapshot, keep seed
        when seed.valid_to is null then scd2.first_snapshot_date -- unbounded seed row, cut off and use scd2 snapshot start
        when seed.valid_to > scd2.first_snapshot_date then scd2.first_snapshot_date -- Bounded seed row, but ending after snapshot begins - cut off
        else seed.valid_to -- Keep seed (case when seed end date < first snapshot date)
    end as valid_to,
    'seed' as source

    from seed
    left join snapshot_starts scd2 on seed.composite_figi = scd2.composite_figi
),


historical_rows as (
    select * from seed_trimmed
    where valid_from < valid_to or valid_to is null -- Only exclude rows where start and end were after first snapshot (end < start) and ones without snapshot
       
),

forward_rows as (
    select
        composite_figi,
        ticker,
        name,
        valid_from,
        valid_to,
        'snapshot' as source
    from snapshot_scd2
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
