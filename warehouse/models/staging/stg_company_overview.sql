select
    ticker,
    name as company_name,
    composite_figi,
    cast(sic_code as string) as sic_code,
    sic_description,
    cast(market_cap as float64) as market_cap,
    cast(weighted_shares_outstanding as float64) as shares_outstanding,
    cast(total_employees as int64) as total_employees,
    cast(list_date as date) as list_date
from {{ source('raw', 'company_overview') }}
