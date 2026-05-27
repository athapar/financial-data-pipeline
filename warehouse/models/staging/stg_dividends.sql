select
    ticker,
    cast(ex_dividend_date as date) as ex_dividend_date,
    cast(pay_date as date) as pay_date,
    cast(declaration_date as date) as declaration_date,
    cast(record_date as date) as record_date,
    cast(cash_amount as float64) as cash_amount,
    cast(frequency as int64) as frequency,
    dividend_type
from {{ source('raw', 'dividends') }}
