select
    symbol,
    cast(t as timestamp) as t,
    cast(o as float64) as o,
    cast(h as float64) as h,
    cast(l as float64) as l,
    cast(c as float64) as c,
    cast(v as float64) as v,
    cast(vw as float64) as vw,
    cast(n as int64) as n
from {{ source('raw', 'daily_bars') }}