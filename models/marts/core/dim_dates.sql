with bounds as (
    select
        min(date(order_purchase_ts)) as min_date,
        max(date(order_purchase_ts)) as max_date
    from {{ ref('stg_orders') }}
),

series as (
    select full_date
    from bounds,
    unnest(generate_date_array(min_date, max_date, interval 1 day)) as full_date
)

select
    cast(format_date('%Y%m%d', full_date) as int64) as date_key,
    full_date,
    extract(year from full_date) as year,
    extract(month from full_date) as month,
    extract(day from full_date) as day,
    extract(quarter from full_date) as quarter,
    format_date('%B', full_date) as month_name,
    format_date('%Y-%m', full_date) as year_month,
    cast(mod(extract(dayofweek from full_date) + 5, 7) + 1 as int64) as day_of_week,
    case
        when extract(dayofweek from full_date) in (1, 7) then true
        else false
    end as is_weekend
from series