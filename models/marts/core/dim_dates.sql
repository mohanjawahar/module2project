with date_spine as (

    select date_day
    from unnest(generate_date_array('2016-01-01', '2019-12-31')) as date_day

)

select
    date_day,
    extract(year from date_day) as year_num,
    extract(quarter from date_day) as quarter_num,
    extract(month from date_day) as month_num,
    format_date('%Y-%m', date_day) as year_month,
    extract(day from date_day) as day_num,
    extract(dayofweek from date_day) as day_of_week_num,
    case
        when extract(dayofweek from date_day) in (1, 7) then true
        else false
    end as is_weekend
from date_spine