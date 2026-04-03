with checks as (

    select 'null_total_sale_amount' as test_name, count(*) as failed_rows
    from {{ ref('fct_sales') }}
    where total_sale_amount is null

    union all

    select 'null_customer_key', count(*)
    from {{ ref('fct_sales') }}
    where customer_key is null

    union all

    select 'null_product_key', count(*)
    from {{ ref('fct_sales') }}
    where product_key is null

    union all

    select 'null_seller_key', count(*)
    from {{ ref('fct_sales') }}
    where seller_key is null

    union all

    select 'duplicate_fct_sales_grain', count(*)
    from (
        select order_id, order_item_id
        from {{ ref('fct_sales') }}
        group by order_id, order_item_id
        having count(*) > 1
    )

    union all

    select 'missing_dim_customer', count(*)
    from {{ ref('fct_sales') }} f
    left join {{ ref('dim_customers') }} c
        on f.customer_key = c.customer_key
    where c.customer_key is null

    union all

    select 'missing_dim_product', count(*)
    from {{ ref('fct_sales') }} f
    left join {{ ref('dim_products') }} p
        on f.product_key = p.product_key
    where p.product_key is null

    union all

    select 'missing_dim_seller', count(*)
    from {{ ref('fct_sales') }} f
    left join {{ ref('dim_sellers') }} s
        on f.seller_key = s.seller_key
    where s.seller_key is null

    union all

    select 'missing_dim_date', count(*)
    from {{ ref('fct_sales') }} f
    left join {{ ref('dim_dates') }} d
        on f.order_date_key = d.date_key
    where d.date_key is null

    union all

    select 'negative_sales_amount', count(*)
    from {{ ref('fct_sales') }}
    where total_sale_amount < 0

    union all

    select 'delivery_days_negative_when_delivered', count(*)
    from {{ ref('fct_sales') }}
    where delivered_flag = true
      and delivery_days < 0

)

select *
from checks
where failed_rows > 0