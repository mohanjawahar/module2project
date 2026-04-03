{{ config(
    materialized='table',
    cluster_by=['seller_id', 'product_id']
) }}

-- Define high-value products as those with avg price in top 25% across all products
with product_price_stats as (
    select
        product_id,
        round(avg(price_amount), 2)     as avg_price,
        round(max(price_amount), 2)     as max_price,
        round(min(price_amount), 2)     as min_price,
        count(distinct order_id)        as times_sold,
        ntile(4) over (
            order by avg(price_amount) asc
        )                               as price_quartile
    from {{ ref('fct_order_items') }}
    group by product_id
),

high_value_products as (
    select product_id, avg_price, max_price, times_sold
    from product_price_stats
    where price_quartile = 4
),

seller_product_stats as (
    select
        oi.seller_id,
        s.seller_city,
        s.seller_state,
        oi.product_id,
        p.product_category_name_english  as product_category,
        count(distinct oi.order_id)      as seller_product_orders,
        round(sum(oi.price_amount), 2)   as seller_product_revenue,
        round(avg(oi.price_amount), 2)   as seller_avg_price
    from {{ ref('fct_order_items') }} oi
    join high_value_products hvp
        on oi.product_id = hvp.product_id
    join {{ ref('dim_sellers') }} s
        on oi.seller_id = s.seller_id
    join {{ ref('dim_products') }} p
        on oi.product_id = p.product_id
    group by
        oi.seller_id,
        s.seller_city,
        s.seller_state,
        oi.product_id,
        p.product_category_name_english
)

select
    sp.seller_id,
    sp.seller_city,
    sp.seller_state,
    sp.product_id,
    sp.product_category,
    hvp.avg_price                        as product_avg_market_price,
    hvp.max_price                        as product_max_market_price,
    hvp.times_sold                       as product_total_market_sales,
    sp.seller_product_orders,
    sp.seller_product_revenue,
    sp.seller_avg_price,
    -- Share of this seller in total sales of this product
    round(
        safe_divide(sp.seller_product_orders, hvp.times_sold) * 100, 2
    )                                    as seller_product_market_share_pct,
    rank() over (
        partition by sp.product_id
        order by sp.seller_product_orders desc
    )                                    as seller_rank_for_product
from seller_product_stats sp
join high_value_products hvp
    on sp.product_id = hvp.product_id
