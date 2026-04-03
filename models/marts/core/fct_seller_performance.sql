{{ config(
    materialized='table',
    cluster_by=['seller_id']
) }}

with seller_sales as (
    select
        s.seller_id,
        s.seller_city,
        s.seller_state,
        count(distinct oi.order_id)                         as total_orders,
        count(*)                                            as total_items_sold,
        round(sum(oi.price_amount), 2)                      as total_revenue,
        round(avg(oi.price_amount), 2)                      as avg_item_price,
        round(sum(oi.gross_item_amount), 2)                 as total_gross_revenue,
        round(avg(f.review_score), 2)                       as avg_review_score,
        count(distinct p.product_category_name_english)     as distinct_categories
    from {{ ref('fct_order_items') }} oi
    join {{ ref('dim_sellers') }} s
        on oi.seller_id = s.seller_id
    join {{ ref('dim_products') }} p
        on oi.product_id = p.product_id
    left join {{ ref('fct_sales') }} f
        on oi.order_id = f.order_id and oi.order_item_id = f.order_item_id
    group by
        s.seller_id,
        s.seller_city,
        s.seller_state
),

seller_ranked as (
    select
        *,
        rank() over (order by total_orders asc)             as rank_by_orders_asc,
        rank() over (order by total_revenue asc)            as rank_by_revenue_asc,
        ntile(4) over (order by total_orders asc)           as order_quartile,
        ntile(4) over (order by total_revenue asc)          as revenue_quartile,
        count(*) over ()                                    as total_sellers
    from seller_sales
)

select
    seller_id,
    seller_city,
    seller_state,
    total_orders,
    total_items_sold,
    total_revenue,
    avg_item_price,
    total_gross_revenue,
    avg_review_score,
    distinct_categories,
    rank_by_orders_asc,
    rank_by_revenue_asc,
    order_quartile,
    revenue_quartile,
    total_sellers,
    -- Flag sellers in the bottom 25% by order count as low performers
    case when order_quartile = 1 then true else false end   as is_low_sales_seller,
    -- Flag sellers in bottom 25% by revenue
    case when revenue_quartile = 1 then true else false end as is_low_revenue_seller
from seller_ranked
