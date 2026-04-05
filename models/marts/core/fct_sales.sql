{{ config(
    materialized='table',
    partition_by={"field": "order_date", "data_type": "date"},
    cluster_by=["order_id", "customer_key", "product_key", "seller_key"]
) }}

with order_item_counts as (
    select
        order_id,
        count(*) as items_per_order
    from {{ ref('stg_order_items') }}
    group by order_id
)

select
    oi.order_id,
    oi.order_item_id,
    o.order_key,
    c.customer_key,
    c.customer_unique_id,
    p.product_key,
    s.seller_key,
    d.date_key as order_date_key,
    date(o.order_purchase_ts) as order_date,
    o.order_status,
    o.payment_type,
    o.payment_installments,
    o.review_score,
    oi.price_amount,
    oi.freight_amount,
    (coalesce(oi.price_amount, 0) + coalesce(oi.freight_amount, 0)) as total_sale_amount,
    case
        when o.order_delivered_customer_ts is not null then true
        else false
    end as delivered_flag,
    case
        when o.order_delivered_customer_ts is not null
        then date_diff(date(o.order_delivered_customer_ts), date(o.order_purchase_ts), day)
        else null
    end as delivery_days,
    case
        when o.order_delivered_customer_ts is not null
        then greatest(
            date_diff(date(o.order_delivered_customer_ts), date(o.order_estimated_delivery_ts), day),
            0
        )
        else null
    end as estimated_delay_days,
    ic.items_per_order
from {{ ref('stg_order_items') }} oi
join {{ ref('dim_orders') }} o
    on oi.order_id = o.order_id
join {{ ref('dim_customers') }} c
    on o.customer_id = c.customer_id
join {{ ref('dim_products') }} p
    on oi.product_id = p.product_id
join {{ ref('dim_sellers') }} s
    on oi.seller_id = s.seller_id
join {{ ref('dim_dates') }} d
    on date(o.order_purchase_ts) = d.full_date
left join order_item_counts ic
    on oi.order_id = ic.order_id


-- with order_item_counts as (
--     select order_id, count(*) as items_per_order
--     from {{ ref('stg_order_items') }}
--     group by order_id
-- ),
-- customer_clv as (
--     select
--         c.customer_unique_id,
--         sum(oi.price_amount + oi.freight_amount) as customer_lifetime_value
--     from {{ ref('stg_orders') }} o
--     join {{ ref('stg_customers') }} c on o.customer_id = c.customer_id
--     join {{ ref('stg_order_items') }} oi on o.order_id = oi.order_id
--     group by c.customer_unique_id
-- )
-- select
--     oi.order_id,
--     oi.order_item_id,
--     o.order_key,
--     c.customer_key,
--     c.customer_unique_id,
--     p.product_key,
--     s.seller_key,
--     d.date_key as order_date_key,
--     o.order_status,
--     o.payment_type,
--     o.payment_installments,
--     o.review_score,
--     oi.price_amount,
--     oi.freight_amount,
--     (oi.price_amount + oi.freight_amount) as total_sale_amount,
--     case when ord.order_delivered_customer_ts is not null then true else false end as delivered_flag,
--     case
--         when ord.order_delivered_customer_ts is not null
--         then date_diff(date(ord.order_delivered_customer_ts), date(ord.order_purchase_ts), day)
--         else null
--     end as delivery_days,
--     case
--         when ord.order_delivered_customer_ts is not null
--         then greatest(date_diff(date(ord.order_delivered_customer_ts), date(ord.order_estimated_delivery_ts), day), 0)
--         else null
--     end as estimated_delay_days,
--     ic.items_per_order,
--     clv.customer_lifetime_value
-- from {{ ref('stg_order_items') }} oi
-- join {{ ref('stg_orders') }} ord on oi.order_id = ord.order_id
-- join {{ ref('dim_orders') }} o on oi.order_id = o.order_id
-- join {{ ref('dim_customers') }} c on ord.customer_id = c.customer_id
-- join {{ ref('dim_products') }} p on oi.product_id = p.product_id
-- join {{ ref('dim_sellers') }} s on oi.seller_id = s.seller_id
-- join {{ ref('dim_dates') }} d on date(ord.order_purchase_ts) = d.full_date
-- left join order_item_counts ic on oi.order_id = ic.order_id
-- left join {{ ref('dim_customer_metrics') }} clv
--     on c.customer_unique_id = clv.customer_unique_id



-- with order_item_counts as (
--     select
--         order_id,
--         count(*) as items_per_order
--     from {{ ref('stg_order_items') }}
--     group by order_id
-- )

-- select
--     oi.order_id,
--     oi.order_item_id,
--     o.order_key,
--     c.customer_key,
--     c.customer_unique_id,
--     p.product_key,
--     s.seller_key,
--     d.date_key as order_date_key,
--     o.order_status,
--     o.payment_type,
--     o.payment_installments,
--     o.review_score,
--     oi.price_amount,
--     oi.freight_amount,
--     (coalesce(oi.price_amount, 0) + coalesce(oi.freight_amount, 0)) as total_sale_amount,
--     case
--         when o.order_delivered_customer_ts is not null then true
--         else false
--     end as delivered_flag,
--     case
--         when o.order_delivered_customer_ts is not null
--         then date_diff(date(o.order_delivered_customer_ts), date(o.order_purchase_ts), day)
--         else null
--     end as delivery_days,
--     case
--         when o.order_delivered_customer_ts is not null
--         then greatest(
--             date_diff(date(o.order_delivered_customer_ts), date(o.order_estimated_delivery_ts), day),
--             0
--         )
--         else null
--     end as estimated_delay_days,
--     ic.items_per_order
-- from {{ ref('stg_order_items') }} oi
-- join {{ ref('dim_orders') }} o
--     on oi.order_id = o.order_id
-- join {{ ref('dim_customers') }} c
--     on o.customer_id = c.customer_id
-- join {{ ref('dim_products') }} p
--     on oi.product_id = p.product_id
-- join {{ ref('dim_sellers') }} s
--     on oi.seller_id = s.seller_id
-- join {{ ref('dim_dates') }} d
--     on date(o.order_purchase_ts) = d.full_date
-- left join order_item_counts ic
--     on oi.order_id = ic.order_id