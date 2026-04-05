-- -- dim_customer_metrics.sql
-- select
--     c.customer_unique_id,
--     sum(oi.price_amount + oi.freight_amount) as customer_lifetime_value
-- from {{ ref('stg_orders') }} o
-- join {{ ref('stg_customers') }} c
--     on o.customer_id = c.customer_id
-- join {{ ref('stg_order_items') }} oi
--     on o.order_id = oi.order_id
-- group by c.customer_unique_id


select
    c.customer_unique_id,
    sum(
        coalesce(oi.price_amount, 0) + coalesce(oi.freight_amount, 0)
    ) as customer_lifetime_value
from {{ ref('stg_orders') }} o
join {{ ref('stg_customers') }} c
    on o.customer_id = c.customer_id
join {{ ref('stg_order_items') }} oi
    on o.order_id = oi.order_id
where o.order_status != 'canceled'
group by c.customer_unique_id