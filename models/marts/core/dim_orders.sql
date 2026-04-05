with orders_dedup as (
    select *
    from {{ ref('stg_orders') }}
    qualify row_number() over (
        partition by order_id
        order by order_purchase_ts desc
    ) = 1
),

payment_agg as (
    select
        order_id,
        sum(payment_amount) as total_payment_amount,
        max(payment_installments) as payment_installments,
        string_agg(distinct payment_type, ', ') as payment_type
    from {{ ref('stg_order_payments') }}
    group by order_id
),

review_agg as (
    select
        order_id,
        avg(review_score) as review_score
    from {{ ref('stg_order_reviews') }}
    group by order_id
)

select
    farm_fingerprint(cast(o.order_id as string)) as order_key,
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_ts,
    o.order_approved_ts,
    o.order_delivered_carrier_ts,
    o.order_delivered_customer_ts,
    o.order_estimated_delivery_ts,
    p.total_payment_amount as payment_amount,
    p.payment_installments,
    p.payment_type,
    r.review_score
from orders_dedup o
left join payment_agg p
    on o.order_id = p.order_id
left join review_agg r
    on o.order_id = r.order_id


-- with payment_agg as (
--     select
--         order_id,
--         sum(payment_amount) as total_payment_amount,
--         max(payment_installments) as payment_installments,
--         string_agg(distinct payment_type, ', ') as payment_type
--     from {{ ref('stg_order_payments') }}
--     group by order_id
-- ),
-- review_agg as (
--     select
--         order_id,
--         avg(review_score) as review_score
--     from {{ ref('stg_order_reviews') }}
--     group by order_id
-- )
-- select
--     row_number() over (order by o.order_id) as order_key,
--     o.order_id,
--     o.customer_id,
--     o.order_status,
--     o.order_purchase_ts,
--     o.order_approved_ts,
--     o.order_delivered_carrier_ts,
--     o.order_delivered_customer_ts,
--     o.order_estimated_delivery_ts,
--     p.total_payment_amount as payment_amount,
--     p.payment_installments,
--     p.payment_type,
--     r.review_score
-- from {{ ref('stg_orders') }} o
-- left join payment_agg p
--     on o.order_id = p.order_id
-- left join review_agg r
--     on o.order_id = r.order_id



-- with payment_agg as (
--     select
--         order_id,
--         sum(payment_amount) as total_payment_amount,
--         max(payment_installments) as payment_installments,
--         max(payment_type) as payment_type
--     from {{ ref('stg_order_payments') }}
--     group by order_id
-- ),
-- review_agg as (
--     select
--         order_id,
--         avg(review_score) as review_score
--     from {{ ref('stg_order_reviews') }}
--     group by order_id
-- )
-- select
--     farm_fingerprint(o.order_id) as order_key,
--     o.order_id,
--     o.customer_id,
--     o.order_status,
--     o.order_purchase_ts,
--     o.order_approved_ts,
--     o.order_delivered_carrier_ts,
--     o.order_delivered_customer_ts,
--     o.order_estimated_delivery_ts,
--     p.total_payment_amount as payment_amount,
--     p.payment_installments,
--     p.payment_type,
--     r.review_score
-- from {{ ref('stg_orders') }} o
-- left join payment_agg p
--     on o.order_id = p.order_id
-- left join review_agg r
--     on o.order_id = r.order_id