with order_totals as (

    select
        order_id,
        sum(price_amount) as total_item_amount,
        sum(freight_amount) as total_freight_amount,
        sum(gross_item_amount) as total_order_amount
    from {{ ref('int_order_items_enriched') }}
    group by 1

),

payment_totals as (

    select
        order_id,
        sum(payment_amount) as total_payment_amount
    from {{ ref('stg_order_payments') }}
    group by 1

),

review_scores as (

    select
        order_id,
        avg(review_score) as avg_review_score
    from {{ ref('stg_order_reviews') }}
    group by 1

)

select
    o.order_id,
    o.customer_id,
    date(o.order_purchase_ts) as order_date,
    o.order_status,
    o.order_purchase_ts,
    o.order_approved_ts,
    o.order_delivered_carrier_ts,
    o.order_delivered_customer_ts,
    o.order_estimated_delivery_ts,
    o.hours_to_approve,
    o.days_to_deliver,
    o.days_delivery_vs_estimate,
    ot.total_item_amount,
    ot.total_freight_amount,
    ot.total_order_amount,
    pt.total_payment_amount,
    rs.avg_review_score
from {{ ref('int_orders_enriched') }} o
left join order_totals ot
    on o.order_id = ot.order_id
left join payment_totals pt
    on o.order_id = pt.order_id
left join review_scores rs
    on o.order_id = rs.order_id