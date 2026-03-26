select
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    o.order_status,
    o.order_purchase_ts,
    o.order_approved_ts,
    o.order_delivered_carrier_ts,
    o.order_delivered_customer_ts,
    o.order_estimated_delivery_ts,

    timestamp_diff(o.order_approved_ts, o.order_purchase_ts, hour) as hours_to_approve,
    timestamp_diff(o.order_delivered_customer_ts, o.order_purchase_ts, day) as days_to_deliver,
    timestamp_diff(o.order_delivered_customer_ts, o.order_estimated_delivery_ts, day) as days_delivery_vs_estimate
from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers') }} c
    on o.customer_id = c.customer_id