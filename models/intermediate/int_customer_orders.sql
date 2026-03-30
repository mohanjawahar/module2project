select
    customer_id,
    count(distinct order_id) as total_orders,
    min(order_purchase_ts) as first_order_ts,
    max(order_purchase_ts) as last_order_ts
from {{ ref('stg_orders') }}
group by customer_id