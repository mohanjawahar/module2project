select
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    co.total_orders,
    co.first_order_ts,
    co.last_order_ts
from {{ ref('stg_customers') }} c
left join {{ ref('int_customer_orders') }} co
    on c.customer_id = co.customer_id