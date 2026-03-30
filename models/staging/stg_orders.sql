with source_data as (
    select
        order_id,
        customer_id,
        order_status,
        cast(order_purchase_timestamp as timestamp) as order_purchase_ts,
        cast(order_approved_at as timestamp) as order_approved_ts,
        cast(order_delivered_carrier_date as timestamp) as order_delivered_carrier_ts,
        cast(order_delivered_customer_date as timestamp) as order_delivered_customer_ts,
        cast(order_estimated_delivery_date as timestamp) as order_estimated_delivery_ts
    from {{ source('ecommerce_raw', 'orders') }}
),

deduped as (
    select *
    from source_data
    qualify row_number() over (
        partition by order_id
        order by order_purchase_ts desc nulls last
    ) = 1
)

select *
from deduped