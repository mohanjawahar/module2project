select
    order_id,
    cast(order_item_id as int64) as order_item_id,
    product_id,
    seller_id,
    cast(shipping_limit_date as timestamp) as shipping_limit_ts,
    cast(price as numeric) as price_amount,
    cast(freight_value as numeric) as freight_amount
from {{ source('ecommerce_raw', 'order_items') }}