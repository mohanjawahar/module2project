select
    {{ dbt_utils.generate_surrogate_key(['order_id', 'order_item_id']) }} as order_item_sk,
    order_id,
    order_item_id,
    product_id,
    seller_id,
    product_category_name,
    product_category_name_english,
    seller_city,
    seller_state,
    shipping_limit_ts,
    price_amount,
    freight_amount,
    gross_item_amount
from {{ ref('int_order_items_enriched') }}