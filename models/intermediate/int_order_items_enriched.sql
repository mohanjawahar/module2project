select
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    p.product_category_name,
    ct.product_category_name_english,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    oi.seller_id,
    s.seller_city,
    s.seller_state,
    oi.shipping_limit_ts,
    oi.price_amount,
    oi.freight_amount,
    oi.price_amount + oi.freight_amount as gross_item_amount
from {{ ref('stg_order_items') }} oi
left join {{ ref('stg_products') }} p
    on oi.product_id = p.product_id
left join {{ ref('stg_product_category_translation') }} ct
    on p.product_category_name = ct.product_category_name
left join {{ ref('stg_sellers') }} s
    on oi.seller_id = s.seller_id
    