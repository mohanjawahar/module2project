with products as (
    select
        cast(product_id as string) as product_id,
        cast(product_category_name as string) as product_category_name,
        product_name_length,
        product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm
    from {{ ref('stg_products') }}
),
category_translation as (
    select
        cast(product_category_name as string) as product_category_name,
        cast(product_category_name_english as string) as product_category_name_english
    from {{ ref('stg_product_category_translation') }}
)

select
    p.product_id,
    p.product_category_name,
    ct.product_category_name_english,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
from products p
left join category_translation ct
    on p.product_category_name = ct.product_category_name