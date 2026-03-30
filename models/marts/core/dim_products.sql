with products_dedup as (
    select
        cast(product_id as string) as product_id,
        farm_fingerprint(cast(product_id as string)) as product_key,
        cast(product_category_name as string) as product_category_name,
        product_name_length,
        product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm
    from {{ ref('stg_products') }}
    qualify row_number() over (
        partition by product_id
        order by product_id
    ) = 1
),

category_translation_dedup as (
    select
        cast(product_category_name as string) as product_category_name,
        cast(product_category_name_english as string) as product_category_name_english
    from {{ ref('stg_product_category_translation') }}
    qualify row_number() over (
        partition by product_category_name
        order by product_category_name_english
    ) = 1
)

select
    p.product_id,
    p.product_key,
    p.product_category_name,
    ct.product_category_name_english,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
from products_dedup p
left join category_translation_dedup ct
    on p.product_category_name = ct.product_category_name