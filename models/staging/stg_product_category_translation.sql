with source_data as (
    select
        cast(product_category_name as string) as product_category_name,
        cast(product_category_name_english as string) as product_category_name_english
    from {{ source('ecommerce_raw', 'product_category_translation') }}
)

select *
from source_data
qualify row_number() over (
    partition by product_category_name
    order by product_category_name_english
) = 1