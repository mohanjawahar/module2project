select
    product_category_name,
    product_category_name_english
from {{ source('ecommerce_raw', 'product_category_translation') }}