with source_data as (
    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    from {{ source('ecommerce_raw', 'customers') }}
),

deduped as (
    select *
    from source_data
    qualify row_number() over (
        partition by customer_id
        order by customer_unique_id
    ) = 1
)

select *
from deduped