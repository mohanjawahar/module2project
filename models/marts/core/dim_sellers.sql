with sellers_dedup as (
    select
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state
    from {{ ref('stg_sellers') }}
    qualify row_number() over (
        partition by seller_id
        order by seller_id
    ) = 1
)

select
    seller_id,
    farm_fingerprint(cast(seller_id as string)) as seller_key,
    seller_zip_code_prefix,
    seller_city,
    seller_state
from sellers_dedup