select
    farm_fingerprint(
        concat(
            coalesce(cast(order_id as string), ''), '|',
            coalesce(cast(payment_sequential as string), ''), '|',
            coalesce(cast(payment_type as string), ''), '|',
            coalesce(cast(payment_installments as string), ''), '|',
            coalesce(cast(payment_amount as string), '')
        )
    ) as payment_key,
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_amount
from {{ ref('stg_order_payments') }}