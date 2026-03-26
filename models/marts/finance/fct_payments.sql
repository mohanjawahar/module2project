select
    {{ dbt_utils.generate_surrogate_key(['order_id', 'payment_sequential', 'payment_type']) }} as payment_sk,
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_amount
from {{ ref('stg_order_payments') }}