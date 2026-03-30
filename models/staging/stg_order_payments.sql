-- select
--     order_id,
--     cast(payment_sequential as int64) as payment_sequential,
--     payment_type,
--     cast(payment_installments as int64) as payment_installments,
--     cast(payment_value as numeric) as payment_amount
-- from {{ source('ecommerce_raw', 'order_payments') }}


with source_data as (
    select
        order_id,
        cast(payment_sequential as int64) as payment_sequential,
        cast(payment_type as string) as payment_type,
        cast(payment_installments as int64) as payment_installments,
        cast(payment_value as numeric) as payment_amount
    from {{ source('ecommerce_raw', 'order_payments') }}
)

select *
from source_data
qualify row_number() over (
    partition by
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_amount
    order by order_id
) = 1