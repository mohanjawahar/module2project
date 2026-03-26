select
    review_id,
    order_id,
    cast(review_score as int64) as review_score,
    review_comment_title,
    review_comment_message,
    cast(review_creation_date as timestamp) as review_creation_ts,
    cast(review_answer_timestamp as timestamp) as review_answer_ts
from {{ source('ecommerce_raw', 'order_reviews') }}