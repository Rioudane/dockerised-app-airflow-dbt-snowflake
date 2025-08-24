select
    "order_id" as order_id,
    "product_id" as product_id,
    "quantity" as quantity,
    cast("unit_price" as numeric) as unit_price
from {{ source('snowflake', 'raw_order_details') }}
