select
    "product_id" as product_id,
    "product_name" as product_name,
    "price" as price
from {{ source('snowflake', 'raw_products') }}
