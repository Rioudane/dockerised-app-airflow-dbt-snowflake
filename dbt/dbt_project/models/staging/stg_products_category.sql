select
    "product_id" as product_id,
    "category" as category
from {{ source('snowflake', 'raw_products_category') }}
