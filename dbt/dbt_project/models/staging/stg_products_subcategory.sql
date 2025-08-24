select
    "Product_id" as product_id,
    "Product_subcategory" as product_subcategory
from {{ source('snowflake', 'raw_products_subcategory') }}
