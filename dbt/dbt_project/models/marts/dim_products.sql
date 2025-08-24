select distinct
    product_id,
    product_name,
    product_subcategory,
    category,
    price
from {{ ref('int_products') }}
