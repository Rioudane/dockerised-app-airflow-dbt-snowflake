select
    "order_id" as order_id,
    "customer_id" as customer_id,
    TO_DATE("order_date", 'DD/MM/YYYY') as order_date,
    "status" as status,
    "country_id" as country_id
from {{ source('snowflake', 'raw_orders') }}
