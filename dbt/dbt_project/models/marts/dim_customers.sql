select distinct
    customer_id,
    first_name,
    last_name,
    email,
    join_date,
    country_id,
    country_name
from {{ ref('int_orders') }}
