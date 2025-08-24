select
    country_name,
    sum(line_revenue) as total_revenue
from {{ ref('fact_sales') }}
group by country_name
