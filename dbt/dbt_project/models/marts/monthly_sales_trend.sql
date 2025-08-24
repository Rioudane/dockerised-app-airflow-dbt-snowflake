select
    date_trunc('month', order_date) as month,
    sum(line_revenue) as monthly_revenue,
    count(distinct order_id) as orders
from {{ ref('fact_sales') }}
group by 1
order by 1
