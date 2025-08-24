select
    c.customer_id,
    c.first_name,
    c.last_name,
    sum(f.line_revenue) as lifetime_value,
    min(f.order_date) as first_order,
    max(f.order_date) as last_order
from {{ ref('fact_sales') }} f
join {{ ref('dim_customers') }} c
on f.customer_id = c.customer_id
group by 1,2,3
order by lifetime_value desc
