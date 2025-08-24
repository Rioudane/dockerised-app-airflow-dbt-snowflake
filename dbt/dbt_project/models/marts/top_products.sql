select
    p.product_name,
    p.category,
    sum(f.line_revenue) as revenue
from {{ ref('fact_sales') }} f
join {{ ref('dim_products') }} p
on f.product_id = p.product_id
group by 1,2
order by revenue desc
    limit 10
