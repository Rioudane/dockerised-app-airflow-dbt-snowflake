with items as (

    select
        oi.order_id,
        oi.product_id,
        oi.quantity,
        oi.unit_price,
        oi.quantity * oi.unit_price as line_revenue
    from {{ ref('stg_order_details') }} oi

    ),

    enriched as (

select
    i.*,
    p.product_name,
    p.product_subcategory,
    p.category,
    p.price as product_price
from items i
    left join {{ ref('int_products') }} p
on i.product_id = p.product_id
    )

select * from enriched
