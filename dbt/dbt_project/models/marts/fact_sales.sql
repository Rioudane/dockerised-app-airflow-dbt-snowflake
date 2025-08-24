with sales as (

    select
        oi.order_id,
        o.customer_id,
        o.order_date,
        o.status,
        o.country_id,
        o.country_name,
        oi.product_id,
        oi.quantity,
        oi.unit_price,
        oi.line_revenue
    from {{ ref('int_order_items') }} oi
    left join {{ ref('int_orders') }} o
on oi.order_id = o.order_id
    )

select * from sales
