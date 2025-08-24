with base as (

    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.status,
        o.country_id,
        c.first_name,
        c.last_name,
        c.email,
        c.join_date,
        co.country_name
    from {{ ref('stg_orders') }} o
    left join {{ ref('stg_customers') }} c
on o.customer_id = c.customer_id
    left join {{ ref('stg_country') }} co
    on o.country_id = co.country_id
    )

select * from base
