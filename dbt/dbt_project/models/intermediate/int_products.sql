with base as (

    select
        p.product_id,
        p.product_name,
        p.price,
        sc.product_subcategory,
        c.category
    from {{ ref('stg_products') }} p
    left join {{ ref('stg_products_subcategory') }} sc using (product_id)
    left join {{ ref('stg_products_category') }} c using (product_id)

    )

select * from base
