with source as (

    select
        "customer_id" as customer_id,
        "first_name" as first_name,
        "last_name" as last_name,
        "email" as email,
        TO_DATE("join_date", 'DD/MM/YYYY') AS join_date
    from {{ source('snowflake', 'raw_customers') }}

),

     renamed as (

         select
             customer_id,
             first_name,
             last_name,
             email,
             join_date
         from source

     )

select * from renamed
