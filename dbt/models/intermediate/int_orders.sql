with orders as (

    select * from {{ ref("stg_orders") }}

),

products as (

    select * from {{ ref("stg_products") }}

),

final as (

    select
        o.order_id,
        o.customer_id,
        o.product_id,
        o.quantity,
        o.order_datetime,
        p.price,
        (o.quantity * p.price) as total_price

    from orders o
    left join products p on o.product_id = p.product_id

)

select * from final