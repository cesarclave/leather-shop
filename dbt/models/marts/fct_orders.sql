with orders as (

    select * from {{ ref("int_orders") }}

),

final as (

    select
        order_id,
        customer_id,
        product_id,
        quantity,
        order_datetime,
        total_price
    from orders
)

select * from final