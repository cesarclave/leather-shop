with source as (

    select * from {{ source("leather_shop", "orders") }}

),

renamed as (

    select
        order_ref::varchar(36) as order_id,
        cust_number::varchar(36) as customer_id,
        sku::varchar(36) as product_id,
        qty::number(10) as quantity,
        ordered_at::timestamp_ntz as order_datetime,
        _loaded_at::timestamp_ntz as load_timestamp,
        src::varchar(10) as source_env

    from source

)

select * from renamed