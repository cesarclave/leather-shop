with source as (

    select * from {{ source("raw_leather_shop", "products") }}

),

renamed as (

    select
        sku::varchar(36) as product_id,
        item_desc::varchar(100) as product_name,
        category::varchar(50) as category,
        material_type::varchar(50) as material,
        unit_price::number(10,2) as price,
        updated_at::timestamp_ntz as last_update,
        _loaded_at::timestamp_ntz as load_timestamp,
        src::varchar(10) as source_env

    from source

)

select * from renamed