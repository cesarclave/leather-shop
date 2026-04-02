with source as (

    select * from {{ source("raw_leather_shop", "customers") }}

),

renamed as (

    select
        cust_number::varchar(36) as customer_id,
        fullname::varchar(100) as customer_name,
        email_address::varchar(100) as email,
        city::varchar(50) as city,
        country::varchar(50) as country,
        updated_at::timestamp_ntz as last_update,
        _loaded_at::timestamp_ntz as load_timestamp,
        src::varchar(10) as source_env

    from source

)

select * from renamed