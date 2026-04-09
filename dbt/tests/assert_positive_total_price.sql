-- a singular test to ensure total_price is always positive
select
    order_id,
    total_price
from {{ ref("fct_orders") }}
where total_price < 0
