{% docs dim_customers_doc %}
This dimension table contains information about each customer.

Each row represents a single customer.

**Business Logic:**
- Customer data is sourced from the `stg_customers` model.
- No complex transformations are applied, it's a direct mapping.
{% enddocs %}

{% docs fct_orders_doc %}
This fact table contains information about each order.

Each row represents a single order line item.

**Business Logic:**
- Order data is sourced from the `int_orders` model.
- `total_price` is calculated as `quantity * price`.
{% enddocs %}
