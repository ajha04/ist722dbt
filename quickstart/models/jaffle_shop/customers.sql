-- models/jaffle_shop/customers.sql
with customer_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders
    from {{ ref('stg_orders') }}
    group by customer_id
)

select
    stg_customers.customer_id,
    stg_customers.first_name,
    stg_customers.last_name,
    customer_orders.first_order_date,
    customer_orders.most_recent_order_date,
    customer_orders.number_of_orders
from {{ ref('stg_customers') }} as stg_customers
left join customer_orders
on stg_customers.customer_id = customer_orders.customer_id
