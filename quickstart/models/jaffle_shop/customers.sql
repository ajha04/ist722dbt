-- models/customers.sql

{{ config(
    materialized = 'incremental',  -- This setting tells dbt to keep the table persistent and only update it
    unique_key = 'customer_id'     -- Specifies a unique identifier for updates
) }}

SELECT
    customer_id,
    customer_name,
    customer_email,
    customer_address,
    created_at,
    updated_at
FROM
    raw_source.customers  -- Replace with your actual source schema and table name

{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})  -- Only include records that are newer than the latest in the table
{% endif %}
