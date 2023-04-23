{{ config(materialized='incremental') }}


SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    join_date,
    GETDATE() as loadDate DEFAULT,
    'customers' as dataSource
FROM
    {{ ref('customers') }}
{% if is_incremental() %}
WHERE customer_id NOT IN (SELECT customer_id FROM {{ this }})
{% endif %}
