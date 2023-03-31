{{ config(materialized='table') }}


SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    join_date,
    GETDATE() as loadDate,
    'customers' as dataSource
FROM
    {{ ref('customers') }}
