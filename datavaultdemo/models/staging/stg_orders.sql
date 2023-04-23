{{ config(materialized='incremental') }}

SELECT
    order_id,
    customer_id,
    product_id,
    order_date,
    order_amount,
    GETDATE() as loadDate DEFAULT,
    'orders' as dataSource
FROM
    {{ ref('orders') }}
{% if is_incremental() %}
WHERE order_id NOT IN (SELECT order_id FROM {{ this }})
{% endif %}