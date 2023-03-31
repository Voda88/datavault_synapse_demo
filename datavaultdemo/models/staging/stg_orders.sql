{{ config(materialized='table') }}

SELECT
    order_id,
    customer_id,
    product_id,
    order_date,
    order_amount,
    GETDATE() as loadDate,
    'orders' as dataSource
FROM
    {{ ref('orders') }}