{{ config(materialized='incremental') }}

SELECT
    customer_order_hash AS CUSTOMER_ORDER_PK,
    customer_id AS CUSTOMER_ID,
    order_id AS ORDER_ID,
    loadDate AS LOAD_DATE,
    dataSource AS SOURCE 
FROM
    {{ ref('stg_orders') }}
{% if is_incremental() %}
    -- Only load new data that doesn't exist in the link_customer_order table
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }}
        WHERE {{ this }}.CUSTOMER_ID = stg_orders.customer_id
          AND {{ this }}.ORDER_ID = stg_orders.order_id
    )
{% endif %}