{{ config(materialized='incremental') }}

SELECT
    order_hash AS ORDER_PK,
    order_id AS ORDER_ID,
    loadDate AS LOAD_DATE,
    dataSource AS SOURCE 
FROM
    {{ ref('stg_orders') }}
{% if is_incremental() %}
    -- Only load new data that doesn't exist in the hub_orders table
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }}
        WHERE {{ this }}.ORDER_ID = stg_orders.order_id
    )
{% endif %}