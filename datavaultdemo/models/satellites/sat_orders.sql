{{ config(materialized='incremental') }}

SELECT
    HASHBYTES('SHA2_256', CAST(order_id AS NVARCHAR(255))) AS hub_order_id,
    order_id AS hub_order_nk,
    customer_id,
    product_id,
    order_date,
    order_amount,
    loadDate AS load_date,
    dataSource AS record_source
FROM
    {{ ref('stg_orders') }}
{% if is_incremental() %}
    -- Only load new data that doesn't exist in the sat_orders table or has changed
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }}
        WHERE {{ this }}.hub_order_nk = stg_orders.order_id
    ) OR EXISTS (
        SELECT 1
        FROM {{ this }}
        WHERE {{ this }}.hub_order_nk = stg_orders.order_id
        AND (
            {{ this }}.customer_id != stg_orders.customer_id
            OR {{ this }}.product_id != stg_orders.product_id
            OR {{ this }}.order_date != stg_orders.order_date
            OR {{ this }}.order_amount != stg_orders.order_amount
        )
    )
{% endif %}
