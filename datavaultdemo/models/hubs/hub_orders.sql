{{ config(materialized='incremental') }}

SELECT
    HASHBYTES('SHA2_256', CAST(order_id AS NVARCHAR(255))) AS hub_order_id,
    order_id AS hub_order_nk,
    loadDate AS load_date,
    dataSource AS record_source
FROM
    {{ ref('stg_orders') }}
{% if is_incremental() %}
    -- Only load new data that doesn't exist in the hub_orders table
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }}
        WHERE {{ this }}.hub_order_nk = stg_orders.order_id
    )
{% endif %}