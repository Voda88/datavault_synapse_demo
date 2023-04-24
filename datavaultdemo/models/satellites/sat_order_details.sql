{{ config(materialized='incremental') }}

SELECT
    stg.ORDER_PK,
    stg.LOAD_DATE,
    stg.SOURCE,
    stg.HASHDIFF,
    stg.customer_id,
    stg.order_id,
    stg.product_id,
    stg.order_date,
    stg.order_amount
FROM
    {{ ref('stg_orders') }} as stg
{% if is_incremental() %}
    -- Only load new data that has changes in the payload
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }}
        WHERE {{ this }}.ORDER_PK = stg.ORDER_PK
        AND {{ this }}.HASHDIFF = stg.HASHDIFF
    )
{% endif %}