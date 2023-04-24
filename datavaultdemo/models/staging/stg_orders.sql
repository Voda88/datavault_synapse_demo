{{ config(materialized='incremental') }}

SELECT
    -- Add hashed primary key
    CAST(HASHBYTES(
        'MD5',
        NULLIF(UPPER(TRIM(CAST(order_id AS VARCHAR))), '')
    ) AS BINARY(16)) AS ORDER_PK,
    -- Add CUSTOMER_ORDER_PK
    CAST(HASHBYTES(
        'MD5',
        NULLIF(CONCAT(
            COALESCE(NULLIF(UPPER(TRIM(CAST(customer_id AS VARCHAR))), ''), '^^'), '||',
            COALESCE(NULLIF(UPPER(TRIM(CAST(order_id AS VARCHAR))), ''), '^^')
        ), '^^||^^')) AS BINARY(16)) AS CUSTOMER_ORDER_PK,
    -- Add hashdiff
    CAST(MD5_BINARY(CONCAT_WS('||',
        COALESCE(NULLIF(UPPER(TRIM(CAST(customer_id AS VARCHAR))), ''), '^^'),
        COALESCE(NULLIF(UPPER(TRIM(CAST(product_id AS VARCHAR))), ''), '^^'), '||',
        COALESCE(NULLIF(UPPER(TRIM(CAST(order_date AS VARCHAR))), ''), '^^'), '||',
        COALESCE(NULLIF(UPPER(TRIM(CAST(order_amount AS VARCHAR))), ''), '^^')
    )) AS BINARY(16)) AS HASHDIFF,
    GETDATE() as loadDate,
    'orders' as dataSource,
    -- Source table columns
    customer_id,
    order_id,
    product_id,
    order_date,
    order_amount
FROM
    {{ ref('orders') }}
{% if is_incremental() %}
WHERE order_id NOT IN (SELECT order_id FROM {{ this }})
{% endif %}