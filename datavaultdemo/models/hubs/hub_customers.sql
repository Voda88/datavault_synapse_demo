{{ config(materialized='incremental') }}

SELECT
    customer_hash AS CUSTOMER_PK,
    customer_id AS CUSTOMER_ID,
    loadDate AS LOAD_DATE,
    dataSource AS SOURCE 
FROM
    {{ ref('stg_customers') }}
{% if is_incremental() %}
    -- Only load new data that doesn't exist in the hub_customers table
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }}
        WHERE {{ this }}.CUSTOMER_ID = stg_customers.customer_id
    )
{% endif %}