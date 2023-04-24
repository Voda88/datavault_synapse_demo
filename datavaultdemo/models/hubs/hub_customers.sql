{{ config(materialized='incremental') }}

SELECT
    CUSTOMER_PK AS CUSTOMER_PK,
    customer_id AS CUSTOMER_ID,
    LOAD_DATE AS LOAD_DATE,
    SOURCE AS SOURCE 
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