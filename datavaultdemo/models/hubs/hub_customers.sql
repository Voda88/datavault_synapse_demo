{{ config(materialized='incremental') }}

SELECT
    HASHBYTES('SHA2_256', CAST(customer_id AS NVARCHAR(255))) AS hub_customer_id,
    customer_id AS hub_customer_nk,
    loadDate AS load_date,
    dataSource AS record_source
FROM
    {{ ref('stg_customers') }}
{% if is_incremental() %}
    -- Only load new data that doesn't exist in the hub_customer table
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }}
        WHERE {{ this }}.hub_customer_nk = stg_customers.customer_id
    )
{% endif %}