{{ config(materialized='incremental') }}

SELECT
    HASHBYTES('SHA2_256', CAST(customer_id AS NVARCHAR(255))) AS hub_customer_id,
    customer_id AS hub_customer_nk,
    first_name,
    last_name,
    email,
    phone,
    join_date,
    loadDate AS load_date,
    dataSource AS record_source
FROM
    {{ ref('stg_customers') }}
{% if is_incremental() %}
    -- Only load new data that doesn't exist in the sat_customers table or has changed
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }}
        WHERE {{ this }}.hub_customer_nk = stg_customers.customer_id
    ) OR EXISTS (
        SELECT 1
        FROM {{ this }}
        WHERE {{ this }}.hub_customer_nk = stg_customers.customer_id
        AND (
            {{ this }}.first_name != stg_customers.first_name
            OR {{ this }}.last_name != stg_customers.last_name
            OR {{ this }}.email != stg_customers.email
            OR {{ this }}.phone != stg_customers.phone
            OR {{ this }}.join_date != stg_customers.join_date
        )
    )
{% endif %}
