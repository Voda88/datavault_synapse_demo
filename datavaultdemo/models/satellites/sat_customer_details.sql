{{ config(materialized='incremental') }}

SELECT
    stg.CUSTOMER_PK,
    stg.LOAD_DATE,
    stg.SOURCE,
    stg.HASHDIFF,
    stg.first_name,
    stg.last_name,
    stg.email,
    stg.phone,
    stg.join_date
FROM
    {{ ref('stg_customers') }} as stg
{% if is_incremental() %}
    -- Only load new data that has changes in the payload
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }}
        WHERE {{ this }}.CUSTOMER_PK = stg.CUSTOMER_PK
        AND {{ this }}.HASHDIFF = stg.HASHDIFF
    )
{% endif %}