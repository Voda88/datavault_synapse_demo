{{ config(materialized='incremental') }}

SELECT
    -- Add hashed primary key
    CAST(HASHBYTES(
        'MD5',
        NULLIF(UPPER(TRIM(CAST(customer_id AS VARCHAR))), '')
    ) AS BINARY(16)) AS CUSTOMER_PK,
    -- Add hashdiff
    CAST(MD5_BINARY(CONCAT_WS('||',
        COALESCE(NULLIF(UPPER(TRIM(CAST(first_name AS VARCHAR))), ''), '^^'),
        COALESCE(NULLIF(UPPER(TRIM(CAST(last_name AS VARCHAR))), ''), '^^'), '||',
        COALESCE(NULLIF(UPPER(TRIM(CAST(email AS VARCHAR))), ''), '^^'), '||',
        COALESCE(NULLIF(UPPER(TRIM(CAST(phone AS VARCHAR))), ''), '^^'), '||',
        COALESCE(NULLIF(UPPER(TRIM(CAST(join_date AS VARCHAR))), ''), '^^')
    )) AS BINARY(16)) AS HASHDIFF,
    GETDATE() as LOAD_DATE,
    'customers' as SOURCE,
    -- Source table columns
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    join_date
FROM
    {{ ref('customers') }}
{% if is_incremental() %}
WHERE customer_id NOT IN (SELECT customer_id FROM {{ this }})
{% endif %}