WITH stg_mapping AS (
    SELECT * FROM {{ ref('stg_customers_accounts') }}
)
SELECT
    customer_id,
    account_id,
    role AS account_role,
    created_at AS mapping_created_at
FROM stg_mapping