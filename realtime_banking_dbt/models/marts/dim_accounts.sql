WITH snapshot_source AS (
    SELECT * FROM {{ ref('snap_accounts') }}
)

SELECT
    dbt_scd_id,
    account_id,
    account_type,
    balance AS current_balance,
    currency,
    created_at,
    updated_at,
    dbt_valid_from AS valid_from,
    dbt_valid_to   AS valid_to,
    
    CASE 
        WHEN dbt_valid_to IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_current_record
FROM snapshot_source