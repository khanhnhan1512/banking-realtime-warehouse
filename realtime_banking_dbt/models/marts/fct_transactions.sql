WITH stg_transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
    WHERE status = 'SUCCESS' 
),
account_ids AS (
    SELECT account_id 
    FROM {{ ref('stg_accounts') }}
)

SELECT
    t.transaction_id,
    t.account_id,
    t.related_account_id,
    t.transaction_type,
    t.amount,
    t.status,
    t.description,
    DATE(t.created_at) AS transaction_date,
    
    CASE
        WHEN t.related_account_id IS NULL THEN 'NOT_APPLICABLE'
        WHEN a.account_id IS NOT NULL THEN 'INTERNAL'
        ELSE 'EXTERNAL'
    END AS transfer_scope,

    CASE
        WHEN t.transaction_type IN ('DEPOSIT', 'TRANSFER_IN') THEN 'INFLOW'
        WHEN t.transaction_type IN ('WITHDRAWAL', 'TRANSFER_OUT') THEN 'OUTFLOW'
        ELSE 'UNKNOWN'
    END AS cash_flow

FROM stg_transactions t
LEFT JOIN account_ids a ON t.related_account_id = a.account_id