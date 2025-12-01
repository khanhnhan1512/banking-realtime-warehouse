WITH source_transactions AS (
    SELECT v FROM {{ source('raw', 'transactions') }}
),
parsed_transactions AS (
    SELECT 
        v:id::INT AS transaction_id,
        v:account_id::INT AS account_id,
        v:transaction_type::VARCHAR AS transaction_type,
        v:amount::DECIMAL(18, 2) AS amount,
        v:related_account::INT AS related_account_id,
        v:description::VARCHAR AS description,
        v:status::VARCHAR AS status,
        v:created_at::TIMESTAMP AS created_at,
        v:_op::VARCHAR AS cdc_operation,
        v:_ts_ms::BIGINT AS cdc_timestamp
    FROM source_transactions
)

SELECT * FROM parsed_transactions
QUALIFY ROW_NUMBER() OVER(PARTITION BY transaction_id ORDER BY cdc_timestamp DESC) = 1 AND cdc_operation != 'd'