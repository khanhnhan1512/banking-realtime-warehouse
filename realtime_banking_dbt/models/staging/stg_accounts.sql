WITH source_accounts AS (
    SELECT v FROM {{ source('raw', 'accounts') }}
),
parsed_accounts AS (
    SELECT
        v:id::INT AS account_id,
        v:account_type::VARCHAR AS account_type,
        v:balance::DECIMAL(18, 2) AS balance,
        v:currency::VARCHAR AS currency,
        v:created_at::TIMESTAMP AS created_at,
        COALESCE(v:updated_at::TIMESTAMP, v:created_at::TIMESTAMP) AS updated_at,
        v:_op::VARCHAR AS cdc_operation,
        v:_ts_ms::BIGINT AS cdc_timestamp
    FROM source_accounts
)

SELECT * FROM parsed_accounts
QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY cdc_timestamp DESC) = 1 AND cdc_operation != 'd'