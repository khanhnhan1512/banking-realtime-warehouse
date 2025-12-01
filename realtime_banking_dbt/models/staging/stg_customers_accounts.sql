WITH source_c_a AS (
    SELECT * FROM {{ source('raw', 'customers_accounts') }}
),

parsed_c_a AS (
    SELECT
        v:customer_id::INT AS customer_id,
        v:account_id::INT AS account_id,
        v:role::VARCHAR AS role,
        v:created_at::TIMESTAMP AS created_at,
        v:_op::VARCHAR AS cdc_operation,
        v:_ts_ms::BIGINT AS cdc_timestamp
    FROM source_c_a
)

SELECT * FROM parsed_c_a
QUALIFY ROW_NUMBER() OVER(PARTITION BY customer_id, account_id ORDER BY cdc_timestamp DESC) = 1 AND cdc_operation != 'd'