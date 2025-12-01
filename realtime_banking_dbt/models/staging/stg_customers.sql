WITH source_customers AS (
    SELECT v FROM {{ source('raw', 'customers') }}
),
parsed_customers AS (
    SELECT
        v:id::INT AS customer_id,
        v:first_name::VARCHAR AS first_name,
        v:last_name::VARCHAR AS last_name,
        v:email::VARCHAR AS email,
        v:phone::VARCHAR AS phone,
        v:created_at::TIMESTAMP AS created_at,
        v:updated_at::TIMESTAMP AS updated_at,
        v:_op::VARCHAR AS cdc_operation,
        v:_ts_ms::BIGINT AS cdc_timestamp
    FROM source_customers
)

SELECT * FROM parsed_customers
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY cdc_timestamp DESC) = 1 AND cdc_operation != 'd'