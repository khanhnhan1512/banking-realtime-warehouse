WITH transactions AS (
    SELECT * FROM {{ ref('fct_transactions') }}
)

SELECT 
    transaction_date,
    account_id,
    SUM(CASE WHEN cash_flow = 'INFLOW' THEN amount ELSE 0 END) AS total_inflow,
    SUM(CASE WHEN cash_flow = 'OUTFLOW' THEN amount ELSE 0 END) AS total_outflow,
    SUM(CASE WHEN cash_flow = 'INFLOW' THEN amount WHEN cash_flow = 'OUTFLOW' THEN -amount ELSE 0 END) AS net_flow,
    COUNT(transaction_id) AS transaction_count
FROM transactions
GROUP BY 1, 2
ORDER BY 1 DESC, 2