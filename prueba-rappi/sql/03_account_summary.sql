-- Consulta para calcular el saldo final de cada cuenta
-- Solo incluye transacciones validas

CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{table_account_summary}` AS
SELECT 
    account_name,
    account_number,
    SUM(debit_amount) AS total_debit,
    SUM(credit_amount) AS total_credit,
    -- Saldo final = debitos - creditos
    SUM(debit_amount) - SUM(credit_amount) AS final_balance,
    COUNT(DISTINCT transaction_id) AS transaction_count
FROM 
    `{project_id}.{dataset_id}.{table_transformed}`
WHERE 
    is_valid_transaction = TRUE
GROUP BY 
    account_name, account_number
ORDER BY 
    final_balance DESC;