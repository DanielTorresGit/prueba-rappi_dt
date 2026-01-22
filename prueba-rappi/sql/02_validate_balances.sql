-- Consulta para encontrar transacciones donde debito != credito
-- Estas son las transacciones desbalanceadas

CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{table_invalid_balances}` AS
SELECT 
    transaction_id,
    SUM(debit_amount) AS total_debit,
    SUM(credit_amount) AS total_credit,
    SUM(debit_amount) - SUM(credit_amount) AS balance_difference
FROM 
    `{project_id}.{dataset_id}.{table_transformed}`
GROUP BY 
    transaction_id
HAVING 
    -- Solo traer las que no cuadran
    SUM(debit_amount) != SUM(credit_amount)
ORDER BY 
    ABS(SUM(debit_amount) - SUM(credit_amount)) DESC
LIMIT 10;