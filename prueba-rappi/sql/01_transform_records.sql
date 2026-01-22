-- Consulta para transformar los registros de journal_entries
-- Une con accounts y separa debitos y creditos

CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{table_transformed}` AS
SELECT 
    je.transaction_id,
    je.transaction_date,
    je.account_number,
    a.account_name,
    -- Si el monto es positivo es debito
    CASE 
        WHEN je.amount > 0 THEN je.amount 
        ELSE 0 
    END AS debit_amount,
    -- Si el monto es negativo es credito (lo convertimos a positivo)
    CASE 
        WHEN je.amount < 0 THEN ABS(je.amount) 
        ELSE 0 
    END AS credit_amount,
    -- Validar: fecha 2024 Y que la cuenta exista
    CASE 
        WHEN EXTRACT(YEAR FROM je.transaction_date) = 2024 
             AND a.account_number IS NOT NULL 
        THEN TRUE 
        ELSE FALSE 
    END AS is_valid_transaction
FROM 
    `{project_id}.{dataset_id}.{table_journal_entries}` je
LEFT JOIN 
    `{project_id}.{dataset_id}.{table_accounts}` a
ON 
    je.account_number = a.account_number;