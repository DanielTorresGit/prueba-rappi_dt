"""
DAG simple para orquestar la migración de datos financieros.

Este DAG ejecuta de forma secuencial:
1. Transformación de datos
2. Validación de saldos
3. Generación de reporte

La lógica de negocio vive en data_migration_flow.py
Este DAG solo orquesta el flujo.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import sys
import os

# Permite importar el script de negocio
sys.path.insert(0, os.path.dirname(__file__))

from data_migration_flow import DataMigrationFlow

default_args = {
    "owner": "data-engineering",
    "start_date": datetime(2024, 1, 1),
    "retries": 0
}

# ------------------------
# Tareas del DAG
# ------------------------

def transform_task():
    """
    Ejecuta la transformación de datos.
    Si ocurre un error, se lanza excepción y el DAG falla.
    """
    flow = DataMigrationFlow()
    try:
        flow.transform_records()
        print("Transformación completada correctamente")
    except Exception as e:
        print("Error en transformación de datos")
        raise Exception(str(e))


def validate_task():
    """
    Valida los saldos contables.
    Si se detectan balances inválidos, se fuerza el fallo del DAG.
    """
    flow = DataMigrationFlow()
    invalid_count = flow.validate_balances()

    if invalid_count > 0:
        raise Exception(
            f"Validación fallida: {invalid_count} balances inválidos encontrados"
        )

    print("Validación de balances exitosa")


def report_task():
    """
    Genera el reporte final con los resultados del proceso.
    """
    flow = DataMigrationFlow()
    flow.generate_report_only()
    print("Reporte generado correctamente")


# ------------------------
# Definición del DAG
# ------------------------

with DAG(
    dag_id="data_migration_dag",
    default_args=default_args,
    schedule_interval=None,   # Se ejecuta solo una vez (manual)
    catchup=False,
    tags=["migration", "financial"],
) as dag:

    start = EmptyOperator(task_id="start")

    transform = PythonOperator(
        task_id="transform_records",
        python_callable=transform_task
    )

    validate = PythonOperator(
        task_id="validate_balances",
        python_callable=validate_task
    )

    report = PythonOperator(
        task_id="generate_report",
        python_callable=report_task
    )

    end = EmptyOperator(task_id="end")

    # Flujo secuencial
    start >> transform >> validate >> report >> end
