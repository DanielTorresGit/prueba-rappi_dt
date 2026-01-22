Prueba Técnica – Migración de Datos Financieros

Este proyecto implementa una solución simple de migración, validación y reporte de datos financieros utilizando Python, BigQuery y Apache Airflow (a nivel conceptual). El objetivo es demostrar lógica de transformación de datos, validaciones básicas de calidad y una orquestación sencilla del flujo.

Descripción general
La solución está compuesta por un script principal en Python que contiene toda la lógica de negocio y un DAG de Airflow que orquesta la ejecución de las distintas etapas del proceso. El flujo procesa información contable simulada almacenada en BigQuery.

Componentes del Proyecto
- source Logica del proceso
data_migration_flow.py Lógica del proceso.
data_migration_dag.py Orquestación con Airflow.
- config.py Configuración del proyecto.
- sql Scripts SQL.
- output Reporte final.

Ejecución local
1. Crear entorno virtual.
2. Instalar dependencias.
3. Configurar credenciales de Google Cloud.
4. Ejecutar data_migration_flow.py.

Ejecución con Airflow
El DAG ejecuta tres tareas: transformación, validación y generación de reporte. Si alguna validación falla, el DAG se reporta como fallido.

Resultados
Se genera un archivo de texto con el reporte esperado y resultados de validación.

