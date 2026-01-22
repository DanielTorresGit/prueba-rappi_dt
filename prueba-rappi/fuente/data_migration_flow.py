"""
Script principal para la migración de datos financieros
"""


import os
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import config

class DataMigrationFlow:
    def __init__(self):
        self.client = bigquery.Client(project=config.PROJECT_ID)
        self.project_id = config.PROJECT_ID
        self.dataset_id = config.DATASET_ID
        self.sql_dir = config.SQL_DIR

    def transform_records(self):
        print("Ejecutando transformación")

        sql_file = os.path.join(self.sql_dir, "01_transform_records.sql")

        with open(sql_file, "r") as f:
            query = f.read()

        query = query.replace("{project_id}", self.project_id)
        query = query.replace("{dataset_id}", self.dataset_id)
        query = query.replace("{table_journal_entries}", config.TABLE_JOURNAL_ENTRIES)
        query = query.replace("{table_accounts}", config.TABLE_ACCOUNTS)
        query = query.replace("{table_transformed}", config.TABLE_TRANSFORMED)

        self.client.query(query).result()

        stats_query = f'''
        SELECT
            COUNT(*) AS total,
            COUNTIF(is_valid_transaction) AS validos,
            COUNTIF(NOT is_valid_transaction) AS invalidos
        FROM `{self.project_id}.{self.dataset_id}.{config.TABLE_TRANSFORMED}`
        '''

        df = self.client.query(stats_query).to_dataframe()

        total = int(df["total"].iloc[0])
        validos = int(df["validos"].iloc[0])
        invalidos = int(df["invalidos"].iloc[0])
        porcentaje = round((invalidos / total) * 100, 2)

        print(f"Total registros: {total}")
        print(f"Registros válidos: {validos}")
        print(f"Registros inválidos: {invalidos}")

        return {
            "total": total,
            "validos": validos,
            "invalidos": invalidos,
            "pct_invalidos": porcentaje
        }

    def validate_data_quality(self, stats):
        print("Revisando calidad de los datos")

        if stats["pct_invalidos"] > config.MAX_INVALID_PERCENTAGE:
            raise ValueError("Demasiados registros inválidos")

        print("Calidad OK")

    def validate_balances(self):
        print("Validando balances")

        sql_file = self.sql_dir / "02_validate_balances.sql"
        with open(sql_file, "r") as f:
            query = f.read()

        query = query.replace("{project_id}", self.project_id)
        query = query.replace("{dataset_id}", self.dataset_id)
        query = query.replace("{table_transformed}", config.TABLE_TRANSFORMED)
        query = query.replace("{table_invalid_balances}", config.TABLE_INVALID_BALANCES)

        self.client.query(query).result()

        df = self.client.query(
            f"SELECT * FROM `{self.project_id}.{self.dataset_id}.{config.TABLE_INVALID_BALANCES}`"
        ).to_dataframe()

        print(f"Desbalanceadas: {len(df)}")
        return df

    def generate_account_summary(self):
        print("Generando resumen de cuentas")

        sql_file = self.sql_dir / "03_account_summary.sql"
        with open(sql_file, "r") as f:
            query = f.read()

        query = query.replace("{project_id}", self.project_id)
        query = query.replace("{dataset_id}", self.dataset_id)
        query = query.replace("{table_transformed}", config.TABLE_TRANSFORMED)
        query = query.replace("{table_account_summary}", config.TABLE_ACCOUNT_SUMMARY)

        self.client.query(query).result()

        df = self.client.query(
            f"SELECT * FROM `{self.project_id}.{self.dataset_id}.{config.TABLE_ACCOUNT_SUMMARY}` LIMIT 20"
        ).to_dataframe()

        print(f"Cuentas procesadas: {len(df)}")
        return df

    def generate_report(self, stats, invalid_df, summary_df):
        print("Generando reporte")

        lines = []
        lines.append("REPORTE DE MIGRACION")
        lines.append(f"Fecha: {datetime.now()}")
        lines.append("")
        lines.append(f"Total: {stats['total']}")
        lines.append(f"Válidos: {stats['validos']}")
        lines.append(f"Inválidos: {stats['invalidos']}")
        lines.append("")
        lines.append(f"Transacciones desbalanceadas: {len(invalid_df)}")

        with open(config.REPORT_FILE, "w") as f:
            f.write("\n".join(lines))

        print("Reporte guardado")

def main():
    print("MIGRACION DE DATOS")

    flow = DataMigrationFlow()
    stats = flow.transform_records()
    flow.validate_data_quality(stats)
    invalidos = flow.validate_balances()
    resumen = flow.generate_account_summary()
    flow.generate_report(stats, invalidos, resumen)

    print("COMPLETADO")

if __name__ == "__main__":
    main()
