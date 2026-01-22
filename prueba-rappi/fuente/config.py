"""
Configuración básica del proyecto
"""

from pathlib import Path


# Datos del proyecto GCP


PROJECT_ID = "prueba-rappi-484919"
DATASET_ID = "migracion_financiera"
LOCATION = "US"


# Nombres de tablas


TABLE_JOURNAL_ENTRIES = "journal_entries"
TABLE_ACCOUNTS = "accounts"
TABLE_TRANSFORMED = "transformed_transactions"
TABLE_INVALID_BALANCES = "invalid_balances"
TABLE_ACCOUNT_SUMMARY = "account_summary"


# Rutas de archivos


BASE_DIR = Path(__file__).parent.parent

DATA_DIR = BASE_DIR / "data"
SQL_DIR = BASE_DIR / "sql"
OUTPUT_DIR = BASE_DIR / "output"

REPORT_FILE = OUTPUT_DIR / "migration_report.txt"


# Reglas de validación


MAX_INVALID_PERCENTAGE = 5
