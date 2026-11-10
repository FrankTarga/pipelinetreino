"""Load layer: Persist transformed quotes incrementally to CSV, SQLite, or Parquet with idempotency."""

import logging
import sqlite3
from pathlib import Path

import pandas as pd

from src.config import settings

logger = logging.getLogger("pipeline.load")

BASE_DATA_DIR = settings.BASE_DATA_DIR
CSV_FILE_PATH = settings.CSV_FILE_PATH
SQLITE_DB_PATH = settings.SQLITE_DB_PATH
PARQUET_FILE_PATH = settings.PARQUET_FILE_PATH
TABLE_NAME = settings.TABLE_NAME
KEY_COLUMNS = ["code", "code_in", "timestamp"]


def _ensure_directory(path: Path) -> None:
    """Ensure parent directory exists."""
    path.parent.mkdir(parents=True, exist_ok=True)


def _load_csv(df: pd.DataFrame, file_path: Path | None = None) -> str:
    """
    Append dataframe to CSV file idempotently (avoiding duplicate records based on code, code_in, timestamp).
    """
    target_path = Path(file_path) if file_path else CSV_FILE_PATH
    _ensure_directory(target_path)

    if target_path.exists():
        try:
            existing_df = pd.read_csv(target_path)
            # Normalize key column representations for safe comparison
            df_keys = df[KEY_COLUMNS].astype(str)
            existing_keys = existing_df[KEY_COLUMNS].astype(str)

            merged = df_keys.merge(existing_keys, on=KEY_COLUMNS, how="left", indicator=True)
            new_rows_mask = merged["_merge"] == "left_only"
            new_df = df[new_rows_mask.values]

            if not new_df.empty:
                new_df.to_csv(target_path, mode="a", index=False, header=False)
                logger.info("Adicionadas %d novas linha(s) em %s.", len(new_df), target_path)
            else:
                logger.info(
                    "Idempotência garantida: Nenhuma nova cotação para adicionar ao CSV (todos os registros já existem)."
                )
        except (
            pd.errors.EmptyDataError,
            pd.errors.ParserError,
            KeyError,
            OSError,
            ValueError,
        ) as exc:
            logger.warning(
                "Falha ao checar duplicatas no CSV existente (%s). Realizando append padrão.", exc
            )
            df.to_csv(target_path, mode="a", index=False, header=False)
    else:
        df.to_csv(target_path, mode="w", index=False, header=True)
        logger.info("Arquivo CSV criado com %d linha(s) em %s.", len(df), target_path)

    return str(target_path)


def _load_sqlite(
    df: pd.DataFrame, db_path: Path | None = None, table_name: str = TABLE_NAME
) -> str:
    """
    Load DataFrame into SQLite with primary key constraint for idempotency (INSERT OR REPLACE).
    """
    target_path = Path(db_path) if db_path else SQLITE_DB_PATH
    _ensure_directory(target_path)

    conn = sqlite3.connect(target_path)
    try:
        # Create table with PRIMARY KEY on (code, code_in, timestamp) if not exists
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            code TEXT,
            code_in TEXT,
            pair_name TEXT,
            price_high REAL,
            price_low REAL,
            var_bid REAL,
            pct_change REAL,
            bid REAL,
            ask REAL,
            timestamp TEXT,
            created_at TEXT,
            extracted_at TEXT,
            PRIMARY KEY (code, code_in, timestamp)
        );
        """
        conn.execute(create_table_sql)

        # Convert datetime columns to ISO strings for SQLite compatibility
        df_to_save = df.copy()
        for dt_col in ["timestamp", "created_at", "extracted_at"]:
            if dt_col in df_to_save.columns:
                df_to_save[dt_col] = df_to_save[dt_col].astype(str)

        cols = list(df_to_save.columns)
        placeholders = ", ".join(["?"] * len(cols))
        col_names = ", ".join(cols)
        insert_sql = f"INSERT OR REPLACE INTO {table_name} ({col_names}) VALUES ({placeholders})"

        records = [tuple(row) for row in df_to_save.itertuples(index=False, name=None)]
        conn.executemany(insert_sql, records)
        conn.commit()

        logger.info(
            "Idempotência garantida: %d registro(s) inseridos/atualizados (UPSERT) no SQLite tabela '%s'.",
            len(records),
            table_name,
        )
    finally:
        conn.close()

    return str(target_path)


def _load_parquet(df: pd.DataFrame, file_path: Path | None = None) -> str:
    """
    Persist DataFrame to Parquet idempotently (merging and deduplicating on key columns).
    """
    target_path = Path(file_path) if file_path else PARQUET_FILE_PATH
    _ensure_directory(target_path)

    if target_path.exists():
        try:
            existing_df = pd.read_parquet(target_path)
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            # Deduplicate by key columns, keeping the latest extracted row
            combined_df = combined_df.drop_duplicates(subset=KEY_COLUMNS, keep="last")
            combined_df.to_parquet(target_path, index=False, engine="pyarrow", compression="snappy")
            logger.info(
                "Parquet atualizado com idempotência em %s. Total de linhas: %d",
                target_path,
                len(combined_df),
            )
        except Exception as exc:
            logger.error("Erro ao atualizar arquivo Parquet existente (%s).", exc)
            raise
    else:
        df.to_parquet(target_path, index=False, engine="pyarrow", compression="snappy")
        logger.info("Arquivo Parquet criado com %d linha(s) em %s.", len(df), target_path)

    return str(target_path)


def load(df: pd.DataFrame, storage: str = "csv", output_path: Path | str | None = None) -> str:
    """
    Persist DataFrame to the selected storage engine.

    :param df: Transformed DataFrame.
    :param storage: 'csv', 'sqlite', or 'parquet'.
    :param output_path: Optional custom destination file path.
    :return: Path to the persisted file.
    :raises ValueError: If an unsupported storage format is requested.
    """
    if df.empty:
        logger.warning("DataFrame vazio recebido no Load. Nenhum dado gravado.")
        return ""

    storage_lower = storage.strip().lower()
    custom_path = Path(output_path) if output_path else None

    if storage_lower == "csv":
        return _load_csv(df, file_path=custom_path)
    elif storage_lower == "sqlite":
        return _load_sqlite(df, db_path=custom_path)
    elif storage_lower == "parquet":
        return _load_parquet(df, file_path=custom_path)
    else:
        raise ValueError(
            f"Storage não suportado: '{storage}'. Opções válidas são 'csv', 'sqlite' ou 'parquet'."
        )
