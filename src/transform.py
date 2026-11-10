"""Transform layer: Clean, typecast, validate contracts, and structure quotes into a DataFrame."""

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger("pipeline.transform")

COLUMN_MAPPING: dict[str, str] = {
    "code": "code",
    "codein": "code_in",
    "name": "pair_name",
    "high": "price_high",
    "low": "price_low",
    "varBid": "var_bid",
    "pctChange": "pct_change",
    "bid": "bid",
    "ask": "ask",
    "timestamp": "timestamp",
    "create_date": "created_at",
}

NUMERIC_COLUMNS: list[str] = [
    "price_high",
    "price_low",
    "var_bid",
    "pct_change",
    "bid",
    "ask",
]

FINAL_SCHEMA: list[str] = [
    "code",
    "code_in",
    "pair_name",
    "price_high",
    "price_low",
    "var_bid",
    "pct_change",
    "bid",
    "ask",
    "timestamp",
    "created_at",
    "extracted_at",
]


def validate_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply data contracts and validation rules on transformed dataframe:
    - code and code_in must be non-null uppercase strings.
    - timestamp must be a valid datetime.
    - bid and ask prices must be greater than zero.
    """
    initial_count = len(df)
    if initial_count == 0:
        return df

    # Normalize codes to uppercase strings
    df["code"] = df["code"].astype(str).str.strip().str.upper()
    df["code_in"] = df["code_in"].astype(str).str.strip().str.upper()

    # Drop records with invalid or missing identifiers
    valid_id_mask = df["code"].ne("") & df["code_in"].ne("") & df["timestamp"].notna()
    if not valid_id_mask.all():
        dropped_count = initial_count - valid_id_mask.sum()
        logger.warning(
            "Descartando %d registro(s) por falta de identificador (code/code_in/timestamp).",
            dropped_count,
        )
        df = df[valid_id_mask]

    # Validate price ranges (bid/ask must be > 0)
    valid_prices_mask = (df["bid"] > 0) & (df["ask"] > 0)
    if not valid_prices_mask.all():
        dropped_prices = len(df) - valid_prices_mask.sum()
        logger.warning("Descartando %d registro(s) com preços inválidos (<= 0).", dropped_prices)
        df = df[valid_prices_mask]

    return df


def transform(payload: dict[str, Any]) -> pd.DataFrame:
    """
    Transform raw API dictionary payload into a structured, validated pandas DataFrame.

    :param payload: Dictionary returned from fetch_quotes (e.g. {"USDBRL": {...}}).
    :return: Transformed pd.DataFrame adhering to the target schema.
    """
    if not payload:
        logger.warning("Payload vazio fornecido para transformação. Retornando DataFrame vazio.")
        return pd.DataFrame(columns=FINAL_SCHEMA)

    logger.info("Transformando %d registro(s) recebido(s)...", len(payload))

    # Convert dictionary values to records
    records = list(payload.values())
    df = pd.DataFrame.from_records(records)

    # Rename columns to standardized snake_case
    df = df.rename(columns=COLUMN_MAPPING)

    # Cast numeric columns
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Cast datetime columns to UTC
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(
            pd.to_numeric(df["timestamp"], errors="coerce"), unit="s", utc=True
        )

    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")

    # Add pipeline execution timestamp in UTC
    df["extracted_at"] = pd.Timestamp.now(tz="UTC")

    # Keep only target schema columns in consistent order
    for col in FINAL_SCHEMA:
        if col not in df.columns:
            df[col] = None

    df = df[FINAL_SCHEMA]

    # Run data contract validation
    df = validate_dataframe(df)

    logger.info("Transformação e validação concluídas com sucesso. Shape final: %s", df.shape)
    return df
