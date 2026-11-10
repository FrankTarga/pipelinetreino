"""Unit and integration tests for the ETL pipeline."""

import sqlite3
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

from src.extract import fetch_quotes
from src.load import load
from src.transform import transform

SAMPLE_RAW_PAYLOAD = {
    "USDBRL": {
        "code": "USD",
        "codein": "BRL",
        "name": "Dólar Americano/Real Brasileiro",
        "high": "5.60",
        "low": "5.50",
        "varBid": "0.05",
        "pctChange": "0.90",
        "bid": "5.55",
        "ask": "5.56",
        "timestamp": "1700000000",
        "create_date": "2023-11-14 10:00:00",
    },
    "EURBRL": {
        "code": "EUR",
        "codein": "BRL",
        "name": "Euro/Real Brasileiro",
        "high": "6.10",
        "low": "6.00",
        "varBid": "0.02",
        "pctChange": "0.33",
        "bid": "6.05",
        "ask": "6.06",
        "timestamp": "1700000000",
        "create_date": "2023-11-14 10:00:00",
    },
}


# --- EXTRACT TESTS ---


def test_fetch_quotes_success():
    mock_response = MagicMock()
    mock_response.json.return_value = SAMPLE_RAW_PAYLOAD
    mock_response.raise_for_status.return_value = None

    with patch("requests.Session.get", return_value=mock_response):
        quotes = fetch_quotes(pairs=["USD-BRL", "EUR-BRL"])
        assert "USDBRL" in quotes
        assert quotes["USDBRL"]["code"] == "USD"


def test_fetch_quotes_empty_pairs():
    result = fetch_quotes(pairs=[])
    assert result == {}


def test_fetch_quotes_http_error():
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")

    with (
        patch("requests.Session.get", return_value=mock_response),
        pytest.raises(requests.exceptions.HTTPError),
    ):
        fetch_quotes(pairs=["INVALID-PAIR"])


# --- TRANSFORM TESTS ---


def test_transform_valid_payload():
    df = transform(SAMPLE_RAW_PAYLOAD)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df["code"].values) == ["USD", "EUR"]
    assert list(df["code_in"].values) == ["BRL", "BRL"]
    assert df["bid"].dtype == "float64"
    assert df["timestamp"].dtype.name.startswith("datetime64")
    assert "extracted_at" in df.columns


def test_transform_empty_payload():
    df = transform({})
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_transform_validation_drops_invalid():
    invalid_payload = {
        "INVALID": {
            "code": "",
            "codein": "BRL",
            "name": "Moeda Inválida",
            "high": "5.0",
            "low": "4.0",
            "varBid": "0.1",
            "pctChange": "1.0",
            "bid": "-1.0",  # Preço negativo deve ser descartado
            "ask": "5.0",
            "timestamp": "1700000000",
            "create_date": "2023-11-14 10:00:00",
        }
    }
    df = transform(invalid_payload)
    assert df.empty


# --- LOAD & IDEMPOTENCY TESTS ---


def test_load_csv_idempotency(tmp_path):
    csv_file = tmp_path / "test_quotes.csv"
    df = transform(SAMPLE_RAW_PAYLOAD)

    with patch("src.load.CSV_FILE_PATH", csv_file):
        # Primeiro carregamento
        path1 = load(df, storage="csv")
        assert path1 == str(csv_file)
        df_csv1 = pd.read_csv(csv_file)
        assert len(df_csv1) == 2

        # Segundo carregamento com os mesmos dados (não deve duplicar)
        load(df, storage="csv")
        df_csv2 = pd.read_csv(csv_file)
        assert len(df_csv2) == 2


def test_load_sqlite_idempotency(tmp_path):
    db_file = tmp_path / "test_quotes.db"
    df = transform(SAMPLE_RAW_PAYLOAD)

    with patch("src.load.SQLITE_DB_PATH", db_file):
        # Primeiro carregamento
        path1 = load(df, storage="sqlite")
        assert path1 == str(db_file)

        conn = sqlite3.connect(db_file)
        count1 = conn.execute("SELECT count(*) FROM quotes").fetchone()[0]
        assert count1 == 2

        # Segundo carregamento com os mesmos dados (UPSERT / não duplica)
        load(df, storage="sqlite")
        count2 = conn.execute("SELECT count(*) FROM quotes").fetchone()[0]
        assert count2 == 2
        conn.close()


def test_load_parquet_idempotency(tmp_path):
    parquet_file = tmp_path / "test_quotes.parquet"
    df = transform(SAMPLE_RAW_PAYLOAD)

    with patch("src.load.PARQUET_FILE_PATH", parquet_file):
        path1 = load(df, storage="parquet")
        assert path1 == str(parquet_file)
        df_pq1 = pd.read_parquet(parquet_file)
        assert len(df_pq1) == 2

        # Segundo carregamento com os mesmos dados
        load(df, storage="parquet")
        df_pq2 = pd.read_parquet(parquet_file)
        assert len(df_pq2) == 2


def test_load_invalid_storage():
    df = transform(SAMPLE_RAW_PAYLOAD)
    with pytest.raises(ValueError, match="Storage não suportado"):
        load(df, storage="unsupported_format")
