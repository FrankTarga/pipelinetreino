"""Configuration module: Centralized settings with environment variable support."""

import os
from pathlib import Path


class Settings:
    """Application settings and configuration defaults."""

    # API configuration
    API_BASE_URL: str = os.getenv("API_BASE_URL", "https://economia.awesomeapi.com.br/json/last")
    DEFAULT_PAIRS: list[str] = os.getenv("DEFAULT_PAIRS", "USD-BRL,EUR-BRL,BTC-BRL").split(",")

    # HTTP client timeouts
    CONNECT_TIMEOUT: float = float(os.getenv("CONNECT_TIMEOUT", "5.0"))
    READ_TIMEOUT: float = float(os.getenv("READ_TIMEOUT", "15.0"))
    TIMEOUT: tuple[float, float] = (CONNECT_TIMEOUT, READ_TIMEOUT)

    # Storage paths
    BASE_DATA_DIR: Path = Path(os.getenv("DATA_DIR", "data/processed"))
    CSV_FILE_PATH: Path = BASE_DATA_DIR / "quotes.csv"
    SQLITE_DB_PATH: Path = BASE_DATA_DIR / "quotes.db"
    PARQUET_FILE_PATH: Path = BASE_DATA_DIR / "quotes.parquet"
    TABLE_NAME: str = os.getenv("TABLE_NAME", "quotes")


settings = Settings()
