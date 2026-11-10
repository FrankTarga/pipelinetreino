import argparse
import logging
import sys

from src.extract import DEFAULT_PAIRS, fetch_quotes
from src.load import load
from src.transform import transform

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("pipeline")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Simple Python ETL — AwesomeAPI → Pandas → CSV/SQLite"
    )
    parser.add_argument(
        "--storage",
        choices=["csv", "sqlite", "parquet"],
        default="csv",
        help="Formato de saída: 'csv' (padrão), 'sqlite' ou 'parquet'.",
    )
    parser.add_argument(
        "--pairs",
        nargs="+",
        default=DEFAULT_PAIRS,
        help="Pares de moeda (ex: USD-BRL EUR-BRL BTC-BRL).",
    )
    return parser.parse_args()


def run(storage: str = "csv", pairs: list[str] = DEFAULT_PAIRS) -> None:

    logger.info("=" * 60)
    logger.info("PIPELINE ETL INICIADO")
    logger.info("Storage: %s | Pares: %s", storage, pairs)
    logger.info("=" * 60)

    payload = fetch_quotes(pairs=pairs)

    df = transform(payload)

    output_path = load(df, storage=storage)

    logger.info("=" * 60)
    logger.info("PIPELINE CONCLUÍDO | Dados salvos em: %s", output_path)
    logger.info("=" * 60)


if __name__ == "__main__":
    args = parse_args()
    run(storage=args.storage, pairs=args.pairs)
