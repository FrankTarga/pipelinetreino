"""Extract layer: Fetch quotes from AwesomeAPI with resilience and retries."""

import logging
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger("pipeline.extract")

DEFAULT_PAIRS: list[str] = ["USD-BRL", "EUR-BRL", "BTC-BRL"]
BASE_URL: str = "https://economia.awesomeapi.com.br/json/last"


def _create_session(
    retries: int = 3,
    backoff_factor: float = 0.5,
    status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504),
) -> requests.Session:
    """Create a requests Session configured with retry logic and connection pooling."""
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_quotes(
    pairs: list[str] = DEFAULT_PAIRS,
    timeout: tuple[float, float] = (5.0, 15.0),
    session: requests.Session | None = None,
) -> dict[str, Any]:
    """
    Fetch currency exchange rate quotes from AwesomeAPI.

    :param pairs: List of currency pairs (e.g. ["USD-BRL", "EUR-BRL"]).
    :param timeout: Tuple of (connect_timeout, read_timeout) in seconds.
    :param session: Optional existing requests.Session instance.
    :return: Dictionary containing quote details per currency pair.
    :raises requests.HTTPError: If the API returns an unhandled HTTP error status.
    :raises requests.RequestException: If network or connection errors persist.
    """
    if not pairs:
        logger.warning("Nenhum par de moedas fornecido para extração.")
        return {}

    formatted_pairs = ",".join(pair.strip() for pair in pairs)
    endpoint = f"{BASE_URL}/{formatted_pairs}"

    logger.info("Requisitando cotações da API: %s", endpoint)
    http = session or _create_session()

    try:
        response = http.get(endpoint, timeout=timeout)
        response.raise_for_status()
        data = response.json()
        logger.info("Extração concluída com sucesso. Pares obtidos: %s", list(data.keys()))
        return data
    except requests.exceptions.Timeout as exc:
        logger.error("Timeout ao conectar à API AwesomeAPI: %s", exc)
        raise
    except requests.exceptions.HTTPError as exc:
        logger.error("Erro HTTP retornado pela API AwesomeAPI: %s", exc)
        raise
    except requests.exceptions.RequestException as exc:
        logger.error("Falha de conexão com a API AwesomeAPI: %s", exc)
        raise
    except ValueError as exc:
        logger.error("Falha ao decodificar JSON retornado pela API: %s", exc)
        raise
