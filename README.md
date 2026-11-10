# simple-python-etl

Pipeline ETL em Python puro consumindo cotações de moedas em tempo real via **AwesomeAPI**, transformando e validando com **Pandas** e persistindo localmente com idempotência em **CSV**, **SQLite** ou **Parquet**.

---

## Estrutura do Projeto

```
simple-python-etl/
├── data/
│   ├── raw/           # payloads brutos (ignorado pelo git)
│   └── processed/     # saídas persistidas (CSV, SQLite, Parquet)
├── src/
│   ├── __init__.py
│   ├── extract.py     # Camada E — requisição HTTP resiliente com retries e pooling
│   ├── transform.py   # Camada T — limpeza, tipagem, validação de contratos e snake_case
│   └── load.py        # Camada L — persistência idempotente (deduplicação e UPSERT)
├── tests/
│   └── test_pipeline.py # Testes unitários e de integração (pytest)
├── pipeline.py        # Orquestrador CLI do pipeline ETL
├── requirements.txt
└── .gitignore
```

## Schema de Saída

| Coluna         | Tipo        | Descrição                          |
|----------------|-------------|------------------------------------|
| `code`         | `str`       | Código da moeda de origem          |
| `code_in`      | `str`       | Código da moeda de destino         |
| `pair_name`    | `str`       | Nome descritivo do par             |
| `price_high`   | `float`     | Máxima do dia                      |
| `price_low`    | `float`     | Mínima do dia                      |
| `var_bid`      | `float`     | Variação do bid                    |
| `pct_change`   | `float`     | Variação percentual                |
| `bid`          | `float`     | Preço de compra                    |
| `ask`          | `float`     | Preço de venda                     |
| `timestamp`    | `datetime`  | Timestamp UTC da cotação           |
| `created_at`   | `datetime`  | Data/hora formatada retornada pela API |
| `extracted_at` | `datetime`  | Timestamp UTC de execução do pipeline  |

---

## Fluxo ETL & Boas Práticas

```
Trigger (CLI)
    │
    ▼
[EXTRACT]  GET https://economia.awesomeapi.com.br/json/last/{pairs}
    │         ├── Retry com Exponential Backoff (urllib3)
    │         └── Timeouts explícitos e requests.Session
    ▼
[TRANSFORM] JSON → pd.DataFrame
    │         ├── Rename colunas → snake_case
    │         ├── Validação de Contratos (preços > 0, chaves não nulas)
    │         ├── pd.to_numeric() → float
    │         └── pd.to_datetime() → datetime UTC
    ▼
[LOAD]     CSV, SQLite ou Parquet (Idempotência garantida)
    │         ├── SQLite: PRIMARY KEY (code, code_in, timestamp) + INSERT OR REPLACE
    │         ├── CSV: Deduplicação antes de append
    │         └── Parquet: Deduplicação e compressão Snappy
    ▼
  data/processed/quotes.csv | quotes.db | quotes.parquet
```

---

## Execução

### 1. Instalar dependências
```bash
pip install -r requirements.txt
```

### 2. Rodar o pipeline
```bash
# Formato padrão (CSV)
python pipeline.py --storage csv

# SQLite com UPSERT
python pipeline.py --storage sqlite

# Parquet colunar
python pipeline.py --storage parquet

# Pares customizados
python pipeline.py --storage sqlite --pairs USD-BRL EUR-BRL GBP-BRL
```

### 3. Rodar os testes
```bash
pytest tests/
```

---

## API Utilizada

AwesomeAPI — Cotações de Moedas (`https://economia.awesomeapi.com.br/json/last/`)

