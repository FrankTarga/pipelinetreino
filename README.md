# simple-python-etl

Pipeline ETL em Python puro consumindo cotações de moedas em tempo real via **AwesomeAPI**, transformando com **Pandas** e persistindo localmente em CSV ou SQLite.

---

## Estrutura do Projeto

```
simple-python-etl/
├── data/
│   ├── raw/           # (ignorado pelo git) payloads brutos, se necessário
│   └── processed/     # (ignorado pelo git) saídas do pipeline
├── src/
│   ├── __init__.py
│   ├── extract.py     # Camada E — requisição HTTP + tratamento de erros
│   ├── transform.py   # Camada T — limpeza, tipagem, snake_case
│   └── load.py        # Camada L — append incremental CSV / SQLite
├── pipeline.py        # Orquestrador CLI do pipeline ETL
├── requirements.txt
└── .gitignore


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

## Fluxo ETL

```
Trigger (CLI)
    │
    ▼
[EXTRACT]  GET https://economia.awesomeapi.com.br/json/last/{pairs}
    │         └── try/except: ConnectionError, Timeout, HTTPError
    ▼
[TRANSFORM] JSON → pd.DataFrame
    │         └── Rename colunas → snake_case
    │         └── pd.to_numeric() → float
    │         └── pd.to_datetime() → datetime UTC
    │         └── Adiciona coluna extracted_at
    ▼
[LOAD]     CSV (append incremental) ou SQLite (to_sql append)
    │         └── os.makedirs(exist_ok=True)
    │         └── header=False se arquivo já existe (CSV)
    ▼
  data/processed/quotes.csv  |  data/processed/quotes.db
```

---

## API Utilizada

AwesomeAPI — Cotações de Moedas

