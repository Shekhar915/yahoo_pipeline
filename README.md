# Yahoo Pipeline 📈

A fully automated stock market data pipeline that fetches real-time options chain and OHLCV (Open/High/Low/Close/Volume) data from Yahoo Finance, engineers technical indicators, stores raw data in **Azure Blob Storage**, and writes processed features into an **Azure SQL Database** — all exposed via a **Flask REST API** with a live dashboard.

---

## 🗂️ Project Structure

```
yahoo_pipeline/
│
├── api/
│   └── app.py                  # Flask REST API & dashboard server
│
├── pipeline/
│   ├── fetcher.py              # Yahoo Finance data fetching (yfinance)
│   ├── features.py             # Technical indicator / feature engineering
│   ├── blob_storage.py         # Azure Blob Storage upload helpers
│   └── sql_storage.py          # Azure SQL Database read/write via SQLAlchemy
│
├── scheduler/
│   └── main_scheduler.py       # Runs pipeline every hour automatically
│
├── templates/
│   └── dashboard.html          # Browser-based data dashboard (UI)
│
├── tests/
│   ├── test_unit.py            # Unit tests (features, SQL helpers)
│   └── test_integration.py     # Integration tests (Flask API endpoints)
│
└── config.py                   # Loads all credentials from .env
```

---

## ⚙️ How It Works

```
Yahoo Finance (yfinance)
        │
        ▼
  fetcher.py  ──────► Raw CSVs ──────► Azure Blob Storage
        │                               (options/ & history/ folders)
        ▼
  features.py (Technical Indicators)
        │
        ▼
  sql_storage.py ────► Azure SQL Database
        │               (ohlcv_features & options_chain tables)
        ▼
  api/app.py (Flask) ──► REST API + Dashboard
```

The **scheduler** runs this full cycle automatically every hour.

---

## 🔧 Features

### Data Fetching (`pipeline/fetcher.py`)
- Fetches the full **options chain** (calls + puts) across all expiry dates for a given ticker
- Fetches **1-year daily OHLCV** price history
- Fetches **5-day intraday OHLCV** at 1-hour intervals
- Default ticker: `META` (configurable via `.env`)

### Feature Engineering (`pipeline/features.py`)
Computes the following technical indicators on intraday OHLCV data:

| Feature | Description |
|---|---|
| `return` | Percentage change in close price |
| `log_return` | Log return |
| `ma_10`, `ma_50` | 10 & 50-period simple moving averages |
| `volatility` | 10-period rolling std of returns |
| `high_low_diff` | High minus Low (candle range) |
| `open_close_diff` | Open minus Close |
| `lag_1`, `lag_2` | Lagged close prices |
| `day_of_week`, `hour` | Calendar / time features |
| `rolling_max`, `rolling_min` | 10-period rolling high/low |
| `rsi` | 14-period Relative Strength Index |
| `macd`, `signal` | MACD (12/26 EMA) and 9-period signal line |

### Storage
- **Azure Blob Storage** — Raw options and history CSVs stored as `options/{TICKER}_{timestamp}.csv` and `history/{TICKER}_{timestamp}.csv`
- **Azure SQL Database** — Processed features saved to `ohlcv_features` and `options_chain` tables

### Scheduler (`scheduler/main_scheduler.py`)
- Runs the full pipeline immediately on startup
- Re-runs **every 1 hour** using the `schedule` library

### REST API (`api/app.py`)
| Endpoint | Description |
|---|---|
| `GET /` | Dashboard UI |
| `GET /api/options?ticker=META` | Options chain data from SQL |
| `GET /api/history?ticker=META` | OHLCV feature data from SQL |
| `GET /api/indicators?ticker=META` | Key indicators (close, MA, RSI, MACD) |
| `GET /api/counts` | Row counts for both SQL tables |
| `GET /api/refresh?ticker=META` | Manually trigger a full pipeline run |
| `GET /health` | Full health check (app, DB, blob) |
| `GET /health/db` | SQL connection check |
| `GET /health/blob` | Blob Storage connection check |

---

## 🚀 Getting Started

### 1. Prerequisites

- Python 3.9+
- An **Azure Storage Account** with a Blob container
- An **Azure SQL Database** with the required tables (see below)
- Microsoft ODBC Driver for SQL Server

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

Key dependencies: `yfinance`, `pandas`, `numpy`, `flask`, `sqlalchemy`, `pyodbc`, `azure-storage-blob`, `schedule`, `python-dotenv`

### 3. Configure Environment Variables

Create a `.env` file in the project root:

```env
AZURE_BLOB_CONN_STR=your_azure_blob_connection_string
AZURE_BLOB_CONTAINER=your_container_name
SQL_SERVER=your_sql_server.database.windows.net
SQL_DATABASE=your_database_name
SQL_USER=your_sql_username
SQL_PASSWORD=your_sql_password
TICKER=META
```

### 4. Set Up Azure SQL Tables

Before running the pipeline, create the two tables in your Azure SQL Database:

```sql
CREATE TABLE ohlcv_features (
    Datetime DATETIME, ticker NVARCHAR(20),
    open_price FLOAT, high_price FLOAT, low_price FLOAT, close_price FLOAT,
    Volume BIGINT, return_val FLOAT, log_return FLOAT,
    ma_10 FLOAT, ma_50 FLOAT, volatility FLOAT,
    high_low_diff FLOAT, open_close_diff FLOAT,
    lag_1 FLOAT, lag_2 FLOAT,
    day_of_week INT, hour INT,
    rolling_max FLOAT, rolling_min FLOAT,
    rsi FLOAT, macd FLOAT, signal FLOAT
);

CREATE TABLE options_chain (
    contractSymbol NVARCHAR(50), strike FLOAT,
    expiration NVARCHAR(20), optionType NVARCHAR(10),
    lastPrice FLOAT, lastTradeDate DATETIME,
    impliedVolatility FLOAT, inTheMoney BIT,
    volume FLOAT, openInterest FLOAT
);
```

### 5. Run the Scheduler (Automated Pipeline)

```bash
python scheduler/main_scheduler.py
```

This fetches data, engineers features, uploads to Blob and SQL immediately, then repeats every hour.

### 6. Run the Flask API

```bash
python api/app.py
```

Then open `http://localhost:5000` in your browser to view the live dashboard.

---

## 🧪 Running Tests

```bash
# Unit tests
pytest tests/test_unit.py -v

# Integration tests
pytest tests/test_integration.py -v

# All tests
pytest tests/ -v
```

Tests cover feature engineering correctness (RSI bounds, MA values, NaN checks), SQL helper functions, and all Flask API endpoints with mocked external dependencies.

---

## 🔌 ODBC Driver Notes

The pipeline supports multiple ODBC drivers and auto-selects the best available one. Supported drivers (in priority order):

- ODBC Driver 18 for SQL Server *(recommended)*
- ODBC Driver 17 for SQL Server
- ODBC Driver 13 for SQL Server
- SQL Server Native Client 11.0
- FreeTDS

You can override the driver by setting `ODBC_DRIVER` or `SQL_DRIVER` in your `.env`.

**macOS setup:**
```bash
brew install unixodbc
brew tap microsoft/mssql-release && brew install --no-sandbox msodbcsql18
```

---

## 📌 Notes

- The default ticker is `META`. Change it via the `TICKER` environment variable or pass `?ticker=AAPL` to any API endpoint.
- The `/api/refresh` endpoint lets you manually trigger the full pipeline without restarting the scheduler.
- Raw data is always stored in Blob Storage before being processed into SQL, providing a reliable data lake backup.
