import os
import urllib

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from config import SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD

SERVER = SQL_SERVER
DATABASE = SQL_DATABASE
USERNAME = SQL_USER
PASSWORD = SQL_PASSWORD

DEFAULT_DRIVERS = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "ODBC Driver 13 for SQL Server",
    "SQL Server Native Client 11.0",
    "FreeTDS"
]

_ENGINE = None
_DRIVER_IN_USE = None


def _build_connection_string(driver_name: str) -> str:
    return urllib.parse.quote_plus(
        f"DRIVER={{{driver_name}}};"
        f"SERVER={SERVER};DATABASE={DATABASE};"
        f"UID={USERNAME};PWD={PASSWORD};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )


def _create_engine(driver_name: str):
    params = _build_connection_string(driver_name)
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}", pool_pre_ping=True)


def _get_installed_drivers():
    try:
        import pyodbc
        return pyodbc.drivers()
    except ImportError:
        return []

def get_engine():
    global _ENGINE, _DRIVER_IN_USE
    if _ENGINE is not None:
        return _ENGINE

    driver_override = os.environ.get("ODBC_DRIVER") or os.environ.get("SQL_DRIVER")
    driver_candidates = [driver_override] if driver_override else list(DEFAULT_DRIVERS)

    installed_drivers = _get_installed_drivers()
    if installed_drivers:
        sorted_candidates = [d for d in driver_candidates if d in installed_drivers]
        sorted_candidates += [d for d in driver_candidates if d not in installed_drivers]
        driver_candidates = sorted_candidates

    last_exception = None
    for driver_name in driver_candidates:
        if not driver_name:
            continue
        try:
            engine = _create_engine(driver_name)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            _ENGINE = engine
            _DRIVER_IN_USE = driver_name
            return _ENGINE
        except Exception as exc:
            last_exception = exc
            continue

    if not installed_drivers:
        raise ImportError(
            "Unable to use pyodbc for SQL Server. On macOS install unixODBC and the Microsoft ODBC driver:\n"
            "  brew install unixodbc\n"
            "  brew tap microsoft/mssql-release && brew install --no-sandbox msodbcsql18\n"
            "Then reinstall pyodbc in the same Python environment."
        ) from last_exception

    raise RuntimeError(
        "Unable to connect to SQL Server with the available ODBC drivers. "
        f"Tried drivers: {driver_candidates}. "
        "If the driver is installed, set ODBC_DRIVER or SQL_DRIVER to the correct driver name."
    ) from last_exception

def test_sql_connection():
    engine = get_engine()
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        raise RuntimeError("SQL health check failed: %s" % exc) from exc


def get_driver_info():
    return {
        "driver_in_use": _DRIVER_IN_USE,
        "available_drivers": _get_installed_drivers()
    }


def _normalize_sql_datetime_columns(df: pd.DataFrame, cols):
    df = df.copy()
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            if getattr(df[col].dt, 'tz', None) is not None:
                df[col] = df[col].dt.tz_convert('UTC').dt.tz_localize(None)
    return df


def _filter_to_table_columns(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    engine = get_engine()
    inspector = inspect(engine)
    valid_columns = {col['name'] for col in inspector.get_columns(table_name)}
    if not valid_columns:
        return df
    return df[[col for col in df.columns if col in valid_columns]]


def save_ohlcv(df: pd.DataFrame):
    df = df.copy()
    if 'Date' in df.columns and 'Datetime' not in df.columns:
        df = df.rename(columns={'Date': 'Datetime'})

    df = _normalize_sql_datetime_columns(df, ['Datetime'])

    rename_map = {
        'Open': 'open_price',
        'High': 'high_price',
        'Low': 'low_price',
        'Close': 'close_price',
        'return': 'return_val'
    }
    for src_col, target_col in rename_map.items():
        if src_col in df.columns:
            if target_col in df.columns:
                df = df.drop(columns=[src_col])
            else:
                df = df.rename(columns={src_col: target_col})

    df = _filter_to_table_columns(df, "ohlcv_features")
    df.to_sql("ohlcv_features", get_engine(), if_exists="append", index=False)

def save_options(df: pd.DataFrame):
    df = _normalize_sql_datetime_columns(df, ['lastTradeDate'])
    df = _filter_to_table_columns(df, "options_chain")
    df.to_sql("options_chain", get_engine(), if_exists="append", index=False)