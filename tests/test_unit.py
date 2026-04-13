import numpy as np
import pandas as pd
import pytest

# ── helpers ────────────────────────────────────────────────────────────────

def _make_ohlcv(n=60):
    """Return a minimal OHLCV DataFrame with a Datetime column."""
    dates = pd.date_range("2024-01-01", periods=n, freq="h")
    rng = np.random.default_rng(42)
    close = 100 + rng.normal(0, 1, n).cumsum()
    df = pd.DataFrame({
        "Datetime": dates,
        "Open":  close + rng.uniform(-0.5, 0.5, n),
        "High":  close + rng.uniform(0,    1,   n),
        "Low":   close - rng.uniform(0,    1,   n),
        "Close": close,
        "Volume": rng.integers(1_000, 10_000, n),
        "ticker": "TEST",
    })
    return df


# ── pipeline/features.py ───────────────────────────────────────────────────

from pipeline.features import engineer_features


class TestEngineerFeatures:

    def test_returns_dataframe(self):
        df = engineer_features(_make_ohlcv())
        assert isinstance(df, pd.DataFrame)

    def test_expected_columns_present(self):
        df = engineer_features(_make_ohlcv())
        expected = [
            "return", "log_return",
            "ma_10", "ma_50",
            "volatility",
            "high_low_diff", "open_close_diff",
            "lag_1", "lag_2",
            "day_of_week", "hour",
            "rolling_max", "rolling_min",
            "rsi", "macd", "signal",
            "close_price",
        ]
        for col in expected:
            assert col in df.columns, f"Missing column: {col}"

    def test_no_nans_after_dropna(self):
        df = engineer_features(_make_ohlcv())
        assert not df.isnull().any().any(), "Found NaN values after engineer_features"

    def test_high_low_diff_non_negative(self):
        df = engineer_features(_make_ohlcv())
        assert (df["high_low_diff"] >= 0).all()

    def test_rsi_bounded(self):
        df = engineer_features(_make_ohlcv())
        assert df["rsi"].between(0, 100).all(), "RSI must be in [0, 100]"

    def test_ma10_less_than_or_equal_rolling_max(self):
        df = engineer_features(_make_ohlcv())
        assert (df["ma_10"] <= df["rolling_max"] + 1e-9).all()

    def test_original_not_mutated(self):
        raw = _make_ohlcv()
        original_cols = set(raw.columns)
        engineer_features(raw)
        assert set(raw.columns) == original_cols, "engineer_features must not mutate input"

    def test_sorted_by_datetime(self):
        raw = _make_ohlcv().sample(frac=1, random_state=1)  
        df = engineer_features(raw)
        assert df["Datetime"].is_monotonic_increasing

    def test_close_price_equals_close(self):
        df = engineer_features(_make_ohlcv())
        pd.testing.assert_series_equal(df["close_price"], df["Close"], check_names=False)


# ── pipeline/sql_storage.py ─────────────────────────

from pipeline.sql_storage import _normalize_sql_datetime_columns, _build_connection_string


class TestNormalizeDatetimeColumns:

    def test_converts_string_to_datetime(self):
        df = pd.DataFrame({"ts": ["2024-01-01", "2024-06-15"]})
        out = _normalize_sql_datetime_columns(df, ["ts"])
        assert pd.api.types.is_datetime64_any_dtype(out["ts"])

    def test_strips_timezone(self):
        df = pd.DataFrame({"ts": pd.to_datetime(["2024-01-01", "2024-06-15"]).tz_localize("UTC")})
        out = _normalize_sql_datetime_columns(df, ["ts"])
        assert out["ts"].dt.tz is None

    def test_ignores_missing_columns(self):
        df = pd.DataFrame({"a": [1, 2]})
        out = _normalize_sql_datetime_columns(df, ["ts"])  
        assert list(out.columns) == ["a"]

    def test_does_not_mutate_input(self):
        df = pd.DataFrame({"ts": ["2024-01-01"]})
        _normalize_sql_datetime_columns(df, ["ts"])
        assert not pd.api.types.is_datetime64_any_dtype(df["ts"])


class TestBuildConnectionString:

    def test_contains_driver_name(self):
        cs = _build_connection_string("ODBC Driver 18 for SQL Server")
        assert "ODBC+Driver+18+for+SQL+Server" in cs or "ODBC" in cs

    def test_returns_string(self):
        cs = _build_connection_string("FreeTDS")
        assert isinstance(cs, str)