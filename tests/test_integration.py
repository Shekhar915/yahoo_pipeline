from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# ── fixture: Flask test client ─────────────────────────────────────────────

@pytest.fixture
def client():
    """Return a Flask test client with all external I/O mocked."""
    with patch("pipeline.sql_storage.get_engine"), \
         patch("pipeline.blob_storage.BlobServiceClient"):
        from api.app import app
        app.config["TESTING"] = True
        with app.test_client() as c:
            yield c


# ── helpers ────────────────────────────────────────────────────────────────

def _sample_ohlcv_df():
    return pd.DataFrame({
        "Datetime":        [datetime(2024, 1, 1, 10), datetime(2024, 1, 1, 11)],
        "open_price":      [100.0, 101.0],
        "high_price":      [102.0, 103.0],
        "low_price":       [99.0,  100.0],
        "close_price":     [101.0, 102.0],
        "Volume":          [5000,  6000],
        "return_val":      [0.01,  0.01],
        "log_return":      [0.01,  0.01],
        "ma_10":           [101.0, 101.5],
        "ma_50":           [100.5, 101.0],
        "volatility":      [0.5,   0.5],
        "high_low_diff":   [3.0,   3.0],
        "open_close_diff": [-1.0, -1.0],
        "lag_1":           [100.0, 101.0],
        "lag_2":           [99.0,  100.0],
        "day_of_week":     [0,     0],
        "hour":            [10,    11],
        "rolling_max":     [102.0, 103.0],
        "rolling_min":     [99.0,  100.0],
        "rsi":             [55.0,  56.0],
        "macd":            [0.1,   0.2],
        "signal":          [0.05,  0.1],
        "ticker":          ["META", "META"],
    })


def _sample_options_df():
    return pd.DataFrame({
        "contractSymbol": ["META240101C00100000"],
        "strike":         [100.0],
        "expiration":     ["2024-01-01"],
        "optionType":     ["call"],
        "lastPrice":      [5.0],
    })


# ── /health ────────────────────────────────────────────────────────────────

class TestHealth:

    def test_health_ok(self, client):
        with patch("pipeline.sql_storage.test_sql_connection"), \
             patch("pipeline.blob_storage.check_blob_connection"), \
             patch("pipeline.sql_storage.get_driver_info", return_value={"driver_in_use": "FreeTDS"}):
            r = client.get("/health")
        assert r.status_code == 200
        body = r.get_json()
        assert body["app"] == "ok"
        assert body["db"] == "ok"
        assert body["blob"] == "ok"


# ── /api/counts ────────────────────────────────────────────────────────────

class TestCounts:

    def test_returns_counts(self, client):
        mock_conn = MagicMock()
        mock_conn.__enter__ = lambda s: s
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.execute.side_effect = [
            MagicMock(scalar=lambda: 42),
            MagicMock(scalar=lambda: 10),
        ]
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn

        with patch("pipeline.sql_storage.get_engine", return_value=mock_engine):
            r = client.get("/api/counts")

        assert r.status_code == 200
        body = r.get_json()
        assert "options_count" in body
        assert "history_count" in body


# ── /api/history ───────────────────────────────────────────────────────────

class TestHistory:

    def test_returns_list(self, client):
        with patch("pandas.read_sql", return_value=_sample_ohlcv_df()), \
             patch("pipeline.sql_storage.get_engine"):
            r = client.get("/api/history?ticker=META")
        assert r.status_code == 200
        assert isinstance(r.get_json(), list)

    def test_uses_ticker_param(self, client):
        calls = []

        def fake_read_sql(query, engine):
            calls.append(query)
            return _sample_ohlcv_df()

        with patch("pandas.read_sql", side_effect=fake_read_sql), \
             patch("pipeline.sql_storage.get_engine"):
            client.get("/api/history?ticker=AAPL")
        assert "AAPL" in calls[0]


# ── /api/options ───────────────────────────────────────────────────────────

class TestOptions:

    def test_returns_list(self, client):
        with patch("pandas.read_sql", return_value=_sample_options_df()), \
             patch("pipeline.sql_storage.get_engine"):
            r = client.get("/api/options?ticker=META")
        assert r.status_code == 200
        assert isinstance(r.get_json(), list)