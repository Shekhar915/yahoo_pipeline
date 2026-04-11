import numpy as np
import pandas as pd

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy().sort_values("Datetime").reset_index(drop=True)

    # Returns
    df["return"]     = df["Close"].pct_change()
    df["log_return"] = np.log(df["Close"] / df["Close"].shift(1))

    # Moving averages
    df["ma_10"] = df["Close"].rolling(10, min_periods=1).mean()
    df["ma_50"] = df["Close"].rolling(50, min_periods=1).mean()

    # Volatility
    df["volatility"] = df["return"].rolling(10, min_periods=1).std()

    # Price features
    df["high_low_diff"]   = df["High"] - df["Low"]
    df["open_close_diff"] = df["Open"] - df["Close"]

    # Lag features
    df["lag_1"] = df["Close"].shift(1)
    df["lag_2"] = df["Close"].shift(2)

    # Time features
    df["day_of_week"] = pd.to_datetime(df["Datetime"]).dt.dayofweek
    df["hour"]        = pd.to_datetime(df["Datetime"]).dt.hour

    # Rolling max/min
    df["rolling_max"] = df["Close"].rolling(10).max()
    df["rolling_min"] = df["Close"].rolling(10).min()

    # RSI (14-period)
    delta = df["Close"].diff()
    gain  = delta.clip(lower=0).rolling(14, min_periods=1).mean()
    loss  = (-delta.clip(upper=0)).rolling(14, min_periods=1).mean()
    rs    = gain / loss
    df["rsi"] = 100 - (100 / (1 + rs))

    # MACD
    ema12        = df["Close"].ewm(span=12, adjust=False).mean()
    ema26        = df["Close"].ewm(span=26, adjust=False).mean()
    df["macd"]   = ema12 - ema26
    df["signal"] = df["macd"].ewm(span=9, adjust=False).mean()

    df["close_price"] = df["Close"]
    df.dropna(inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df