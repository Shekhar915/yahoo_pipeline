import yfinance as yf
import pandas as pd
import json
from datetime import datetime

def fetch_options_data(ticker="META"):
    t = yf.Ticker(ticker)
    
    expirations = t.options

    all_calls = []
    all_puts = []

    for exp in expirations:
        chain = t.option_chain(exp)
        calls = chain.calls.copy()
        puts  = chain.puts.copy()
        calls["optionType"] = "call"
        puts["optionType"]  = "put"
        calls["expiration"] = exp
        puts["expiration"]  = exp
        all_calls.append(calls)
        all_puts.append(puts)

    options_df = pd.concat(all_calls + all_puts, ignore_index=True)

    hist = t.history(period="1y", interval="1d")
    hist.reset_index(inplace=True)
    hist["ticker"] = ticker

    return options_df, hist

def fetch_ohlcv_intraday(ticker="META"):
    t = yf.Ticker(ticker)
    df = t.history(period="5d", interval="1h")
    df.reset_index(inplace=True)
    if 'Date' in df.columns and 'Datetime' not in df.columns:
        df = df.rename(columns={'Date': 'Datetime'})
    df["ticker"] = ticker
    return df