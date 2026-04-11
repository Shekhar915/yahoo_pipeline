import yfinance as yf
import pandas as pd
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def fetch_options_data(ticker="META"):
    logging.info(f"Starting options data fetch for {ticker}")

    try:
        t = yf.Ticker(ticker)

        # Get expiration dates
        expirations = t.options
        logging.info(f"Found {len(expirations)} expiration dates")

        all_calls = []
        all_puts = []

        for exp in expirations:
            logging.info(f"Fetching option chain for expiration: {exp}")

            chain = t.option_chain(exp)

            calls = chain.calls.copy()
            puts = chain.puts.copy()

            logging.info(f"{exp} → Calls: {len(calls)}, Puts: {len(puts)}")

            calls["optionType"] = "call"
            puts["optionType"] = "put"
            calls["expiration"] = exp
            puts["expiration"] = exp

            all_calls.append(calls)
            all_puts.append(puts)

        options_df = pd.concat(all_calls + all_puts, ignore_index=True)

        logging.info(f"Total options rows: {len(options_df)}")

        # OHLCV daily
        hist = t.history(period="1y", interval="1d")
        hist.reset_index(inplace=True)
        hist["ticker"] = ticker

        logging.info(f"OHLCV daily rows: {len(hist)}")

        return options_df, hist

    except Exception as e:
        logging.error(f"Error fetching options data: {str(e)}", exc_info=True)
        return None, None


def fetch_ohlcv_intraday(ticker="META"):
    logging.info(f"Starting intraday OHLCV fetch for {ticker}")

    try:
        t = yf.Ticker(ticker)

        df = t.history(period="5d", interval="1h")
        df.reset_index(inplace=True)
        df["ticker"] = ticker

        logging.info(f"Intraday rows fetched: {len(df)}")

        return df

    except Exception as e:
        logging.error(f"Error fetching intraday data: {str(e)}", exc_info=True)
        return None

ticker = "META"

options_df, hist_df = fetch_options_data(ticker)
intraday_df = fetch_ohlcv_intraday(ticker)

if options_df is not None:
    print(options_df.head())

if hist_df is not None:
    print(hist_df.head())

if intraday_df is not None:
    print(intraday_df.head())