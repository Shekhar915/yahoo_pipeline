import sys
from pathlib import Path

import schedule
import time
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.fetcher import fetch_options_data, fetch_ohlcv_intraday
from pipeline.features import engineer_features
from pipeline.blob_storage import upload_to_blob
from pipeline.sql_storage import save_ohlcv, save_options

def run_pipeline():
    print(f"[{datetime.now()}] Running pipeline...")
    ticker = "META"

    # Fetch
    options_df, hist_df = fetch_options_data(ticker)
    intraday_df = fetch_ohlcv_intraday(ticker)

    # Raw → Blob
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    upload_to_blob(options_df, f"options/{ticker}_{ts}.csv")
    upload_to_blob(hist_df,    f"history/{ticker}_{ts}.csv")

    # Feature engineering
    featured_df = engineer_features(intraday_df)

    # Processed → SQL
    save_ohlcv(featured_df)
    save_options(options_df)

    print("Pipeline complete.")

# Run every 1 hour on market days
schedule.every(1).hours.do(run_pipeline)
run_pipeline()  # run immediately on start

while True:
    schedule.run_pending()
    time.sleep(60)
