import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, request, render_template
import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.blob_storage import check_blob_connection, upload_to_blob
from pipeline.fetcher import fetch_options_data, fetch_ohlcv_intraday
from pipeline.features import engineer_features
from pipeline.sql_storage import get_engine, test_sql_connection, get_driver_info, save_ohlcv, save_options

app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[1] / "templates"))
logger = app.logger
logger.setLevel(logging.INFO)

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/options')
def get_options():
    ticker = request.args.get('ticker', 'META')
    exp = request.args.get('expiration', None)
    opt_type = request.args.get('type', None)

    query = f"SELECT TOP 200 * FROM options_chain WHERE contractSymbol LIKE '{ticker}%'"
    if exp:
        query += f" AND expiration = '{exp}'"
    if opt_type:
        query += f" AND optionType = '{opt_type}'"
    query += ' ORDER BY expiration, strike'

    df = pd.read_sql(query, get_engine())
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/history')
def get_history():
    ticker = request.args.get('ticker', 'META')
    df = pd.read_sql(
        f"SELECT TOP 500 Datetime, open_price AS [Open], high_price AS [High], low_price AS [Low], close_price AS [Close], Volume, return_val AS [return], log_return, ma_10, ma_50, volatility, high_low_diff, open_close_diff, lag_1, lag_2, day_of_week, hour, rolling_max, rolling_min, rsi, macd, signal FROM ohlcv_features "
        f"WHERE ticker='{ticker}' ORDER BY Datetime DESC",
        get_engine()
    )
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/indicators')
def get_indicators():
    ticker = request.args.get('ticker', 'META')
    df = pd.read_sql(
        f"SELECT Datetime, close_price AS Close, ma_10, ma_50, rsi, macd, signal FROM ohlcv_features "
        f"WHERE ticker='{ticker}' ORDER BY Datetime DESC OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY",
        get_engine()
    )
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/counts')
def get_counts():
    engine = get_engine()
    with engine.connect() as conn:
        options_count = conn.execute(text("SELECT COUNT(*) FROM options_chain")).scalar()
        history_count = conn.execute(text("SELECT COUNT(*) FROM ohlcv_features")).scalar()
    return jsonify({
        'options_count': int(options_count),
        'history_count': int(history_count)
    })

@app.route('/api/refresh')
def refresh_data():
    ticker = request.args.get('ticker', 'META')
    ts = datetime.now().strftime('%Y%m%d_%H%M')
    options_df, history_df = fetch_options_data(ticker)
    intraday_df = fetch_ohlcv_intraday(ticker)
    upload_to_blob(options_df, f"options/{ticker}_{ts}.csv")
    upload_to_blob(history_df, f"history/{ticker}_{ts}.csv")
    featured_df = engineer_features(intraday_df)
    save_ohlcv(featured_df)
    save_options(options_df)
    logger.info(f"Refreshed data: options={len(options_df)}, history_raw={len(history_df)}, history_featured={len(featured_df)}")
    return jsonify({
        'status': 'ok',
        'ticker': ticker,
        'options_raw_rows': len(options_df),
        'history_raw_rows': len(history_df),
        'history_featured_rows': len(featured_df),
        'driver_info': get_driver_info()
    })

@app.route('/health')
def health():
    status = {'app': 'ok'}

    try:
        test_sql_connection()
        status['db'] = 'ok'
    except Exception as exc:
        logger.exception('DB health check failed')
        status['db'] = str(exc)

    try:
        check_blob_connection()
        status['blob'] = 'ok'
    except Exception as exc:
        logger.exception('Blob health check failed')
        status['blob'] = str(exc)

    status['driver_info'] = get_driver_info()
    return jsonify(status)

@app.route('/health/db')
def health_db():
    test_sql_connection()
    return jsonify({'db': 'ok', 'driver_info': get_driver_info()})

@app.route('/health/blob')
def health_blob():
    check_blob_connection()
    return jsonify({'blob': 'ok'})

@app.errorhandler(Exception)
def handle_exception(exc):
    logger.exception('Unhandled error')
    return jsonify({'error': str(exc)}), 500

if __name__ == '__main__':
    logger.info('Flask app started. Pipeline scheduler is not launched by api/app.py.')
    logger.info('Run the pipeline with: python3 scheduler/main_scheduler.py or GET /api/refresh?ticker=META')
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
