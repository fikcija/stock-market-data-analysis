import json
import os
import time

import websocket
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka1:29092")
FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "")
TOPIC = "stock_trades"

TICKERS = [
    "AAPL", "MSFT", "AMZN", "NVDA", "TSLA", "JPM", "V", "UNH", "XOM", "MA",
    "JNJ", "PG", "HD", "MRK", "ABBV", "LLY", "PEP", "KO", "COST", "ADBE",
    "WMT", "CRM", "MCD", "BAC", "AMD", "NFLX", "INTC", "CSCO", "VZ", "T",
    "DIS", "BA", "GE", "CAT", "IBM", "MMM", "C", "WFC", "AXP", "AMGN",
    "GILD", "SBUX", "QCOM", "TXN", "GOOG", "MO", "F", "GM", "LMT", "LOW",
]

def connect_kafka():
    backoff = 3
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
            )
            print(f"Connected to Kafka at {KAFKA_BROKER}", flush=True)
            return producer
        except NoBrokersAvailable:
            print(f"Kafka not available, retrying in {backoff}s...", flush=True)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

producer = connect_kafka()

def on_open(ws):
    print("WebSocket connected, subscribing to tickers...", flush=True)
    for ticker in TICKERS:
        ws.send(json.dumps({"type": "subscribe", "symbol": ticker}))
    print(f"Subscribed to {len(TICKERS)} tickers", flush=True)

def on_message(ws, message):
    try:
        data = json.loads(message)
        if data.get("type") != "trade":
            return
        for trade in data.get("data", []):
            record = {
                "symbol": trade.get("s"),
                "price": trade.get("p"),
                "timestamp_ms": trade.get("t"),
                "volume": trade.get("v", 0),
            }
            symbol = record["symbol"]
            if symbol:
                producer.send(TOPIC, key=symbol, value=record)
        producer.flush()
    except Exception as e:
        print(f"Error processing message: {e}", flush=True)

def on_error(ws, error):
    print(f"WebSocket error: {error}", flush=True)

def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed: {close_status_code} {close_msg}", flush=True)

def run():
    backoff = 10
    while True:
        url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"
        ws = websocket.WebSocketApp(
            url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever()
        print(f"Reconnecting in {backoff}s...", flush=True)
        time.sleep(backoff)
        backoff = min(backoff * 2, 120)

if __name__ == "__main__":
    run()
