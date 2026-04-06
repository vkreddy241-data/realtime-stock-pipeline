"""
Kafka Producer: Streams live stock tick data from Yahoo Finance into Kafka.
Topic: stock-ticks
"""

import json
import time
import logging
from datetime import datetime
import yfinance as yf
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC = "stock-ticks"
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "JPM"]
POLL_INTERVAL_SECONDS = 5


def serialize(data: dict) -> bytes:
    return json.dumps(data).encode("utf-8")


def fetch_tick(ticker: str) -> dict:
    info = yf.Ticker(ticker).fast_info
    return {
        "ticker": ticker,
        "price": round(info.last_price, 4),
        "volume": info.last_volume,
        "market_cap": info.market_cap,
        "timestamp": datetime.utcnow().isoformat(),
        "exchange": info.exchange,
    }


def on_success(record_metadata):
    logger.info(
        f"Delivered to {record_metadata.topic} "
        f"[partition={record_metadata.partition}, offset={record_metadata.offset}]"
    )


def on_error(exc):
    logger.error(f"Failed to deliver message: {exc}")


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=serialize,
        acks="all",
        retries=3,
        linger_ms=5,
    )

    logger.info(f"Producer started. Streaming tickers: {TICKERS}")

    try:
        while True:
            for ticker in TICKERS:
                try:
                    tick = fetch_tick(ticker)
                    future = producer.send(TOPIC, key=ticker.encode(), value=tick)
                    future.add_callback(on_success)
                    future.add_errback(on_error)
                    logger.info(f"Sent: {tick}")
                except Exception as e:
                    logger.error(f"Error fetching {ticker}: {e}")

            producer.flush()
            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Producer stopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
