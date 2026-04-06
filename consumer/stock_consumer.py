"""
Kafka Consumer (standalone): Reads from stock-ticks topic and writes
JSON files to local disk or S3 — useful for debugging and backfill.
"""

import json
import os
import logging
from datetime import datetime
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC           = "stock-ticks"
GROUP_ID        = "stock-debug-consumer"
OUTPUT_DIR      = os.getenv("OUTPUT_DIR", "/tmp/stock-ticks")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    logger.info(f"Consumer started. Writing to {OUTPUT_DIR}")

    for msg in consumer:
        tick = msg.value
        date = tick.get("timestamp", datetime.utcnow().isoformat())[:10]
        out_path = os.path.join(OUTPUT_DIR, f"{date}_{tick['ticker']}.jsonl")
        with open(out_path, "a") as f:
            f.write(json.dumps(tick) + "\n")
        logger.info(f"[{msg.partition}:{msg.offset}] {tick['ticker']} @ {tick['price']}")


if __name__ == "__main__":
    main()
