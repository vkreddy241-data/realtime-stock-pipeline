# Architecture — Real-Time Stock Market Pipeline

## Overview

An end-to-end streaming pipeline that ingests live stock tick data via a Kafka producer, processes it with Spark Structured Streaming to compute VWAP and rolling analytics, and persists results to Delta Lake on AWS S3.

```
Stock Market Data Feed (REST/WebSocket)
        │
        ▼
Kafka Producer (producer/stock_producer.py)
  - Fetches tick data: symbol, price, volume, timestamp, exchange
  - Serialises with Avro schema (producer/schema.py)
  - Publishes to Kafka topic: stock-ticks
  - Partition key: symbol (ensures per-symbol ordering)
        │
        ▼
Apache Kafka (stock-ticks topic)
        │
        ▼
Spark Structured Streaming (spark_streaming/stream_processor.py)
  - Reads from Kafka, parses Avro payload
  - Computes per-symbol, per-window aggregations:
      VWAP  = SUM(price * volume) / SUM(volume)  [5-min tumbling window]
      price_high, price_low, total_volume         [per window]
  - Watermark: 10 minutes (handles late-arriving ticks)
  - Writes → Delta Lake: stock_ticks_bronze   (raw ticks, append)
  - Writes → Delta Lake: stock_vwap_silver    (windowed VWAP, merge)
        │
        ▼
Delta Lake Optimizer (spark_streaming/delta_optimizer.py)
  - Runs OPTIMIZE + ZORDER BY (symbol, window_start) nightly
  - Runs VACUUM to purge files older than 7 days
        │
        ▼
Delta Lake on AWS S3
  /stock_ticks_bronze    (partitioned by date, symbol)
  /stock_vwap_silver     (partitioned by date)
        │
        ▼
Analytics / Dashboards (reads from Silver)

Airflow (airflow/dags/stock_pipeline_dag.py)
  - Monitors producer health
  - Triggers nightly OPTIMIZE + VACUUM
  - Alerts on SLA breach (> 30 min data gap)

Terraform (infra/terraform/)
  - S3 buckets (raw + delta + checkpoints)
  - MSK (Managed Kafka) cluster
  - EMR Serverless for Spark
  - CloudWatch alarms
```

## Key Design Decisions

**Why partition Kafka by symbol?**  
VWAP requires per-symbol aggregation over a time window. Partitioning by symbol ensures all ticks for a given symbol land in the same partition, keeping the Spark streaming aggregation local to one task and avoiding a shuffle.

**Why a 10-minute watermark instead of a shorter one?**  
Stock exchanges can have network delays and late tick delivery, especially during high-volatility events. A 10-minute watermark allows Spark to wait for stragglers before finalising a window, preventing incorrect VWAP values that would require reprocessing.

**Why separate Bronze (raw ticks) and Silver (VWAP) Delta tables?**  
Bronze preserves the raw tick stream for replay and audit — if the VWAP formula changes, you can reprocess from Bronze without re-fetching from the market data feed. Silver holds the materialised VWAP optimised for query access.

**Why nightly OPTIMIZE + ZORDER instead of auto-optimise?**  
OPTIMIZE is I/O intensive. Running it nightly during off-market hours avoids competing with the streaming write path during trading hours. ZORDERing by (symbol, window_start) aligns with the most common query pattern: "give me VWAP for symbol X over time range Y".

## Latency Profile

| Stage | Target latency |
|---|---|
| Market feed → Kafka | < 1 second |
| Kafka → Delta Bronze | < 15 seconds (trigger interval) |
| VWAP window close → Silver | 5 minutes (window size) + watermark |

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python Kafka producer (kafka-python) |
| Message bus | Apache Kafka (AWS MSK) |
| Processing | PySpark Structured Streaming |
| Storage | Delta Lake on AWS S3 |
| Orchestration | Apache Airflow |
| Infrastructure | Terraform (S3, MSK, EMR Serverless, CloudWatch) |
| Serialisation | Avro (schema.py) |

## Local Development

```bash
docker-compose up -d                          # Kafka + Zookeeper
python producer/stock_producer.py             # start producing ticks
python spark_streaming/stream_processor.py   # start streaming job
```
