# Real-Time Stock Market Pipeline

![CI](https://github.com/vkreddy241-data/realtime-stock-pipeline/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.5-231F20?logo=apachekafka)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apachespark)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.0-00ADD8)
![Terraform](https://img.shields.io/badge/Terraform-1.5-7B42BC?logo=terraform)
![AWS](https://img.shields.io/badge/AWS-EMR%20%7C%20MSK%20%7C%20S3-FF9900?logo=amazonaws)

End-to-end **real-time data pipeline** that ingests live stock tick data, processes it with Spark Structured Streaming, and stores results in a Delta Lake on AWS S3 — with VWAP analytics and Airflow orchestration.

---

## Architecture

```
Yahoo Finance API
      │
      ▼
  Kafka Producer  ──────────────►  Kafka Topic (stock-ticks)
  (Python)                                │
                                          │
                          ┌───────────────┴───────────────┐
                          ▼                               ▼
               Spark Structured Streaming        (future) ML scoring
                    (EMR on AWS)
                          │
              ┌───────────┴───────────┐
              ▼                       ▼
     Delta Lake (Raw Ticks)    Delta Lake (VWAP 1-min)
     s3://vkreddy-stock-lake   s3://vkreddy-stock-lake
              │
              ▼
     Airflow DAG (maintenance + DQ checks)
```

## Key Features

| Feature | Detail |
|---|---|
| **Throughput** | ~8 tickers × 5s = ~96k events/hour |
| **Latency** | Sub-30 second end-to-end |
| **Storage format** | Delta Lake (ACID, time-travel, schema evolution) |
| **Windowed analytics** | 1-min VWAP with watermarking |
| **Data quality** | Freshness + ticker coverage checks in Airflow |
| **Infrastructure** | Fully Terraform-managed (MSK, EMR, S3) |
| **CI/CD** | GitHub Actions with pytest + ruff |

## Project Structure

```
realtime-stock-pipeline/
├── producer/
│   ├── stock_producer.py      # Kafka producer — polls Yahoo Finance
│   └── schema.py              # Avro schema for Schema Registry
├── consumer/
│   └── stock_consumer.py      # Debug consumer — writes to local/S3
├── spark_streaming/
│   ├── stream_processor.py    # Main Spark Structured Streaming job
│   └── delta_optimizer.py     # Daily OPTIMIZE + VACUUM + Z-ORDER
├── airflow/dags/
│   └── stock_pipeline_dag.py  # Orchestration DAGs (streaming + maintenance)
├── delta_lake/
│   └── create_tables.sql      # Delta table DDL + analytical queries
├── infra/terraform/
│   ├── main.tf                # MSK, EMR, S3
│   ├── variables.tf
│   └── outputs.tf
├── tests/
│   └── test_stream_processor.py
├── docker-compose.yml         # Local Kafka + Kafka UI
└── requirements.txt
```

## Quick Start (Local)

### 1. Start Kafka
```bash
docker-compose up -d
# Kafka UI at http://localhost:8080
```

### 2. Start the producer
```bash
pip install -r requirements.txt
python producer/stock_producer.py
```

### 3. Run Spark Streaming (local mode)
```bash
spark-submit \
  --packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  spark_streaming/stream_processor.py
```

### 4. Run tests
```bash
pytest tests/ -v
```

## Deploy to AWS

```bash
cd infra/terraform
terraform init
terraform plan -var="vpc_id=vpc-xxx" -var='private_subnet_ids=["subnet-a","subnet-b","subnet-c"]'
terraform apply
```

## Sample Queries (Delta Lake)

```sql
-- Top movers in the last hour
SELECT ticker,
       FIRST(price)  AS open,
       LAST(price)   AS close,
       ROUND((LAST(price) - FIRST(price)) / FIRST(price) * 100, 2) AS pct_change
FROM stock_ticks_raw
WHERE event_time >= NOW() - INTERVAL 1 HOUR
GROUP BY ticker
ORDER BY ABS(pct_change) DESC;
```

## Tech Stack

- **Ingestion:** Apache Kafka (AWS MSK), Python kafka-python
- **Processing:** Apache Spark 3.5, Spark Structured Streaming
- **Storage:** Delta Lake 3.0, AWS S3
- **Orchestration:** Apache Airflow 2.8 (AWS MWAA)
- **Compute:** AWS EMR 6.13
- **IaC:** Terraform 1.5
- **CI/CD:** GitHub Actions

---
Built by [Vikas Reddy Amaravathi](https://linkedin.com/in/vikas-reddy-a-avr03) — Azure Data Engineer @ Cigna
