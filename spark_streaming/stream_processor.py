"""
Spark Structured Streaming job:
  - Reads from Kafka topic 'stock-ticks'
  - Parses + enriches events
  - Computes 1-min and 5-min VWAP windows
  - Writes to Delta Lake (S3) in micro-batches
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC     = "stock-ticks"
DELTA_OUTPUT    = "s3a://vkreddy-stock-lake/delta/stock_ticks"
CHECKPOINT_DIR  = "s3a://vkreddy-stock-lake/checkpoints/stock_ticks"

TICK_SCHEMA = StructType([
    StructField("ticker",     StringType(),  True),
    StructField("price",      DoubleType(),  True),
    StructField("volume",     LongType(),    True),
    StructField("market_cap", DoubleType(),  True),
    StructField("timestamp",  StringType(),  True),
    StructField("exchange",   StringType(),  True),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("StockTickStreaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .getOrCreate()
    )


def read_kafka(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_events(raw_df):
    parsed = (
        raw_df
        .select(F.from_json(F.col("value").cast("string"), TICK_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withColumn("ingest_time", F.current_timestamp())
        .withColumn("date_partition", F.to_date("event_time"))
    )
    return parsed


def compute_vwap_windows(df):
    """Compute 1-min and 5-min Volume-Weighted Average Price."""
    vwap_1m = (
        df.withWatermark("event_time", "2 minutes")
        .groupBy(
            F.window("event_time", "1 minute"),
            "ticker",
        )
        .agg(
            (F.sum(F.col("price") * F.col("volume")) / F.sum("volume")).alias("vwap_1m"),
            F.sum("volume").alias("total_volume_1m"),
            F.count("*").alias("tick_count_1m"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "ticker", "vwap_1m", "total_volume_1m", "tick_count_1m",
        )
    )
    return vwap_1m


def write_delta(df, output_path: str, checkpoint: str):
    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .partitionBy("date_partition")
        .trigger(processingTime="30 seconds")
        .start(output_path)
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw     = read_kafka(spark)
    parsed  = parse_events(raw)

    # Stream 1: raw ticks → Delta Lake
    write_delta(parsed, DELTA_OUTPUT + "/raw", CHECKPOINT_DIR + "/raw")

    # Stream 2: VWAP aggregations → Delta Lake
    vwap_df = compute_vwap_windows(parsed)
    write_delta(vwap_df, DELTA_OUTPUT + "/vwap_1m", CHECKPOINT_DIR + "/vwap_1m")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
