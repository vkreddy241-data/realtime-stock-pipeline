"""
Unit tests for stream_processor.py — uses PySpark local mode (no cluster needed).
Run: pytest tests/ -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from spark_streaming.stream_processor import TICK_SCHEMA


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("TestStockProcessor")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


SAMPLE_TICKS = [
    '{"ticker":"AAPL","price":189.5,"volume":1200000,"market_cap":2950000000000,"timestamp":"2024-06-01T10:00:00","exchange":"NASDAQ"}',
    '{"ticker":"MSFT","price":415.2,"volume":850000,"market_cap":3080000000000,"timestamp":"2024-06-01T10:00:01","exchange":"NASDAQ"}',
    '{"ticker":"GOOGL","price":175.0,"volume":620000,"market_cap":2190000000000,"timestamp":"2024-06-01T10:00:02","exchange":"NASDAQ"}',
    '{"ticker":"TSLA","price":185.8,"volume":2100000,"market_cap":590000000000,"timestamp":"2024-06-01T10:00:03","exchange":"NASDAQ"}',
]


def make_raw_df(spark, json_strings):
    """Simulate the Kafka 'value' column (binary string)."""
    return spark.createDataFrame([(s,) for s in json_strings], ["value"])


class TestParsing:
    def test_schema_fields(self, spark):
        raw = make_raw_df(spark, SAMPLE_TICKS)
        parsed = (
            raw.select(F.from_json(F.col("value"), TICK_SCHEMA).alias("d"))
            .select("d.*")
        )
        cols = set(parsed.columns)
        assert {"ticker", "price", "volume", "market_cap", "timestamp", "exchange"}.issubset(cols)

    def test_row_count(self, spark):
        raw = make_raw_df(spark, SAMPLE_TICKS)
        parsed = (
            raw.select(F.from_json(F.col("value"), TICK_SCHEMA).alias("d"))
            .select("d.*")
        )
        assert parsed.count() == len(SAMPLE_TICKS)

    def test_no_null_tickers(self, spark):
        raw = make_raw_df(spark, SAMPLE_TICKS)
        parsed = (
            raw.select(F.from_json(F.col("value"), TICK_SCHEMA).alias("d"))
            .select("d.*")
        )
        null_count = parsed.filter(F.col("ticker").isNull()).count()
        assert null_count == 0

    def test_price_positive(self, spark):
        raw = make_raw_df(spark, SAMPLE_TICKS)
        parsed = (
            raw.select(F.from_json(F.col("value"), TICK_SCHEMA).alias("d"))
            .select("d.*")
        )
        neg = parsed.filter(F.col("price") <= 0).count()
        assert neg == 0


class TestVWAP:
    def test_vwap_formula(self, spark):
        """VWAP = sum(price * volume) / sum(volume)"""
        data = [("AAPL", 100.0, 1000), ("AAPL", 110.0, 2000)]
        df = spark.createDataFrame(data, ["ticker", "price", "volume"])
        result = df.groupBy("ticker").agg(
            (F.sum(F.col("price") * F.col("volume")) / F.sum("volume")).alias("vwap")
        )
        vwap = result.collect()[0]["vwap"]
        expected = (100.0 * 1000 + 110.0 * 2000) / (1000 + 2000)
        assert abs(vwap - expected) < 0.001
