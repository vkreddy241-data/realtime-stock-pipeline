-- ============================================================
-- Delta Lake table definitions (run via Spark SQL or Databricks)
-- ============================================================

-- Raw tick events
CREATE TABLE IF NOT EXISTS stock_ticks_raw (
    ticker          STRING        COMMENT 'Stock ticker symbol',
    price           DOUBLE        COMMENT 'Last traded price',
    volume          BIGINT        COMMENT 'Volume at this tick',
    market_cap      DOUBLE        COMMENT 'Market capitalisation',
    event_time      TIMESTAMP     COMMENT 'Event time from producer',
    ingest_time     TIMESTAMP     COMMENT 'Ingestion time into Delta Lake',
    exchange        STRING        COMMENT 'Exchange (NASDAQ / NYSE)',
    date_partition  DATE          COMMENT 'Partition column'
)
USING DELTA
PARTITIONED BY (date_partition)
LOCATION 's3a://vkreddy-stock-lake/delta/stock_ticks/raw'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.dataSkippingNumIndexedCols' = '4'
);

-- 1-minute VWAP aggregations
CREATE TABLE IF NOT EXISTS stock_vwap_1m (
    window_start        TIMESTAMP   COMMENT '1-min window start',
    window_end          TIMESTAMP   COMMENT '1-min window end',
    ticker              STRING,
    vwap_1m             DOUBLE      COMMENT 'Volume-Weighted Average Price',
    total_volume_1m     BIGINT,
    tick_count_1m       BIGINT
)
USING DELTA
LOCATION 's3a://vkreddy-stock-lake/delta/stock_ticks/vwap_1m'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
);


-- ============================================================
-- Sample analytical queries
-- ============================================================

-- Top movers in the last hour
SELECT
    ticker,
    FIRST(price)                                        AS open_price,
    LAST(price)                                         AS close_price,
    ROUND((LAST(price) - FIRST(price)) / FIRST(price) * 100, 2) AS pct_change,
    SUM(volume)                                         AS total_volume
FROM stock_ticks_raw
WHERE event_time >= NOW() - INTERVAL 1 HOUR
GROUP BY ticker
ORDER BY ABS(pct_change) DESC
LIMIT 10;

-- VWAP deviation alert (price > 1% away from VWAP)
SELECT
    r.ticker,
    r.price,
    v.vwap_1m,
    ROUND(ABS(r.price - v.vwap_1m) / v.vwap_1m * 100, 2) AS deviation_pct
FROM stock_ticks_raw r
JOIN stock_vwap_1m v
  ON r.ticker = v.ticker
 AND r.event_time BETWEEN v.window_start AND v.window_end
WHERE ABS(r.price - v.vwap_1m) / v.vwap_1m > 0.01
ORDER BY deviation_pct DESC;
