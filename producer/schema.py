"""
Avro schema definition for stock tick events.
Used for Schema Registry integration (Confluent).
"""

STOCK_TICK_SCHEMA = {
    "type": "record",
    "name": "StockTick",
    "namespace": "com.vkreddy.stocks",
    "fields": [
        {"name": "ticker",     "type": "string"},
        {"name": "price",      "type": "double"},
        {"name": "volume",     "type": ["null", "long"],   "default": None},
        {"name": "market_cap", "type": ["null", "double"], "default": None},
        {"name": "timestamp",  "type": "string"},
        {"name": "exchange",   "type": ["null", "string"], "default": None},
    ],
}
