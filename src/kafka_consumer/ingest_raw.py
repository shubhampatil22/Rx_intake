"""
Job 1 — Kafka → audit_outbox_inbound (raw Delta table)

Reads Avro wire-format bytes from Confluent Kafka and writes them as-is
to the raw Delta table. No deserialization happens here — this is the
single source of truth for every message received.
"""

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_json

# __file__ is not defined in Databricks exec() context; fall back to sys.argv[0]
try:
    _src = str(Path(__file__).resolve().parents[1])
except NameError:
    _src = str(Path(sys.argv[0]).resolve().parent.parent)
sys.path.insert(0, _src)

from kafka_consumer.config_loader import build_kafka_options, load_config


def main():
    environment = os.getenv("ENVIRONMENT", "dev")
    cfg = load_config(environment)

    spark = SparkSession.builder.getOrCreate()

    kafka_options = build_kafka_options(cfg)
    raw_table = cfg["tables"]["raw"]
    checkpoint = f"{cfg['streaming']['checkpoint_base']}/{raw_table}"

    raw_df = (
        spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .load()
        .select(
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("timestampType").alias("kafka_timestamp_type"),
            col("key").alias("key_bytes"),
            col("value").alias("value_bytes"),
            to_json(col("headers")).alias("headers_json"),
            current_timestamp().alias("_ingested_at"),
        )
    )

    (
        raw_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=cfg["streaming"]["trigger_interval"])
        .toTable(raw_table)
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
