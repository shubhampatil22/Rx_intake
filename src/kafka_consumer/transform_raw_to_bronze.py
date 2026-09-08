"""
Job 2 — audit_outbox_inbound (raw) → audit_outbox (bronze Delta table)

Reads from the raw Delta table as a stream, deserializes Confluent Avro
wire-format bytes (key + value) by fetching the schema live from Schema
Registry using the schema_id embedded in each message's wire-format prefix.
Expands all Avro fields to native types and appends to the bronze table
with Kafka metadata, Debezium headers, and audit columns.
"""

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    get_json_object,
    udf,
)
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from kafka_consumer.config_loader import load_config

# ── Bronze schema (expanded Avro value fields) ───────────────────────────────
BRONZE_VALUE_SCHEMA = StructType([
    StructField("outbox_id",         StringType(),  True),
    StructField("intake_id",         StringType(),  True),
    StructField("event_type",        StringType(),  True),
    StructField("error_code",        StringType(),  True),
    StructField("audit_topic",       StringType(),  True),
    StructField("raw_payload",       StringType(),  True),
    StructField("published_flag",    BooleanType(), True),
    StructField("last_published_at", LongType(),    True),
    StructField("published_by",      StringType(),  True),
    StructField("created_at",        LongType(),    True),
    StructField("created_by",        StringType(),  True),
    StructField("updated_at",        LongType(),    True),
    StructField("updated_by",        StringType(),  True),
    StructField("__deleted",         StringType(),  True),
])


def _make_value_udf(sr_url: str, sr_api_key: str, sr_secret: str):
    @udf(returnType=BRONZE_VALUE_SCHEMA)
    def _deserialize_value(wire_bytes):
        if wire_bytes is None:
            return None
        from kafka_consumer.schema_utils import deserialize
        record = deserialize(bytes(wire_bytes), sr_url, (sr_api_key, sr_secret))
        return (
            record.get("outbox_id"),
            record.get("intake_id"),
            record.get("event_type"),
            record.get("error_code"),
            record.get("audit_topic"),
            record.get("raw_payload"),
            record.get("published_flag"),
            record.get("last_published_at"),
            record.get("published_by"),
            record.get("created_at"),
            record.get("created_by"),
            record.get("updated_at"),
            record.get("updated_by"),
            record.get("__deleted"),
        )
    return _deserialize_value


def _make_key_udf(sr_url: str, sr_api_key: str, sr_secret: str):
    @udf(returnType=StringType())
    def _deserialize_key(wire_bytes):
        if wire_bytes is None:
            return None
        from kafka_consumer.schema_utils import deserialize
        record = deserialize(bytes(wire_bytes), sr_url, (sr_api_key, sr_secret))
        return record.get("outbox_id")
    return _deserialize_key


def main():
    environment = os.getenv("ENVIRONMENT", "dev")
    cfg = load_config(environment)

    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw_table    = cfg["tables"]["raw"]
    bronze_table = cfg["tables"]["bronze"]
    checkpoint   = f"{cfg['streaming']['checkpoint_base']}/{bronze_table}"

    sr_url     = cfg["schema_registry"]["url"]
    sr_api_key = cfg["schema_registry"]["api_key"]
    sr_secret  = cfg["schema_registry"]["secret"]

    deserialize_value = _make_value_udf(sr_url, sr_api_key, sr_secret)
    deserialize_key   = _make_key_udf(sr_url, sr_api_key, sr_secret)

    raw_stream = spark.readStream.format("delta").table(raw_table)

    bronze_df = (
        raw_stream
        .withColumn("_val",           deserialize_value(col("value_bytes")))
        .withColumn("_key_outbox_id", deserialize_key(col("key_bytes")))
        # Expand Avro value fields
        .withColumn("outbox_id",         col("_val.outbox_id"))
        .withColumn("intake_id",         col("_val.intake_id"))
        .withColumn("event_type",        col("_val.event_type"))
        .withColumn("error_code",        col("_val.error_code"))
        .withColumn("audit_topic",       col("_val.audit_topic"))
        .withColumn("raw_payload",       col("_val.raw_payload"))
        .withColumn("published_flag",    col("_val.published_flag"))
        .withColumn("last_published_at", col("_val.last_published_at"))
        .withColumn("published_by",      col("_val.published_by"))
        .withColumn("created_at",        col("_val.created_at"))
        .withColumn("created_by",        col("_val.created_by"))
        .withColumn("updated_at",        col("_val.updated_at"))
        .withColumn("updated_by",        col("_val.updated_by"))
        .withColumn("__deleted",         col("_val.__deleted"))
        # Avro key field
        .withColumn("key_outbox_id",     col("_key_outbox_id"))
        # Debezium headers
        .withColumn("hdr_op",            get_json_object(col("headers_json"), "$[?(@.key=='__op')].value"))
        .withColumn("hdr_ts_ms",         get_json_object(col("headers_json"), "$[?(@.key=='__ts_ms')].value").cast(LongType()))
        .withColumn("hdr_source_table",  get_json_object(col("headers_json"), "$[?(@.key=='__table')].value"))
        .withColumn("hdr_source_schema", get_json_object(col("headers_json"), "$[?(@.key=='__schema')].value"))
        # Audit
        .withColumn("_bronze_loaded_at", current_timestamp())
        .drop("_val", "_key_outbox_id", "key_bytes", "value_bytes",
              "headers_json", "kafka_timestamp_type", "_ingested_at")
    )

    (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=cfg["streaming"]["trigger_interval"])
        .toTable(bronze_table)
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
