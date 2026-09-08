"""
Unit tests for schema_utils.deserialize().
SR network call is mocked — no cluster or real credentials needed.
Run locally with: pytest tests/test_schema_utils.py -v
"""

import io
import struct
import sys
from pathlib import Path
from unittest.mock import patch

import fastavro
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from kafka_consumer.schema_utils import deserialize

# ── Schemas ──────────────────────────────────────────────────────────────────

KEY_SCHEMA = {
    "type": "record",
    "name": "AuditOutboxKey",
    "namespace": "digital.audit_outbox",
    "fields": [{"name": "outbox_id", "type": "string"}],
}

VALUE_SCHEMA = {
    "type": "record",
    "name": "AuditOutboxValue",
    "namespace": "digital.audit_outbox",
    "fields": [
        {"name": "outbox_id",         "type": "string"},
        {"name": "intake_id",         "type": ["null", "string"],  "default": None},
        {"name": "event_type",        "type": ["null", "string"],  "default": None},
        {"name": "published_flag",    "type": ["null", "boolean"], "default": None},
        {"name": "created_at",        "type": ["null", "long"],    "default": None},
        {"name": "__deleted",         "type": ["null", "string"],  "default": None},
    ],
}


def _make_wire_bytes(schema_dict: dict, record: dict, schema_id: int = 1) -> bytes:
    """Encode a record to Confluent wire format: [0x00][4-byte schema_id][avro bytes]."""
    parsed = fastavro.parse_schema(schema_dict)
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed, record)
    return struct.pack(">bI", 0, schema_id) + buf.getvalue()


def _mock_sr(schema_dict: dict):
    """Patches _fetch_schema to return schema_dict without any network call."""
    return patch("kafka_consumer.schema_utils._fetch_schema", return_value=schema_dict)


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_deserialize_key_basic():
    record = {"outbox_id": "abc-123"}
    wire = _make_wire_bytes(KEY_SCHEMA, record)
    with _mock_sr(KEY_SCHEMA):
        result = deserialize(wire, "https://mock-sr", ("key", "secret"))
    assert result["outbox_id"] == "abc-123"


def test_deserialize_value_all_fields_present():
    record = {
        "outbox_id":      "uuid-001",
        "intake_id":      "uuid-002",
        "event_type":     "INTAKE_RECEIVED",
        "published_flag": False,
        "created_at":     1_700_000_000_000_000,
        "__deleted":      "false",
    }
    wire = _make_wire_bytes(VALUE_SCHEMA, record)
    with _mock_sr(VALUE_SCHEMA):
        result = deserialize(wire, "https://mock-sr", ("key", "secret"))

    assert result["outbox_id"]      == "uuid-001"
    assert result["intake_id"]      == "uuid-002"
    assert result["event_type"]     == "INTAKE_RECEIVED"
    assert result["published_flag"] is False
    assert result["created_at"]     == 1_700_000_000_000_000
    assert result["__deleted"]      == "false"


def test_deserialize_value_nullable_fields_are_none():
    record = {
        "outbox_id":      "uuid-003",
        "intake_id":      None,
        "event_type":     None,
        "published_flag": None,
        "created_at":     None,
        "__deleted":      None,
    }
    wire = _make_wire_bytes(VALUE_SCHEMA, record)
    with _mock_sr(VALUE_SCHEMA):
        result = deserialize(wire, "https://mock-sr", ("key", "secret"))

    assert result["intake_id"]      is None
    assert result["published_flag"] is None


def test_deserialize_returns_empty_dict_for_none():
    result = deserialize(None, "https://mock-sr", ("key", "secret"))
    assert result == {}


def test_deserialize_raises_on_too_short_bytes():
    with pytest.raises(ValueError, match="too short"):
        deserialize(b"\x00\x01", "https://mock-sr", ("key", "secret"))


def test_deserialize_raises_on_bad_magic_byte():
    record = {"outbox_id": "x"}
    parsed = fastavro.parse_schema(KEY_SCHEMA)
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed, record)
    wire = struct.pack(">bI", 1, 1) + buf.getvalue()  # magic=1, invalid

    with pytest.raises(ValueError, match="magic byte"):
        deserialize(wire, "https://mock-sr", ("key", "secret"))
