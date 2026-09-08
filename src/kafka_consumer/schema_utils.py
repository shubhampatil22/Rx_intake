import io
import struct

import fastavro
import requests


# Confluent wire-format: [0x00][4-byte big-endian schema_id][avro bytes]
_MAGIC_BYTE = 0
_PREFIX_LEN = 5


def _fetch_schema(sr_url: str, schema_id: int, auth: tuple) -> dict:
    url = f"{sr_url}/schemas/ids/{schema_id}"
    resp = requests.get(url, auth=auth, timeout=10)
    resp.raise_for_status()
    import json
    return json.loads(resp.json()["schema"])


def deserialize(wire_bytes: bytes, sr_url: str, sr_auth: tuple) -> dict:
    """
    Decode a Confluent wire-format byte payload.
    Strips the 5-byte prefix, fetches the schema from Schema Registry by schema_id,
    and returns the decoded record as a Python dict.
    """
    if wire_bytes is None:
        return {}

    if len(wire_bytes) < _PREFIX_LEN:
        raise ValueError(f"Payload too short ({len(wire_bytes)} bytes); expected >= {_PREFIX_LEN}")

    # 1. EXTRACT schema_id from the wire bytes
    magic, schema_id = struct.unpack(">bI", wire_bytes[:_PREFIX_LEN])
    if magic != _MAGIC_BYTE:
        raise ValueError(f"Invalid magic byte: {magic!r}; expected 0x00")

    avro_bytes = wire_bytes[_PREFIX_LEN:]

    # 2. PULL schema from Schema Registry using that schema_id
    schema_dict = _fetch_schema(sr_url, schema_id, sr_auth)

    # 3. MAP / DECODE the remaining bytes using that schema
    parsed_schema = fastavro.parse_schema(schema_dict)

    return fastavro.schemaless_reader(io.BytesIO(avro_bytes), parsed_schema)


