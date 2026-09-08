import json
import os
from pathlib import Path


def _load_env_file(env_file: Path) -> None:
    """Load key=value pairs from a .env file into os.environ (no-op if file absent)."""
    if not env_file.exists():
        return
    with env_file.open() as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


def _get_secret(key: str) -> str:
    """
    Resolution order:
      1. Databricks Secret Scope  (production / cluster runtime)
      2. Environment variable     (local dev via conf/<env>.env)
    """
    try:
        from pyspark.dbutils import DBUtils  # noqa: PLC0415
        from pyspark.sql import SparkSession  # noqa: PLC0415

        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        return dbutils.secrets.get(scope="kafka-secrets", key=key)
    except Exception:
        value = os.environ.get(key)
        if not value:
            raise EnvironmentError(
                f"Secret '{key}' not found in Databricks Secret Scope or environment. "
                f"Copy conf/dev.env.example to conf/dev.env and fill in real values."
            )
        return value


def load_config(environment: str = "dev") -> dict:
    """
    Returns a merged config dict with all Kafka + Schema Registry settings.
    Secrets are never stored in the returned dict's source files.
    """
    project_root = Path(__file__).resolve().parents[2]

    # Load .env file for local dev (gitignored, never committed)
    _load_env_file(project_root / "conf" / f"{environment}.env")

    # Load non-secret config from conf/kafka_config.json
    with open(project_root / "conf" / "kafka_config.json") as f:
        cfg = json.load(f)

    # Inject secrets at runtime
    cfg["kafka"]["sasl_username"] = _get_secret("KAFKA_SASL_USERNAME")
    cfg["kafka"]["sasl_password"] = _get_secret("KAFKA_SASL_PASSWORD")
    cfg["schema_registry"]["api_key"] = _get_secret("SR_API_KEY")
    cfg["schema_registry"]["secret"] = _get_secret("SR_SECRET")

    return cfg


def build_kafka_options(cfg: dict) -> dict:
    """Build the Spark readStream kafka options dict from config."""
    kafka = cfg["kafka"]
    sr = cfg["schema_registry"]
    return {
        "kafka.bootstrap.servers": kafka["bootstrap_servers"],
        "subscribe": kafka["topic"],
        "startingOffsets": "earliest",
        "kafka.security.protocol": kafka["security_protocol"],
        "kafka.sasl.mechanism": kafka["sasl_mechanisms"],
        "kafka.sasl.jaas.config": (
            "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{kafka["sasl_username"]}" '
            f'password="{kafka["sasl_password"]}";'
        ),
        "includeHeaders": "true",
    }
