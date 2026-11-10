"""Central configuration, read from environment variables.

Every service (generator, auditor, API, dashboard) calls ``Settings.from_env()``
instead of hardcoding hosts or passwords. Docker Compose injects the values
from the ``.env`` file, so secrets never live in the source code.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


class ConfigError(ValueError):
    """Raised when a required environment variable is missing or invalid."""


def _get_str(name: str, default: Optional[str] = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and (value is None or value == ""):
        raise ConfigError(f"Missing required environment variable: {name}")
    return value if value is not None else ""


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"Environment variable {name} must be an integer, got {raw!r}") from exc


def _get_float(name: str, default: float, low: float = 0.0, high: float = 1.0e9) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ConfigError(f"Environment variable {name} must be a number, got {raw!r}") from exc
    if not low <= value <= high:
        raise ConfigError(f"Environment variable {name}={value} is outside [{low}, {high}]")
    return value


@dataclass(frozen=True)
class PostgresSettings:
    host: str = "localhost"
    port: int = 5432
    database: str = "adpulse"
    user: str = "adpulse"
    password: str = field(default="", repr=False)  # repr=False: never printed in logs

    @property
    def conninfo(self) -> str:
        """libpq connection string used by psycopg."""
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password}"
        )


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str = "localhost:9094"
    events_topic: str = "ad-events"
    dlq_topic: str = "ad-events-dlq"
    partitions: int = 3


@dataclass(frozen=True)
class S3Settings:
    endpoint_url: str = "http://localhost:8333"
    access_key: str = field(default="", repr=False)
    secret_key: str = field(default="", repr=False)
    bucket: str = "adpulse-lake"
    region: str = "us-east-1"


@dataclass(frozen=True)
class GeneratorSettings:
    events_per_second: float = 40.0
    duration_seconds: int = 0  # 0 = run forever
    duplicate_rate: float = 0.02
    invalid_rate: float = 0.01
    late_event_rate: float = 0.03
    bot_burst_probability: float = 0.01
    seed: Optional[int] = None


@dataclass(frozen=True)
class Settings:
    postgres: PostgresSettings
    kafka: KafkaSettings
    s3: S3Settings
    generator: GeneratorSettings
    api_base_url: str = "http://localhost:8000"
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "Settings":
        """Build settings from environment variables (with local-friendly defaults)."""
        seed_raw = os.getenv("GENERATOR_SEED", "")
        return cls(
            postgres=PostgresSettings(
                host=_get_str("POSTGRES_HOST", "localhost"),
                port=_get_int("POSTGRES_PORT", 5432),
                database=_get_str("POSTGRES_DB", "adpulse"),
                user=_get_str("POSTGRES_USER", "adpulse"),
                password=_get_str("POSTGRES_PASSWORD", ""),
            ),
            kafka=KafkaSettings(
                bootstrap_servers=_get_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"),
                events_topic=_get_str("KAFKA_EVENTS_TOPIC", "ad-events"),
                dlq_topic=_get_str("KAFKA_DLQ_TOPIC", "ad-events-dlq"),
                partitions=_get_int("KAFKA_TOPIC_PARTITIONS", 3),
            ),
            s3=S3Settings(
                endpoint_url=_get_str("S3_ENDPOINT_URL", "http://localhost:8333"),
                access_key=_get_str("S3_ACCESS_KEY", ""),
                secret_key=_get_str("S3_SECRET_KEY", ""),
                bucket=_get_str("S3_BUCKET", "adpulse-lake"),
                region=_get_str("S3_REGION", "us-east-1"),
            ),
            generator=GeneratorSettings(
                events_per_second=_get_float("EVENTS_PER_SECOND", 40.0, 0.1, 100_000),
                duration_seconds=_get_int("GENERATOR_DURATION_SECONDS", 0),
                duplicate_rate=_get_float("DUPLICATE_RATE", 0.02, 0.0, 1.0),
                invalid_rate=_get_float("INVALID_RATE", 0.01, 0.0, 1.0),
                late_event_rate=_get_float("LATE_EVENT_RATE", 0.03, 0.0, 1.0),
                bot_burst_probability=_get_float("BOT_BURST_PROBABILITY", 0.01, 0.0, 1.0),
                seed=int(seed_raw) if seed_raw.strip() else None,
            ),
            api_base_url=_get_str("API_BASE_URL", "http://localhost:8000"),
            log_level=_get_str("LOG_LEVEL", "INFO").upper(),
        )
