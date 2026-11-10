"""Spark-side configuration from environment variables (no secrets in code)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class SparkJobSettings:
    kafka_bootstrap: str = "kafka:9092"
    events_topic: str = "ad-events"
    s3_endpoint: str = "http://seaweedfs:8333"
    s3_access_key: str = field(default="", repr=False)
    s3_secret_key: str = field(default="", repr=False)
    s3_bucket: str = "adpulse-lake"
    pg_host: str = "postgres"
    pg_port: int = 5432
    pg_db: str = "adpulse"
    pg_user: str = "adpulse"
    pg_password: str = field(default="", repr=False)
    trigger_seconds: int = 20
    watermark_minutes: int = 10
    suspicious_click_threshold: int = 8
    checkpoint_dir: str = "/opt/adpulse/checkpoints"
    max_offsets_per_trigger: int = 20000

    @classmethod
    def from_env(cls) -> "SparkJobSettings":
        env = os.getenv
        return cls(
            kafka_bootstrap=env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            events_topic=env("KAFKA_EVENTS_TOPIC", "ad-events"),
            s3_endpoint=env("S3_ENDPOINT_URL", "http://seaweedfs:8333"),
            s3_access_key=env("S3_ACCESS_KEY", ""),
            s3_secret_key=env("S3_SECRET_KEY", ""),
            s3_bucket=env("S3_BUCKET", "adpulse-lake"),
            pg_host=env("POSTGRES_HOST", "postgres"),
            pg_port=int(env("POSTGRES_PORT", "5432")),
            pg_db=env("POSTGRES_DB", "adpulse"),
            pg_user=env("POSTGRES_USER", "adpulse"),
            pg_password=env("POSTGRES_PASSWORD", ""),
            trigger_seconds=int(env("SPARK_TRIGGER_SECONDS", "20")),
            watermark_minutes=int(env("SPARK_WATERMARK_MINUTES", "10")),
            suspicious_click_threshold=int(env("SUSPICIOUS_CLICK_THRESHOLD", "8")),
            checkpoint_dir=env("SPARK_CHECKPOINT_DIR", "/opt/adpulse/checkpoints"),
        )

    @property
    def lake_path(self) -> str:
        return f"s3a://{self.s3_bucket}"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_db}"

    @property
    def jdbc_properties(self) -> dict[str, str]:
        return {"user": self.pg_user, "password": self.pg_password, "driver": "org.postgresql.Driver"}
