"""Create the Kafka topics AdPulse needs (safe to run many times).

Run:  python -m adpulse.kafka_io.admin
"""

from __future__ import annotations

import logging
import sys
import time

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from adpulse.config import KafkaSettings, Settings
from adpulse.logging_setup import setup_logging

log = logging.getLogger("adpulse.kafka.admin")


def build_topics(settings: KafkaSettings) -> list[NewTopic]:
    """Describe the topics. replication_factor=1 because we have one broker locally
    (in production you would use 3 so a broker can die without data loss)."""
    return [
        NewTopic(
            settings.events_topic,
            num_partitions=settings.partitions,
            replication_factor=1,
            config={"retention.ms": str(24 * 60 * 60 * 1000)},  # keep events 1 day
        ),
        NewTopic(
            settings.dlq_topic,
            num_partitions=1,
            replication_factor=1,
            config={"retention.ms": str(7 * 24 * 60 * 60 * 1000)},  # keep bad events 7 days
        ),
    ]


def ensure_topics(settings: KafkaSettings, retries: int = 30, delay_s: float = 2.0) -> None:
    """Create topics; ignore 'already exists'; retry while the broker starts."""
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    for attempt in range(1, retries + 1):
        try:
            existing = set(admin.list_topics(timeout=10).topics)
            missing = [t for t in build_topics(settings) if t.topic not in existing]
            if not missing:
                log.info("All topics exist: %s, %s", settings.events_topic, settings.dlq_topic)
                return
            futures = admin.create_topics(missing)
            for topic, future in futures.items():
                try:
                    future.result()
                    log.info("Created topic %s", topic)
                except KafkaException as exc:
                    if "TOPIC_ALREADY_EXISTS" in str(exc):
                        log.info("Topic %s already exists", topic)
                    else:
                        raise
            return
        except KafkaException as exc:
            log.warning("Kafka not ready (attempt %d/%d): %s", attempt, retries, exc)
            time.sleep(delay_s)
    raise RuntimeError("Could not create Kafka topics - is Kafka running?")


def main() -> int:
    settings = Settings.from_env()
    setup_logging(settings.log_level)
    ensure_topics(settings.kafka)
    return 0


if __name__ == "__main__":
    sys.exit(main())
