"""Kafka consumer "auditor" - a classic consumer-group application.

What it demonstrates:
  * consumer groups   - run 1..3 copies; Kafka splits the 3 partitions between them
  * manual offsets    - commit only AFTER a message was handled (at-least-once)
  * error handling    - invalid events go to a dead-letter queue (DLQ) topic
  * graceful shutdown - SIGTERM -> finish current message, commit, close

Run:  python -m adpulse.kafka_io.consumer
"""

from __future__ import annotations

import json
import logging
import signal
import sys
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from adpulse.config import Settings
from adpulse.logging_setup import setup_logging
from adpulse.validation import validate_event

log = logging.getLogger("adpulse.kafka.auditor")


@dataclass
class AuditStats:
    processed: int = 0
    valid: int = 0
    invalid: int = 0
    by_type: Counter = field(default_factory=Counter)
    by_error: Counter = field(default_factory=Counter)
    by_partition: Counter = field(default_factory=Counter)


def build_dlq_record(raw_value: bytes, error: str, partition: int, offset: int) -> bytes:
    """Wrap a bad message with the reason, so someone can investigate later."""
    return json.dumps(
        {
            "error": error,
            "source_partition": partition,
            "source_offset": offset,
            "raw_value": raw_value.decode("utf-8", errors="replace"),
        }
    ).encode()


class EventAuditor:
    def __init__(self, settings: Settings, consumer: Any = None, dlq_producer: Any = None) -> None:
        self.settings = settings
        self.stats = AuditStats()
        self._running = True
        self.consumer = consumer or Consumer(
            {
                "bootstrap.servers": settings.kafka.bootstrap_servers,
                "group.id": "adpulse-auditor",
                "auto.offset.reset": "earliest",  # new group starts from the beginning
                "enable.auto.commit": False,  # we commit ourselves
            }
        )
        self.dlq = dlq_producer or Producer(
            {"bootstrap.servers": settings.kafka.bootstrap_servers, "acks": "all"}
        )

    def stop(self, *_: Any) -> None:
        log.info("Shutdown requested")
        self._running = False

    def handle(self, value: Optional[bytes], partition: int, offset: int) -> bool:
        """Validate one message. Returns True if it was valid."""
        self.stats.processed += 1
        self.stats.by_partition[partition] += 1
        result = validate_event(value or b"")
        if result.is_valid:
            self.stats.valid += 1
            self.stats.by_type[result.event["event_type"]] += 1
            return True
        self.stats.invalid += 1
        self.stats.by_error[result.error] += 1
        self.dlq.produce(
            self.settings.kafka.dlq_topic,
            value=build_dlq_record(value or b"", result.error or "unknown", partition, offset),
        )
        self.dlq.poll(0)
        return False

    def run(self, max_messages: Optional[int] = None) -> AuditStats:
        self.consumer.subscribe(
            [self.settings.kafka.events_topic],
            on_assign=lambda c, parts: log.info("Assigned partitions: %s", [p.partition for p in parts]),
        )
        try:
            while self._running:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())
                self.handle(msg.value(), msg.partition(), msg.offset())
                # Commit after processing: if we crash before this line the
                # message is read again (at-least-once delivery).
                self.consumer.commit(message=msg, asynchronous=True)
                if self.stats.processed % 1000 == 0:
                    log.info(
                        "processed=%d valid=%d invalid=%d partitions=%s errors=%s",
                        self.stats.processed, self.stats.valid, self.stats.invalid,
                        dict(self.stats.by_partition), dict(self.stats.by_error),
                    )
                if max_messages and self.stats.processed >= max_messages:
                    break
        finally:
            self.dlq.flush(10)
            self.consumer.close()  # commits final offsets and leaves the group cleanly
            log.info("Auditor stopped. %s", self.stats)
        return self.stats


def main() -> int:
    settings = Settings.from_env()
    setup_logging(settings.log_level)
    auditor = EventAuditor(settings)
    signal.signal(signal.SIGTERM, auditor.stop)
    signal.signal(signal.SIGINT, auditor.stop)
    auditor.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
