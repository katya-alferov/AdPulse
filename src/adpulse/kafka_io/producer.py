"""Kafka producer that publishes ad events as JSON."""

from __future__ import annotations

import json
import logging
from typing import Any, Callable, Optional, Union

from confluent_kafka import KafkaError, Message, Producer

from adpulse.models import AdEvent

log = logging.getLogger("adpulse.kafka.producer")

Payload = Union[AdEvent, dict, str]


def serialize(payload: Payload) -> tuple[Optional[bytes], bytes]:
    """Turn a payload into (key, value) bytes.

    * AdEvent  -> key = user_id, value = JSON
    * dict     -> key = user_id if present, value = JSON
    * str      -> raw (possibly broken) text, no key
    """
    if isinstance(payload, AdEvent):
        return payload.kafka_key.encode(), payload.to_json().encode()
    if isinstance(payload, dict):
        key = payload.get("user_id")
        return (str(key).encode() if key else None), json.dumps(payload, separators=(",", ":")).encode()
    return None, payload.encode()


class AdEventProducer:
    """Thin wrapper around confluent_kafka.Producer with safe defaults.

    acks=all + enable.idempotence=true: the broker confirms only after the
    message is safely written, and retries never create duplicates *inside Kafka*.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        producer_factory: Callable[[dict[str, Any]], Any] = Producer,
    ) -> None:
        self.topic = topic
        self.delivered = 0
        self.failed = 0
        self._producer = producer_factory(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "adpulse-generator",
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 20,  # wait up to 20ms to send messages in batches
                "compression.type": "lz4",
                "retries": 10,
            }
        )

    def _on_delivery(self, err: Optional[KafkaError], msg: Message) -> None:
        """Called by the Kafka library (from poll/flush) for every message."""
        if err is not None:
            self.failed += 1
            log.error("Delivery failed: %s", err)
        else:
            self.delivered += 1

    def send(self, payload: Payload) -> None:
        key, value = serialize(payload)
        while True:
            try:
                self._producer.produce(self.topic, key=key, value=value, on_delivery=self._on_delivery)
                break
            except BufferError:
                # Local queue is full: let the library send what it has, then retry.
                self._producer.poll(0.5)
        self._producer.poll(0)  # serve delivery callbacks without blocking

    def flush(self, timeout: float = 30.0) -> int:
        """Block until all queued messages are delivered. Returns messages still in queue."""
        return self._producer.flush(timeout)
