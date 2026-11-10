import json

from adpulse.config import Settings
from adpulse.kafka_io.consumer import EventAuditor, build_dlq_record
from adpulse.kafka_io.producer import AdEventProducer, serialize
from adpulse.models import AdEvent


def sample_event():
    return AdEvent("e1", "click", "2026-11-10T10:00:00.000Z", "cmp_001", "pub_01", "u_000042",
                   "mobile", "IL", "10.0.0.1", cost_usd=0.45)


class FakeProducer:
    def __init__(self, config=None):
        self.config = config
        self.sent = []

    def produce(self, topic, key=None, value=None, on_delivery=None):
        self.sent.append((topic, key, value))
        if on_delivery:
            on_delivery(None, None)

    def poll(self, timeout):
        return 0

    def flush(self, timeout=None):
        return 0


def test_serialize_uses_user_id_as_key():
    key, value = serialize(sample_event())
    assert key == b"u_000042"
    assert json.loads(value)["event_id"] == "e1"


def test_serialize_broken_string_has_no_key():
    key, value = serialize('{"broken"')
    assert key is None and value == b'{"broken"'


def test_producer_safe_config_and_delivery_count():
    fake = FakeProducer()
    producer = AdEventProducer("broker:9092", "ad-events", producer_factory=lambda cfg: fake.__class__(cfg))
    assert producer._producer.config["acks"] == "all"
    assert producer._producer.config["enable.idempotence"] is True
    producer.send(sample_event())
    assert producer.delivered == 1
    assert producer._producer.sent[0][0] == "ad-events"


def test_auditor_routes_invalid_to_dlq():
    dlq = FakeProducer()
    auditor = EventAuditor(Settings.from_env(), consumer=object(), dlq_producer=dlq)
    assert auditor.handle(sample_event().to_json().encode(), partition=1, offset=10) is True
    assert auditor.handle(b'{"oops"', partition=2, offset=11) is False
    assert auditor.stats.valid == 1 and auditor.stats.invalid == 1
    topic, _, value = dlq.sent[0]
    record = json.loads(value)
    assert topic == "ad-events-dlq"
    assert record["error"] == "malformed_json"
    assert record["source_partition"] == 2 and record["source_offset"] == 11


def test_dlq_record_keeps_raw_text():
    record = json.loads(build_dlq_record(b"\xffbad", "malformed_json", 0, 5))
    assert record["raw_value"].endswith("bad")
