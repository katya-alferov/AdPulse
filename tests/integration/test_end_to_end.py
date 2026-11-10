"""End-to-end tests against the RUNNING docker compose stack.

    docker compose up -d --build
    # wait ~2 minutes for the first Spark micro-batches
    pytest -m integration tests/integration -v

They talk to the services through the ports published on localhost.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone

import pytest
import requests

pytestmark = pytest.mark.integration

confluent_kafka = pytest.importorskip("confluent_kafka")
psycopg = pytest.importorskip("psycopg")

from adpulse.config import S3Settings  # noqa: E402
from adpulse.storage.object_store import ObjectStore  # noqa: E402

KAFKA = os.getenv("IT_KAFKA", "localhost:9094")
API = os.getenv("IT_API", "http://localhost:8000")
TIMEOUT_S = int(os.getenv("IT_TIMEOUT", "150"))


def _load_dotenv() -> None:
    """Read .env (if present) so the tests use the same passwords as the stack."""
    path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
    if os.path.exists(path):
        for line in open(path, encoding="utf-8"):
            if "=" in line and not line.lstrip().startswith("#"):
                k, v = line.strip().split("=", 1)
                os.environ.setdefault(k, v)


_load_dotenv()


def pg_query(sql: str, params: tuple = ()) -> list[tuple]:
    conninfo = (f"host=localhost port=5432 dbname={os.environ.get('POSTGRES_DB', 'adpulse')} "
                f"user={os.environ.get('POSTGRES_USER', 'adpulse')} password={os.environ.get('POSTGRES_PASSWORD', '')}")
    with psycopg.connect(conninfo, connect_timeout=5) as conn:
        return conn.execute(sql, params).fetchall()


def wait_for(check, timeout: int = TIMEOUT_S, every: float = 5.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = check()
        if last:
            return last
        time.sleep(every)
    return last


def event(event_type: str, user: str, ip: str, campaign: str = "cmp_002", event_id: str | None = None) -> dict:
    now = datetime.now(timezone.utc)
    return {"event_id": event_id or str(uuid.uuid4()), "event_type": event_type,
            "event_time": now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z", "campaign_id": campaign,
            "publisher_id": "pub_03", "user_id": user, "device_type": "desktop", "country": "IL",
            "ip_address": ip, "cost_usd": 0.7 if event_type == "click" else 0.002, "revenue_usd": 0.0}


@pytest.fixture(scope="module")
def producer():
    p = confluent_kafka.Producer({"bootstrap.servers": KAFKA, "acks": "all"})
    yield p
    p.flush(10)


def send(producer, payload, key: str | None = None):
    value = payload if isinstance(payload, str) else json.dumps(payload)
    producer.produce("ad-events", key=key, value=value.encode())
    producer.flush(10)


def test_api_health():
    assert requests.get(f"{API}/health", timeout=5).json()["status"] == "ok"


def test_kafka_topics_exist():
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": KAFKA})
    topics = admin.list_topics(timeout=10).topics
    assert "ad-events" in topics and "ad-events-dlq" in topics
    assert len(topics["ad-events"].partitions) == 3


def test_generator_data_reaches_postgres_and_api():
    rows = wait_for(lambda: pg_query("SELECT COALESCE(SUM(impressions),0) FROM metrics_minute")[0][0] > 0)
    assert rows, "Spark did not write any metrics to PostgreSQL"
    summary = requests.get(f"{API}/api/v1/summary", timeout=5).json()
    assert summary["impressions"] > 0
    assert 0 < summary["ctr_pct"] < 100


def test_bot_burst_is_flagged(producer):
    user, ip = f"it_bot_{uuid.uuid4().hex[:8]}", "10.250.250.250"
    for _ in range(12):
        send(producer, event("click", user, ip), key=user)
    found = wait_for(lambda: pg_query("SELECT click_count FROM suspicious_activity WHERE user_id = %s", (user,)))
    assert found and found[0][0] >= 12


def test_duplicate_event_detected(producer):
    eid = f"it-dup-{uuid.uuid4()}"
    e = event("impression", "it_user_dup", "10.1.1.1", event_id=eid)
    send(producer, e, key="it_user_dup")
    send(producer, e, key="it_user_dup")
    found = wait_for(lambda: pg_query("SELECT occurrences FROM duplicate_events WHERE event_id = %s", (eid,)))
    assert found and found[0][0] == 2


def test_invalid_event_goes_to_dlq(producer):
    marker = f"it-bad-{uuid.uuid4()}"
    send(producer, '{"event_id": "' + marker + '", "event_type": ')  # malformed JSON
    consumer = confluent_kafka.Consumer({"bootstrap.servers": KAFKA, "group.id": f"it-{uuid.uuid4()}",
                                         "auto.offset.reset": "earliest"})
    consumer.subscribe(["ad-events-dlq"])
    deadline, hit = time.time() + 60, None
    try:
        while time.time() < deadline and not hit:
            msg = consumer.poll(1.0)
            if msg is not None and not msg.error() and marker.encode() in msg.value():
                hit = json.loads(msg.value())
    finally:
        consumer.close()
    assert hit and hit["error"] == "malformed_json"


def test_parquet_files_in_object_storage():
    store = ObjectStore(S3Settings(endpoint_url="http://localhost:8333",
                                   access_key=os.environ.get("S3_ACCESS_KEY", ""),
                                   secret_key=os.environ.get("S3_SECRET_KEY", ""),
                                   bucket=os.environ.get("S3_BUCKET", "adpulse-lake")))
    summary = wait_for(lambda: store.summary("events/") if store.summary("events/")["parquet_files"] else None)
    assert summary and summary["parquet_files"] > 0
