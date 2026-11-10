"""The data model of an advertising event.

One JSON message in Kafka == one ``AdEvent``. The same field names are used
by the Spark schema (``spark_jobs/adpulse_spark/schemas.py``) and by the
PostgreSQL tables, so the whole pipeline speaks one language.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


class EventType(str, Enum):
    """The three steps of the advertising funnel."""

    IMPRESSION = "impression"  # the ad was shown
    CLICK = "click"  # the user clicked the ad
    CONVERSION = "conversion"  # the user bought / signed up after clicking


class DeviceType(str, Enum):
    MOBILE = "mobile"
    DESKTOP = "desktop"
    TABLET = "tablet"


REQUIRED_FIELDS: tuple[str, ...] = (
    "event_id",
    "event_type",
    "event_time",
    "campaign_id",
    "publisher_id",
    "user_id",
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(ts: datetime) -> str:
    """ISO-8601 UTC string with milliseconds, e.g. 2026-11-10T21:00:00.123Z"""
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


@dataclass
class AdEvent:
    """A single synthetic advertising event (no real personal data)."""

    event_id: str
    event_type: str
    event_time: str  # when it happened on the user's device (event time)
    campaign_id: str
    publisher_id: str
    user_id: str
    device_type: str
    country: str
    ip_address: str
    cost_usd: float = 0.0  # what the advertiser pays for this event
    revenue_usd: float = 0.0  # value of a conversion for the advertiser
    parent_event_id: Optional[str] = None  # click -> impression, conversion -> click
    sent_at: Optional[str] = None  # when the producer sent it (processing side)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))

    @property
    def kafka_key(self) -> str:
        """Kafka message key: all events of one user go to the same partition,
        so they stay in order relative to each other."""
        return self.user_id
