"""Creates realistic synthetic ad events, including the "dirty" ones.

Real ad traffic is messy, so the factory deliberately injects:
  * duplicates      - the same event sent twice (network retries do this)
  * invalid events  - missing fields / bad types / malformed JSON
  * late events     - event_time minutes in the past (phone was offline)
  * bot bursts      - one "user" clicking many times in a few seconds (fraud)

This lets us prove that the Spark pipeline really handles each case.
"""

from __future__ import annotations

import random
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Deque, Iterator, Optional, Union

from adpulse.config import GeneratorSettings
from adpulse.generator.catalog import (
    CAMPAIGNS,
    CAMPAIGNS_BY_ID,
    COUNTRIES,
    COUNTRY_WEIGHTS,
    PUBLISHERS,
)
from adpulse.models import AdEvent, DeviceType, EventType, to_iso, utc_now

# A generated item is either a proper AdEvent or a raw (possibly broken) payload.
Payload = Union[AdEvent, dict, str]


@dataclass
class GeneratorStats:
    impressions: int = 0
    clicks: int = 0
    conversions: int = 0
    duplicates: int = 0
    invalid: int = 0
    late: int = 0
    bot_clicks: int = 0

    @property
    def total(self) -> int:
        return self.impressions + self.clicks + self.conversions + self.duplicates + self.invalid + self.bot_clicks


@dataclass
class EventFactory:
    """Stateful factory: remembers recent impressions/clicks so that clicks
    point to a real impression and conversions point to a real click."""

    settings: GeneratorSettings
    user_pool_size: int = 5000
    stats: GeneratorStats = field(default_factory=GeneratorStats)

    def __post_init__(self) -> None:
        self.rng = random.Random(self.settings.seed)
        self._recent_impressions: Deque[AdEvent] = deque(maxlen=2000)
        self._recent_clicks: Deque[AdEvent] = deque(maxlen=500)
        self._recent_events: Deque[AdEvent] = deque(maxlen=200)
        self._pub_weights = [p.traffic_weight for p in PUBLISHERS]

    # ------------------------------------------------------------------ helpers
    def _uuid(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def _user_id(self) -> str:
        return f"u_{self.rng.randint(1, self.user_pool_size):06d}"

    def _ip(self) -> str:
        # 10.x.x.x is a private range: guaranteed not to be a real person's IP.
        return f"10.{self.rng.randint(0, 255)}.{self.rng.randint(0, 255)}.{self.rng.randint(1, 254)}"

    def _device(self) -> str:
        return self.rng.choices(
            [DeviceType.MOBILE.value, DeviceType.DESKTOP.value, DeviceType.TABLET.value], [0.62, 0.30, 0.08]
        )[0]

    def _event_time(self, now: datetime) -> str:
        """Usually 'now' (+ a little network delay). Sometimes late."""
        if self.rng.random() < self.settings.late_event_rate:
            self.stats.late += 1
            # 1-14 minutes late: some inside the 10-minute watermark, some outside.
            return to_iso(now - timedelta(seconds=self.rng.randint(60, 14 * 60)))
        return to_iso(now - timedelta(milliseconds=self.rng.randint(0, 1500)))

    # ------------------------------------------------------------- event types
    def make_impression(self, now: Optional[datetime] = None) -> AdEvent:
        now = now or utc_now()
        campaign = self.rng.choice(CAMPAIGNS)
        publisher = self.rng.choices(PUBLISHERS, self._pub_weights)[0]
        event = AdEvent(
            event_id=self._uuid(),
            event_type=EventType.IMPRESSION.value,
            event_time=self._event_time(now),
            campaign_id=campaign.campaign_id,
            publisher_id=publisher.publisher_id,
            user_id=self._user_id(),
            device_type=self._device(),
            country=self.rng.choices(COUNTRIES, COUNTRY_WEIGHTS)[0],
            ip_address=self._ip(),
            cost_usd=round(self.rng.uniform(0.001, 0.004), 4),  # CPM-style cost per view
            sent_at=to_iso(now),
        )
        self._recent_impressions.append(event)
        self.stats.impressions += 1
        return event

    def make_click(self, impression: AdEvent, now: Optional[datetime] = None) -> AdEvent:
        now = now or utc_now()
        campaign = CAMPAIGNS_BY_ID[impression.campaign_id]
        event = AdEvent(
            event_id=self._uuid(),
            event_type=EventType.CLICK.value,
            event_time=self._event_time(now),
            campaign_id=impression.campaign_id,
            publisher_id=impression.publisher_id,
            user_id=impression.user_id,
            device_type=impression.device_type,
            country=impression.country,
            ip_address=impression.ip_address,
            cost_usd=campaign.cpc_usd,
            parent_event_id=impression.event_id,
            sent_at=to_iso(now),
        )
        self._recent_clicks.append(event)
        self.stats.clicks += 1
        return event

    def make_conversion(self, click: AdEvent, now: Optional[datetime] = None) -> AdEvent:
        now = now or utc_now()
        campaign = CAMPAIGNS_BY_ID[click.campaign_id]
        event = AdEvent(
            event_id=self._uuid(),
            event_type=EventType.CONVERSION.value,
            event_time=self._event_time(now),
            campaign_id=click.campaign_id,
            publisher_id=click.publisher_id,
            user_id=click.user_id,
            device_type=click.device_type,
            country=click.country,
            ip_address=click.ip_address,
            revenue_usd=round(campaign.avg_order_value_usd * self.rng.uniform(0.6, 1.5), 2),
            parent_event_id=click.event_id,
            sent_at=to_iso(now),
        )
        self.stats.conversions += 1
        return event

    def make_invalid(self, now: Optional[datetime] = None) -> Payload:
        """Return one broken payload of a random kind."""
        now = now or utc_now()
        self.stats.invalid += 1
        kind = self.rng.choice(["missing_field", "bad_type", "malformed", "future", "negative"])
        base = self.make_impression(now)
        self.stats.impressions -= 1  # it was only a template, not a real impression
        data = base.to_dict()
        if kind == "missing_field":
            data.pop(self.rng.choice(["campaign_id", "user_id", "event_time", "publisher_id"]))
        elif kind == "bad_type":
            data["event_type"] = "hover"
        elif kind == "malformed":
            return '{"event_id": "' + base.event_id + '", "event_type": "click", '  # cut JSON
        elif kind == "future":
            data["event_time"] = to_iso(now + timedelta(hours=3))
        else:
            data["cost_usd"] = -1.0
        return data

    def make_bot_burst(self, now: Optional[datetime] = None) -> list[AdEvent]:
        """One fake user clicks the same campaign 10-25 times in seconds."""
        now = now or utc_now()
        impression = self.make_impression(now)
        bot_user = f"bot_{self.rng.randint(1, 50):03d}"
        impression.user_id = bot_user
        clicks: list[AdEvent] = []
        for _ in range(self.rng.randint(10, 25)):
            click = self.make_click(impression, now)
            click.event_time = to_iso(now - timedelta(milliseconds=self.rng.randint(0, 3000)))
            clicks.append(click)
        self.stats.clicks -= len(clicks)
        self.stats.bot_clicks += len(clicks)
        return [impression, *clicks]

    # --------------------------------------------------------------- main loop
    def next_batch(self, now: Optional[datetime] = None) -> list[Payload]:
        """Produce the events of one "tick": an impression plus whatever
        clicks/conversions/anomalies happen to follow it."""
        now = now or utc_now()
        batch: list[Payload] = []

        impression = self.make_impression(now)
        batch.append(impression)

        campaign = CAMPAIGNS_BY_ID[impression.campaign_id]
        if self.rng.random() < campaign.base_ctr:
            click = self.make_click(impression, now)
            batch.append(click)
            if self.rng.random() < campaign.base_cvr:
                batch.append(self.make_conversion(click, now))

        if self.rng.random() < self.settings.bot_burst_probability:
            batch.extend(self.make_bot_burst(now))

        if self.rng.random() < self.settings.invalid_rate:
            batch.append(self.make_invalid(now))

        for item in list(batch):
            if isinstance(item, AdEvent):
                self._recent_events.append(item)
        if self._recent_events and self.rng.random() < self.settings.duplicate_rate:
            # Re-send an identical copy of a recent event (same event_id!).
            batch.append(self.rng.choice(self._recent_events))
            self.stats.duplicates += 1

        return batch

    def stream(self) -> Iterator[Payload]:
        """Endless iterator of payloads."""
        while True:
            yield from self.next_batch()
