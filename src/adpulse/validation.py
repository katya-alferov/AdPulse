"""Pure-Python validation of raw events.

Used by the Kafka auditor (consumer) to route bad messages to the
dead-letter queue. Spark has the *same rules* implemented with DataFrame
expressions in ``adpulse_spark/transformations.py``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from adpulse.models import REQUIRED_FIELDS, EventType

MAX_FUTURE_SKEW = timedelta(minutes=5)
VALID_EVENT_TYPES = {e.value for e in EventType}


@dataclass(frozen=True)
class ValidationResult:
    is_valid: bool
    error: Optional[str] = None
    event: Optional[dict[str, Any]] = None


def parse_event_time(value: Any) -> Optional[datetime]:
    """Parse an ISO-8601 timestamp; return None if it is not a valid time."""
    if not isinstance(value, str) or not value:
        return None
    try:
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def validate_event(raw: bytes | str | dict[str, Any], now: Optional[datetime] = None) -> ValidationResult:
    """Check one event. Returns the first problem found (or is_valid=True).

    Rules (each protects the metrics from garbage):
      1. Must be valid JSON object        -> malformed_json
      2. Required fields present/non-empty -> missing_field:<name>
      3. event_type in the known set       -> invalid_event_type
      4. event_time parseable              -> invalid_event_time
      5. event_time not in the future      -> event_time_in_future
      6. money fields not negative         -> negative_amount
    """
    now = now or datetime.now(timezone.utc)

    if isinstance(raw, (bytes, str)):
        try:
            event = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return ValidationResult(False, "malformed_json")
    else:
        event = raw

    if not isinstance(event, dict):
        return ValidationResult(False, "malformed_json")

    for name in REQUIRED_FIELDS:
        if event.get(name) in (None, ""):
            return ValidationResult(False, f"missing_field:{name}", event)

    if event["event_type"] not in VALID_EVENT_TYPES:
        return ValidationResult(False, "invalid_event_type", event)

    ts = parse_event_time(event["event_time"])
    if ts is None:
        return ValidationResult(False, "invalid_event_time", event)
    if ts > now + MAX_FUTURE_SKEW:
        return ValidationResult(False, "event_time_in_future", event)

    for money in ("cost_usd", "revenue_usd"):
        value = event.get(money, 0) or 0
        if not isinstance(value, (int, float)) or value < 0:
            return ValidationResult(False, "negative_amount", event)

    return ValidationResult(True, None, event)
