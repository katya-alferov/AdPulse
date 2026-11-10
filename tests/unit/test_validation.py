import json
from datetime import datetime, timedelta, timezone

import pytest

from adpulse.validation import validate_event

NOW = datetime(2026, 11, 10, 12, 0, tzinfo=timezone.utc)


def good_event(**overrides):
    event = {
        "event_id": "e1",
        "event_type": "click",
        "event_time": "2026-11-10T11:59:00.000Z",
        "campaign_id": "cmp_001",
        "publisher_id": "pub_01",
        "user_id": "u_000001",
        "cost_usd": 0.45,
        "revenue_usd": 0.0,
    }
    event.update(overrides)
    return event


def test_valid_event_passes():
    result = validate_event(json.dumps(good_event()), now=NOW)
    assert result.is_valid and result.error is None


def test_bytes_input():
    assert validate_event(json.dumps(good_event()).encode(), now=NOW).is_valid


@pytest.mark.parametrize("field", ["event_id", "event_type", "event_time", "campaign_id", "publisher_id", "user_id"])
def test_missing_required_field(field):
    event = good_event()
    del event[field]
    result = validate_event(event, now=NOW)
    assert not result.is_valid
    assert result.error == f"missing_field:{field}"


def test_empty_string_counts_as_missing():
    assert validate_event(good_event(user_id=""), now=NOW).error == "missing_field:user_id"


def test_malformed_json():
    assert validate_event('{"event_id": "x", ', now=NOW).error == "malformed_json"


def test_json_that_is_not_an_object():
    assert validate_event("[1,2,3]", now=NOW).error == "malformed_json"


def test_unknown_event_type():
    assert validate_event(good_event(event_type="hover"), now=NOW).error == "invalid_event_type"


def test_bad_timestamp():
    assert validate_event(good_event(event_time="yesterday"), now=NOW).error == "invalid_event_time"


def test_future_timestamp_rejected_but_small_skew_allowed():
    far = (NOW + timedelta(hours=1)).isoformat()
    near = (NOW + timedelta(minutes=2)).isoformat()
    assert validate_event(good_event(event_time=far), now=NOW).error == "event_time_in_future"
    assert validate_event(good_event(event_time=near), now=NOW).is_valid


def test_late_event_is_still_valid():
    """Late != invalid. Spark decides with the watermark whether it still counts."""
    old = (NOW - timedelta(minutes=30)).isoformat()
    assert validate_event(good_event(event_time=old), now=NOW).is_valid


def test_negative_amount():
    assert validate_event(good_event(cost_usd=-1), now=NOW).error == "negative_amount"
