from collections import Counter

from adpulse.config import GeneratorSettings
from adpulse.generator.catalog import CAMPAIGNS_BY_ID, PUBLISHERS_BY_ID
from adpulse.generator.event_factory import EventFactory
from adpulse.models import AdEvent
from adpulse.validation import validate_event


def make_factory(**kw):
    defaults = dict(seed=7, duplicate_rate=0.0, invalid_rate=0.0, late_event_rate=0.0, bot_burst_probability=0.0)
    defaults.update(kw)
    return EventFactory(GeneratorSettings(**defaults))


def generate(factory, ticks):
    out = []
    for _ in range(ticks):
        out.extend(factory.next_batch())
    return out


def test_clean_events_are_all_valid_and_reference_known_dimensions():
    events = generate(make_factory(), 2000)
    assert all(isinstance(e, AdEvent) for e in events)
    for e in events:
        assert validate_event(e.to_dict()).is_valid
        assert e.campaign_id in CAMPAIGNS_BY_ID
        assert e.publisher_id in PUBLISHERS_BY_ID


def test_funnel_shape():
    events = generate(make_factory(), 5000)
    types = Counter(e.event_type for e in events)
    assert types["impression"] == 5000
    assert 0 < types["click"] < types["impression"] * 0.1  # CTR a few %
    assert 0 < types["conversion"] < types["click"]


def test_click_points_to_impression():
    factory = make_factory()
    events = generate(factory, 3000)
    impressions = {e.event_id for e in events if e.event_type == "impression"}
    clicks = [e for e in events if e.event_type == "click"]
    assert clicks and all(c.parent_event_id in impressions for c in clicks)


def test_same_seed_same_data():
    """Same seed + same clock -> identical events (reproducible tests and demos)."""
    from datetime import datetime, timezone
    now = datetime(2026, 11, 10, 12, 0, tzinfo=timezone.utc)
    fa, fb = make_factory(), make_factory()
    a = [e.to_json() for _ in range(200) for e in fa.next_batch(now)]
    b = [e.to_json() for _ in range(200) for e in fb.next_batch(now)]
    assert a == b


def test_duplicates_are_injected():
    events = generate(make_factory(duplicate_rate=0.2), 1000)
    ids = Counter(e.event_id for e in events if isinstance(e, AdEvent))
    assert any(n > 1 for n in ids.values())


def test_invalid_events_fail_validation():
    factory = make_factory(invalid_rate=1.0)
    events = generate(factory, 300)
    bad = [e for e in events if not isinstance(e, AdEvent)]
    assert len(bad) == 300 == factory.stats.invalid
    assert all(not validate_event(b).is_valid for b in bad)


def test_bot_burst_many_clicks_same_user():
    factory = make_factory()
    burst = factory.make_bot_burst()
    clicks = [e for e in burst if e.event_type == "click"]
    assert len(clicks) >= 10
    assert len({c.user_id for c in clicks}) == 1
    assert clicks[0].user_id.startswith("bot_")


def test_late_events_have_old_timestamps():
    factory = make_factory(late_event_rate=1.0)
    e = factory.make_impression()
    sent = e.sent_at
    assert e.event_time < sent
    assert factory.stats.late == 1
