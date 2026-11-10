"""Unit tests for the PySpark business logic (run inside the Spark container):

    docker compose run --rm --no-deps spark-streaming python3 -m pytest tests/spark -q
"""

import json
import time
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from adpulse_spark.batch_logic import compute_rankings
from adpulse_spark.transformations import (
    aggregate_metrics,
    deduplicate,
    detect_duplicates,
    detect_suspicious_clicks,
    parse_events,
    split_valid_invalid,
    validate_and_enrich,
    with_event_time_watermark,
)

pytestmark = pytest.mark.spark


def ev(event_id, event_type="impression", t="2026-11-10T10:00:10.000Z", campaign="cmp_001",
       publisher="pub_01", user="u_1", ip="10.0.0.1", cost=0.002, revenue=0.0, **extra):
    d = {"event_id": event_id, "event_type": event_type, "event_time": t, "campaign_id": campaign,
         "publisher_id": publisher, "user_id": user, "device_type": "mobile", "country": "IL",
         "ip_address": ip, "cost_usd": cost, "revenue_usd": revenue}
    d.update(extra)
    return json.dumps(d)


def to_df(spark, values):
    return spark.createDataFrame([(v,) for v in values], ["value"])


def pipeline(spark, dims, values):
    campaigns, publishers = dims
    return split_valid_invalid(validate_and_enrich(parse_events(to_df(spark, values)), campaigns, publishers))


def test_parse_typed_columns(spark):
    row = parse_events(to_df(spark, [ev("e1", cost=0.5)])).first()
    assert row.event_id == "e1"
    assert row.cost_usd == 0.5
    assert str(row.event_ts) == "2026-11-10 10:00:10"


def test_validation_rules(spark, dims):
    bad_missing = json.loads(ev("e3"))
    del bad_missing["user_id"]
    values = [
        ev("e1"),
        '{"event_id": "e2", "event_type": "cli',          # malformed JSON
        json.dumps(bad_missing),                          # missing field
        ev("e4", event_type="hover"),                    # bad type
        ev("e5", t="not-a-time"),                         # bad time
        ev("e6", t="2099-01-01T00:00:00Z"),               # future
        ev("e7", cost=-3.0),                              # negative money
        ev("e8", campaign="cmp_999"),                     # unknown campaign (join)
        ev("e9", publisher="pub_99"),                     # unknown publisher (join)
    ]
    valid, invalid = pipeline(spark, dims, values)
    assert [r.event_id for r in valid.collect()] == ["e1"]
    errors = sorted(r.validation_error for r in invalid.collect())
    assert errors == sorted([
        "malformed_json", "missing_field:user_id", "invalid_event_type", "invalid_event_time",
        "event_time_in_future", "negative_amount", "unknown_campaign", "unknown_publisher",
    ])


def test_enrichment_join_adds_names(spark, dims):
    valid, _ = pipeline(spark, dims, [ev("e1")])
    row = valid.first()
    assert row.campaign_name == "Summer Sneakers Sale"
    assert row.publisher_name == "DailyNews Hub"


def test_dedup_and_duplicate_detection(spark, dims):
    valid, _ = pipeline(spark, dims, [ev("e1"), ev("e1"), ev("e1"), ev("e2")])
    wm = with_event_time_watermark(valid, 10)
    assert sorted(r.event_id for r in deduplicate(wm).collect()) == ["e1", "e2"]
    dups = detect_duplicates(wm).collect()
    assert [(d.event_id, d.occurrences) for d in dups] == [("e1", 3)]


def test_aggregate_metrics_per_window(spark, dims):
    values = [ev(f"i{i}", "impression", t="2026-11-10T10:00:05Z") for i in range(10)]
    values += [ev("c1", "click", t="2026-11-10T10:00:30Z", cost=0.45),
               ev("c2", "click", t="2026-11-10T10:00:40Z", cost=0.45),
               ev("v1", "conversion", t="2026-11-10T10:00:50Z", cost=0.0, revenue=80.0),
               ev("i_next", "impression", t="2026-11-10T10:01:05Z")]
    valid, _ = pipeline(spark, dims, values)
    rows = {str(r.window_start): r for r in aggregate_metrics(valid).collect()}
    first = rows["2026-11-10 10:00:00"]
    assert (first.impressions, first.clicks, first.conversions) == (10, 2, 1)
    assert first.revenue_usd == 80.0
    assert rows["2026-11-10 10:01:00"].impressions == 1


def test_suspicious_clicks(spark, dims):
    bot = [ev(f"b{i}", "click", t="2026-11-10T10:00:1%dZ" % (i % 10), user="bot_007", ip="10.9.9.9")
           for i in range(12)]
    human = [ev("h1", "click", user="u_2"), ev("h2", "click", user="u_2")]
    valid, _ = pipeline(spark, dims, bot + human)
    flagged = detect_suspicious_clicks(valid, threshold=8).collect()
    assert len(flagged) == 1
    assert flagged[0].user_id == "bot_007" and flagged[0].click_count == 12


def test_rankings_window_function(spark):
    events = spark.createDataFrame(
        [("cmp_001", "pub_01", "impression")] * 100 + [("cmp_001", "pub_01", "click")] * 5
        + [("cmp_001", "pub_02", "impression")] * 50 + [("cmp_001", "pub_02", "click")] * 9,
        ["campaign_id", "publisher_id", "event_type"],
    )
    rows = {r.publisher_id: r for r in compute_rankings(events).collect()}
    assert rows["pub_02"].rank_in_campaign == 1 and rows["pub_01"].rank_in_campaign == 2
    assert rows["pub_02"].ctr_pct == 18.0


def test_streaming_watermark_drops_too_late_events(spark, dims, tmp_path: Path):
    """A real streaming query: late-but-within-watermark events update the window,
    events older than the watermark are ignored."""
    src = tmp_path / "in"
    src.mkdir()
    campaigns, publishers = dims
    stream = spark.readStream.option("maxFilesPerTrigger", 1).text(str(src))
    valid, _ = split_valid_invalid(validate_and_enrich(parse_events(stream), campaigns, publishers))
    agg = aggregate_metrics(deduplicate(with_event_time_watermark(valid, 10)))
    query = (agg.writeStream.format("memory").queryName("wm_test").outputMode("update")
             .option("checkpointLocation", str(tmp_path / "ckpt")).start())
    try:
        # Batch 1: events at 10:00 and 10:20  -> watermark becomes 10:20 - 10 min = 10:10
        (src / "1.json").write_text("\n".join([ev("a", t="2026-11-10T10:00:10Z"),
                                               ev("b", t="2026-11-10T10:20:10Z")]))
        query.processAllAvailable()
        # Batch 2: one event at 10:00 (too late: older than 10:10) and one at 10:15 (late but OK)
        time.sleep(1)
        (src / "2.json").write_text("\n".join([ev("late_drop", t="2026-11-10T10:00:20Z"),
                                               ev("late_ok", t="2026-11-10T10:15:00Z")]))
        query.processAllAvailable()
    finally:
        query.stop()
    out = spark.sql("SELECT window_start, max(impressions) AS imp FROM wm_test GROUP BY window_start")
    result = {str(r.window_start): r.imp for r in out.collect()}
    assert result["2026-11-10 10:00:00"] == 1      # late_drop was NOT counted
    assert result["2026-11-10 10:15:00"] == 1      # late_ok WAS counted
    assert result["2026-11-10 10:20:00"] == 1
