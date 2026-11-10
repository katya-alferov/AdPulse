import json

import pytest

from adpulse.dashboard.demo_source import DemoDataSource


@pytest.fixture
def demo_dir(tmp_path):
    files = {
        "summary": {"impressions": 10, "clicks": 1},
        "metadata": {"exported_at": "2026-11-10T12:00:00+00:00"},
        "campaigns": [{"campaign_id": "cmp_001"}],
        "publishers": [], "suspicious": [{"user_id": f"bot_{i}"} for i in range(5)], "duplicates": [],
        "invalid": [], "rankings": [],
        "timeseries": [{"window_start": f"2026-11-10T11:{m:02d}:00Z", "impressions": m} for m in range(60)],
        "timeseries_by_campaign": {"cmp_001": [{"window_start": "2026-11-10T11:59:00Z", "impressions": 1}]},
    }
    for name, data in files.items():
        (tmp_path / f"{name}.json").write_text(json.dumps(data))
    return tmp_path


def test_reads_snapshot(demo_dir):
    src = DemoDataSource(demo_dir)
    assert src.summary()["impressions"] == 10
    assert len(src.suspicious(limit=2)) == 2


def test_timeseries_window_relative_to_latest(demo_dir):
    src = DemoDataSource(demo_dir)
    assert len(src.timeseries(minutes=15)) == 16  # 11:44 .. 11:59 inclusive
    assert src.timeseries(60, "cmp_001")[0]["impressions"] == 1
    assert src.timeseries(60, "cmp_404") == []


def test_missing_snapshot_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        DemoDataSource(tmp_path)
