"""API tests with a fake repository (no database needed)."""

from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from adpulse.api.main import app, get_repository

NOW = datetime(2026, 11, 10, 12, 0, tzinfo=timezone.utc)
CAMPAIGN = {"campaign_id": "cmp_001", "campaign_name": "Summer Sneakers Sale", "advertiser": "StrideWear",
            "category": "fashion", "impressions": 1000, "clicks": 30, "conversions": 3, "spend_usd": 13.5,
            "revenue_usd": 255.0, "ctr_pct": 3.0, "cvr_pct": 10.0}


class FakeRepo:
    healthy = True

    def ping(self):
        return self.healthy

    def summary(self):
        return {"impressions": 1000, "clicks": 30, "conversions": 3, "spend_usd": 13.5, "revenue_usd": 255.0,
                "ctr_pct": 3.0, "conversion_rate_pct": 10.0, "suspicious_users": 1, "duplicate_events": 2,
                "invalid_events": 4, "last_update": NOW}

    def campaigns(self):
        return [CAMPAIGN]

    def campaign(self, campaign_id):
        return CAMPAIGN if campaign_id == "cmp_001" else None

    def publishers(self):
        return [{"publisher_id": "pub_01", "publisher_name": "DailyNews Hub", "site_category": "news",
                 "country": "US", "impressions": 1000, "clicks": 30, "conversions": 3, "spend_usd": 13.5,
                 "ctr_pct": 3.0, "cvr_pct": 10.0}]

    def timeseries(self, minutes, campaign_id=None):
        self.last_ts = (minutes, campaign_id)
        return [{"window_start": NOW, "impressions": 100, "clicks": 3, "conversions": 0, "ctr_pct": 3.0}]

    def suspicious(self, limit):
        return [{"window_start": NOW, "window_end": NOW, "user_id": "bot_001", "ip_address": "10.0.0.1",
                 "campaign_id": "cmp_001", "campaign_name": "Summer Sneakers Sale", "click_count": 15,
                 "reason": "click_burst>=8/min"}]

    def duplicates(self, limit):
        return []

    def invalid_summary(self):
        return [{"validation_error": "malformed_json", "event_count": 4}]

    def rankings(self):
        return []


@pytest.fixture
def repo():
    fake = FakeRepo()
    app.dependency_overrides[get_repository] = lambda: fake
    yield fake
    app.dependency_overrides.clear()


@pytest.fixture
def client(repo):
    return TestClient(app)


def test_health(client):
    assert client.get("/health").json() == {"status": "ok", "database": True}


def test_health_db_down(client, repo):
    repo.healthy = False
    assert client.get("/health").status_code == 503


def test_summary(client):
    body = client.get("/api/v1/summary").json()
    assert body["ctr_pct"] == 3.0 and body["impressions"] == 1000


def test_campaign_found_and_not_found(client):
    assert client.get("/api/v1/campaigns/cmp_001").json()["campaign_name"] == "Summer Sneakers Sale"
    assert client.get("/api/v1/campaigns/cmp_404").status_code == 404


def test_timeseries_params_validated(client, repo):
    assert client.get("/api/v1/timeseries?minutes=30&campaign_id=cmp_001").status_code == 200
    assert repo.last_ts == (30, "cmp_001")
    assert client.get("/api/v1/timeseries?minutes=0").status_code == 422  # below minimum


def test_lists(client):
    assert client.get("/api/v1/campaigns").json()[0]["campaign_id"] == "cmp_001"
    assert client.get("/api/v1/publishers").json()[0]["publisher_id"] == "pub_01"
    assert client.get("/api/v1/suspicious").json()[0]["user_id"] == "bot_001"
    assert client.get("/api/v1/invalid").json()[0]["event_count"] == 4
    assert client.get("/api/v1/duplicates").json() == []
    assert client.get("/api/v1/rankings").json() == []


def test_openapi_docs_available(client):
    assert "/api/v1/summary" in client.get("/openapi.json").json()["paths"]
