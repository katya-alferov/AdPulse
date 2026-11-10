import pytest
import requests

from adpulse.dashboard.api_client import AdPulseClient, ApiError, format_number


class FakeResponse:
    def __init__(self, payload, status=200):
        self.payload, self.status = payload, status

    def raise_for_status(self):
        if self.status >= 400:
            raise requests.HTTPError(f"{self.status}")

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, response=None, error=None):
        self.response, self.error, self.calls = response, error, []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        if self.error:
            raise self.error
        return self.response


def test_timeseries_builds_url_and_params():
    session = FakeSession(FakeResponse([]))
    AdPulseClient("http://api:8000/", session=session).timeseries(30, "cmp_001")
    assert session.calls == [("http://api:8000/api/v1/timeseries", {"minutes": 30, "campaign_id": "cmp_001"})]


def test_connection_error_becomes_api_error():
    client = AdPulseClient("http://api:8000", session=FakeSession(error=requests.ConnectionError("down")))
    with pytest.raises(ApiError):
        client.summary()


def test_http_error_becomes_api_error():
    client = AdPulseClient("http://api:8000", session=FakeSession(FakeResponse({}, status=503)))
    with pytest.raises(ApiError):
        client.summary()


@pytest.mark.parametrize("value,expected", [(950, "950"), (12_345, "12.3K"), (1_234_567, "1.23M")])
def test_format_number(value, expected):
    assert format_number(value) == expected
