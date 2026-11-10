"""Small HTTP client the dashboard uses to talk to the FastAPI service.

The dashboard never touches the database directly: it goes through the API,
exactly like a real front-end would.
"""

from __future__ import annotations

from typing import Any, Optional

import requests


class ApiError(RuntimeError):
    pass


class AdPulseClient:
    def __init__(self, base_url: str, timeout: float = 5.0, session: Optional[requests.Session] = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()

    def _get(self, path: str, **params: Any) -> Any:
        try:
            resp = self.session.get(f"{self.base_url}{path}", params=params or None, timeout=self.timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            raise ApiError(f"API call {path} failed: {exc}") from exc

    def summary(self) -> dict[str, Any]:
        return self._get("/api/v1/summary")

    def campaigns(self) -> list[dict[str, Any]]:
        return self._get("/api/v1/campaigns")

    def publishers(self) -> list[dict[str, Any]]:
        return self._get("/api/v1/publishers")

    def timeseries(self, minutes: int = 60, campaign_id: Optional[str] = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"minutes": minutes}
        if campaign_id:
            params["campaign_id"] = campaign_id
        return self._get("/api/v1/timeseries", **params)

    def suspicious(self, limit: int = 50) -> list[dict[str, Any]]:
        return self._get("/api/v1/suspicious", limit=limit)

    def duplicates(self, limit: int = 50) -> list[dict[str, Any]]:
        return self._get("/api/v1/duplicates", limit=limit)

    def invalid(self) -> list[dict[str, Any]]:
        return self._get("/api/v1/invalid")

    def rankings(self) -> list[dict[str, Any]]:
        return self._get("/api/v1/rankings")


def format_number(value: float) -> str:
    """1234567 -> '1.23M', 12345 -> '12.3K' (compact KPI display)."""
    value = float(value or 0)
    for limit, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(value) >= limit:
            return f"{value / limit:.2f}{suffix}" if suffix != "K" else f"{value / limit:.1f}{suffix}"
    return f"{value:,.0f}"
