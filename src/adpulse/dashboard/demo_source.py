"""Demo data source: serves a pre-exported snapshot instead of calling the API.

Why: free hosting (e.g. Streamlit Community Cloud) can run a Streamlit app,
but not Kafka + Spark + PostgreSQL. In demo mode the dashboard reads JSON
files that were exported from a REAL local run of the full pipeline
(tools/export_demo_data.py). All data is synthetic.

It has the same methods as ``AdPulseClient`` so the dashboard code does not
care which one it uses (this is "duck typing" / the strategy pattern).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

DEFAULT_DEMO_DIR = Path(__file__).resolve().parents[3] / "demo_data"


class DemoDataSource:
    def __init__(self, directory: Path = DEFAULT_DEMO_DIR) -> None:
        self.directory = Path(directory)
        if not (self.directory / "summary.json").exists():
            raise FileNotFoundError(f"No demo snapshot found in {self.directory}")

    def _load(self, name: str) -> Any:
        with open(self.directory / f"{name}.json", encoding="utf-8") as fh:
            return json.load(fh)

    def metadata(self) -> dict[str, Any]:
        return self._load("metadata")

    def summary(self) -> dict[str, Any]:
        return self._load("summary")

    def campaigns(self) -> list[dict[str, Any]]:
        return self._load("campaigns")

    def publishers(self) -> list[dict[str, Any]]:
        return self._load("publishers")

    def timeseries(self, minutes: int = 60, campaign_id: Optional[str] = None) -> list[dict[str, Any]]:
        rows = self._load("timeseries_by_campaign" if campaign_id else "timeseries")
        if campaign_id:
            rows = rows.get(campaign_id, [])
        if not rows:
            return []
        latest = max(datetime.fromisoformat(r["window_start"].replace("Z", "+00:00")) for r in rows)
        cutoff = latest - timedelta(minutes=minutes)
        return [r for r in rows if datetime.fromisoformat(r["window_start"].replace("Z", "+00:00")) >= cutoff]

    def suspicious(self, limit: int = 50) -> list[dict[str, Any]]:
        return self._load("suspicious")[:limit]

    def duplicates(self, limit: int = 50) -> list[dict[str, Any]]:
        return self._load("duplicates")[:limit]

    def invalid(self) -> list[dict[str, Any]]:
        return self._load("invalid")

    def rankings(self) -> list[dict[str, Any]]:
        return self._load("rankings")
