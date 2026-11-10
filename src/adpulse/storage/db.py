"""PostgreSQL access layer used by the API (the "repository" pattern).

All SQL lives here, so the API code never builds SQL strings itself.
Every query uses %(name)s parameters - never string formatting - which
protects against SQL injection.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Iterator, Optional, Protocol

import psycopg
from psycopg.rows import dict_row

from adpulse.config import PostgresSettings
from adpulse.metrics import conversion_rate, ctr

log = logging.getLogger("adpulse.storage.db")


class AnalyticsRepository(Protocol):
    """What the API needs. A fake implementation is used in unit tests."""

    def ping(self) -> bool: ...
    def summary(self) -> dict[str, Any]: ...
    def campaigns(self) -> list[dict[str, Any]]: ...
    def campaign(self, campaign_id: str) -> Optional[dict[str, Any]]: ...
    def publishers(self) -> list[dict[str, Any]]: ...
    def timeseries(self, minutes: int, campaign_id: Optional[str] = None) -> list[dict[str, Any]]: ...
    def suspicious(self, limit: int) -> list[dict[str, Any]]: ...
    def duplicates(self, limit: int) -> list[dict[str, Any]]: ...
    def invalid_summary(self) -> list[dict[str, Any]]: ...
    def rankings(self) -> list[dict[str, Any]]: ...


class PostgresRepository:
    def __init__(self, settings: PostgresSettings) -> None:
        self.settings = settings

    @contextmanager
    def _cursor(self) -> Iterator[psycopg.Cursor]:
        with psycopg.connect(self.settings.conninfo, connect_timeout=5, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                yield cur

    def _all(self, sql: str, params: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
        with self._cursor() as cur:
            cur.execute(sql, params or {})
            return [_jsonable(r) for r in cur.fetchall()]

    # ------------------------------------------------------------------ queries
    def ping(self) -> bool:
        try:
            return self._all("SELECT 1 AS ok")[0]["ok"] == 1
        except psycopg.Error as exc:
            log.warning("DB ping failed: %s", exc)
            return False

    def summary(self) -> dict[str, Any]:
        row = self._all(
            """SELECT COALESCE(SUM(impressions),0) AS impressions, COALESCE(SUM(clicks),0) AS clicks,
                      COALESCE(SUM(conversions),0) AS conversions, COALESCE(SUM(spend_usd),0) AS spend_usd,
                      COALESCE(SUM(revenue_usd),0) AS revenue_usd, MAX(updated_at) AS last_update
               FROM metrics_minute"""
        )[0]
        row["ctr_pct"] = ctr(row["clicks"], row["impressions"])
        row["conversion_rate_pct"] = conversion_rate(row["conversions"], row["clicks"])
        row["suspicious_users"] = self._all("SELECT COUNT(DISTINCT user_id) AS n FROM suspicious_activity")[0]["n"]
        row["duplicate_events"] = self._all("SELECT COUNT(*) AS n FROM duplicate_events")[0]["n"]
        row["invalid_events"] = self._all(
            "SELECT COALESCE(SUM(event_count),0) AS n FROM invalid_events_minute")[0]["n"]
        return row

    def campaigns(self) -> list[dict[str, Any]]:
        return self._all("SELECT * FROM v_campaign_performance ORDER BY clicks DESC, campaign_id")

    def campaign(self, campaign_id: str) -> Optional[dict[str, Any]]:
        rows = self._all("SELECT * FROM v_campaign_performance WHERE campaign_id = %(id)s", {"id": campaign_id})
        return rows[0] if rows else None

    def publishers(self) -> list[dict[str, Any]]:
        return self._all("SELECT * FROM v_publisher_performance ORDER BY clicks DESC, publisher_id")

    def timeseries(self, minutes: int, campaign_id: Optional[str] = None) -> list[dict[str, Any]]:
        return self._all(
            """SELECT window_start, SUM(impressions) AS impressions, SUM(clicks) AS clicks,
                      SUM(conversions) AS conversions,
                      ROUND(100.0 * SUM(clicks) / NULLIF(SUM(impressions),0), 2) AS ctr_pct
               FROM metrics_minute
               WHERE window_start >= (SELECT MAX(window_start) FROM metrics_minute)
                                     - make_interval(mins => %(minutes)s)
                 AND (%(cid)s::text IS NULL OR campaign_id = %(cid)s)
               GROUP BY window_start ORDER BY window_start""",
            {"minutes": minutes, "cid": campaign_id},
        )

    def suspicious(self, limit: int) -> list[dict[str, Any]]:
        return self._all(
            """SELECT s.*, c.campaign_name FROM suspicious_activity s
               JOIN campaigns c USING (campaign_id)
               ORDER BY window_start DESC, click_count DESC LIMIT %(limit)s""",
            {"limit": limit},
        )

    def duplicates(self, limit: int) -> list[dict[str, Any]]:
        return self._all("SELECT * FROM duplicate_events ORDER BY detected_at DESC LIMIT %(limit)s",
                         {"limit": limit})

    def invalid_summary(self) -> list[dict[str, Any]]:
        return self._all(
            """SELECT validation_error, SUM(event_count) AS event_count FROM invalid_events_minute
               GROUP BY validation_error ORDER BY event_count DESC"""
        )

    def rankings(self) -> list[dict[str, Any]]:
        return self._all(
            """SELECT r.*, c.campaign_name, p.publisher_name FROM publisher_rankings r
               JOIN campaigns c USING (campaign_id) JOIN publishers p USING (publisher_id)
               ORDER BY campaign_id, rank_in_campaign"""
        )


def _jsonable(row: dict[str, Any]) -> dict[str, Any]:
    """Decimal -> float / int so the API can return clean JSON numbers."""
    from decimal import Decimal

    out = {}
    for k, v in row.items():
        if isinstance(v, Decimal):
            out[k] = int(v) if v == v.to_integral_value() and k not in _FLOAT_KEYS else float(v)
        else:
            out[k] = v
    return out


_FLOAT_KEYS = {"spend_usd", "revenue_usd", "ctr_pct", "cvr_pct", "conversion_rate_pct", "cpc_usd",
               "daily_budget_usd"}
