"""AdPulse REST API.

Run locally:  uvicorn adpulse.api.main:app --reload
Docs:         http://localhost:8000/docs  (Swagger UI, generated automatically)
"""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from adpulse import __version__
from adpulse.api.schemas import (
    CampaignPerformance,
    DuplicateEvent,
    Health,
    InvalidReason,
    PublisherPerformance,
    PublisherRanking,
    Summary,
    SuspiciousActivity,
    TimePoint,
)
from adpulse.config import Settings
from adpulse.logging_setup import setup_logging
from adpulse.storage.db import AnalyticsRepository, PostgresRepository

log = logging.getLogger("adpulse.api")

app = FastAPI(
    title="AdPulse Analytics API",
    version=__version__,
    description="Real-time advertising analytics (synthetic data). Built with FastAPI + PostgreSQL.",
)


@lru_cache(maxsize=1)
def get_repository() -> AnalyticsRepository:
    """Dependency: one repository object for the whole app. Tests replace it with a fake."""
    settings = Settings.from_env()
    setup_logging(settings.log_level)
    return PostgresRepository(settings.postgres)


@app.exception_handler(Exception)
async def unexpected_error(_, exc: Exception) -> JSONResponse:
    log.exception("Unhandled error: %s", exc)
    return JSONResponse(status_code=503, content={"detail": "Analytics backend unavailable"})


@app.get("/health", response_model=Health, tags=["system"])
def health(repo: AnalyticsRepository = Depends(get_repository)) -> Health:
    ok = repo.ping()
    if not ok:
        raise HTTPException(status_code=503, detail="database unavailable")
    return Health(status="ok", database=True)


@app.get("/api/v1/summary", response_model=Summary, tags=["analytics"])
def summary(repo: AnalyticsRepository = Depends(get_repository)):
    """Totals across all campaigns: impressions, clicks, CTR, conversions, conversion rate."""
    return repo.summary()


@app.get("/api/v1/campaigns", response_model=list[CampaignPerformance], tags=["analytics"])
def campaigns(repo: AnalyticsRepository = Depends(get_repository)):
    return repo.campaigns()


@app.get("/api/v1/campaigns/{campaign_id}", response_model=CampaignPerformance, tags=["analytics"])
def campaign(campaign_id: str, repo: AnalyticsRepository = Depends(get_repository)):
    row = repo.campaign(campaign_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"campaign {campaign_id} not found")
    return row


@app.get("/api/v1/campaigns/{campaign_id}/timeseries", response_model=list[TimePoint], tags=["analytics"])
def campaign_timeseries(
    campaign_id: str,
    minutes: int = Query(60, ge=1, le=24 * 60),
    repo: AnalyticsRepository = Depends(get_repository),
):
    if repo.campaign(campaign_id) is None:
        raise HTTPException(status_code=404, detail=f"campaign {campaign_id} not found")
    return repo.timeseries(minutes, campaign_id)


@app.get("/api/v1/publishers", response_model=list[PublisherPerformance], tags=["analytics"])
def publishers(repo: AnalyticsRepository = Depends(get_repository)):
    return repo.publishers()


@app.get("/api/v1/timeseries", response_model=list[TimePoint], tags=["analytics"])
def timeseries(
    minutes: int = Query(60, ge=1, le=24 * 60, description="how many minutes back from the latest window"),
    campaign_id: Optional[str] = None,
    repo: AnalyticsRepository = Depends(get_repository),
):
    return repo.timeseries(minutes, campaign_id)


@app.get("/api/v1/suspicious", response_model=list[SuspiciousActivity], tags=["quality"])
def suspicious(limit: int = Query(50, ge=1, le=500), repo: AnalyticsRepository = Depends(get_repository)):
    """Users/IPs with abnormal click bursts (possible click fraud)."""
    return repo.suspicious(limit)


@app.get("/api/v1/duplicates", response_model=list[DuplicateEvent], tags=["quality"])
def duplicates(limit: int = Query(50, ge=1, le=500), repo: AnalyticsRepository = Depends(get_repository)):
    return repo.duplicates(limit)


@app.get("/api/v1/invalid", response_model=list[InvalidReason], tags=["quality"])
def invalid(repo: AnalyticsRepository = Depends(get_repository)):
    return repo.invalid_summary()


@app.get("/api/v1/rankings", response_model=list[PublisherRanking], tags=["analytics"])
def rankings(repo: AnalyticsRepository = Depends(get_repository)):
    """Output of the Spark batch job (publisher rank inside each campaign)."""
    return repo.rankings()
