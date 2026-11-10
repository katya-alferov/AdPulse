"""Response models: FastAPI uses them to validate output and to build the
interactive documentation at http://localhost:8000/docs"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class Health(BaseModel):
    status: str
    database: bool


class Summary(BaseModel):
    impressions: int
    clicks: int
    conversions: int
    spend_usd: float
    revenue_usd: float
    ctr_pct: float
    conversion_rate_pct: float
    suspicious_users: int
    duplicate_events: int
    invalid_events: int
    last_update: Optional[datetime] = None


class CampaignPerformance(BaseModel):
    campaign_id: str
    campaign_name: str
    advertiser: str
    category: str
    impressions: int
    clicks: int
    conversions: int
    spend_usd: float
    revenue_usd: float
    ctr_pct: Optional[float] = None
    cvr_pct: Optional[float] = None


class PublisherPerformance(BaseModel):
    publisher_id: str
    publisher_name: str
    site_category: str
    country: str
    impressions: int
    clicks: int
    conversions: int
    spend_usd: float
    ctr_pct: Optional[float] = None
    cvr_pct: Optional[float] = None


class TimePoint(BaseModel):
    window_start: datetime
    impressions: int
    clicks: int
    conversions: int
    ctr_pct: Optional[float] = None


class SuspiciousActivity(BaseModel):
    window_start: datetime
    window_end: datetime
    user_id: str
    ip_address: str
    campaign_id: str
    campaign_name: str
    click_count: int
    reason: str


class DuplicateEvent(BaseModel):
    event_id: str
    event_type: str
    campaign_id: str
    user_id: str
    occurrences: int
    detected_at: datetime


class InvalidReason(BaseModel):
    validation_error: str
    event_count: int


class PublisherRanking(BaseModel):
    campaign_id: str
    campaign_name: str
    publisher_id: str
    publisher_name: str
    impressions: int
    clicks: int
    conversions: int
    ctr_pct: float
    cvr_pct: float
    rank_in_campaign: int
