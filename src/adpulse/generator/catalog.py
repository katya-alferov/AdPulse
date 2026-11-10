"""Synthetic reference data: campaigns and publishers.

The same rows are inserted into PostgreSQL by ``sql/init/02_seed_dimensions.sql``.
A unit test (tests/unit/test_catalog.py) makes sure both lists stay identical.
All names are fictional.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Campaign:
    campaign_id: str
    campaign_name: str
    advertiser: str
    category: str
    daily_budget_usd: float
    cpc_usd: float  # cost per click the advertiser pays
    base_ctr: float  # probability that an impression becomes a click
    base_cvr: float  # probability that a click becomes a conversion
    avg_order_value_usd: float


@dataclass(frozen=True)
class Publisher:
    publisher_id: str
    publisher_name: str
    site_category: str
    country: str
    traffic_weight: float  # how much traffic this site sends


CAMPAIGNS: tuple[Campaign, ...] = (
    Campaign("cmp_001", "Summer Sneakers Sale", "StrideWear", "fashion", 500.0, 0.45, 0.035, 0.08, 85.0),
    Campaign("cmp_002", "Budget Flights to Europe", "SkyHop Travel", "travel", 800.0, 0.70, 0.028, 0.05, 240.0),
    Campaign("cmp_003", "Learn Python Online", "CodeNest Academy", "education", 300.0, 0.35, 0.042, 0.11, 49.0),
    Campaign("cmp_004", "Smart Home Starter Kit", "HomeByte", "electronics", 650.0, 0.55, 0.022, 0.06, 129.0),
    Campaign("cmp_005", "Healthy Meal Boxes", "GreenFork", "food", 400.0, 0.40, 0.031, 0.09, 59.0),
    Campaign("cmp_006", "Personal Finance App", "CoinWise", "finance", 700.0, 0.80, 0.018, 0.07, 30.0),
    Campaign("cmp_007", "Mystery Novel Release", "PageTurner Books", "books", 200.0, 0.25, 0.026, 0.10, 18.0),
    Campaign("cmp_008", "Electric Scooter Launch", "VoltRide", "mobility", 900.0, 0.65, 0.024, 0.04, 499.0),
)

PUBLISHERS: tuple[Publisher, ...] = (
    Publisher("pub_01", "DailyNews Hub", "news", "US", 0.25),
    Publisher("pub_02", "TechTalk Online", "technology", "US", 0.18),
    Publisher("pub_03", "Foodie Planet", "food", "GB", 0.12),
    Publisher("pub_04", "TravelMore Magazine", "travel", "DE", 0.12),
    Publisher("pub_05", "SportZone Live", "sports", "IL", 0.18),
    Publisher("pub_06", "FinanceToday", "finance", "US", 0.15),
)

COUNTRIES: tuple[str, ...] = ("US", "GB", "DE", "IL", "FR", "CA", "IN", "BR")
COUNTRY_WEIGHTS: tuple[float, ...] = (0.35, 0.12, 0.10, 0.10, 0.08, 0.08, 0.10, 0.07)

CAMPAIGNS_BY_ID = {c.campaign_id: c for c in CAMPAIGNS}
PUBLISHERS_BY_ID = {p.publisher_id: p for p in PUBLISHERS}
