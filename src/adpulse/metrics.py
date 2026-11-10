"""Business metrics used across the platform.

These formulas are the heart of ad analytics. Spark computes the same
formulas at scale; the API uses these helpers on the aggregated numbers.
"""

from __future__ import annotations


def safe_ratio_percent(numerator: float, denominator: float, digits: int = 2) -> float:
    """Return numerator/denominator*100, or 0.0 when the denominator is 0.

    Dividing by zero would crash the program (ZeroDivisionError); a new
    campaign with zero impressions must show 0%, not an error.
    """
    if denominator is None or denominator <= 0 or numerator is None:
        return 0.0
    return round(numerator / denominator * 100.0, digits)


def ctr(clicks: float, impressions: float) -> float:
    """Click-Through Rate = clicks / impressions * 100."""
    return safe_ratio_percent(clicks, impressions)


def conversion_rate(conversions: float, clicks: float) -> float:
    """Conversion Rate = conversions / clicks * 100."""
    return safe_ratio_percent(conversions, clicks)


def cpc(spend_usd: float, clicks: float) -> float:
    """Cost Per Click = spend / clicks."""
    if not clicks:
        return 0.0
    return round(spend_usd / clicks, 4)


def roas(revenue_usd: float, spend_usd: float) -> float:
    """Return On Ad Spend = revenue / spend (2.0 means $2 earned per $1 spent)."""
    if not spend_usd:
        return 0.0
    return round(revenue_usd / spend_usd, 2)
