"""Batch analytics logic (used by batch_report_job.py)."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def compute_rankings(events: DataFrame) -> DataFrame:
    totals = events.groupBy("campaign_id", "publisher_id").agg(
        F.sum(F.when(F.col("event_type") == "impression", 1).otherwise(0)).alias("impressions"),
        F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("clicks"),
        F.sum(F.when(F.col("event_type") == "conversion", 1).otherwise(0)).alias("conversions"),
    )
    ranked_window = Window.partitionBy("campaign_id").orderBy(F.desc("clicks"), F.desc("impressions"))
    return (
        totals.withColumn("ctr_pct", F.round(F.when(F.col("impressions") > 0,
                                                    F.col("clicks") * 100.0 / F.col("impressions")).otherwise(0.0), 3))
        .withColumn("cvr_pct", F.round(F.when(F.col("clicks") > 0,
                                              F.col("conversions") * 100.0 / F.col("clicks")).otherwise(0.0), 3))
        .withColumn("rank_in_campaign", F.rank().over(ranked_window))
    )
