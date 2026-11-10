"""Pure DataFrame transformations - the business logic of the pipeline.

Each function takes a DataFrame and returns a new DataFrame. They work the
same on a *batch* DataFrame (used in unit tests) and on a *streaming*
DataFrame (used by streaming_job.py). Nothing here runs until an action
(write/collect/count) is called - that is Spark's lazy evaluation.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from adpulse_spark.schemas import EVENT_SCHEMA, REQUIRED_FIELDS, VALID_EVENT_TYPES

MAX_FUTURE_SKEW_SQL = "INTERVAL 5 MINUTES"  # same tolerance as adpulse/validation.py


# --------------------------------------------------------------------------- 1. parse
def parse_events(raw: DataFrame) -> DataFrame:
    """Kafka gives us binary ``value`` + metadata. Turn it into typed columns.

    Expected input columns: value (binary or string); optional: partition,
    offset, timestamp (Kafka metadata).
    """
    cols = raw.columns
    value_str = F.col("value").cast("string")
    meta = [
        (F.col("partition") if "partition" in cols else F.lit(None).cast("int")).alias("kafka_partition"),
        (F.col("offset") if "offset" in cols else F.lit(None).cast("long")).alias("kafka_offset"),
        (F.col("timestamp") if "timestamp" in cols else F.current_timestamp()).alias("kafka_timestamp"),
    ]
    parsed = raw.select(
        value_str.alias("raw_value"),
        *meta,
        F.from_json(value_str, EVENT_SCHEMA).alias("e"),
    )
    return parsed.select("raw_value", "kafka_partition", "kafka_offset", "kafka_timestamp", "e.*").withColumn(
        # Convert ISO-8601 text (e.g. 2026-11-10T21:00:00.123Z) to a real timestamp.
        "event_ts", F.to_timestamp("event_time")
    )


def _blank(c: str) -> Column:
    return F.col(c).isNull() | (F.trim(F.col(c)) == "")


# ------------------------------------------------------------------------ 2. validate
def validation_error_expr() -> Column:
    """One CASE WHEN expression that returns the first validation error or NULL.

    The rules are identical to adpulse/validation.py (the Python auditor).
    """
    all_required_missing = None
    for name in REQUIRED_FIELDS:
        cond = _blank(name)
        all_required_missing = cond if all_required_missing is None else (all_required_missing & cond)

    expr = F.when(all_required_missing, F.lit("malformed_json"))
    for name in REQUIRED_FIELDS:
        expr = expr.when(_blank(name), F.lit(f"missing_field:{name}"))
    return (
        expr.when(~F.col("event_type").isin(*VALID_EVENT_TYPES), F.lit("invalid_event_type"))
        .when(F.col("event_ts").isNull(), F.lit("invalid_event_time"))
        .when(F.col("event_ts") > F.current_timestamp() + F.expr(MAX_FUTURE_SKEW_SQL), F.lit("event_time_in_future"))
        .when((F.coalesce("cost_usd", F.lit(0.0)) < 0) | (F.coalesce("revenue_usd", F.lit(0.0)) < 0),
              F.lit("negative_amount"))
        .otherwise(F.lit(None).cast("string"))
    )


def validate_and_enrich(events: DataFrame, campaigns: DataFrame, publishers: DataFrame) -> DataFrame:
    """Add ``validation_error`` and enrich with campaign/publisher names (JOIN).

    The dimension tables are small, so we *broadcast* them: every worker gets
    a full copy and no data shuffling over the network is needed.
    Events referring to an unknown campaign/publisher become invalid.
    """
    camp = campaigns.select("campaign_id", "campaign_name", "advertiser")
    pubs = publishers.select("publisher_id", "publisher_name")
    enriched = (
        events.withColumn("validation_error", validation_error_expr())
        .join(F.broadcast(camp), on="campaign_id", how="left")
        .join(F.broadcast(pubs), on="publisher_id", how="left")
    )
    return enriched.withColumn(
        "validation_error",
        F.when(F.col("validation_error").isNotNull(), F.col("validation_error"))
        .when(F.col("campaign_name").isNull(), F.lit("unknown_campaign"))
        .when(F.col("publisher_name").isNull(), F.lit("unknown_publisher"))
        .otherwise(F.lit(None).cast("string")),
    )


def split_valid_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = df.filter(F.col("validation_error").isNull()).drop("validation_error", "raw_value")
    invalid = df.filter(F.col("validation_error").isNotNull()).select(
        "raw_value", "validation_error", "kafka_partition", "kafka_offset", "kafka_timestamp"
    )
    return valid, invalid


# ----------------------------------------------------------------------- 3. dedup
def with_event_time_watermark(valid: DataFrame, watermark_minutes: int) -> DataFrame:
    """Watermark = "I will wait at most N minutes for late events".

    Spark keeps state (open windows, seen event_ids) only for that long, so
    memory does not grow forever. Events older than the watermark are dropped.
    """
    return valid.withWatermark("event_ts", f"{watermark_minutes} minutes")


def deduplicate(valid_with_watermark: DataFrame) -> DataFrame:
    """Keep only the first copy of each event_id (seen within the watermark).

    Streaming: state for an event_id is forgotten once the watermark passes.
    Batch (tests, reports): plain dropDuplicates over the whole dataset.
    """
    if valid_with_watermark.isStreaming:
        return valid_with_watermark.dropDuplicatesWithinWatermark(["event_id"])
    return valid_with_watermark.dropDuplicates(["event_id"])


# -------------------------------------------------------------------- 4. aggregate
def _count_type(event_type: str) -> Column:
    return F.sum(F.when(F.col("event_type") == event_type, 1).otherwise(0))


def aggregate_metrics(events: DataFrame, window_duration: str = "1 minute") -> DataFrame:
    """Impressions / clicks / conversions / money per campaign+publisher per time window."""
    return (
        events.groupBy(F.window("event_ts", window_duration), "campaign_id", "publisher_id")
        .agg(
            _count_type("impression").alias("impressions"),
            _count_type("click").alias("clicks"),
            _count_type("conversion").alias("conversions"),
            F.round(F.sum(F.coalesce("cost_usd", F.lit(0.0))), 4).alias("spend_usd"),
            F.round(F.sum(F.coalesce("revenue_usd", F.lit(0.0))), 2).alias("revenue_usd"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "campaign_id", "publisher_id", "impressions", "clicks", "conversions", "spend_usd", "revenue_usd",
        )
    )


def detect_suspicious_clicks(events: DataFrame, threshold: int, window_duration: str = "1 minute") -> DataFrame:
    """Fraud rule: the same user + IP clicking the same campaign >= threshold
    times inside one minute is not human behaviour (likely a bot)."""
    return (
        events.filter(F.col("event_type") == "click")
        .groupBy(F.window("event_ts", window_duration), "user_id", "ip_address", "campaign_id")
        .agg(F.count(F.lit(1)).alias("click_count"))
        .filter(F.col("click_count") >= threshold)
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "user_id", "ip_address", "campaign_id", "click_count",
            F.lit(f"click_burst>={threshold}/min").alias("reason"),
        )
    )


def detect_duplicates(valid_with_watermark: DataFrame, window_duration: str = "10 minutes") -> DataFrame:
    """Find event_ids that arrived more than once (before dedup removes them)."""
    return (
        valid_with_watermark.groupBy(
            F.window("event_ts", window_duration), "event_id", "event_type", "campaign_id", "user_id"
        )
        .agg(F.count(F.lit(1)).alias("occurrences"))
        .filter(F.col("occurrences") > 1)
        .select("event_id", "event_type", "campaign_id", "user_id", "occurrences")
    )


def lake_columns(events: DataFrame) -> DataFrame:
    """Columns written to the data lake (Parquet), plus partition columns."""
    return events.select(
        "event_id", "event_type", "event_ts", "campaign_id", "campaign_name", "advertiser",
        "publisher_id", "publisher_name", "user_id", "device_type", "country", "ip_address",
        "cost_usd", "revenue_usd", "parent_event_id", "kafka_partition", "kafka_offset", "kafka_timestamp",
        F.to_date("event_ts").alias("event_date"),
    )
