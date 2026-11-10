"""AdPulse real-time pipeline (Spark Structured Streaming).

Kafka ad-events ──▶ parse ──▶ validate + join dims ──┬─▶ invalid  ──▶ S3 Parquet (quarantine) + Postgres counts
                                                     └─▶ valid ──▶ watermark ──┬─▶ dedup ──┬─▶ S3 Parquet (data lake)
                                                                               │           ├─▶ 1-min metrics  ──▶ Postgres
                                                                               │           └─▶ fraud detection ─▶ Postgres
                                                                               └─▶ duplicate detection ─▶ Postgres
Submit with:
  spark-submit --master local[*] spark_jobs/streaming_job.py
"""

from __future__ import annotations

import logging
import sys
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from adpulse_spark.session import build_spark
from adpulse_spark.settings import SparkJobSettings
from adpulse_spark.sinks import (
    DUPLICATES_UPSERT,
    INVALID_UPSERT,
    METRICS_UPSERT,
    SUSPICIOUS_UPSERT,
    PostgresWriter,
)
from adpulse_spark.transformations import (
    aggregate_metrics,
    deduplicate,
    detect_duplicates,
    detect_suspicious_clicks,
    lake_columns,
    parse_events,
    split_valid_invalid,
    validate_and_enrich,
    with_event_time_watermark,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
log = logging.getLogger("adpulse.spark.streaming")


def load_dimensions(spark: SparkSession, s: SparkJobSettings, retries: int = 20) -> tuple[DataFrame, DataFrame]:
    """Read the small campaigns/publishers tables from PostgreSQL once (JDBC) and cache them."""
    for attempt in range(1, retries + 1):
        try:
            campaigns = spark.read.jdbc(s.jdbc_url, "campaigns", properties=s.jdbc_properties).cache()
            publishers = spark.read.jdbc(s.jdbc_url, "publishers", properties=s.jdbc_properties).cache()
            log.info("Loaded %d campaigns, %d publishers", campaigns.count(), publishers.count())
            return campaigns, publishers
        except Exception as exc:  # noqa: BLE001 - DB may still be starting
            log.warning("Postgres not ready (attempt %d/%d): %s", attempt, retries, exc)
            time.sleep(3)
    raise RuntimeError("Could not load dimension tables from PostgreSQL")


def read_kafka(spark: SparkSession, s: SparkJobSettings) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", s.kafka_bootstrap)
        .option("subscribe", s.events_topic)
        .option("startingOffsets", "earliest")  # first run: read everything; later: resume from checkpoint
        .option("maxOffsetsPerTrigger", s.max_offsets_per_trigger)  # back-pressure
        .option("failOnDataLoss", "false")
        .load()
    )


def invalid_counts_writer(pg: PostgresWriter):
    """Count invalid events per minute and reason, add them to Postgres."""

    def write(batch_df: DataFrame, batch_id: int) -> None:
        counts = (batch_df.groupBy(F.date_trunc("minute", "kafka_timestamp").alias("window_start"),
                                   "validation_error")
                  .agg(F.count(F.lit(1)).alias("event_count")).collect())
        rows = [(r["window_start"], r["validation_error"], r["event_count"]) for r in counts]
        if rows:
            pg.upsert_rows(INVALID_UPSERT, rows, "(%s, %s, %s, now())")
            log.info("[invalid] batch %d: %d invalid events", batch_id, sum(r[2] for r in rows))

    return write


def main() -> int:
    s = SparkJobSettings.from_env()
    spark = build_spark("AdPulse-Streaming", s)
    campaigns, publishers = load_dimensions(spark, s)
    pg = PostgresWriter(s)
    trigger = {"processingTime": f"{s.trigger_seconds} seconds"}
    ckpt = s.checkpoint_dir

    # ---- shared transformation graph (lazy: nothing runs yet) ----
    parsed = parse_events(read_kafka(spark, s))
    valid, invalid = split_valid_invalid(validate_and_enrich(parsed, campaigns, publishers))
    valid_wm = with_event_time_watermark(valid, s.watermark_minutes)
    clean = deduplicate(valid_wm)

    # ---- streaming queries (each one is an independent "action") ----
    queries = [
        lake_columns(clean).writeStream.queryName("events_to_lake")
        .format("parquet").option("path", f"{s.lake_path}/events")
        .option("checkpointLocation", f"{ckpt}/events_to_lake")
        .partitionBy("event_date", "event_type").outputMode("append").trigger(**trigger).start(),

        aggregate_metrics(clean).writeStream.queryName("metrics_to_postgres")
        .outputMode("update").option("checkpointLocation", f"{ckpt}/metrics_to_postgres")
        .foreachBatch(pg.batch_writer(
            "metrics", METRICS_UPSERT,
            ["window_start", "window_end", "campaign_id", "publisher_id", "impressions", "clicks",
             "conversions", "spend_usd", "revenue_usd"], ", now()"))
        .trigger(**trigger).start(),

        detect_suspicious_clicks(clean, s.suspicious_click_threshold).writeStream.queryName("fraud_to_postgres")
        .outputMode("update").option("checkpointLocation", f"{ckpt}/fraud_to_postgres")
        .foreachBatch(pg.batch_writer(
            "fraud", SUSPICIOUS_UPSERT,
            ["window_start", "window_end", "user_id", "ip_address", "campaign_id", "click_count", "reason"],
            ", now()"))
        .trigger(**trigger).start(),

        detect_duplicates(valid_wm).writeStream.queryName("duplicates_to_postgres")
        .outputMode("update").option("checkpointLocation", f"{ckpt}/duplicates_to_postgres")
        .foreachBatch(pg.batch_writer(
            "duplicates", DUPLICATES_UPSERT,
            ["event_id", "event_type", "campaign_id", "user_id", "occurrences"], ", now()"))
        .trigger(**trigger).start(),

        # Raw invalid messages are kept in the lake for investigation ("quarantine").
        invalid.withColumn("ingest_date", F.to_date("kafka_timestamp"))
        .writeStream.queryName("invalid_to_lake")
        .format("parquet").option("path", f"{s.lake_path}/invalid_events")
        .option("checkpointLocation", f"{ckpt}/invalid_to_lake")
        .partitionBy("ingest_date").outputMode("append").trigger(**trigger).start(),

        invalid.writeStream.queryName("invalid_counts_to_postgres")
        .option("checkpointLocation", f"{ckpt}/invalid_counts_to_postgres")
        .foreachBatch(invalid_counts_writer(pg)).trigger(**trigger).start(),
    ]
    log.info("Started %d streaming queries: %s", len(queries), [q.name for q in queries])
    spark.streams.awaitAnyTermination()  # runs until one query fails or the container stops
    for q in queries:
        if q.exception():
            log.error("Query %s failed: %s", q.name, q.exception())
    return 1


if __name__ == "__main__":
    sys.exit(main())
