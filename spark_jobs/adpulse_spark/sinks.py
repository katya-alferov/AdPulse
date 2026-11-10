"""Writers that push micro-batch results into PostgreSQL (UPSERT).

Structured Streaming's ``foreachBatch`` hands us each micro-batch as a normal
DataFrame. Aggregated results are small, so we collect them to the driver and
write them with one fast ``INSERT ... ON CONFLICT DO UPDATE`` (an "upsert").
Upserting the *full current value* of a window makes the write idempotent:
if Spark replays a batch after a crash, the row just gets the same value again.
"""

from __future__ import annotations

import logging
from typing import Callable, Sequence

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame

from adpulse_spark.settings import SparkJobSettings

log = logging.getLogger("adpulse.spark.sinks")

METRICS_UPSERT = """
INSERT INTO metrics_minute (window_start, window_end, campaign_id, publisher_id,
                            impressions, clicks, conversions, spend_usd, revenue_usd, updated_at)
VALUES %s
ON CONFLICT (window_start, campaign_id, publisher_id) DO UPDATE SET
    impressions = EXCLUDED.impressions,
    clicks      = EXCLUDED.clicks,
    conversions = EXCLUDED.conversions,
    spend_usd   = EXCLUDED.spend_usd,
    revenue_usd = EXCLUDED.revenue_usd,
    updated_at  = now()
"""

SUSPICIOUS_UPSERT = """
INSERT INTO suspicious_activity (window_start, window_end, user_id, ip_address, campaign_id,
                                 click_count, reason, detected_at)
VALUES %s
ON CONFLICT (window_start, user_id, ip_address, campaign_id) DO UPDATE SET
    click_count = EXCLUDED.click_count,
    detected_at = now()
"""

DUPLICATES_UPSERT = """
INSERT INTO duplicate_events (event_id, event_type, campaign_id, user_id, occurrences, detected_at)
VALUES %s
ON CONFLICT (event_id) DO UPDATE SET
    occurrences = GREATEST(duplicate_events.occurrences, EXCLUDED.occurrences),
    detected_at = now()
"""

# Invalid counts are additive per micro-batch (at-least-once: a replayed batch may count twice).
INVALID_UPSERT = """
INSERT INTO invalid_events_minute (window_start, validation_error, event_count, updated_at)
VALUES %s
ON CONFLICT (window_start, validation_error) DO UPDATE SET
    event_count = invalid_events_minute.event_count + EXCLUDED.event_count,
    updated_at  = now()
"""


class PostgresWriter:
    def __init__(self, settings: SparkJobSettings) -> None:
        self.settings = settings

    def _connect(self):
        s = self.settings
        return psycopg2.connect(host=s.pg_host, port=s.pg_port, dbname=s.pg_db, user=s.pg_user,
                                password=s.pg_password, connect_timeout=10)

    def upsert_rows(self, sql: str, rows: Sequence[tuple], template: str) -> int:
        if not rows:
            return 0
        conn = self._connect()
        try:
            with conn, conn.cursor() as cur:  # "with conn" = one transaction: commit or rollback
                execute_values(cur, sql, rows, template=template, page_size=1000)
        finally:
            conn.close()
        return len(rows)

    def batch_writer(self, name: str, sql: str, columns: list[str], extra_template: str = "") -> Callable:
        """Return a function usable as ``writeStream.foreachBatch(...)``."""
        template = "(" + ", ".join(["%s"] * len(columns)) + extra_template + ")"

        def write(batch_df: DataFrame, batch_id: int) -> None:
            rows = [tuple(r[c] for c in columns) for r in batch_df.select(*columns).collect()]
            written = self.upsert_rows(sql, rows, template)
            if written:
                log.info("[%s] batch %d: upserted %d rows", name, batch_id, written)

        return write
