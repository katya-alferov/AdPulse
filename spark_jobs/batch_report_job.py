"""AdPulse batch report (classic batch processing over the data lake).

Reads ALL Parquet events from S3, computes per campaign/publisher KPIs and
ranks publishers inside each campaign using a SQL *window function*
(RANK() OVER (PARTITION BY campaign ORDER BY clicks DESC)).
Result -> PostgreSQL table publisher_rankings.

Run:  docker compose run --rm spark-batch
"""

from __future__ import annotations

import logging
import sys

from adpulse_spark.batch_logic import compute_rankings
from adpulse_spark.session import build_spark
from adpulse_spark.settings import SparkJobSettings
from adpulse_spark.sinks import PostgresWriter

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
log = logging.getLogger("adpulse.spark.batch")

RANKINGS_SQL_TRUNCATE = "TRUNCATE publisher_rankings"
RANKINGS_INSERT = """
INSERT INTO publisher_rankings (campaign_id, publisher_id, impressions, clicks, conversions,
                                ctr_pct, cvr_pct, rank_in_campaign, computed_at)
VALUES %s
"""


def main() -> int:
    s = SparkJobSettings.from_env()
    spark = build_spark("AdPulse-BatchReport", s)
    events = spark.read.parquet(f"{s.lake_path}/events")
    log.info("Events in data lake: %d (schema: %s)", events.count(), events.columns)

    rankings = compute_rankings(events).orderBy("campaign_id", "rank_in_campaign")
    rankings.show(50, truncate=False)

    cols = ["campaign_id", "publisher_id", "impressions", "clicks", "conversions", "ctr_pct", "cvr_pct",
            "rank_in_campaign"]
    rows = [tuple(r[c] for c in cols) for r in rankings.select(*cols).collect()]
    writer = PostgresWriter(s)
    conn = writer._connect()
    try:
        from psycopg2.extras import execute_values

        with conn, conn.cursor() as cur:  # truncate + insert in ONE transaction
            cur.execute(RANKINGS_SQL_TRUNCATE)
            execute_values(cur, RANKINGS_INSERT, rows, template="(%s, %s, %s, %s, %s, %s, %s, %s, now())")
    finally:
        conn.close()
    log.info("Wrote %d publisher ranking rows to PostgreSQL", len(rows))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
