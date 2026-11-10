"""The schema (column names + types) of one ad event JSON message.

Spark needs to know the structure in advance to parse JSON quickly and to
catch type errors. It mirrors ``adpulse.models.AdEvent``.
"""

from pyspark.sql.types import DoubleType, StringType, StructField, StructType

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("event_time", StringType()),  # parsed to timestamp later
        StructField("campaign_id", StringType()),
        StructField("publisher_id", StringType()),
        StructField("user_id", StringType()),
        StructField("device_type", StringType()),
        StructField("country", StringType()),
        StructField("ip_address", StringType()),
        StructField("cost_usd", DoubleType()),
        StructField("revenue_usd", DoubleType()),
        StructField("parent_event_id", StringType()),
        StructField("sent_at", StringType()),
    ]
)

REQUIRED_FIELDS = ("event_id", "event_type", "event_time", "campaign_id", "publisher_id", "user_id")
VALID_EVENT_TYPES = ("impression", "click", "conversion")
