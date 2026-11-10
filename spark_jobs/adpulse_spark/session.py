"""Builds the SparkSession - the "entry door" to Spark."""

from __future__ import annotations

from pyspark.sql import SparkSession

from adpulse_spark.settings import SparkJobSettings


def build_spark(app_name: str, settings: SparkJobSettings) -> SparkSession:
    """Create a SparkSession configured for S3 (via the s3a connector) and UTC time."""
    spark = (
        SparkSession.builder.appName(app_name)
        # All timestamps in UTC: ad events come from every time zone.
        .config("spark.sql.session.timeZone", "UTC")
        # Small local machine -> few shuffle partitions (default 200 is for clusters).
        .config("spark.sql.shuffle.partitions", "4")
        # --- S3A: lets Spark read/write s3a://bucket/path on any S3-compatible store ---
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")  # http://host/bucket/key
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(settings.s3_endpoint.startswith("https")).lower())
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")  # hide noisy INFO/WARN logs
    return spark
