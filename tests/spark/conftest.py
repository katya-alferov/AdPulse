import pytest

pyspark = pytest.importorskip("pyspark")  # skip on machines without PySpark/Java

from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("adpulse-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def dims(spark):
    campaigns = spark.createDataFrame(
        [("cmp_001", "Summer Sneakers Sale", "StrideWear"), ("cmp_002", "Budget Flights", "SkyHop")],
        ["campaign_id", "campaign_name", "advertiser"],
    )
    publishers = spark.createDataFrame([("pub_01", "DailyNews Hub"), ("pub_02", "TechTalk")],
                                       ["publisher_id", "publisher_name"])
    return campaigns, publishers
