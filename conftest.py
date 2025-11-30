import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("SQLBundleTests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()
