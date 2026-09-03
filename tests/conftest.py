import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("lakeflow-tests")
         .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
