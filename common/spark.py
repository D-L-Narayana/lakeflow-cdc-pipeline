import pyspark
from pyspark.sql import SparkSession

from config import settings

KAFKA_PKG = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}"


def get_spark(app: str, with_kafka: bool = False, shuffle_partitions: int = 8) -> SparkSession:
    b = (
        SparkSession.builder.appName(app)
        .master(settings.SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
    )
    if with_kafka:
        b = b.config("spark.jars.packages", KAFKA_PKG)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
