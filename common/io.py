import os
import shutil
import uuid

from pyspark.sql import DataFrame, SparkSession, functions as F


def read_parquet_or_none(spark: SparkSession, path: str):
    if not os.path.isdir(path) or not os.listdir(path):
        return None
    return spark.read.parquet(path)


def atomic_replace(path: str, df: DataFrame) -> None:
    """Materialise df to a temp dir, then swap it in. Safe even when df was derived from `path` itself.
    On Databricks/ADLS/S3 this step becomes a Delta Lake MERGE INTO instead of a directory swap."""
    tmp = f"{path}__tmp_{uuid.uuid4().hex[:8]}"
    df.write.mode("overwrite").parquet(tmp)
    if os.path.isdir(path):
        shutil.rmtree(path)
    os.rename(tmp, path)


def write_quarantine(bad: DataFrame, table: str, root: str) -> int:
    """Dead-letter sink: rows failing data-quality rules, with the failed rule names and a timestamp."""
    n = bad.count()
    if n:
        (bad.withColumn("dq_failures", F.concat_ws(",", "dq_failures"))
            .withColumn("quarantined_at", F.current_timestamp())
            .write.mode("append").parquet(f"{root}/{table}"))
    return n
