from functools import reduce
from operator import and_, or_

from pyspark.sql import DataFrame, Window, functions as F

from streaming.schemas import TABLE_SCHEMAS

KAFKA_META = {"topic": "topic", "partition": "partition", "offset": "offset", "timestamp": "kafka_ts"}


def parse_debezium(raw: DataFrame) -> DataFrame:
    """Kafka record (or any df with a JSON `value` column) -> flat Debezium envelope. Payloads stay as JSON
    strings here so bronze is schema-agnostic; table-specific typing happens in flatten_table."""
    v = F.col("value").cast("string")
    meta = [F.col(c).alias(a) for c, a in KAFKA_META.items() if c in raw.columns]
    return raw.select(
        *meta,
        F.get_json_object(v, "$.op").alias("op"),
        F.get_json_object(v, "$.ts_ms").cast("long").alias("ts_ms"),
        F.get_json_object(v, "$.source.table").alias("table"),
        F.get_json_object(v, "$.source.lsn").cast("long").alias("lsn"),
        F.get_json_object(v, "$.before").alias("before_json"),
        F.get_json_object(v, "$.after").alias("after_json"),
    ).withColumn("ingest_date", F.to_date((F.col("ts_ms") / 1000).cast("timestamp")))


def flatten_table(cdc: DataFrame, table: str) -> DataFrame:
    """Type one table's events. Deletes carry the `before` image so downstream can resolve the key."""
    payload = F.when(F.col("op") == "d", F.col("before_json")).otherwise(F.col("after_json"))
    return (cdc.filter(F.col("table") == table)
            .withColumn("row", F.from_json(payload, TABLE_SCHEMAS[table]))
            .select("op", "ts_ms", "lsn", "row.*"))


def latest_per_key(df: DataFrame, key_cols, order_cols=("ts_ms", "lsn")) -> DataFrame:
    """Collapse many events per key to the final one; (ts_ms, lsn) is the commit order guaranteed by the WAL."""
    w = Window.partitionBy(*key_cols).orderBy(*[F.col(c).desc() for c in order_cols])
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def apply_cdc(current, changes: DataFrame, key_cols) -> DataFrame:
    """Idempotent upsert + delete: rows for touched keys are replaced by their latest image, deletes vanish."""
    latest = latest_per_key(changes, key_cols)
    upserts = latest.filter(F.col("op") != "d").drop("op", "ts_ms", "lsn")
    if current is None:
        return upserts
    kept = current.join(latest.select(*key_cols), list(key_cols), "left_anti")
    return kept.unionByName(upserts)


def scd2_merge(dim, changes: DataFrame, key: str, tracked) -> DataFrame:
    """Incremental SCD Type 2: close the current version when a tracked attribute changes (or the row is
    deleted) and open a new one. No-op updates leave the dimension untouched."""
    latest = latest_per_key(changes, [key]).withColumn("effective_from", (F.col("ts_ms") / 1000).cast("timestamp"))
    new_rows = (latest.filter(F.col("op") != "d").drop("op", "ts_ms", "lsn")
                .withColumn("effective_to", F.lit(None).cast("timestamp")).withColumn("is_current", F.lit(True)))
    if dim is None:
        return new_rows
    l = latest.select([F.col(c) if c == key else F.col(c).alias(f"l_{c}") for c in latest.columns])
    cur = dim.filter(F.col("is_current"))
    joined = cur.join(l, key, "inner")
    changed = reduce(or_, [~F.col(c).eqNullSafe(F.col(f"l_{c}")) for c in tracked]) | (F.col("l_op") == "d")
    to_close = joined.filter(changed).select(*cur.columns, F.col("l_effective_from").alias("_close_at"))
    closed = (to_close.withColumn("effective_to", F.col("_close_at")).withColumn("is_current", F.lit(False))
              .drop("_close_at"))
    unchanged_keys = joined.filter(~changed).select(key)
    inserts = new_rows.join(unchanged_keys, key, "left_anti")
    closing_keys = to_close.select(key).withColumn("_closing", F.lit(True))
    untouched = (dim.join(closing_keys, key, "left")
                 .filter(~(F.col("is_current") & F.col("_closing").isNotNull())).drop("_closing"))
    return untouched.unionByName(closed).unionByName(inserts)


def scd2_from_history(changes: DataFrame, key: str, tracked) -> DataFrame:
    """Batch rebuild of an SCD2 dimension from the full event history (used by backfill)."""
    w = Window.partitionBy(key).orderBy("ts_ms", "lsn")
    noop = (reduce(and_, [F.col(c).eqNullSafe(F.lag(c).over(w)) for c in tracked])
            & F.lag("op").over(w).isNotNull() & (F.col("op") != "d"))
    h = changes.withColumn("_noop", F.coalesce(noop, F.lit(False))).filter(~F.col("_noop")).drop("_noop")
    h = (h.withColumn("effective_from", (F.col("ts_ms") / 1000).cast("timestamp"))
         .withColumn("effective_to", F.lead("effective_from").over(w)))
    return (h.filter(F.col("op") != "d").withColumn("is_current", F.col("effective_to").isNull())
            .drop("op", "ts_ms", "lsn"))
