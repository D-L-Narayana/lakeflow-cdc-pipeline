"""Stage 2 — bronze (streaming file source) -> silver: typed, validated, deduplicated current-state tables
(+ SCD2 history for customers). foreachBatch gives us idempotent merges per micro-batch."""
from common.io import atomic_replace, read_parquet_or_none, write_quarantine
from common.logging_conf import get_logger
from common.spark import get_spark
from config import settings
from streaming.quality import split_valid
from streaming.schemas import SCD2_TABLES, TABLE_KEYS
from streaming.transforms import apply_cdc, flatten_table, scd2_merge

log = get_logger("silver_upsert")


def upsert_batch(batch_df, batch_id):
    spark = batch_df.sparkSession
    for table, key in TABLE_KEYS.items():
        rows = flatten_table(batch_df, table)
        if rows.isEmpty():
            continue
        valid, bad = split_valid(rows, table)
        quarantined = write_quarantine(bad, table, settings.QUARANTINE)
        path = f"{settings.SILVER}/{table}"
        current = read_parquet_or_none(spark, path)
        if table in SCD2_TABLES:
            out = scd2_merge(current, valid, key, SCD2_TABLES[table])
        else:
            out = apply_cdc(current, valid, [key])
        atomic_replace(path, out)
        log.info("silver_upserted", extra={"batch_id": batch_id, "table": table, "events": rows.count(),
                                           "quarantined": quarantined})


def main():
    spark = get_spark("lakeflow-silver")
    schema = spark.read.parquet(settings.BRONZE).schema
    stream = spark.readStream.schema(schema).option("maxFilesPerTrigger", "200").parquet(settings.BRONZE)
    query = (stream.writeStream.foreachBatch(upsert_batch)
             .option("checkpointLocation", f"{settings.CHECKPOINTS}/silver")
             .trigger(processingTime=settings.TRIGGER_INTERVAL)
             .queryName("silver_upsert").start())
    log.info("silver_stream_started", extra={"source": settings.BRONZE, "sink": settings.SILVER})
    query.awaitTermination()


if __name__ == "__main__":
    main()
