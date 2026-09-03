"""Rebuild silver from the immutable bronze history. Same transforms as the streaming path, so a nightly
run makes silver converge even if the stream had gaps; also the recovery path after a bad deploy."""
import json
import os
import time

from common.io import atomic_replace, write_quarantine
from common.logging_conf import get_logger
from common.spark import get_spark
from config import settings
from streaming.quality import split_valid
from streaming.schemas import SCD2_TABLES, TABLE_KEYS
from streaming.transforms import apply_cdc, flatten_table, scd2_from_history

log = get_logger("backfill")


def rebuild_silver(spark) -> dict:
    bronze = spark.read.parquet(settings.BRONZE)
    stats = {"tables": {}, "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    for table, key in TABLE_KEYS.items():
        t0 = time.time()
        rows = flatten_table(bronze, table)
        valid, bad = split_valid(rows, table)
        quarantined = write_quarantine(bad, table, settings.QUARANTINE)
        if table in SCD2_TABLES:
            out = scd2_from_history(valid, key, SCD2_TABLES[table])
        else:
            out = apply_cdc(None, valid, [key])
        path = f"{settings.SILVER}/{table}"
        atomic_replace(path, out)
        s = {"events": rows.count(), "quarantined": quarantined,
             "silver_rows": spark.read.parquet(path).count(), "seconds": round(time.time() - t0, 1)}
        stats["tables"][table] = s
        log.info("silver_rebuilt", extra={"table": table, **s})
    os.makedirs(settings.STATS, exist_ok=True)
    with open(f"{settings.STATS}/backfill.json", "w") as f:
        json.dump(stats, f, indent=2)
    return stats


def main():
    spark = get_spark("lakeflow-backfill")
    print(json.dumps(rebuild_silver(spark), indent=2))


if __name__ == "__main__":
    main()
