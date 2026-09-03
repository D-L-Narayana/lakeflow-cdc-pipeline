"""End-to-end run without Docker/Kafka: synthetic Debezium stream -> bronze -> silver -> gold, with timings.
Usage: python -m scripts.local_demo [n_orders]"""
import json
import shutil
import sys
import time
from pathlib import Path

from batch.backfill import rebuild_silver
from batch.build_gold import build_gold
from common.logging_conf import get_logger
from common.spark import get_spark
from config import settings
from generators.debezium_events import synth_stream
from streaming.transforms import parse_debezium


def main(n_orders: int):
    log = get_logger("local_demo")
    root = Path(settings.LAKE_ROOT)
    shutil.rmtree(root, ignore_errors=True)
    landing = root / "landing"
    landing.mkdir(parents=True)
    t0, n = time.time(), 0
    with open(landing / "cdc_events.jsonl", "w") as f:
        for ev in synth_stream(n_orders):
            f.write(json.dumps(ev) + "\n")
            n += 1
    gen_s = round(time.time() - t0, 1)
    log.info("events_generated", extra={"events": n, "seconds": gen_s})

    spark = get_spark("lakeflow-local-demo")
    t1 = time.time()
    raw = spark.read.text(str(landing))
    parse_debezium(raw).write.mode("overwrite").partitionBy("table", "ingest_date").parquet(settings.BRONZE)
    bronze_rows = spark.read.parquet(settings.BRONZE).count()
    bronze_s = round(time.time() - t1, 1)
    log.info("bronze_written", extra={"rows": bronze_rows, "seconds": bronze_s})

    t2 = time.time()
    silver = rebuild_silver(spark)
    silver_s = round(time.time() - t2, 1)
    t3 = time.time()
    gold = build_gold(spark)
    gold_s = round(time.time() - t3, 1)

    summary = {"events": n, "generate_seconds": gen_s, "bronze_rows": bronze_rows, "bronze_seconds": bronze_s,
               "silver": silver["tables"], "silver_seconds": silver_s, "gold": gold, "gold_seconds": gold_s,
               "pipeline_seconds": round(time.time() - t1, 1),
               "events_per_second": round(n / max(time.time() - t1, 0.001))}
    (root / "demo_summary.json").write_text(json.dumps(summary, indent=2))
    print("DEMO_SUMMARY " + json.dumps(summary))


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 20_000)
