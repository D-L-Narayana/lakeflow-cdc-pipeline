"""Stage 3 — silver -> gold marts via Spark SQL files in sql/analytics/ (one file = one mart)."""
import json
import os
from pathlib import Path

from common.logging_conf import get_logger
from common.spark import get_spark
from config import settings
from streaming.schemas import TABLE_KEYS

SQL_DIR = Path(__file__).resolve().parents[1] / "sql" / "analytics"
log = get_logger("build_gold")


def build_gold(spark) -> dict:
    for table in TABLE_KEYS:
        path = f"{settings.SILVER}/{table}"
        if os.path.isdir(path):
            spark.read.parquet(path).createOrReplaceTempView(f"silver_{table}")
    out = {}
    for sql_file in sorted(SQL_DIR.glob("*.sql")):
        name = sql_file.stem
        target = f"{settings.GOLD}/{name}"
        spark.sql(sql_file.read_text().strip().rstrip(";")).write.mode("overwrite").parquet(target)
        out[name] = spark.read.parquet(target).count()
        log.info("gold_built", extra={"mart": name, "rows": out[name]})
    return out


def main():
    print(json.dumps(build_gold(get_spark("lakeflow-gold")), indent=2))


if __name__ == "__main__":
    main()
