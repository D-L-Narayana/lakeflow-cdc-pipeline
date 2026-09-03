from pyspark.sql import DataFrame, functions as F

from streaming.schemas import VALID_ORDER_STATUS

EMAIL_RE = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

RULES = {
    "customers": [("id_not_null", F.col("id").isNotNull()), ("email_format", F.col("email").rlike(EMAIL_RE))],
    "products": [("id_not_null", F.col("id").isNotNull()), ("price_positive", F.col("price") > 0)],
    "orders": [("id_not_null", F.col("id").isNotNull()),
               ("customer_fk_present", F.col("customer_id").isNotNull()),
               ("amount_non_negative", F.col("total_amount") >= 0),
               ("status_known", F.col("status").isin(VALID_ORDER_STATUS))],
    "order_items": [("id_not_null", F.col("id").isNotNull()),
                    ("quantity_positive", F.col("quantity") > 0),
                    ("unit_price_positive", F.col("unit_price") > 0)],
}


def with_failures(df: DataFrame, table: str) -> DataFrame:
    """Adds `dq_failures`: array of rule names the row violates. Deletes are never quarantined."""
    checks = [F.when(~(F.coalesce(cond, F.lit(False)) | (F.col("op") == "d")), F.lit(name))
              for name, cond in RULES.get(table, [])]
    arr = F.filter(F.array(*checks), lambda x: x.isNotNull()) if checks else F.array().cast("array<string>")
    return df.withColumn("dq_failures", arr)


def split_valid(df: DataFrame, table: str):
    flagged = with_failures(df, table)
    return (flagged.filter(F.size("dq_failures") == 0).drop("dq_failures"),
            flagged.filter(F.size("dq_failures") > 0))
