from pyspark.sql import DataFrame, functions as F

from streaming.schemas import VALID_ORDER_STATUS

EMAIL_RE = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"


def rules(table: str):
    """Declarative data-quality rules per table. Built lazily because Column objects need an active SparkSession."""
    c = F.col
    return {
        "customers": [("id_not_null", c("id").isNotNull()), ("email_format", c("email").rlike(EMAIL_RE))],
        "products": [("id_not_null", c("id").isNotNull()), ("price_positive", c("price") > 0)],
        "orders": [("id_not_null", c("id").isNotNull()),
                   ("customer_fk_present", c("customer_id").isNotNull()),
                   ("amount_non_negative", c("total_amount") >= 0),
                   ("status_known", c("status").isin(VALID_ORDER_STATUS))],
        "order_items": [("id_not_null", c("id").isNotNull()),
                        ("quantity_positive", c("quantity") > 0),
                        ("unit_price_positive", c("unit_price") > 0)],
    }.get(table, [])


def with_failures(df: DataFrame, table: str) -> DataFrame:
    """Adds `dq_failures`: array of rule names the row violates. Deletes are never quarantined."""
    checks = [F.when(~(F.coalesce(cond, F.lit(False)) | (F.col("op") == "d")), F.lit(name)) for name, cond in rules(table)]
    arr = F.filter(F.array(*checks), lambda x: x.isNotNull()) if checks else F.array().cast("array<string>")
    return df.withColumn("dq_failures", arr)


def split_valid(df: DataFrame, table: str):
    flagged = with_failures(df, table)
    return (flagged.filter(F.size("dq_failures") == 0).drop("dq_failures"),
            flagged.filter(F.size("dq_failures") > 0))
