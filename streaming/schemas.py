from pyspark.sql.types import DoubleType as D, IntegerType as I, LongType as L, StringType as S, StructField as F, StructType

TABLE_SCHEMAS = {
    "customers": StructType([F("id", I()), F("name", S()), F("email", S()), F("city", S()), F("tier", S()),
                             F("created_at", S()), F("updated_at", S())]),
    "products": StructType([F("id", I()), F("name", S()), F("category", S()), F("price", D()), F("created_at", S())]),
    "orders": StructType([F("id", L()), F("customer_id", I()), F("status", S()), F("total_amount", D()),
                          F("created_at", S()), F("updated_at", S())]),
    "order_items": StructType([F("id", L()), F("order_id", L()), F("product_id", I()), F("quantity", I()),
                               F("unit_price", D())]),
}

TABLE_KEYS = {"customers": "id", "products": "id", "orders": "id", "order_items": "id"}

# dimensions that keep history (SCD Type 2) and the attributes whose change opens a new version
SCD2_TABLES = {"customers": ["name", "email", "city", "tier"]}

VALID_ORDER_STATUS = ["placed", "paid", "shipped", "delivered", "cancelled", "refunded"]
