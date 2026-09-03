import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
CDC_TOPIC_PATTERN = os.getenv("CDC_TOPIC_PATTERN", r"shop\.public\.(customers|products|orders|order_items)")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")
MAX_OFFSETS_PER_TRIGGER = os.getenv("MAX_OFFSETS_PER_TRIGGER", "50000")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "10 seconds")

LAKE_ROOT = os.getenv("LAKE_ROOT", "./lake")
BRONZE = f"{LAKE_ROOT}/bronze"
SILVER = f"{LAKE_ROOT}/silver"
GOLD = f"{LAKE_ROOT}/gold"
QUARANTINE = f"{LAKE_ROOT}/quarantine"
CHECKPOINTS = f"{LAKE_ROOT}/checkpoints"
STATS = f"{LAKE_ROOT}/_stats"

PG_DSN = os.getenv("PG_DSN", "postgresql://shop:shop@localhost:5432/shop")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
