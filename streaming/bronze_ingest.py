"""Stage 1 — Kafka (Debezium CDC topics) -> bronze parquet, append-only, exactly-once via checkpointed file sink."""
from common.logging_conf import get_logger
from common.spark import get_spark
from config import settings
from monitoring.listener import MetricsListener
from streaming.transforms import parse_debezium


def main():
    log = get_logger("bronze_ingest")
    spark = get_spark("lakeflow-bronze", with_kafka=True)
    spark.streams.addListener(MetricsListener())
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP)
           .option("subscribePattern", settings.CDC_TOPIC_PATTERN)
           .option("startingOffsets", settings.STARTING_OFFSETS)
           .option("maxOffsetsPerTrigger", settings.MAX_OFFSETS_PER_TRIGGER)
           .option("failOnDataLoss", "false")
           .load())
    query = (parse_debezium(raw).writeStream.format("parquet")
             .option("path", settings.BRONZE)
             .option("checkpointLocation", f"{settings.CHECKPOINTS}/bronze")
             .partitionBy("table", "ingest_date")
             .trigger(processingTime=settings.TRIGGER_INTERVAL)
             .queryName("bronze_ingest").start())
    log.info("bronze_stream_started", extra={"topics": settings.CDC_TOPIC_PATTERN, "sink": settings.BRONZE})
    query.awaitTermination()


if __name__ == "__main__":
    main()
