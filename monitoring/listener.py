from pyspark.sql.streaming import StreamingQueryListener

from common.logging_conf import get_logger

log = get_logger("streaming.metrics")


class MetricsListener(StreamingQueryListener):
    """Emits per-micro-batch throughput/latency as JSON log lines (scrape into ELK, Datadog, Log Analytics)."""

    def onQueryStarted(self, e):
        log.info("query_started", extra={"query": e.name, "query_id": str(e.id)})

    def onQueryProgress(self, e):
        p = e.progress
        d = p.durationMs or {}
        log.info("query_progress", extra={
            "query": p.name, "batch_id": p.batchId, "input_rows": p.numInputRows,
            "input_rows_per_sec": round(p.inputRowsPerSecond or 0, 1),
            "processed_rows_per_sec": round(p.processedRowsPerSecond or 0, 1),
            "trigger_ms": d.get("triggerExecution"), "add_batch_ms": d.get("addBatch"),
        })

    def onQueryIdle(self, e):
        pass

    def onQueryTerminated(self, e):
        log.info("query_terminated", extra={"query_id": str(e.id), "exception": e.exception})
