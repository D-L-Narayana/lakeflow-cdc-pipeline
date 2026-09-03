# LakeFlow — Real-time CDC Lakehouse Pipeline

Postgres → Debezium (CDC) → Kafka → Spark Structured Streaming → Bronze / Silver / Gold parquet lakehouse,
with data-quality quarantine, SCD Type 2 history, idempotent upserts, batch backfill, JSON observability
and an Airflow DAG for nightly convergence.

```
 ┌──────────┐  WAL   ┌──────────┐  JSON   ┌────────┐   readStream   ┌──────────────────────────────┐
 │ Postgres │──────▶│ Debezium │───────▶│ Kafka  │──────────────▶│ Spark Structured Streaming   │
 │ (OLTP)   │ CDC    │ Connect  │  events │ topics │  (subscribe    │  bronze_ingest.py            │
 └──────────┘        └──────────┘         └────────┘   pattern)     └──────────────┬───────────────┘
                                                                                   ▼
      lake/bronze  (append-only, partitioned by table / ingest_date, checkpointed = exactly-once sink)
                                                                                   ▼  foreachBatch
      lake/silver  (typed, validated, deduped current state · customers = SCD2 history)  ◀── silver_upsert.py
      lake/quarantine  (rows that failed DQ rules + reason)                              ◀── quality.py
                                                                                   ▼  Spark SQL
      lake/gold    (daily revenue · product ranks · customer LTV)                       ◀── build_gold.py
```

## What it demonstrates

| JD requirement | Where |
|---|---|
| Scalable ingestion of real-time streams, **CDC events** and batch data | `streaming/bronze_ingest.py` (Kafka source), `batch/backfill.py` (batch), Debezium connector config |
| High-performance processing, structured + semi-structured, **harmonization** | `streaming/transforms.py` — JSON envelope → typed tables, latest-per-key dedupe, upsert/delete semantics |
| Scheduling, **orchestrating and validating** pipelines | `orchestration/airflow/dags/lakeflow_dag.py`, `batch/dq_gate.py`, `streaming/quality.py` |
| **Exception handling and log monitoring** | JSON structured logs (`common/logging_conf.py`), `StreamingQueryListener` metrics, dead-letter quarantine |
| Data warehousing concepts | Bronze/Silver/Gold medallion, SCD Type 2 dimension, star-schema style gold marts with window functions |
| Distributed systems / Hadoop ecosystem | Spark, Kafka, HDFS-compatible parquet layout (Hive-style partitions), checkpoint-based fault tolerance |

## Quickstart (full stack, Docker)

```bash
make up            # Postgres (wal_level=logical) + Kafka (KRaft) + Debezium Connect + Kafka UI
make connector     # register the Postgres CDC connector -> topics shop.public.<table>
make simulate      # drive the OLTP db: inserts, status updates, profile changes, deletes
make bronze        # terminal 2: Kafka -> bronze (streaming)
make silver        # terminal 3: bronze -> silver (streaming, foreachBatch upserts)
make gold          # gold marts from silver
```

## Quickstart (no Docker — 60 seconds)

```bash
pip install -r requirements.txt
make test          # pytest: CDC parsing, upsert/delete, SCD2 (incremental + history), DQ rules
make demo          # synthetic Debezium stream -> bronze -> silver -> gold, prints a DEMO_SUMMARY json
```

## Results (local, 2 vCPU laptop-class sandbox)

`make demo` — synthetic Debezium stream for 100,000 orders (`docs/demo_summary.json`):

| Metric | Value |
|---|---|
| CDC events replayed (c / u / d across 4 tables) | **510,663** |
| Bronze write (parse envelope, partition by table & ingest_date) | 11.2 s |
| Silver rebuild (flatten, DQ, upsert/delete, SCD2 for 4 tables) | 15.5 s |
| Gold marts (3 Spark SQL marts) | 2.9 s |
| End-to-end | **29.5 s ≈ 17,300 events/s** on 2 vCPUs |
| Rows quarantined | 5,239 — 103 invalid emails (customers), 5,136 negative-amount order events |
| Silver state | customers 6,807 SCD2 versions over 5,000 keys · orders 97,982 · order_items 249,796 (250 deletes honoured) · products 300 |
| Tests | `7 passed` (pytest on a local SparkSession) |


## Design decisions

- **Bronze is immutable and schema-agnostic.** The Debezium envelope is flattened but payloads stay JSON, so a
  source schema change never breaks ingestion; typing happens in silver where it can be versioned.
- **Idempotency over "exactly once" promises.** Silver is derived with latest-per-key on `(ts_ms, lsn)` — the WAL
  commit order — so replaying the same Kafka offsets or re-running a backfill produces identical tables.
- **Deletes are first-class.** `REPLICA IDENTITY FULL` + `before` images let a delete remove the current row
  (or close an SCD2 version) instead of leaving zombies.
- **SCD2 two ways.** `scd2_merge` for incremental micro-batches, `scd2_from_history` for full rebuilds; both
  collapse no-op updates so a noisy `updated_at` touch does not create phantom versions.
- **Quality as a gate, not a filter.** Failed rows go to `quarantine/<table>` with the rule names; the DAG fails
  when quarantine share crosses a threshold, so bad upstream data pages a human instead of polluting gold.
- **Same code path for stream and batch.** `flatten_table` / `split_valid` / `apply_cdc` power both
  `silver_upsert.py` (streaming) and `backfill.py` (batch) — one set of tests, one set of bugs.
- **Portable to Databricks / cloud.** Swap `LAKE_ROOT` to `abfss://` or `s3a://`, replace the directory swap in
  `common/io.py` with a Delta `MERGE INTO`, and the rest is unchanged.

## Repo layout

```
config/          settings (env-driven) + Debezium connector JSON
streaming/       schemas · transforms (parse/flatten/dedupe/upsert/SCD2) · quality rules · bronze & silver jobs
batch/           backfill (rebuild silver from bronze) · build_gold (Spark SQL marts) · dq_gate
generators/      Debezium-shaped event synthesizer · Postgres OLTP simulator
monitoring/      StreamingQueryListener -> JSON metrics
orchestration/   Airflow DAG (nightly rebuild -> DQ gate -> gold)
sql/             Postgres init (with REPLICA IDENTITY FULL) · gold mart SQL (window functions)
tests/           pytest suite on a local SparkSession
```

## Stack

Python 3.12 · Apache Spark 3.5 (Structured Streaming, Spark SQL) · Apache Kafka 3.6 (KRaft) · Debezium 2.5 ·
PostgreSQL 15 · Parquet · Apache Airflow · Docker Compose · pytest

MIT License.
