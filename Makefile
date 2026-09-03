.PHONY: up connector simulate bronze silver gold backfill dq demo test down

up: ; docker compose up -d
connector: ; curl -s -X POST -H "Content-Type: application/json" --data @config/debezium-postgres-connector.json http://localhost:8083/connectors | python -m json.tool
simulate: ; python -m generators.oltp_simulator --rate 20 --duration 600
bronze: ; python -m streaming.bronze_ingest
silver: ; python -m streaming.silver_upsert
gold: ; python -m batch.build_gold
backfill: ; python -m batch.backfill
dq: ; python -m batch.dq_gate --max-quarantine-pct 5
demo: ; python -m scripts.local_demo 100000
test: ; python -m pytest
down: ; docker compose down -v
