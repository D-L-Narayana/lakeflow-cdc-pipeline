"""Data-quality gate for orchestration: fail the DAG when quarantine share exceeds the threshold."""
import argparse
import json
import sys

from config import settings


def main(max_pct: float) -> int:
    with open(f"{settings.STATS}/backfill.json") as f:
        stats = json.load(f)
    worst = max(((t, 100.0 * s["quarantined"] / max(s["events"], 1)) for t, s in stats["tables"].items()),
                key=lambda x: x[1])
    verdict = {"worst_table": worst[0], "quarantine_pct": round(worst[1], 3), "threshold_pct": max_pct,
               "status": "PASS" if worst[1] <= max_pct else "FAIL"}
    print(json.dumps(verdict))
    return 0 if verdict["status"] == "PASS" else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-quarantine-pct", type=float, default=5.0)
    sys.exit(main(ap.parse_args().max_quarantine_pct))
