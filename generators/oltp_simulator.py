"""Drives the source Postgres like a real shop: inserts, status updates, profile changes, deletes.
Every statement becomes a WAL entry -> Debezium -> Kafka -> the pipeline."""
import argparse
import random
import time

import psycopg2

from common.logging_conf import get_logger
from config import settings
from generators.debezium_events import CITIES, STATUS_FLOW, TIERS


def main(rate: float, duration: int, dirty_rate: float):
    log = get_logger("oltp_simulator")
    rng = random.Random()
    conn = psycopg2.connect(settings.PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT id, price FROM products")
    products = cur.fetchall()
    cur.execute("SELECT id FROM customers")
    customers = [r[0] for r in cur.fetchall()]
    end, n = time.time() + duration, 0
    while time.time() < end:
        cid = rng.choice(customers)
        items = [(rng.choice(products), rng.randint(1, 4)) for _ in range(rng.randint(1, 3))]
        total = float(sum(p * q for (_, p), q in items))
        if rng.random() < dirty_rate:
            total = -total
        cur.execute("INSERT INTO orders(customer_id, status, total_amount) VALUES (%s, 'placed', %s) RETURNING id",
                    (cid, round(total, 2)))
        oid = cur.fetchone()[0]
        cur.executemany("INSERT INTO order_items(order_id, product_id, quantity, unit_price) VALUES (%s, %s, %s, %s)",
                        [(oid, pid, q, p) for (pid, p), q in items])
        for s in STATUS_FLOW[1:1 + rng.randint(0, 3)]:
            cur.execute("UPDATE orders SET status = %s, updated_at = now() WHERE id = %s", (s, oid))
        if rng.random() < 0.05:
            cur.execute("UPDATE customers SET city = %s, tier = %s, updated_at = now() WHERE id = %s",
                        (rng.choice(CITIES), rng.choice(TIERS), cid))
        if rng.random() < 0.02:
            cur.execute("DELETE FROM order_items WHERE id = (SELECT min(id) FROM order_items WHERE order_id = %s)", (oid,))
        n += 1
        if n % 100 == 0:
            log.info("progress", extra={"orders_written": n})
        time.sleep(1.0 / rate)
    log.info("done", extra={"orders_written": n})


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=20, help="orders per second")
    ap.add_argument("--duration", type=int, default=600, help="seconds")
    ap.add_argument("--dirty-rate", type=float, default=0.02)
    a = ap.parse_args()
    main(a.rate, a.duration, a.dirty_rate)
