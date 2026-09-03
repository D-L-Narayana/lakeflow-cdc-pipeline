"""Debezium-shaped CDC envelopes (same JSON the connector emits with schemas.enable=false).
Used by the unit tests and by scripts/local_demo.py to run the whole pipeline without Docker."""
import random
import time

CITIES = ["Chennai", "Bengaluru", "Hyderabad", "Visakhapatnam", "Mumbai", "Pune", "Delhi", "Kolkata"]
TIERS = ["bronze", "silver", "gold"]
CATEGORIES = ["electronics", "grocery", "fashion", "home", "sports", "books"]
STATUS_FLOW = ["placed", "paid", "shipped", "delivered"]


def envelope(table, op, after=None, before=None, ts_ms=None, lsn=0):
    ts = int(time.time() * 1000) if ts_ms is None else ts_ms
    return {"before": before, "after": after,
            "source": {"version": "2.5.0.Final", "connector": "postgresql", "name": "shop", "ts_ms": ts,
                       "db": "shop", "schema": "public", "table": table, "lsn": lsn},
            "op": op, "ts_ms": ts}


def _iso(ms):
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ms / 1000)) + "Z"


def synth_stream(n_orders=100_000, seed=42, dirty_rate=0.02, start_ms=1_756_684_800_000):
    """Yields a realistic commit-ordered CDC stream: snapshot inserts, order lifecycles (u), customer profile
    changes (SCD2 triggers), hard deletes, and a small share of dirty rows for the quality layer."""
    rng = random.Random(seed)
    state = {"lsn": 0, "ts": start_ms}

    def nxt():
        state["lsn"] += rng.randint(1, 50)
        state["ts"] += rng.randint(0, 400)
        return state["ts"], state["lsn"]

    n_customers, n_products = max(100, n_orders // 20), 300
    prices, cust_created = {}, {}
    for pid in range(1, n_products + 1):
        t, l = nxt()
        prices[pid] = round(rng.uniform(49, 4999), 2)
        yield envelope("products", "c", {"id": pid, "name": f"Product {pid}", "category": rng.choice(CATEGORIES),
                                         "price": prices[pid], "created_at": _iso(t)}, ts_ms=t, lsn=l)
    for cid in range(1, n_customers + 1):
        t, l = nxt()
        cust_created[cid] = _iso(t)
        email = f"user{cid}@example.com" if rng.random() > dirty_rate else f"user{cid}@@bad"
        yield envelope("customers", "c", {"id": cid, "name": f"Customer {cid}", "email": email,
                                          "city": rng.choice(CITIES), "tier": "bronze",
                                          "created_at": cust_created[cid], "updated_at": cust_created[cid]}, ts_ms=t, lsn=l)
    item_id = 0
    for oid in range(1, n_orders + 1):
        cid, items, total = rng.randint(1, n_customers), [], 0.0
        for _ in range(rng.randint(1, 4)):
            pid, q = rng.randint(1, n_products), rng.randint(1, 5)
            item_id += 1
            total += q * prices[pid]
            items.append({"id": item_id, "order_id": oid, "product_id": pid, "quantity": q, "unit_price": prices[pid]})
        if rng.random() < dirty_rate:
            total = -abs(total)
        t, l = nxt()
        created = _iso(t)
        prev = {"id": oid, "customer_id": cid, "status": "placed", "total_amount": round(total, 2),
                "created_at": created, "updated_at": created}
        yield envelope("orders", "c", prev, ts_ms=t, lsn=l)
        for it in items:
            t, l = nxt()
            yield envelope("order_items", "c", it, ts_ms=t, lsn=l)
        for s in STATUS_FLOW[1:1 + rng.randint(0, 3)]:
            t, l = nxt()
            cur = dict(prev, status=s, updated_at=_iso(t))
            yield envelope("orders", "u", cur, before=prev, ts_ms=t, lsn=l)
            prev = cur
        if rng.random() < 0.03:
            t, l = nxt()
            yield envelope("orders", "u", dict(prev, status="cancelled", updated_at=_iso(t)), before=prev, ts_ms=t, lsn=l)
        if oid % 50 == 0:
            t, l = nxt()
            yield envelope("customers", "u", {"id": cid, "name": f"Customer {cid}", "email": f"user{cid}@example.com",
                                              "city": rng.choice(CITIES), "tier": rng.choice(TIERS),
                                              "created_at": cust_created[cid], "updated_at": _iso(t)}, ts_ms=t, lsn=l)
        if oid % 400 == 0:
            t, l = nxt()
            yield envelope("order_items", "d", None, before=items[0], ts_ms=t, lsn=l)
