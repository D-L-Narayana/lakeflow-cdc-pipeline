import json

from generators.debezium_events import envelope
from streaming.transforms import apply_cdc, flatten_table, latest_per_key, parse_debezium, scd2_from_history, scd2_merge

CUST = ("op string, ts_ms long, lsn long, id int, name string, email string, city string, tier string, "
        "created_at string, updated_at string")
TRACKED = ["name", "email", "city", "tier"]


def _df(spark, events):
    return spark.createDataFrame([(json.dumps(e),) for e in events], "value string")


def _cust(i, city):
    return {"id": i, "name": f"C{i}", "email": f"c{i}@x.com", "city": city, "tier": "bronze",
            "created_at": "2025-01-01T00:00:00Z", "updated_at": "2025-01-01T00:00:00Z"}


def test_parse_and_flatten_uses_before_image_for_deletes(spark):
    ev = [envelope("customers", "c", _cust(1, "Chennai"), ts_ms=1000, lsn=1),
          envelope("customers", "u", _cust(1, "Pune"), before=_cust(1, "Chennai"), ts_ms=2000, lsn=2),
          envelope("customers", "d", None, before=_cust(1, "Pune"), ts_ms=3000, lsn=3)]
    cdc = parse_debezium(_df(spark, ev))
    assert cdc.count() == 3 and {r.op for r in cdc.collect()} == {"c", "u", "d"}
    assert {r.table for r in cdc.collect()} == {"customers"}
    rows = flatten_table(cdc, "customers").orderBy("lsn").collect()
    assert [r.city for r in rows] == ["Chennai", "Pune", "Pune"]


def test_latest_per_key_orders_by_ts_then_lsn(spark):
    df = spark.createDataFrame([(1, 10, 1, "a"), (1, 10, 2, "b"), (1, 9, 9, "c")], "id int, ts_ms long, lsn long, v string")
    assert latest_per_key(df, ["id"]).first().v == "b"


def test_apply_cdc_upsert_and_delete(spark):
    current = spark.createDataFrame([(1, "a"), (2, "b")], "id int, v string")
    changes = spark.createDataFrame([("u", 10, 1, 2, "b2"), ("d", 11, 2, 1, "a"), ("c", 12, 3, 3, "c"), ("u", 13, 4, 2, "b3")],
                                    "op string, ts_ms long, lsn long, id int, v string")
    out = {r.id: r.v for r in apply_cdc(current, changes, ["id"]).collect()}
    assert out == {2: "b3", 3: "c"}


def test_scd2_merge_opens_and_closes_versions(spark):
    base = spark.createDataFrame([("c", 1000, 1, 1, "C1", "c1@x.com", "Chennai", "bronze", "t0", "t0")], CUST)
    dim = scd2_merge(None, base, "id", TRACKED)
    assert dim.count() == 1 and dim.first().is_current
    upd = spark.createDataFrame([("u", 5000, 2, 1, "C1", "c1@x.com", "Pune", "bronze", "t0", "t1")], CUST)
    dim2 = scd2_merge(dim, upd, "id", TRACKED)
    rows = sorted(dim2.collect(), key=lambda r: r.effective_from)
    assert len(rows) == 2
    assert rows[0].is_current is False and rows[0].effective_to is not None
    assert rows[1].city == "Pune" and rows[1].is_current is True
    noop = spark.createDataFrame([("u", 9000, 3, 1, "C1", "c1@x.com", "Pune", "bronze", "t0", "t2")], CUST)
    assert scd2_merge(dim2, noop, "id", TRACKED).count() == 2
    delete = spark.createDataFrame([("d", 9500, 4, 1, "C1", "c1@x.com", "Pune", "bronze", "t0", "t2")], CUST)
    dim3 = scd2_merge(dim2, delete, "id", TRACKED)
    assert dim3.count() == 2 and dim3.filter("is_current").count() == 0


def test_scd2_from_history_collapses_noops_and_honours_deletes(spark):
    hist = spark.createDataFrame([("c", 1000, 1, 1, "Chennai"), ("u", 2000, 2, 1, "Chennai"), ("u", 3000, 3, 1, "Pune"),
                                  ("d", 4000, 4, 1, "Pune"), ("c", 1500, 5, 2, "Delhi")],
                                 "op string, ts_ms long, lsn long, id int, city string")
    out = scd2_from_history(hist, "id", ["city"]).collect()
    k1 = sorted([r for r in out if r.id == 1], key=lambda r: r.effective_from)
    assert [r.city for r in k1] == ["Chennai", "Pune"]
    assert all(r.effective_to is not None for r in k1) and not any(r.is_current for r in k1)
    assert [r.is_current for r in out if r.id == 2] == [True]
