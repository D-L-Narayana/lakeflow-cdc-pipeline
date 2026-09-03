from streaming.quality import split_valid

CUST = ("op string, ts_ms long, lsn long, id int, name string, email string, city string, tier string, "
        "created_at string, updated_at string")
ORD = "op string, ts_ms long, lsn long, id long, customer_id int, status string, total_amount double, created_at string, updated_at string"


def test_bad_email_is_quarantined_but_deletes_pass(spark):
    df = spark.createDataFrame([("c", 1, 1, 1, "C1", "ok@x.com", "Chennai", "bronze", "t", "t"),
                                ("c", 2, 2, 2, "C2", "bad@@x", "Chennai", "bronze", "t", "t"),
                                ("d", 3, 3, 3, "C3", "bad@@x", "Chennai", "bronze", "t", "t")], CUST)
    valid, bad = split_valid(df, "customers")
    assert valid.count() == 2 and bad.count() == 1
    assert bad.first().dq_failures == ["email_format"]


def test_multiple_rule_failures_are_all_recorded(spark):
    df = spark.createDataFrame([("c", 1, 1, 1, None, "weird", -5.0, "t", "t"),
                                ("c", 2, 2, 2, 7, "paid", 99.0, "t", "t")], ORD)
    valid, bad = split_valid(df, "orders")
    assert valid.count() == 1
    assert sorted(bad.first().dq_failures) == ["amount_non_negative", "customer_fk_present", "status_known"]
