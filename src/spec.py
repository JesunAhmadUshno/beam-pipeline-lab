"""Frozen transformation registry, mirroring the comparison study.

Adapted from JesunAhmadUshno/data-engineering-scale-lab src/spec.py
(Apache License 2.0, same author). The transformation keys, output
relation names, and canonical output columns are identical to B2's,
because the correctness gate of this study is agreement with B2's
checksums, which are computed over exactly these columns in this
order (the checksum orders by all of them).
"""

LINEITEM_COLS = [
    "l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity",
    "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus",
    "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct",
    "l_shipmode", "l_comment",
]

TRANSFORMATIONS = [
    {
        "key": "t1",
        "output": "filtered_lineitem",
        "columns": LINEITEM_COLS,
        "depends": ["lineitem"],
    },
    {
        "key": "t2",
        "output": "lineitem_orders",
        "columns": LINEITEM_COLS + [
            "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate",
            "o_orderpriority",
        ],
        "depends": ["lineitem", "orders"],
    },
    {
        "key": "t3",
        "output": "daily_sales",
        "columns": ["order_date", "orders_count", "line_count", "gross_revenue", "net_revenue"],
        "depends": ["lineitem", "orders"],
    },
    {
        "key": "t4",
        "output": "ranked_sales",
        "columns": ["order_date", "l_orderkey", "l_linenumber", "line_amount", "rnk"],
        "depends": ["lineitem_orders"],
    },
    {
        "key": "t5",
        "output": "converted",
        "columns": [
            "l_orderkey", "l_linenumber", "quantity_int", "extended_price",
            "total_price", "shipdate_text", "orderdate_text", "discount_pct",
        ],
        "depends": ["lineitem", "orders"],
    },
    {
        "key": "t6",
        "output": "top_10_daily",
        "columns": ["order_date", "orders_count", "gross_revenue", "top_line_amount"],
        "depends": ["daily_sales", "ranked_sales"],
    },
]

BY_KEY = {t["key"]: t for t in TRANSFORMATIONS}
ORDER = [t["key"] for t in TRANSFORMATIONS]
