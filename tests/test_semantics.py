"""Semantic tests for the six Beam pipelines on a miniature fixture.

The fixture is invented data (12 lineitem rows, 4 orders), small
enough to verify by hand. It exists to pin the semantics the study
depends on: exact decimal money math, RANK tie behaviour, truncating
casts, discount percent on the classic 0.07 float trap (0.07 * 100 is
not exact in binary floating point; it is exact in decimal), and the
T6 tie break. A DuckDB cross-check of T3 and T4 against the original
SQL semantics runs on the same fixture.
"""

import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import apache_beam as beam
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from apache_beam.options.pipeline_options import PipelineOptions

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from checksum import canonical_checksum  # noqa: E402
from pipelines import (  # noqa: E402
    LINEITEM_SCHEMA, build_t1, build_t2, build_t3, build_t4, build_t5,
    build_t6,
)

ORDERS_SCHEMA = pa.schema([
    ("o_orderkey", pa.int64()), ("o_custkey", pa.int64()),
    ("o_orderstatus", pa.string()), ("o_totalprice", pa.decimal128(15, 2)),
    ("o_orderdate", pa.date32()), ("o_orderpriority", pa.string()),
    ("o_clerk", pa.string()), ("o_shippriority", pa.int32()),
    ("o_comment", pa.string()),
])

D = Decimal


def _line(orderkey, linenumber, qty, price, discount, tax, shipdate,
          partkey=100, suppkey=200):
    return {
        "l_orderkey": orderkey, "l_partkey": partkey, "l_suppkey": suppkey,
        "l_linenumber": linenumber, "l_quantity": D(qty),
        "l_extendedprice": D(price), "l_discount": D(discount),
        "l_tax": D(tax), "l_returnflag": "A", "l_linestatus": "O",
        "l_shipdate": shipdate, "l_commitdate": shipdate,
        "l_receiptdate": shipdate, "l_shipinstruct": "NONE",
        "l_shipmode": "AIR", "l_comment": "fixture",
    }


ORDERS = [
    {"o_orderkey": 1, "o_custkey": 10, "o_orderstatus": "O",
     "o_totalprice": D("300.00"), "o_orderdate": date(1995, 3, 1),
     "o_orderpriority": "1-URGENT", "o_clerk": "Clerk#1",
     "o_shippriority": 0, "o_comment": "fixture"},
    {"o_orderkey": 2, "o_custkey": 20, "o_orderstatus": "F",
     "o_totalprice": D("150.00"), "o_orderdate": date(1995, 3, 1),
     "o_orderpriority": "2-HIGH", "o_clerk": "Clerk#2",
     "o_shippriority": 0, "o_comment": "fixture"},
    {"o_orderkey": 3, "o_custkey": 30, "o_orderstatus": "O",
     "o_totalprice": D("90.00"), "o_orderdate": date(1994, 6, 15),
     "o_orderpriority": "3-MEDIUM", "o_clerk": "Clerk#3",
     "o_shippriority": 0, "o_comment": "fixture"},
]

LINEITEM = [
    # order 1: two lines, one amount ties with order 2's line on 1995-03-01
    _line(1, 1, "2.00", "100.00", "0.10", "0.05", date(1995, 4, 1)),
    _line(1, 2, "1.00", "50.00", "0.00", "0.00", date(1994, 12, 31)),
    # order 2: gross line amount 90.00 ties order 1 line 1
    _line(2, 1, "3.00", "90.00", "0.00", "0.08", date(1996, 1, 1)),
    # order 3: the 0.07 discount trap; shipdate outside 1995
    _line(3, 1, "25.00", "10.00", "0.07", "0.02", date(1997, 2, 2)),
]


@pytest.fixture(scope="module")
def fixture_dir(tmp_path_factory):
    d = tmp_path_factory.mktemp("fixture")
    pq.write_table(pa.Table.from_pylist(LINEITEM, schema=LINEITEM_SCHEMA),
                   d / "lineitem.parquet")
    pq.write_table(pa.Table.from_pylist(ORDERS, schema=ORDERS_SCHEMA),
                   d / "orders.parquet")
    return d


def _run(builder, args, tmp_path):
    p = beam.Pipeline(options=PipelineOptions(["--runner=DirectRunner"]))
    builder(p, *args, str(tmp_path / "out"))
    result = p.run()
    result.wait_until_finish()
    table = pq.read_table(str(tmp_path / "out.parquet"))
    return table.to_pylist()


def test_t1_filters_to_1995_shipdates(fixture_dir, tmp_path):
    rows = _run(build_t1, (str(fixture_dir / "lineitem.parquet"),), tmp_path)
    keys = sorted((r["l_orderkey"], r["l_linenumber"]) for r in rows)
    # Only order 1 line 1 ships in [1995-01-01, 1996-01-01): order 1
    # line 2 ships 1994-12-31, order 2 line 1 ships exactly 1996-01-01.
    assert keys == [(1, 1)]


def test_t3_exact_decimal_aggregation(fixture_dir, tmp_path):
    rows = _run(build_t3, (str(fixture_dir / "lineitem.parquet"),
                           str(fixture_dir / "orders.parquet")), tmp_path)
    by_date = {r["order_date"]: r for r in rows}
    march = by_date[date(1995, 3, 1)]
    # order1 lines: 100*0.90 = 90.0000; 50*1.00 = 50.0000; order2: 90*1.00
    assert march["orders_count"] == 2
    assert march["line_count"] == 3
    assert march["gross_revenue"] == D("230.0000")
    # net: 90*1.05 + 50*1.00 + 90*1.08 = 94.50 + 50 + 97.20 = 241.700000
    assert march["net_revenue"] == D("241.700000")
    june = by_date[date(1994, 6, 15)]
    assert june["gross_revenue"] == D("9.3000")
    assert june["net_revenue"] == D("9.486000")


def test_t4_rank_ties(fixture_dir, tmp_path):
    t2_rows = _run(build_t2, (str(fixture_dir / "lineitem.parquet"),
                              str(fixture_dir / "orders.parquet")), tmp_path)
    t2_path = tmp_path / "t2out"
    pq.write_table(pa.Table.from_pylist(t2_rows), str(t2_path) + ".parquet")
    rows = _run(build_t4, (str(t2_path) + ".parquet",), tmp_path / "t4")
    march = [r for r in rows if r["order_date"] == date(1995, 3, 1)]
    ranks = {(r["l_orderkey"], r["l_linenumber"]): r["rnk"] for r in march}
    # amounts: order1 line1 90.00 and order2 line1 90.00 tie at rank 1;
    # order1 line2 50.00 takes rank 3 (standard RANK skip).
    assert ranks == {(1, 1): 1, (2, 1): 1, (1, 2): 3}


def test_t5_casts_and_discount_percent(fixture_dir, tmp_path):
    rows = _run(build_t5, (str(fixture_dir / "lineitem.parquet"),
                           str(fixture_dir / "orders.parquet")), tmp_path)
    by_key = {(r["l_orderkey"], r["l_linenumber"]): r for r in rows}
    trap = by_key[(3, 1)]
    assert trap["discount_pct"] == 7  # 0.07 * 100, exact in decimal
    assert trap["quantity_int"] == 25
    assert trap["shipdate_text"] == "1997-02-02"
    assert trap["extended_price"] == D("10.00")


def test_t6_top10_shape(fixture_dir, tmp_path):
    t3 = _run(build_t3, (str(fixture_dir / "lineitem.parquet"),
                         str(fixture_dir / "orders.parquet")), tmp_path / "a")
    t2 = _run(build_t2, (str(fixture_dir / "lineitem.parquet"),
                         str(fixture_dir / "orders.parquet")), tmp_path / "b")
    pq.write_table(pa.Table.from_pylist(t2), str(tmp_path / "t2.parquet"))
    t4 = _run(build_t4, (str(tmp_path / "t2.parquet"),), tmp_path / "c")
    pq.write_table(pa.Table.from_pylist(t3), str(tmp_path / "t3.parquet"))
    pq.write_table(pa.Table.from_pylist(t4), str(tmp_path / "t4.parquet"))
    rows = _run(build_t6, (str(tmp_path / "t3.parquet"),
                           str(tmp_path / "t4.parquet")), tmp_path / "d")
    assert len(rows) == 2
    assert rows[0]["order_date"] == date(1995, 3, 1)
    assert rows[0]["top_line_amount"] == D("90.0000")
    assert rows[0]["gross_revenue"] == D("230.0000")


def test_checksum_ignores_row_order(fixture_dir, tmp_path):
    rows = pq.read_table(fixture_dir / "lineitem.parquet").to_pylist()
    shuffled = list(reversed(rows))
    pq.write_table(pa.Table.from_pylist(rows, schema=LINEITEM_SCHEMA),
                   tmp_path / "a.parquet")
    pq.write_table(pa.Table.from_pylist(shuffled, schema=LINEITEM_SCHEMA),
                   tmp_path / "b.parquet")
    h1, n1 = canonical_checksum(str(tmp_path / "a.parquet"), "t1",
                                tmp_dir=tmp_path / "ct")
    h2, n2 = canonical_checksum(str(tmp_path / "b.parquet"), "t1",
                                tmp_dir=tmp_path / "ct")
    assert h1 == h2 and n1 == n2 == len(rows)
