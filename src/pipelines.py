"""The six transformations as Apache Beam pipelines (Python SDK).

Semantics are fixed by the comparison study's SQL (B2 sql/t1 to t6);
see PREREGISTRATION.md Section 6. Money arithmetic is exact decimal:
lineitem measures arrive from Parquet as Python Decimal at scale 2,
so gross products carry scale 4 and net products scale 6, matching
the DECIMAL(38,4) and DECIMAL(38,6) columns B2's arms stored.

Implementation choices that do not change semantics (documented in
the README portability analysis): dimension joins are side-input
joins, T3 aggregates with CombinePerKey, T4 groups by key to rank
inside each date partition, T6 takes its top 10 with combiners.Top.
"""

from datetime import date
from decimal import Decimal

import apache_beam as beam
import pyarrow as pa
from apache_beam import pvalue

DEC152 = pa.decimal128(15, 2)

LINEITEM_SCHEMA = pa.schema([
    ("l_orderkey", pa.int64()), ("l_partkey", pa.int64()),
    ("l_suppkey", pa.int64()), ("l_linenumber", pa.int64()),
    ("l_quantity", DEC152), ("l_extendedprice", DEC152),
    ("l_discount", DEC152), ("l_tax", DEC152),
    ("l_returnflag", pa.string()), ("l_linestatus", pa.string()),
    ("l_shipdate", pa.date32()), ("l_commitdate", pa.date32()),
    ("l_receiptdate", pa.date32()), ("l_shipinstruct", pa.string()),
    ("l_shipmode", pa.string()), ("l_comment", pa.string()),
])

T2_SCHEMA = pa.schema(list(LINEITEM_SCHEMA) + [
    ("o_custkey", pa.int64()), ("o_orderstatus", pa.string()),
    ("o_totalprice", DEC152), ("o_orderdate", pa.date32()),
    ("o_orderpriority", pa.string()),
])

T3_SCHEMA = pa.schema([
    ("order_date", pa.date32()), ("orders_count", pa.int64()),
    ("line_count", pa.int64()), ("gross_revenue", pa.decimal128(38, 4)),
    ("net_revenue", pa.decimal128(38, 6)),
])

T4_SCHEMA = pa.schema([
    ("order_date", pa.date32()), ("l_orderkey", pa.int64()),
    ("l_linenumber", pa.int64()), ("line_amount", pa.decimal128(38, 4)),
    ("rnk", pa.int64()),
])

T5_SCHEMA = pa.schema([
    ("l_orderkey", pa.int64()), ("l_linenumber", pa.int64()),
    ("quantity_int", pa.int32()), ("extended_price", pa.decimal128(18, 2)),
    ("total_price", pa.decimal128(18, 2)), ("shipdate_text", pa.string()),
    ("orderdate_text", pa.string()), ("discount_pct", pa.int32()),
])

T6_SCHEMA = pa.schema([
    ("order_date", pa.date32()), ("orders_count", pa.int64()),
    ("gross_revenue", pa.decimal128(38, 4)),
    ("top_line_amount", pa.decimal128(38, 4)),
])

ONE = Decimal(1)
SHIP_LO = date(1995, 1, 1)
SHIP_HI = date(1996, 1, 1)


def line_gross(row):
    """extended price times (1 minus discount), exact decimal, scale 4."""
    return row["l_extendedprice"] * (ONE - row["l_discount"])


def line_net(row):
    """gross times (1 plus tax), exact decimal, scale 6."""
    return line_gross(row) * (ONE + row["l_tax"])


def _write(pcoll, prefix, schema):
    return pcoll | beam.io.WriteToParquet(
        prefix, schema=schema, file_name_suffix=".parquet",
        num_shards=1, shard_name_template="",
    )


def build_t1(p, lineitem_path, out_prefix):
    rows = p | "ReadLineitem" >> beam.io.ReadFromParquet(lineitem_path)
    kept = rows | "ShipIn1995" >> beam.Filter(
        lambda r: SHIP_LO <= r["l_shipdate"] < SHIP_HI
    )
    _write(kept, out_prefix, LINEITEM_SCHEMA)
    return kept


def _orders_full_kv(row):
    return (row["o_orderkey"], (
        row["o_custkey"], row["o_orderstatus"], row["o_totalprice"],
        row["o_orderdate"], row["o_orderpriority"],
    ))


def _join_full(row, orders_by_key):
    found = orders_by_key.get(row["l_orderkey"])
    if found is None:
        return
    out = dict(row)
    (out["o_custkey"], out["o_orderstatus"], out["o_totalprice"],
     out["o_orderdate"], out["o_orderpriority"]) = found
    yield out


def build_t2(p, lineitem_path, orders_path, out_prefix):
    orders = (
        p | "ReadOrders" >> beam.io.ReadFromParquet(orders_path)
        | "OrdersKV" >> beam.Map(_orders_full_kv)
    )
    lines = p | "ReadLineitem" >> beam.io.ReadFromParquet(lineitem_path)
    joined = lines | "JoinOrders" >> beam.FlatMap(
        _join_full, pvalue.AsDict(orders)
    )
    _write(joined, out_prefix, T2_SCHEMA)
    return joined


class SalesCombineFn(beam.CombineFn):
    """Accumulator: (set of order keys, line count, gross sum, net sum)."""

    def create_accumulator(self):
        return (set(), 0, Decimal(0), Decimal(0))

    def add_input(self, accumulator, element):
        keys, count, gross, net = accumulator
        orderkey, g, n = element
        keys.add(orderkey)
        return (keys, count + 1, gross + g, net + n)

    def merge_accumulators(self, accumulators):
        keys, count, gross, net = set(), 0, Decimal(0), Decimal(0)
        for k, c, g, n in accumulators:
            keys |= k
            count += c
            gross += g
            net += n
        return (keys, count, gross, net)

    def extract_output(self, accumulator):
        keys, count, gross, net = accumulator
        return (len(keys), count, gross, net)


def _dated_line(row, order_dates):
    order_date = order_dates.get(row["l_orderkey"])
    if order_date is None:
        return
    # Grouping keys are ISO date strings: Beam requires deterministic
    # key coders, and datetime.date has none (DECISIONS.md D4). The
    # value is converted back to a date when the output row is shaped.
    yield (order_date.isoformat(),
           (row["l_orderkey"], line_gross(row), line_net(row)))


def _t3_row(kv):
    order_date_text, (orders_count, line_count, gross, net) = kv
    return {
        "order_date": date.fromisoformat(order_date_text),
        "orders_count": orders_count,
        "line_count": line_count, "gross_revenue": gross,
        "net_revenue": net,
    }


def build_t3(p, lineitem_path, orders_path, out_prefix):
    order_dates = (
        p | "ReadOrders" >> beam.io.ReadFromParquet(orders_path)
        | "OrderDatesKV" >> beam.Map(lambda r: (r["o_orderkey"], r["o_orderdate"]))
    )
    lines = p | "ReadLineitem" >> beam.io.ReadFromParquet(lineitem_path)
    dated = lines | "AttachDate" >> beam.FlatMap(
        _dated_line, pvalue.AsDict(order_dates)
    )
    combined = dated | "DailySales" >> beam.CombinePerKey(SalesCombineFn())
    out = combined | "Shape" >> beam.Map(_t3_row)
    _write(out, out_prefix, T3_SCHEMA)
    return out


def _rank_partition(kv):
    order_date_text, values = kv
    order_date = date.fromisoformat(order_date_text)
    ordered = sorted(values, key=lambda v: v[2], reverse=True)
    prev_amount = None
    prev_rank = 0
    for i, (orderkey, linenumber, amount) in enumerate(ordered):
        if prev_amount is not None and amount == prev_amount:
            rank = prev_rank
        else:
            rank = i + 1
        prev_amount, prev_rank = amount, rank
        yield {
            "order_date": order_date, "l_orderkey": orderkey,
            "l_linenumber": linenumber, "line_amount": amount,
            "rnk": rank,
        }


def build_t4(p, t2_path, out_prefix):
    rows = p | "ReadJoined" >> beam.io.ReadFromParquet(t2_path)
    keyed = rows | "ByDate" >> beam.Map(
        lambda r: (r["o_orderdate"].isoformat(),
                   (r["l_orderkey"], r["l_linenumber"], line_gross(r)))
    )
    grouped = keyed | "GroupDates" >> beam.GroupByKey()
    ranked = grouped | "Rank" >> beam.FlatMap(_rank_partition)
    _write(ranked, out_prefix, T4_SCHEMA)
    return ranked


def _convert(row, orders_by_key):
    found = orders_by_key.get(row["l_orderkey"])
    if found is None:
        return
    total_price, order_date = found
    yield {
        "l_orderkey": row["l_orderkey"],
        "l_linenumber": row["l_linenumber"],
        "quantity_int": int(row["l_quantity"]),
        "extended_price": row["l_extendedprice"],
        "total_price": total_price,
        "shipdate_text": row["l_shipdate"].isoformat(),
        "orderdate_text": order_date.isoformat(),
        "discount_pct": int(row["l_discount"] * 100),
    }


def build_t5(p, lineitem_path, orders_path, out_prefix):
    orders = (
        p | "ReadOrders" >> beam.io.ReadFromParquet(orders_path)
        | "OrdersKV" >> beam.Map(
            lambda r: (r["o_orderkey"], (r["o_totalprice"], r["o_orderdate"]))
        )
    )
    lines = p | "ReadLineitem" >> beam.io.ReadFromParquet(lineitem_path)
    converted = lines | "Convert" >> beam.FlatMap(
        _convert, pvalue.AsDict(orders)
    )
    _write(converted, out_prefix, T5_SCHEMA)
    return converted


def _t6_row(kv):
    order_date_text, values = kv
    daily = None
    top_amount = None
    for tag, payload in values:
        if tag == "d":
            daily = payload
        elif tag == "r":
            top_amount = payload if top_amount is None else max(top_amount, payload)
    if daily is None or top_amount is None:
        return
    orders_count, gross = daily
    yield {
        "order_date": date.fromisoformat(order_date_text),
        "orders_count": orders_count,
        "gross_revenue": gross, "top_line_amount": top_amount,
    }


def build_t6(p, t3_path, t4_path, out_prefix):
    daily = (
        p | "ReadDaily" >> beam.io.ReadFromParquet(t3_path)
        | "DailyKV" >> beam.Map(lambda r: (
            r["order_date"].isoformat(),
            ("d", (r["orders_count"], r["gross_revenue"]))))
    )
    tops = (
        p | "ReadRanked" >> beam.io.ReadFromParquet(t4_path)
        | "OnlyRank1" >> beam.Filter(lambda r: r["rnk"] == 1)
        | "TopKV" >> beam.Map(lambda r: (
            r["order_date"].isoformat(), ("r", r["line_amount"])))
    )
    grouped = (daily, tops) | "Merge" >> beam.Flatten() | "GroupDates" >> beam.GroupByKey()
    joined = grouped | "JoinSides" >> beam.FlatMap(_t6_row)
    top = joined | "Top10" >> beam.combiners.Top.Of(
        10, key=lambda r: (r["gross_revenue"], -r["order_date"].toordinal())
    )
    out = top | "Unwrap" >> beam.FlatMap(lambda rows: rows)
    _write(out, out_prefix, T6_SCHEMA)
    return out


BUILDERS = {
    "t1": build_t1, "t2": build_t2, "t3": build_t3,
    "t4": build_t4, "t5": build_t5, "t6": build_t6,
}
