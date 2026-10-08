#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Independent oracle for one frozen Decimal grouping/window fixture.

Exact SQL hash freezes setup, aliases, expressions and ordering. Only literal
input text supplies values. Decimal aggregates use the existing scale and
HALF_UP policy. The nullable percentage expression retains its existing
Decimal128 intermediate-overflow behavior, independently checked with Python
integers. No engine output or checked-in golden supplies expected cells.
"""

import argparse
import csv
import hashlib
import json
import re
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP, localcontext
from pathlib import Path

SQL_SHA256 = "7d93f3aa5d3fd236cee1afa7874f7ef518bc48255da03f607302b2a5b66da4ba"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def formatted(value, scale):
    value = value.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)
    require(abs(value) < Decimal(10) ** (38 - scale), "aggregate outside declared precision")
    return format(value, f".{scale}f")


def groups(rows, key):
    grouped = defaultdict(list)
    for row in rows:
        grouped[key(row)].append(row)
    return sorted(grouped.items())


def derive(source):
    require(hashlib.sha256(source.encode()).hexdigest() == SQL_SHA256, "frozen SQL changed; oracle review required")
    setup = re.sub(r"--[^\n]*", "", re.split(r"(?m)^-- query 2\n", source)[0])
    tables = {}
    for table, body in re.findall(r"INSERT INTO \$\{case_db\}\.(\w+) VALUES\s*(.*?);", setup, re.S):
        tables.setdefault(table, []).extend(
            [value.strip() for value in next(csv.reader([text], quotechar="'", skipinitialspace=True))]
            for text in re.findall(r"\(([^()]*)\)", body)
        )
    range_rows = tables["decimal_range_partition"]
    amount_rows = tables["decimal_amount_partition"]
    require(len(range_rows) == 15 and len(amount_rows) == 9, "literal row count changed")
    ranges = []
    for identity, date, amount, balance, kind in range_rows:
        ranges.append({"id": identity, "date": date, "amount": Decimal(amount), "balance": Decimal(balance), "kind": kind})
    amounts = []
    for identity, customer, amount, large, category, bucket in amount_rows:
        amounts.append({"id": identity, "customer": customer, "amount": Decimal(amount), "large": Decimal(large), "category": category, "bucket": bucket})
    require(len({row["date"] for row in ranges}) == 15, "window ordering has peers")
    for row in ranges:
        require(formatted(row["amount"], 15) == format(row["amount"], ".15f"), "input rounding required")
        require(formatted(row["balance"], 20) == format(row["balance"], ".20f"), "input rounding required")
    for row in amounts:
        require(formatted(row["amount"], 15) == format(row["amount"], ".15f"), "input rounding required")
        require(formatted(row["large"], 0) == format(row["large"], ".0f"), "input rounding required")
    headers = {
        2: "test_name month transaction_count total_amount avg_balance max_amount min_amount",
        3: "test_name id transaction_date amount balance account_type",
        4: "test_name account_type transaction_count total_amount avg_amount total_deposits total_withdrawals",
        5: "test_name amount_range category count total_amount avg_amount min_amount max_amount",
        6: "test_name id customer_id amount large_amount category",
        7: "test_name month transaction_count total_amount avg_balance",
        8: "test_name id transaction_date amount balance account_type rank_in_month running_total_by_type prev_balance",
        9: "test_name transaction_date amount account_type monthly_total percent_of_monthly_total",
        10: "test_name partition_month row_count positive_transactions negative_transactions min_balance_in_partition max_balance_in_partition",
    }
    labels = {int(n): re.search(r"SELECT\s+'([^']+)' as test_name", body)[1] for n, body in re.findall(r"(?ms)^-- query (\d+)\n(.*?)(?=^-- query |\Z)", source) if int(n) > 1}
    output = {n: (header.split(), []) for n, header in headers.items()}
    def add(n, values):
        output[n][1].append([labels[n], *values])
    for month, rows in groups(ranges, lambda row: row["date"][:7]):
        total = sum(row["amount"] for row in rows)
        avg_balance = sum(row["balance"] for row in rows) / len(rows)
        basic = [month, str(len(rows)), formatted(total, 15), formatted(avg_balance, 20)]
        add(2, basic + [formatted(max(row["amount"] for row in rows), 15), formatted(min(row["amount"] for row in rows), 15)])
        add(7, basic)
        add(10, [month, str(len(rows)), str(sum(row["amount"] > 0 for row in rows)), str(sum(row["amount"] < 0 for row in rows)), formatted(min(row["balance"] for row in rows), 20), formatted(max(row["balance"] for row in rows), 20)])
    for row in sorted(ranges, key=lambda row: row["date"]):
        if "2024-02-01" <= row["date"] < "2024-03-01":
            add(3, [row["id"], row["date"], formatted(row["amount"], 15), formatted(row["balance"], 20), row["kind"]])
    for kind, rows in groups(ranges, lambda row: row["kind"]):
        total = sum(row["amount"] for row in rows)
        add(4, [kind, str(len(rows)), formatted(total, 15), formatted(total / len(rows), 15), formatted(sum((row["amount"] for row in rows if row["amount"] > 0), Decimal(0)), 15), formatted(sum((-row["amount"] for row in rows if row["amount"] < 0), Decimal(0)), 15)])
    for (bucket, category), rows in groups(amounts, lambda row: (row["bucket"], row["category"])):
        total = sum(row["amount"] for row in rows)
        add(5, [bucket, category, str(len(rows)), formatted(total, 15), formatted(total / len(rows), 15), formatted(min(row["amount"] for row in rows), 15), formatted(max(row["amount"] for row in rows), 15)])
    for row in sorted((row for row in amounts if row["bucket"] == "LARGE"), key=lambda row: -row["amount"]):
        add(6, [row["id"], row["customer"], formatted(row["amount"], 15), formatted(row["large"], 0), row["category"]])
    ranks = {}
    for _, rows in groups(ranges, lambda row: row["date"][:7]):
        require(len({row["amount"] for row in rows}) == len(rows), "rank has unordered ties")
        ranks.update({row["id"]: str(index) for index, row in enumerate(sorted(rows, key=lambda row: -row["amount"]), 1)})
    running, previous = defaultdict(Decimal), {}
    for row in sorted(ranges, key=lambda row: (row["date"], row["kind"])):
        kind = row["kind"]
        running[kind] += row["amount"]
        add(8, [row["id"], row["date"], formatted(row["amount"], 15), formatted(row["balance"], 20), kind, ranks[row["id"]], formatted(running[kind], 15), previous.get(kind, "NULL")])
        previous[kind] = formatted(row["balance"], 20)
    monthly = dict(groups(ranges, lambda row: (row["date"][:7], row["kind"])))
    for row in sorted(ranges, key=lambda row: row["date"]):
        peers = monthly[(row["date"][:7], row["kind"])]
        require(len(peers) == 1 and abs(row["amount"]) > 1000, "percentage fixture shape changed")
        # Both inputs/output have scale 15. Division rescales the left
        # unscaled value by 10^15 before division; this fixture exceeds i128.
        numerator = int(row["amount"] * 10 ** 15) * 10 ** 15
        require(not -(2 ** 127) <= numerator < 2 ** 127, "percentage intermediate no longer overflows")
        add(9, [row["date"], formatted(row["amount"], 15), row["kind"], formatted(row["amount"], 15), "NULL"])
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sql", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--audit", type=Path, required=True)
    args = parser.parse_args()
    with localcontext() as context:
        context.prec = 128
        results = derive(args.sql.read_text())
    text = "\n\n".join(f"-- query {n}\n" + "\t".join(headers) + "\n" + "\n".join("\t".join(row) for row in rows) for n, (headers, rows) in results.items()) + "\n"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(text)
    args.audit.write_text(json.dumps({"schema_version": 1, "sql_sha256": SQL_SHA256, "oracle_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), "result_sha256": hashlib.sha256(text.encode()).hexdigest(), "queries": len(results), "rows": sum(len(rows) for _, rows in results.values()), "basis": "Exact fixture literals, Decimal precision 128, declared scale and existing HALF_UP average policy; independent grouping/order/window evaluation; nullable i128 division intermediate overflow for fixed percentage expression. No database output used."}, indent=2) + "\n")
    print(f"derived {len(results)} queries")


if __name__ == "__main__":
    main()
