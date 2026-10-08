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

"""Independent oracle for the frozen twelve-row Decimal window fixture.

Input values come from exact literal text, never database output or a golden.
The full SQL hash freezes setup, expressions, frames, aliases and ordering.
AVG uses the existing Decimal scale and HALF_UP policy. All product values
are NULL under the existing nullable Decimal128 intermediate/precision
limits; the oracle checks that condition using independent Python arithmetic.
"""

import argparse
import csv
import hashlib
import json
import re
from decimal import Decimal, ROUND_HALF_UP, localcontext
from pathlib import Path

SQL_SHA256 = "764b3b708201c35d7a3b7bea3651254b0be49e3fddc3df1f61aecb59d210735a"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def formatted(value, scale):
    value = value.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)
    require(abs(value) < Decimal(10) ** (38 - scale), "window value outside declared precision")
    return format(value, f".{scale}f")


def derive(source):
    require(hashlib.sha256(source.encode()).hexdigest() == SQL_SHA256, "frozen SQL changed; oracle review required")
    setup = re.sub(r"--[^\n]*", "", re.split(r"(?m)^-- query 2\n", source)[0])
    match = re.search(r"INSERT INTO \$\{case_db\}\.decimal_window_test VALUES\s*(.*?);", setup, re.S)
    require(match, "literal setup missing")
    rows = []
    for text in re.findall(r"\(([^()]*)\)", match[1]):
        values = [value.strip() for value in next(csv.reader([text], quotechar="'", skipinitialspace=True))]
        require(len(values) == 5, "literal tuple width changed")
        rows.append({"id": int(values[0]), "category": values[1], "d50": Decimal(values[2]), "d20": Decimal(values[3]), "d0": Decimal(values[4])})
    rows.sort(key=lambda row: row["id"])
    require([row["id"] for row in rows] == list(range(1, 13)), "literal row identities changed")
    categories = {name: [row for row in rows if row["category"] == name] for name in "ABC"}
    require(all(len(partition) == 4 for partition in categories.values()), "partition size changed")
    for field, scale in [("d50", 15), ("d20", 20), ("d0", 0)]:
        require(len({row[field] for row in rows}) == 12, "ordering contains unruled peers")
        for row in rows:
            require(formatted(row[field], scale) == format(row[field], f".{scale}f"), "input requires rounding")
    headers = {
        2: "test_name id category d50_15 row_num rank_val dense_rank_val",
        3: "test_name id category d76_20 row_num_by_cat rank_by_cat",
        4: "test_name id category d50_15 moving_sum running_avg_by_cat count_by_cat running_max",
        5: "test_name id category d76_20 prev_val next_val prev_val_by_cat next_val_by_cat",
        6: "test_name id category d76_0 first_val_by_cat last_val_by_cat first_val_window",
        7: "test_name id category d50_15 d76_20 running_product_sum moving_sum_avg",
    }
    labels = {int(n): re.search(r"SELECT\s+'([^']+)' as test_name", body)[1] for n, body in re.findall(r"(?ms)^-- query (\d+)\n(.*?)(?=^-- query |\Z)", source) if int(n) > 1}
    output = {n: (header.split(), []) for n, header in headers.items()}
    def add(number, row, cells):
        output[number][1].append([labels[number], str(row["id"]), row["category"], *cells])
    for rank, row in enumerate(sorted(rows, key=lambda row: row["d50"]), 1):
        add(2, row, [formatted(row["d50"], 15), str(rank), str(rank), str(rank)])
    for name in "ABC":
        for rank, row in enumerate(sorted(categories[name], key=lambda row: row["d20"]), 1):
            add(3, row, [formatted(row["d20"], 20), str(rank), str(rank)])
    for index, row in enumerate(rows):
        frame = rows[max(0, index - 1):index + 2]
        partition = [item for item in categories[row["category"]] if item["id"] <= row["id"]]
        add(4, row, [formatted(row["d50"], 15), formatted(sum(item["d50"] for item in frame), 15), formatted(sum(item["d50"] for item in partition) / len(partition), 15), "4", formatted(max(item["d0"] for item in partition), 0)])
    ordered = sorted(rows, key=lambda row: row["d20"])
    for index, row in enumerate(ordered):
        partition = sorted(categories[row["category"]], key=lambda row: row["d20"])
        own = partition.index(row)
        add(5, row, [formatted(row["d20"], 20), formatted(ordered[index - 1]["d20"], 20) if index else "NULL", formatted(ordered[index + 1]["d20"], 20) if index + 1 < len(ordered) else "NULL", formatted(partition[own - 1]["d20"], 20) if own else formatted(Decimal(0), 20), formatted(partition[own + 1]["d20"], 20) if own + 1 < len(partition) else formatted(Decimal(0), 20)])
    for name in "ABC":
        partition = sorted(categories[name], key=lambda row: row["d0"])
        for row in partition:
            # The CURRENT ROW..2 FOLLOWING frame starts at this row.
            add(6, row, [formatted(row["d0"], 0), formatted(partition[0]["d0"], 0), formatted(partition[-1]["d0"], 0), formatted(row["d50"], 15)])
    for index, row in enumerate(rows):
        unscaled_product = int(row["d50"] * 10 ** 15) * int(row["d20"] * 10 ** 20)
        require(unscaled_product >= 2 ** 127 and unscaled_product >= 10 ** 38, "product no longer exceeds existing i128/Decimal(38,35) limits")
        frame = rows[max(0, index - 2):index + 3]
        combined_average = sum(item["d50"] + item["d20"] for item in frame) / len(frame)
        add(7, row, [formatted(row["d50"], 15), formatted(row["d20"], 20), "NULL", formatted(combined_average, 20)])
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
    args.audit.write_text(json.dumps({"schema_version": 1, "sql_sha256": SQL_SHA256, "oracle_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), "result_sha256": hashlib.sha256(text.encode()).hexdigest(), "queries": len(results), "rows": sum(len(rows) for _, rows in results.values()), "basis": "Exact literals; independent rank/frame/group/lag/lead evaluation; Decimal precision 128 and existing HALF_UP average scale; existing nullable product intermediate overflow. No database output used."}, indent=2) + "\n")
    print(f"derived {len(results)} queries")


if __name__ == "__main__":
    main()
