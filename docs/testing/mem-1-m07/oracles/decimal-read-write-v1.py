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

"""Independent numeric oracle for the existing read/write Decimal fixture.

Only this fixture's literal INSERTs and 88 read shapes are supported. Types
come from actual DDL, never historical table names or comments. Python Decimal
with 128-digit precision applies the fixture's documented HALF_UP downscale
and nullable overflow contract. No target/reference output supplies values.
The script writes an isolated candidate result file; it never edits SQL or
the checked-in golden. Unknown SQL shapes are refused.
"""

import argparse
import csv
import hashlib
import json
import re
from decimal import Decimal, ROUND_HALF_UP, localcontext
from pathlib import Path


def require(condition, message):
    if not condition:
        raise ValueError(message)


def decimal_cell(literal, precision, scale, nullable):
    require(re.fullmatch(r"[+-]?\d+(?:\.\d+)?", literal), "unsupported numeric literal")
    value = Decimal(literal).quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)
    if abs(value) >= Decimal(10) ** (precision - scale):
        require(nullable, "non-nullable fixture value overflows its declared precision")
        return "NULL"
    if value.is_zero():
        value = value.copy_abs()
    return format(value, f".{scale}f")


def derive(sql):
    # This fixture has no comment delimiters inside its literals.
    source = re.sub(r"--[^\n]*", "", sql)
    definitions = re.findall(
        r"CREATE TABLE \$\{case_db\}\.(\w+)\s*\((.*?)\)\s*TBLPROPERTIES",
        source, re.S | re.I,
    )
    require(len(definitions) == 86, "fixture DDL shape changed")
    schemas = {}
    rows = {}
    direct = set()
    for name, body in definitions:
        decimals = {}
        for field, precision, scale, not_null in re.findall(
            r"(\w+)\s+decimal\((\d+),\s*(\d+)\)(\s+NOT NULL)?", body, re.I
        ):
            precision, scale = int(precision), int(scale)
            require(0 <= scale <= precision <= 38, "fixture declared Decimal type changed")
            decimals[field] = (precision, scale, not bool(not_null))
        require(decimals, "fixture table has no declared Decimal field")
        schemas[name] = decimals
        rows[name] = []
        if re.fullmatch(r"\s*d1\s+decimal\(\d+,\s*\d+\)\s*", body, re.I):
            direct.add(name)
    require(len(direct) == 84, "single-column fixture shape changed")
    insertions = re.findall(r"INSERT INTO \$\{case_db\}\.(\w+)\s+([^;]+);", source, re.I)
    require(len(insertions) == 301, "fixture insertion count changed")
    append_fields = {
        "decimal_append_test_p39_s0": ["user_id", "asset_id", "timestamp", "shop_id", "day_bucket", "value"],
        "decimal_append_test_p50_s10": ["id1", "id2", "id3", "decimal_value", "decimal_nullable", "regular_value"],
    }
    for name, expression in insertions:
        require(name in schemas, "INSERT has no exact table declaration")
        if name in direct:
            match = re.fullmatch(r"SELECT\s+([+-]?\d+(?:\.\d+)?)\s*", expression, re.I)
            require(match, "single-column INSERT is not a literal")
            rows[name].append({"d1": decimal_cell(match[1], *schemas[name]["d1"])})
            continue
        require(name in append_fields, "unsupported append table")
        match = re.fullmatch(r"VALUES\s*\((.*)\)\s*", expression, re.S | re.I)
        require(match, "append INSERT is not a literal tuple")
        values = next(csv.reader([match[1]], quotechar="'", skipinitialspace=True))
        fields = append_fields[name]
        require(len(values) == len(fields), "append tuple width changed")
        row = dict(zip(fields, (value.strip() for value in values)))
        for field, geometry in schemas[name].items():
            if row[field] == "NULL":
                require(geometry[2], "NULL in non-nullable Decimal field")
            else:
                row[field] = decimal_cell(row[field], *geometry)
        rows[name].append(row)

    reads = re.split(r"(?m)^-- query (\d+)\s*\n", sql)
    require([int(reads[i]) for i in range(1, len(reads), 2)] == list(range(1, 90)), "read shapes changed")
    results = {}
    for index in range(3, len(reads), 2):
        number, query = int(reads[index]), reads[index + 1].strip()
        if number <= 85:
            match = re.fullmatch(r"SELECT \* FROM \$\{case_db\}\.(\w+) ORDER BY d1;", query)
            require(match and match[1] in direct, "unsupported scalar read")
            values = [row["d1"] for row in rows[match[1]]]
            values.sort(key=lambda value: (value != "NULL", Decimal(0) if value == "NULL" else Decimal(value)))
            results[number] = (["d1"], [[value] for value in values])
        elif number == 86:
            require(query == "SELECT user_id, asset_id, shop_id, value FROM ${case_db}.decimal_append_test_p39_s0 ORDER BY user_id, asset_id, shop_id;", "append read changed")
            fields = ["user_id", "asset_id", "shop_id", "value"]
            items = sorted(rows["decimal_append_test_p39_s0"], key=lambda row: (row["user_id"], row["asset_id"], int(row["shop_id"])))
            results[number] = (fields, [[row[field] for field in fields] for row in items])
        else:
            items = sorted(rows["decimal_append_test_p50_s10"], key=lambda row: int(row["id1"]))
            if number == 87:
                require(re.fullmatch(r"SELECT id1, id2, id3, decimal_value, decimal_nullable, regular_value\s+FROM \$\{case_db\}.decimal_append_test_p50_s10\s+ORDER BY id1, id2, id3;", query), "append projection changed")
                fields = append_fields["decimal_append_test_p50_s10"]
                results[number] = (fields, [[row[field] for field in fields] for row in items])
            elif number == 88:
                require(query == "SELECT decimal_value FROM ${case_db}.decimal_append_test_p50_s10 WHERE id1 = 2 AND id2 = 'key002' AND id3 = 200;", "point read changed")
                selected = [row for row in items if row["id1"] == "2" and row["id2"] == "key002" and row["id3"] == "200"]
                require(len(selected) == 1, "point read does not identify one literal row")
                results[number] = (["decimal_value"], [[selected[0]["decimal_value"]]])
            elif number == 89:
                require(re.fullmatch(r"SELECT COUNT\(\*\), SUM\(regular_value\), MIN\(decimal_value\), MAX\(decimal_value\)\s+FROM \$\{case_db\}.decimal_append_test_p50_s10;", query), "aggregate read changed")
                values = [Decimal(row["decimal_value"]) for row in items]
                regular = [int(row["regular_value"]) for row in items if row["regular_value"] != "NULL"]
                results[number] = (["count(*)", "sum(regular_value)", "min(decimal_value)", "max(decimal_value)"], [[str(len(items)), str(sum(regular)), format(min(values), ".10f"), format(max(values), ".10f")]])
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sql", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--audit", type=Path, required=True)
    args = parser.parse_args()
    source = args.sql.read_text()
    with localcontext() as context:
        context.prec = 128
        results = derive(source)
    require(set(results) == set(range(2, 90)), "incomplete oracle")
    text = "\n\n".join(f"-- query {number}\n" + "\t".join(columns) + "\n" + "\n".join("\t".join(row) for row in values) for number, (columns, values) in results.items()) + "\n"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(text)
    args.audit.write_text(json.dumps({"schema_version": 1, "sql_sha256": hashlib.sha256(source.encode()).hexdigest(), "oracle_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), "result_sha256": hashlib.sha256(text.encode()).hexdigest(), "basis": "Exact literal text, actual DDL precision/scale, fixture HALF_UP and nullable-overflow contract, explicit SELECT/aggregate shapes. No database output used.", "queries": len(results), "rows": sum(len(values) for _, values in results.values()), "result": str(args.output)}, indent=2) + "\n")
    print(f"derived {len(results)} queries from exact fixture facts")


if __name__ == "__main__":
    main()
