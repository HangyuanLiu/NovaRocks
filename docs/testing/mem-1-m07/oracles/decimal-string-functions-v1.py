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

"""Independent oracle for the frozen seven-row Decimal/string SQL fixture.

The exact SQL hash pins setup, expressions, aliases and ordering. Values come
only from literal INSERT text and actual DDL. Decimal arithmetic and ASCII
string operations derive all cells without database results or golden input.
Any SQL change requires a new reviewed oracle version, rather than fallback.
"""

import argparse
import csv
import hashlib
import json
import re
from decimal import Decimal, localcontext
from pathlib import Path

SQL_SHA256 = "ed98c27d550d18ac4bdf46c9c07a85823c3ce5442cdf34315c01eeed1fe99af7"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def derive(source):
    require(hashlib.sha256(source.encode()).hexdigest() == SQL_SHA256, "frozen SQL changed; oracle review required")
    setup = re.split(r"(?m)^-- query 2\s*\n", source)[0]
    setup = re.sub(r"--[^\n]*", "", setup).strip()
    declaration = re.fullmatch(
        r'CREATE TABLE \$\{case_db\}\.decimal_string_test\s*\((.*?)\)\s*TBLPROPERTIES\s*\("format-version"\s*=\s*"3"\);\s*INSERT INTO \$\{case_db\}\.decimal_string_test VALUES\s*(.*);',
        setup, re.S,
    )
    require(declaration, "unsupported setup")
    fields = re.findall(r"(d\w+) DECIMAL\((\d+),(\d+)\)", declaration[1])
    require(fields == [("d50_15", "38", "15"), ("d76_20", "38", "20"), ("d76_0", "38", "0")], "declared Decimal geometry changed")
    tuples = re.findall(r"\(([^()]*)\)", declaration[2])
    require(len(tuples) == 7, "literal row count changed")
    rows = []
    for item in tuples:
        literals = next(csv.reader([item], quotechar="'", skipinitialspace=True))
        require(len(literals) == 5, "literal tuple width changed")
        identifier = int(literals[0])
        values = []
        for literal, (_, precision, scale) in zip(literals[1:4], fields):
            require(re.fullmatch(r"[+-]?\d+(?:\.\d+)?", literal), "unsupported decimal literal")
            value = Decimal(literal)
            quantum = Decimal(1).scaleb(-int(scale))
            require(value == value.quantize(quantum), "fixture literal requires unruled rounding")
            require(abs(value) < Decimal(10) ** (int(precision) - int(scale)), "fixture literal overflows")
            values.append(format(value, f".{scale}f"))
        rows.append((identifier, *values, literals[4]))
    rows.sort(key=lambda row: row[0])
    require([row[0] for row in rows] == list(range(1, 8)), "row identities changed")
    headers = {
        2: "test_name id d50_15 d50_as_string formatted_d50 combined_string",
        3: "test_name id d76_0 d76_string string_length left_10_chars right_10_chars first_20_chars",
        4: "test_name id d50_15 decimal_string decimal_point_position negative_sign_position number_type",
        5: "test_name id d76_20 decimal_string integer_part decimal_part trimmed_trailing_zeros first_5_decimal_places",
        6: "test_name id d50_15 original_string integer_part_split decimal_part_split sign_part",
        7: "test_name id d76_0 original_string zero_padded space_padded",
        8: "test_name id category d50_15 upper_formatted lower_formatted",
        9: "test_name id d50_15 d76_20 d76_0 pipe_separated symbol_replaced reversed_string",
    }
    output = {number: (header.split(), []) for number, header in headers.items()}
    for identifier, d50, d20, d0, category in rows:
        identity = str(identifier)
        output[2][1].append(["Test1_BASIC_STRING_CONVERSION", identity, d50, d50, "Value: " + d50, "D50=" + d50 + ", D76=" + d20])
        output[3][1].append(["Test2_STRING_LENGTH_TRUNCATION", identity, d0, d0, str(len(d0)), d0[:10], d0[-10:], d0[:20]])
        output[4][1].append(["Test3_STRING_PATTERN_MATCHING", identity, d50, d50, str(d50.find(".") + 1), str(d50.find("-") + 1), "NEGATIVE_DECIMAL" if d50.startswith("-") else "POSITIVE_DECIMAL"])
        if Decimal(d20) != 0:
            output[5][1].append(["Test4_REGEX_OPERATIONS", identity, d20, d20, re.match(r"^(-?[0-9]+)", d20)[1], re.search(r"\.([0-9]+)$", d20)[1], re.sub(r"0+$", "", d20), re.match(r"^(-?[0-9]+\.[0-9]{0,5})", d20)[1]])
        integer, fraction = d50.lstrip("-").split(".")
        output[6][1].append(["Test5_STRING_SPLITTING", identity, d50, d50, integer, fraction, "-" if d50.startswith("-") else "+"])
        output[7][1].append(["Test6_STRING_FORMATTING", identity, d0, d0, d0.lstrip("-").rjust(80, "0"), d0[:20].ljust(20, " ")])
        formatted = category + "_" + d50
        output[8][1].append(["Test7_CASE_SPECIAL_FORMATTING", identity, category, d50, formatted.upper(), formatted.lower()])
        output[9][1].append(["Test8_COMPLEX_STRING_OPERATIONS", identity, d50, d20, d0, " | ".join(["D50: " + d50, "D76_20: " + d20, "D76_0: " + d0]), d50.replace(".", "_DOT_").replace("-", "_MINUS_"), d50.lstrip("-")[::-1]])
    return output


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
    text = "\n\n".join(f"-- query {n}\n" + "\t".join(headers) + "\n" + "\n".join("\t".join(row) for row in rows) for n, (headers, rows) in results.items()) + "\n"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(text)
    args.audit.write_text(json.dumps({"schema_version": 1, "sql_sha256": SQL_SHA256, "oracle_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), "result_sha256": hashlib.sha256(text.encode()).hexdigest(), "basis": "Exact literal Decimal values at actual DDL scale, fixed SQL hash, independently evaluated ASCII string operations. No database or golden values used.", "queries": len(results), "rows": sum(len(rows) for _, rows in results.values())}, indent=2) + "\n")
    print(f"derived {len(results)} queries and 56 rows from frozen literal facts")


if __name__ == "__main__":
    main()
