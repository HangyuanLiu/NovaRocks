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
"""Generate the native depth-64 value case and its expected result.

The expected result is derived from the fixed inputs and the documented
struct display format ({"name":value}), never recorded from a run. Run from
the repository root:

    python3 tests/sql/fixtures/uea7b3-native-depth64/generate.py
"""

from pathlib import Path

LEVELS = 63  # 63 struct containers plus the INT leaf: logical depth 64
SUITE = Path("tests/sql/correctness/iceberg-ddl")
NAME = "native_depth64_values_hadoop"


def struct_type(levels: int) -> str:
    text = "INT"
    for level in range(levels, 0, -1):
        text = f"STRUCT<n{level} {text}>"
    return text


def struct_display(levels: int, leaf: int) -> str:
    text = str(leaf)
    for level in range(levels, 0, -1):
        text = f'{{"n{level}":{text}}}'
    return text


LEAF_PATH = "deep." + ".".join(f"n{level}" for level in range(1, LEVELS + 1))
LICENSE = """-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
"""

CATALOG = """CREATE EXTERNAL CATALOG nativedepth64_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_test_warehouse}/native_depth64_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "static",
  "credential.object-store-metadata.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "static",
  "credential.object-store-data.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE nativedepth64_${uuid0}.ns_${uuid0};
SET CATALOG nativedepth64_${uuid0};
USE ns_${uuid0};"""


def main() -> None:
    queries = []
    results = []

    def query(sql: str, result: str | None, directives: tuple[str, ...] = ()) -> None:
        number = len(queries) + 1
        head = [f"-- query {number}", *directives]
        queries.append("\n".join(head) + "\n" + sql)
        if result is not None:
            results.append(f"-- query {number}\n{result}")

    query(CATALOG, None, ("-- @skip_result_check=true",))
    query(
        f"CREATE TABLE deep_values (id BIGINT, deep {struct_type(LEVELS)})\n"
        'TBLPROPERTIES ("format-version"="3");',
        None,
        ("-- @skip_result_check=true",),
    )
    # Deep values are built one level per statement: a single nested
    # named_struct expression would itself be 63 expressions deep, beyond the
    # existing native expression wire bound, which is not what this case tests.
    query(
        "CREATE TABLE seed (id BIGINT, leaf INT);\n"
        "INSERT INTO seed VALUES (1, 7), (3, -5);",
        None,
        ("-- @skip_result_check=true",),
    )
    query(
        f"CREATE TABLE lvl{LEVELS} AS SELECT id, named_struct('n{LEVELS}', leaf) AS v FROM seed;",
        None,
        ("-- @skip_result_check=true",),
    )
    for level in range(LEVELS - 1, 0, -1):
        query(
            f"CREATE TABLE lvl{level} AS SELECT id, named_struct('n{level}', v) AS v "
            f"FROM lvl{level + 1};",
            None,
            ("-- @skip_result_check=true",),
        )
    query(
        "INSERT INTO deep_values SELECT id, v FROM lvl1;\n"
        "INSERT INTO deep_values VALUES (2, NULL);",
        None,
        ("-- @skip_result_check=true",),
    )
    rows = [(1, struct_display(LEVELS, 7)), (2, "NULL"), (3, struct_display(LEVELS, -5))]
    query(
        "SELECT id, deep FROM deep_values ORDER BY id;",
        "id\tdeep\n" + "\n".join(f"{i}\t{d}" for i, d in rows),
    )
    # A 63-step field path is itself 63 nested expressions; typeof() is folded
    # during analysis, so it proves the leaf type without that expression.
    query(
        f"SELECT typeof({LEAF_PATH}) AS leaf_type, count(*) AS row_count FROM deep_values;",
        "leaf_type\trow_count\nint\t3",
    )
    query(
        "INSERT INTO deep_values SELECT id + 10, deep FROM deep_values WHERE id = 1;",
        None,
        ("-- @skip_result_check=true",),
    )
    query(
        "CREATE TABLE deep_copy AS SELECT id, deep FROM deep_values;",
        None,
        ("-- @skip_result_check=true",),
    )
    copy_rows = rows + [(11, struct_display(LEVELS, 7))]
    query(
        "SELECT id, deep FROM deep_copy ORDER BY id;",
        "id\tdeep\n" + "\n".join(f"{i}\t{d}" for i, d in copy_rows),
    )
    query(
        "SELECT count(*) AS total, count(deep) AS non_null FROM deep_copy;",
        "total\tnon_null\n4\t3",
    )
    query(
        "SELECT count(*) AS snapshot_count FROM deep_values$snapshots;",
        "snapshot_count\n3",
    )
    query(
        f"CREATE TABLE too_deep (id BIGINT, deep {struct_type(LEVELS + 1)});",
        None,
        ("-- @expect_error=DDL type exceeds its node or depth budget",),
    )
    query(
        "SELECT count(*) AS snapshot_count FROM deep_values$snapshots;",
        "snapshot_count\n3",
    )
    drops = "".join(
        f"DROP TABLE IF EXISTS nativedepth64_${{uuid0}}.ns_${{uuid0}}.lvl{level} FORCE;\n"
        for level in range(1, LEVELS + 1)
    )
    query(
        drops
        + "DROP TABLE IF EXISTS nativedepth64_${uuid0}.ns_${uuid0}.seed FORCE;\n"
        "DROP TABLE IF EXISTS nativedepth64_${uuid0}.ns_${uuid0}.deep_copy FORCE;\n"
        "DROP TABLE IF EXISTS nativedepth64_${uuid0}.ns_${uuid0}.deep_values FORCE;\n"
        "DROP DATABASE nativedepth64_${uuid0}.ns_${uuid0};\n"
        "DROP CATALOG nativedepth64_${uuid0};",
        None,
        ("-- @cleanup=true", "-- @skip_result_check=true"),
    )

    header = (
        LICENSE
        + "\n-- @sequential=true\n-- @order_sensitive=true\n"
        "-- @tags=iceberg,metadata_depth,native_depth64\n"
        "-- Native CTAS, INSERT SELECT and SELECT of non-NULL values whose\n"
        "-- type has logical depth 64 (63 struct containers plus an INT leaf), and a\n"
        "-- 65-level column refused by DDL without effect. Generated by\n"
        "-- tests/sql/fixtures/uea7b3-native-depth64/generate.py; expected values are\n"
        "-- derived from the fixed inputs, never recorded.\n\n"
    )
    (SUITE / "sql" / f"{NAME}.sql").write_text(header + "\n\n".join(queries) + "\n")
    (SUITE / "result" / f"{NAME}.result").write_text("\n\n".join(results) + "\n")


if __name__ == "__main__":
    main()
