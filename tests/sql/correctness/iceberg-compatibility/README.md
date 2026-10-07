<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Iceberg Compatibility SQL Suite

This suite validates cross-engine Iceberg compatibility.

The cases create Iceberg format-v3 tables with Spark through the workspace REST
Catalog and MinIO object store, then read the tables through NovaRocks. Current
coverage includes:

- basic Parquet reads
- primitive/date/timestamp/decimal/NULL reads
- ARRAY/MAP/STRUCT/nested field reads
- partitioned-table filtering and aggregation
- Spark-side schema evolution, including DROP plus re-ADD of the same name
- Spark-side partition evolution across historical specs
- Spark-written row-level DELETE, UPDATE, and MERGE visibility
- Spark-created refs with NovaRocks time-travel reads
- NovaRocks snapshot/history metadata-table reads over Spark commits
- NovaRocks Iceberg MV refresh over Spark-written base-table commits

Run it against the generated local environment (see
[`docker/iceberg-rest/README.md`](../../docker/iceberg-rest/README.md) for
how to bring it up):

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility --mode verify
```


The visible bag cases use checked-in official Iceberg SDK fixture code under
`tests/sql/fixtures/mv-visible-content-encodings/fixture.scala`:

- `novarocks_mv_dictionary_encodings` proves controlled dictionary/plain scalar
  Parquet pages, duplicate content, a real source/target DV, and FULL replacement.
- `novarocks_mv_visible_content_encodings` adds nullable and empty arrays, maps,
  structs and array order, checked by complete independent Spark content bags.
- `novarocks_mv_recursive_schema` checks required recursive children, optional
  list elements/map values, both map entry orders, actual source/target UUID and
  schema/field IDs, SDK and Spark complete bags, real retractions and additions,
  and FULL with exact file summary totals and no delete files.

These cases freeze the fixture publication before Spark invocation and retain
stage logs/receipts under `reports/uea7b3`. They do not establish a particular
Arrow array encoding inside the matcher. The system scenario
`mv/recursive-type-restart` separately covers native lake-only recovery after
FE replacement and owns each Spark job's exact bounded lifecycle and cleanup.


`novarocks_mv_recursive_ddl_ctas` separately freezes an empty ordinary DDL
schema, inserts the complete six-row source bag, and creates a CTAS table from
the same source. Independent SDK checks distinguish ordinary optional child
defaults from CTAS required children and verify exact source/DDL identities,
new CTAS bindings, actual Parquet IDs and complete SDK/Spark bags. All four
cases have native complete row goldens derived from the fixed input recipe and
existing value-formatting contract; row order is ignored, while container
order and multiplicity are preserved.
