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
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility --mode verify
```
