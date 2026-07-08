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

# low-cardinality-native

StarRocks **native**-storage (default_catalog) companion to the `low-cardinality`
suite. These cases stay native on purpose — they exercise 128-bit `LARGEINT`
scan min-max statistics, which do not survive Iceberg's
`LARGEINT -> DECIMAL(38,0)` type mapping (`DECIMAL(38,0)` max ≈ 9.99e37 <
2^127 ≈ 1.7e38), so they cannot be migrated to Iceberg v3 without losing the
128-bit coverage they were written to provide.

They were split out of `low-cardinality` when that suite migrated its
dictionary-rewrite cases (`rewrite` / `stale` / `disabled`) to Iceberg v3
(Option A: iceberg/HDFS scan dict-encode execution). The runner's `init.sql`
`@catalog` is suite-wide with no per-case override, so a single suite cannot
cleanly mix Iceberg and native cases — hence the separate native suite (no
`init.sql`; runs in `default_catalog`).

- `compressed_key` / `compressed_key2`: legacy `test_agg` compressed-key
  coverage (DUPLICATE / AGGREGATE KEY tables, `LARGEINT`, scan min-max stats).

After R0 retired the standalone native low-cardinality rewrite, this suite keeps
the native storage and `LARGEINT`/min-max coverage, but it must not require
legacy native `DECODE` plan nodes. `EXPLAIN` assertions in this suite should
guard against reintroducing that shape rather than waiting for it.
