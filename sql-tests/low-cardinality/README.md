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

# low-cardinality

End-to-end coverage for low-cardinality dictionary metadata and carrier
compatibility after R0. Cases here exercise:

- `ANALYZE FULL TABLE` populates `dictionary.snapshot` metadata for
  string-typed columns, while standalone SQL results continue to follow plain
  string semantics;
- write paths (INSERT / UPDATE / MERGE / TRUNCATE / DELETE) advance table
  snapshots so stale dictionary metadata does not affect query correctness;
- DROP TABLE / DROP DATABASE remove dictionary metadata;
- runtime filters stay value-domain correct over low-cardinality string data;
- runtime observability reports dictionary carrier input, kept, and hydrated
  counters without restoring legacy native rewrite plan shapes.

R0 retired the standalone native low-cardinality rewrite path. Standalone SQL
plans should not contain FE-compatible `DECODE` nodes or scan dictionary hints;
cases that need plan-shape protection use `@explain_not_contains` on the query
under test. `EXPLAIN COSTS` is **not** suitable in standalone mode —
`try_explain_costs` short-circuits to an ESTIMATE / cardinality summary and
never renders the physical plan tree.

## Storage (Iceberg v3)

All cases here run on **Iceberg v3** via `init.sql`'s
`lowcard_cat_${suite_uuid0}` external catalog. `ANALYZE FULL` builds Iceberg
dictionary metadata; a subsequent write advances the table snapshot so stale
metadata must not change the rows returned by standalone SQL (see `stale`).

The legacy compressed-key cases now live with the aggregate correctness cases
in **`aggregate`**. Focused 128-bit `LARGEINT` statistics coverage lives in
**`statistics`**.
