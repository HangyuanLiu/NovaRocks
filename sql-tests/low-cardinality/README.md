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

End-to-end coverage for the runtime low-cardinality carrier. Cases here
exercise:

- Parquet dictionary pages flow into execution as self-describing Arrow
  `DictionaryArray` values without table-level dictionary metadata;
- filter, aggregate, NULL and runtime-filter semantics match flat string
  execution;
- runtime filters stay value-domain correct over low-cardinality string data;
- runtime observability reports dictionary carrier input, kept, and hydrated
  counters.

Standalone SQL does not build or consume table-level global dictionary
snapshots. The FE-compatible `query_global_dicts` / `DECODE_NODE` path is a
separate protocol path driven by StarRocks FE plans. `EXPLAIN COSTS` is
**not** suitable in standalone mode —
`try_explain_costs` short-circuits to an ESTIMATE / cardinality summary and
never renders the physical plan tree.

## Storage (Iceberg v3)

All cases here run on **Iceberg v3** via `init.sql`'s
`lowcard_cat_${suite_uuid0}` external catalog. Low-cardinality execution uses
file- or batch-local dictionary carriers and does not require statistics
collection.

The legacy compressed-key cases now live with the aggregate correctness cases
in **`aggregate`**. Focused 128-bit `LARGEINT` statistics coverage lives in
**`statistics`**.
