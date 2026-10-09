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

# HMS read-only compatibility fixture

Start the already provisioned `docker/iceberg-rest/` and
`docker/iceberg-hive/` fixtures, then source their exact generated publications.
The SQL suite remains `explicit_only` and runs on native 1FE+3BE with `-j 1`.
`fixture.py` does not provision images or download inputs.

Spark creates format v1/v3 partitioned tables and a v2 unpartitioned table, a v2 position delete,
a v3 deletion vector, and v2 branch/tag references. Preparation checks for a PARQUET position-delete file on v2 and a PUFFIN deletion vector on v3; the v2 rows share one file to avoid a whole-file delete. Spark changes the v2 delete mode to copy-on-write after preparing its position delete, so both mutation modes can be refused through valid SQL requests. Tables use locations under
`<generated HMS warehouse>/iru7/<unique namespace>/`. NovaRocks only reads
these tables and attempts operations expected to be refused.

The before/after evidence includes namespace/table/view lists, each table's
**current** HMS metadata location (via `TableOperations.current()`), and every
object's key, etag and size. ListObjectsV2 uses two objects per page and rejects
missing or repeated continuation tokens. Inventory covers the private namespace
location and the exact Hive default namespace locations (including `.db`) for
attempted namespace/table creation. It never compares just one table's files.
Evidence is saved under `tests/sql/.runtime/hms-evidence/<namespace>/`.

Cleanup executes Spark PURGE/DROP, then deletes remaining objects only in those
unique namespace prefixes and verifies that they are empty. Shared HMS services,
other namespaces and other worktrees are retained. Provider entry tests cover
operations that cannot be reached through SQL on an HMS-managed MV, as HMS
cannot create that target.
