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

# SQL Benchmark Baselines

This directory holds reviewed, committed comparison baselines for SQL benchmark
reports. It is not a CI scheduler and it does not receive generated run output.

Run the local benchmark entry point from the repository root:

```bash
tools/benchmark/run-sql-benchmark.sh
```

The wrapper builds `target/release/novarocks`, exports that path as
`NOVAROCKS_BIN`, and runs the SSB workload by default. Pass benchmark-runner
arguments to select a different workload or location:

```bash
tools/benchmark/run-sql-benchmark.sh --suite tpc-h --backend-count 2
tools/benchmark/run-sql-benchmark.sh --suite all --backend-count 4 --output-dir /tmp/novarocks-benchmarks
```

Generated reports are written to `reports/sql-benchmarks/` by default and are
ignored by Git. Each report records result verification, one warmup pass, five
serial measured passes, and one profile pass per query. Benchmark data is the
shared immutable fixture resolved by the runner; a missing fixture may be built
once, while an invalid published fixture fails closed.

The benchmark process always uses the cross-process harness. `--backend-count`
selects the BE count for a run (default: one); scenario files do not set it.
An external controller can pass `--controller-environment <JSON_OBJECT>` and
`--comparison-key <KEY>`. Those values are copied to `run.json` and
`SUMMARY.md`; the runner does not collect or enforce CPU, memory, or OS
requirements. The controller owns cross-machine comparison policy.

Add a baseline only after review. Keep the captured environment, fixture
identity, exact revision, and comparison conclusion alongside any committed
baseline artifact.
