# Iceberg Variant interoperability fixture

This correctness fixture uses the BOM-pinned Iceberg Java 1.11.0 runtime inside
the existing Spark 3.5 fixture. Java APIs create Variant/UUID tables; the optional generic reader probe has the
BOM limitation described below.
Spark SQL does not need to represent Variant values.

The protected product properties are reading externally authored unshredded and
shredded Variant, preserving ordinary struct identity, refusing unsupported data
writes before publication or physical data creation, projecting unrelated fields
of nested-Variant tables, and retaining supported DV/equality-delete behavior.
The existing NovaRocks-only tests cannot supply independent Variant files or an
independent reader, so a Java-backed SQL correctness fixture is the minimum
sufficient level. Expected values come from fixed Java construction inputs and
the Variant physical-type specification, not from NovaRocks output.

The Iceberg connector owns this fixture and its maintenance. Consumers are the
`java_variant_*`, `java_uuid_write_rejected`, and
`java_timestamptz_write_adaptation` cases in `iceberg-compatibility`.

```bash
# Resolve the generated immutable fixture publication once before invoking consumers.
fixture_publication="$(python3 -c 'from pathlib import Path; print(Path("docker/iceberg-rest/runtime/current/published").resolve(strict=True))')"
source "$fixture_publication/env.sh"
receipt_parent="${XDG_STATE_HOME:-$HOME/.local/state}/novarocks/experiments/iceberg-variant"
mkdir -p "$receipt_parent"
out="$(mktemp -d "$receipt_parent/$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX")"
tests/sql/fixtures/iceberg-variant/run.sh \
  --mode unshredded --namespace iru9_example --table variants --output "$out"
```

Modes `unshredded`, `shredded`, `bounds`, `nested`, `uuid`, and `write-target`
create one new table. `observe` prints sorted IDs through `IcebergGenerics`.
`unchanged` compares both the current snapshot and the physical object-store
`data/` listing with facts saved before the rejected operations. The shredded
mode supplies an explicit `VariantShreddingFunction`; its manifest omits Variant
bounds. The bounds mode retains and asserts real Java writer Variant bounds,
so the NovaRocks hint test cannot pass accidentally on a bounds-free file.

Each run requires a fresh output directory. SQL consumers allocate it under the
existing XDG state experiment root; `IRU9_ARTIFACT_DIR` in the native shell output
links retained Scala and Spark output to that case. The connector owner retains
failed and formally referenced inputs until their investigation and handoff are
complete; successful probes are not automatically deleted. It retains the exact generated Scala
script and complete Spark log, and succeeds only with `IRU9_FIXTURE_COMPLETE`.
Scala exceptions exit nonzero; Scala compilation errors are also caught by the
missing completion marker. Runtime versions are checked explicitly. SQL cases
use unique table names and drop their tables on success. Failed run artifacts
and tables remain for investigation under the runner's normal evidence policy;
the fixture does not purge shared storage or rewrite expected results.

The full independent footer/type matrix and Spark row/vectorized interoperability
matrix were explicitly waived for IRU-9 on 2026-10-15. They are not implemented or
claimed by this fixture. Focused data/equality writer tests independently inspect
Parquet footer annotations with parquet-rs; this fixture retains the independent
Java Variant read/write and no-side-effect checks.

The current BOM image's generic Java Variant reader fails with
`NoSuchMethodError: commons.lang3.stream.Streams.of(Iterable)`. `observe` remains
an explicit independent probe and is not called by the default SQL cases while
the approved independent-read exception applies. No implicit host Maven jar or
classpath override is used. Generation receipts depend on successful Java writes,
actual writer metrics/bounds, and catalog publication; NovaRocks reads the fixed
fixture values in the SQL cases.
