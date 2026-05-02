# R0 - IVM on Iceberg Baseline Summary

**Branch:** `codex/iceberg-ivm-object-delete-projection`
**Baseline commit:** `82c80b8 Support Iceberg IVM delete projection on object stores`

## Supported

- Aggregate MV full refresh over managed-lake storage.
- Aggregate MV incremental refresh for Iceberg append snapshots.
- Aggregate MV DELETE retract for Iceberg v2 Parquet position deletes.
- Aggregate MV DELETE retract for Iceberg v3 Puffin deletion vectors.
- Object-store delete reverse projection for S3/S3A-style paths through configured catalog credentials.
- Empty change stream metadata advance without writing data chunks.
- `_row_id` and `_last_updated_sequence_number` reads on Iceberg v3 row-lineage base tables.

## Explicitly Unsupported

- `INSERT OVERWRITE` incremental bridging.
- Projection/filter MV DELETE apply.
- Equality deletes.
- Schema evolution where the MV uses the changed column.
- Partition evolution policy.
- Concurrent refresh on the same MV.

## Verification

- `cargo test --lib`
- `cargo fmt --check`
- `git diff --check`
