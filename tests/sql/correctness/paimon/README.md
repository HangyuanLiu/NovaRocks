# Paimon correctness suite

**DISABLED** pending a detailed Paimon connector design. Cases, expected results,
fixture writers, and production provider registration are retained. SQL runner
selection refuses `--suite paimon`, including explicit requests; full CI neither
selects this suite nor prepares its Spark/Paimon fixture. The commands below
are the retained historical recipe, not an enabled validation entrypoint.

This explicit suite reads the immutable fixture published by
`docker/paimon-read/prepare.sh`. Spark/Paimon is the external writer and oracle;
NovaRocks only reads the published Filesystem Catalog.

Historical fixture and suite recipe:

```bash
docker/paimon-read/prepare.sh --run-id "$RUN_ID" --output-dir "$OUTPUT_DIR"
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$OUTPUT_DIR/sql-runner.toml" --suite paimon --mode verify
docker/paimon-read/cleanup.sh --run-id "$RUN_ID" --output-dir "$OUTPUT_DIR"
```

The suite covers append-only files and codecs, current-snapshot deduplicate
merge semantics, field-ID schema evolution, engine-side filter/limit behavior,
read-only capability rejection, unsupported Paimon encodings, and one
Iceberg/Paimon cross-catalog query.
