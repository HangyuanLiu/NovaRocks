# UEA-7 Iceberg REST publication fixture

This test-only image extends the pinned
`apache/iceberg-rest-fixture:1.10.1` JDBC catalog. It does not add a private
catalog protocol and it is not a production configuration.

The custom `JdbcCatalog` wraps the real `TableOperations` returned by
`JdbcCatalog.newTableOps`. A one-shot hold is entered at
`TableOperations.commit(base, updated)`, before the delegated JDBC commit.
For the REST server's standard table-commit path this point is reached only
after the request requirements have been checked against that request's base
and its updates have produced `updated`. The release continues the exact same
`base` and `updated` values; it never refreshes or substitutes them.

This location matters. The SQL runner's existing transparent proxy can pause a
request before forwarding it or discard a response after downstream success,
but it cannot prove the interval between server-side requirement validation and
the persistent JDBC compare-and-swap.

## Local prerequisites

The build is deliberately offline with respect to container registries. These
exact images must already exist locally:

- `apache/iceberg-rest-fixture:1.10.1`
- `apache/spark:3.5.5-java17`

Build and run the low-level V07 oracle:

```bash
docker build --pull=false \
  -t novarocks/iceberg-rest-publication-fixture:test \
  tests/fixtures/iceberg-rest-publication

tests/fixtures/iceberg-rest-publication/run-v07.sh
```

Set `UEA7_SKIP_BUILD=1` to reuse the already built image. The script creates one
uniquely named container, publishes random loopback ports, uses an isolated
SQLite catalog and local warehouse, and removes the container on every exit.
It never addresses the shared `nr-iceberg-rest` project.

Set `UEA7_ARTIFACT_DIR` to a new or empty absolute directory to retain the
bounded NDJSON trace, request/response bodies, container log, and a manifest
binding the evidence to the Git HEAD and exact image identities.

## Evidence

The control endpoint keeps at most 512 NDJSON trace events. Each held commit
records:

1. `requirements-passed-before-persistent-commit`;
2. `hold-reached` before any delegated commit;
3. `hold-released`;
4. one `delegate-commit-start` using the recorded base and updated identities;
5. `delegate-commit-success` or `delegate-commit-conflict`.

The V07 script covers both orderings from the same schema base. When the newer
request commits first, releasing the old request must produce a JDBC base
conflict; the REST handler then refreshes and rejects the unchanged original
`assert-current-schema-id` requirement. When the held request commits first,
the frozen competing request is rejected by that same requirement. A third
case terminates the waiting HTTP client and proves that the server-owned held
request can still be released and committed.
