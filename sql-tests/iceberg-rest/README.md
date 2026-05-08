# Iceberg REST SQL Suite

Focused end-to-end smoke for NovaRocks's own behaviour against an Iceberg REST
catalog. Unlike [`iceberg-compatibility/`](../iceberg-compatibility/) (cross-
engine: Spark writes, NovaRocks reads), this suite has NovaRocks both write
and read, exercising the REST commit protocol branches that the Hadoop-catalog
[`iceberg/`](../iceberg/) suite does not touch.

## What this covers

| Case | What |
| --- | --- |
| `iceberg_rest_namespace_ddl` | REST namespace API: CREATE/DROP DATABASE, IF (NOT) EXISTS, error path |
| `iceberg_rest_table_ddl` | createTable / dropTable commit, partitioned table, CREATE TABLE IF NOT EXISTS no-op |
| `iceberg_rest_insert_select` | appendData / overwrite commit (INSERT VALUES / SELECT / OVERWRITE) |
| `iceberg_rest_schema_evolution` | updateSchema commit: ADD/RENAME/DROP COLUMN, INT→BIGINT widen |
| `iceberg_rest_branch_tag_ddl` | updateRefs commit: CREATE BRANCH/TAG, branch write, FOR VERSION AS OF '<branch>' |
| `iceberg_rest_v3_default_columns` | format-v3 ADD COLUMN with DEFAULT + initial-default backfill + v2 rejection |
| `iceberg_rest_time_travel` | `FOR VERSION AS OF '<branch>'` against REST loadTable + cross-ref join |

`iceberg_rest_metadata_tables` (`$snapshots` / `$refs` / `$history` count
assertions) was removed pending an analyzer fix for 3-part metadata-table
references; see the spawn-out task in the project notes.

## Running

Bring up the docker fixture and standalone-server, then run the suite:

```bash
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh

NO_PROXY=127.0.0.1,localhost \
cargo run -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG" &
until nc -z 127.0.0.1 "$NOVA_ENV_MYSQL_PORT"; do sleep 1; done

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --mode verify
```

To regenerate the result files after intentional behaviour changes:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --mode record --record-from target
```

Then review the diff under `result/` and commit.

## Conventions

Every case file uses three-part naming
`iceberg_rest_${suite_uuid0}.<db>_${uuid0}.<tbl>_${uuid0}` so parallel runs do
not collide. Results are recorded with `--record-from target` (the local
NovaRocks server is the source of truth — there is no reference DB for
NovaRocks-specific REST behaviour).
