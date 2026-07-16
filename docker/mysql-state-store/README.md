# MySQL state-store fixture

This fixture is the production acceptance environment for the NovaRocks MySQL
state-store provider. It runs the Docker Official MySQL 8.4.10 LTS image at the
index digest frozen in `compose.yml`, with a 16 KiB InnoDB page, DYNAMIC row
format, UTC, and strict SQL mode.

The fixture is isolated per worktree. It derives a Compose project and host port
from the workspace path and writes credentials only to generated mode-0600
files below `runtime/`. It does not read a Homebrew installation and does not
reuse either Iceberg fixture.

Callers must install cleanup before startup:

```bash
trap 'docker/mysql-state-store/down.sh --docker' EXIT
docker/mysql-state-store/up.sh
source docker/mysql-state-store/runtime/current/env.sh
```

`up.sh --prepare-only` creates the private runtime files without starting
Docker. `up.sh` starts MySQL, provisions a non-destructive readiness database
through the sole database owner, removes any prior readiness database owned by
the same worktree, and verifies real SQL readiness. `status.sh` repeats the SQL
checks. While the Compose project is running, `down.sh` retains its backing
runtime so the container cannot lose its generated secrets or configuration.
`down.sh --docker` stops the derived worktree Compose project and then removes
the runtime; both forms derive the project identity and are safe before prepare
or after a partial startup.

Tests that mutate schema or coordinate multiple processes must request a unique
database:

```bash
db="$(docker/mysql-state-store/provision-test-database.sh create my-case)"
trap 'docker/mysql-state-store/provision-test-database.sh drop "$db"' EXIT
```

The ordinary provider user has only table DDL/DML privileges on the fixed
state-store, readiness, and physical-probe table names inside databases created
by this helper. It cannot create or drop databases. The independent provisioner
credential never belongs in provider configuration, helper protocols, process
arguments, or debug output.

Concurrent physical probes use explicit MySQL named-lock barriers. Each worker
publishes a readiness marker only after establishing the transaction state
under test, then blocks behind a gate connection until the coordinator performs
the competing operation. Gate release uses the discovered connection ID, so
snapshot, deadlock, and lock-timeout ordering does not depend on fixed sleeps.

## Auxiliary mechanism evidence

A local MySQL 9.7.1 experiment demonstrated SERIALIZABLE gap/next-key locking:
two transactions that observed the same empty range and inserted different
keys resolved as one commit and one deadlock. This is mechanism evidence only.
The provider design uses nonlocking REPEATABLE READ observations followed by
commit-time OCC so the first commit is not blocked by another public reader.
