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
through the sole database owner, and verifies real SQL readiness. `status.sh`
repeats the SQL checks. `down.sh` removes only this worktree runtime by default;
`down.sh --docker` also stops its Compose project and is safe before prepare or
after a partial startup.

Tests that mutate schema or coordinate multiple processes must request a unique
database:

```bash
db="$(docker/mysql-state-store/provision-test-database.sh create my-case)"
trap 'docker/mysql-state-store/provision-test-database.sh drop "$db" || true' EXIT
```

The ordinary provider user has only table DDL/DML privileges inside databases
created by this helper. The independent provisioner credential never belongs
in provider configuration, helper protocols, or debug output.

## Auxiliary mechanism evidence

A local MySQL 9.7.1 experiment demonstrated SERIALIZABLE gap/next-key locking:
two transactions that observed the same empty range and inserted different
keys resolved as one commit and one deadlock. This is mechanism evidence only.
The provider design uses nonlocking REPEATABLE READ observations followed by
commit-time OCC so the first commit is not blocked by another public reader.
