# NovaRocks patches over apache/iceberg-rust 0.11.0-rc.2

Upstream tag: `v0.11.0-rc.2`
Upstream commit: `859048563bc71a3167d0545cb66756d26c64e4ca`
Upstream repository: https://github.com/apache/iceberg-rust
Normalized package: `iceberg-catalog-hms-0.11.0.crate`
Package SHA-256: `d934fc971b51044affb2b183a19f4fefcddaa513deb3350af6b361d6970010c0`

Generation command (Rust/Cargo 1.98.1, from the verified upstream checkout):

```sh
cargo +1.98.1 package --no-verify --exclude-lockfile --offline \
  -p iceberg-property-macro -p iceberg -p iceberg-catalog-rest -p iceberg-catalog-hms
```

`--exclude-lockfile` avoids resolving unrelated unpublished/workspace development
dependencies while preserving Cargo's normalized manifest and original published
source files. Each extracted file was compared byte-for-byte with this package.
The initial source commit is unmodified except for this inventory.

## Dependency policy

Retain upstream registry dependency versions and lints. The four Iceberg packages
are patched together at version `0.11.0`; `iceberg-property-macro` is vendored
without source patches. Do not vendor runtime/network/Arrow crates or add resource
accounting hooks. Only the approved protocol/API patch inventory may diverge.

## Validation

- Compare the initial source commit against the recorded package.
- Use workspace dependency patches for consumer checks and tests.
- Independently check this crate with sibling Iceberg patches when resolvable;
  record unavailable upstream development dependencies as a validation gap.

## Patch inventory

### H3: typed missing-table result

- Purpose: preserve `load_table`'s exact `O2(NoSuchObjectException)` mapping to
  `TableNotFound`; other declared Thrift exceptions and transport failures retain
  `Unexpected`. The upstream runtime/KMS table construction is unchanged.
- Files: `src/catalog.rs` (`HmsCatalog::load_table`).
- Upstream status: no PR/issue tracked. rc.2 still classifies this load error as
  `Unexpected`; `get_namespace`'s corresponding namespace mapping is already
  upstream and has no local patch.
- Production dependency/owner: NovaRocks Iceberg connector's HMS catalog control
  and load paths distinguish explicit absence from unavailable catalog service by
  `ErrorKind`; the Iceberg connector maintainers own this compatibility contract.
- Exit: remove in an HMS upstream-convergence follow-up owned by those maintainers
  once the adopted upstream load API preserves the typed absence/transport
  distinction and the consumer coverage passes without this patch. No accepted
  IRU subtask currently claims that deletion.
- Coverage: existing NovaRocks `catalog::error` unit tests cover the consumer's
  `TableNotFound` versus `Unexpected` classification; `iceberg-hms` SQL covers
  normal table loading. Standalone crate checking validates the match against
  the generated Thrift exception vocabulary. There is no isolated RPC-absence
  test in this port; no new test hooks or mock classification helper are added.
