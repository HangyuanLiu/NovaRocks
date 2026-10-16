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

Retain upstream registry dependency versions and lints. The root
`[patch.crates-io]` selects `iceberg`, `iceberg-catalog-hms` and
`iceberg-property-macro` at version `0.11.0`; the macro has no source patches.
The unpatched REST SDK is a connector dev-only path dependency, outside the
production normal/build dependency closures. Do not vendor runtime/network/Arrow
crates or add resource accounting hooks. Only the approved protocol/API patch
inventory may diverge.

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

### H4: classify HMS table listings from entity parameters

- Purpose: preserve serial `get_table_objects_by_name` batches of at most 100
  names on the existing client. Emit only entities whose `parameters["table_type"]`
  is `iceberg` ignoring ASCII case, validate the entire returned batch before
  projection, and omit entities that disappeared after the initial name list.
  This is an RPC item-count bound; it does not claim allocation or response-byte
  accounting. rc.2's namespace-existence precheck remains in place.
- Files: `src/catalog.rs` (`HmsCatalog::list_tables`), `src/lib.rs`,
  `src/table_projection.rs` (copied unchanged from the 0.9 vendor).
- Upstream status: no upstream PR/issue tracked; NovaRocks M07 PR #1173 introduced
  this classification. rc.2's `get_all_tables` still returns unclassified names.
- Production dependency/owner: NovaRocks Iceberg connector's catalog listing
  contract; Iceberg connector/MEM-1 maintainers own classification and listing
  admission. No metadata-file loads, retries, client owners, view capability,
  paging capability, or allocation hooks are added.
- Exit: the MEM-1 follow-up that replaces the HMS listing boundary owns deletion,
  after its replacement classifies actual HMS entities with the same omission,
  validation, and error semantics and passes the existing coverage. Alternatively,
  remove through that owner when adopted upstream provides the same behavior.
- Coverage: the nine existing `table_projection_tests` are carried unchanged;
  NovaRocks connector tests compile this source through the existing `#[path]`
  seam (the new path is wired in T6). They cover classification, historical case,
  missing entities, ordering, identity, duplicates, and the 100-name item bound.
  Existing `hms_listing_observer_tests` cover listing admission/exit ownership.
