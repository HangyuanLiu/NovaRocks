# NovaRocks patches on top of crates.io iceberg-catalog-hms 0.9.0

- `src/schema.rs` `HiveSchemaBuilder::primitive`: added a
  `PrimitiveType::Variant` match arm. The NovaRocks-vendored `iceberg` crate
  carries PATCH 6, which adds `PrimitiveType::Variant` (Iceberg v3 unshredded
  variant) to the `PrimitiveType` enum. That enum change is **not** additive for
  downstream exhaustive `match` expressions, so crates.io
  `iceberg-catalog-hms 0.9.0` — written against an upstream `iceberg` without
  `Variant` — fails to compile (`E0004: pattern \`&PrimitiveType::Variant\` not
  covered`) when built against the vendored iceberg. Hive Metastore has no
  native variant column type, so the patch routes `Variant` into the existing
  `FeatureUnsupported` arm (the same treatment as `Timestamptz` /
  `TimestamptzNs`) rather than guessing a mapping.

- `src/catalog.rs` `HmsCatalog::update_table` and `src/utils.rs`
  `update_hive_table_metadata`: implemented Iceberg table update commits for
  HMS. Upstream `iceberg-catalog-hms 0.9.0` ships `update_table` as a
  `FeatureUnsupported` stub, which allows `CREATE TABLE` but makes any commit
  that advances table metadata fail. The patch applies the `TableCommit`, writes
  the staged metadata file, verifies the HMS `metadata_location` has not changed
  since load time, then updates the HMS table parameters and storage descriptor
  with the new Iceberg metadata pointer.

Its `iceberg` dependency (`version = "0.9.0"`) is redirected to
`vendor/iceberg-0.9.0` by the root `[patch.crates-io]` block, the same way
`iceberg-catalog-rest` is.


- `src/catalog.rs` `load_table` / `get_namespace`: gave a missing table or
  database the error kind the metastore's own answer proves. Upstream funnels
  every thrift exception -- `NoSuchObjectException` included -- through
  `from_thrift_exception` into the generic `Unexpected`, so a caller cannot tell
  "the metastore says it is not there" from "the metastore could not be
  reached". `table_exists` and `namespace_exists` in the same file already match
  the typed `O2(NoSuchObjectException)` / `O1(NoSuchObjectException)` variants,
  which makes this an internal inconsistency rather than an upstream choice.
  `load_table` now returns `TableNotFound` and `get_namespace` returns
  `NamespaceNotFound`; real thrift failures keep `Unexpected`. NovaRocks
  classifies absence from the error kind alone (there is no message-sniffing
  fallback), so without this a Hive catalog would report an absent table as an
  unavailable control plane.

- `src/catalog.rs` `HmsCatalog::list_tables`: classify names from `get_all_tables`
  using actual HMS entity parameters from `get_table_objects_by_name`, in
  serial batches of at most 100 requested names on the existing client. Only
  `parameters["table_type"]` equal to `iceberg` ignoring ASCII case is emitted
  as an Iceberg table; views and foreign table kinds are excluded. Validate
  returned entity identity, membership, uniqueness and batch count before
  projection. Entities missing after the initial name listing are omitted,
  matching the stock Java catalog's non-snapshot listing behavior. Transport
  and declared RPC failures remain errors. This is an accurate-classification
  fix, not SDK allocation accounting or a response-byte bound. It adds no
  Iceberg table/metadata-file loads, new client ownership, retries, view
  capability or paged listing capability.

  The pure `src/table_projection.rs` source is compiled directly by the existing
  NovaRocks Iceberg connector's tests so its classification contract is covered
  in workspace C0 without adding the vendor package or its external integration
  tests as workspace members.
