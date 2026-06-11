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

This crate is otherwise an unmodified copy of crates.io
`iceberg-catalog-hms 0.9.0`. Its `iceberg` dependency (`version = "0.9.0"`) is
redirected to `vendor/iceberg-0.9.0` by the root `[patch.crates-io]` block, the
same way `iceberg-catalog-rest` is.
