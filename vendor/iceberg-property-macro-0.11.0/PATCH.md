# NovaRocks patches over apache/iceberg-rust 0.11.0-rc.2

Upstream tag: `v0.11.0-rc.2`
Upstream commit: `859048563bc71a3167d0545cb66756d26c64e4ca`
Upstream repository: https://github.com/apache/iceberg-rust
Normalized package: `iceberg-property-macro-0.11.0.crate`
Package SHA-256: `23215cd7fcefd7d7c02f2590052cf1dc5311f7867b2e626d8b7b0b77692e4dd3`

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

No patches applied in the initial source commit.
