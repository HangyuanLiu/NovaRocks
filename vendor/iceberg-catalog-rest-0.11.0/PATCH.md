# NovaRocks patches over apache/iceberg-rust 0.11.0-rc.2

Upstream tag: `v0.11.0-rc.2`
Upstream commit: `859048563bc71a3167d0545cb66756d26c64e4ca`
Upstream repository: https://github.com/apache/iceberg-rust
Normalized package: `iceberg-catalog-rest-0.11.0.crate`
Package SHA-256: `fde7674c80f19dc54ae2b0b3fb95f0715a4407df615fa63fe42801bc3ec2d2d4`

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

Standalone validation uses the root development profile parameters and sibling
patches. From this crate directory:

```sh
export CARGO_PROFILE_DEV_DEBUG=line-tables-only
export CARGO_PROFILE_DEV_OPT_DEBUG=line-tables-only
cargo check --lib --profile dev-opt --target-dir ../../target \
  --config 'patch.crates-io.iceberg.path="../iceberg-0.11.0"' \
  --config 'patch.crates-io.iceberg-property-macro.path="../iceberg-property-macro-0.11.0"' \
  --config 'profile.dev-opt.inherits="dev"' \
  --config 'profile.dev-opt.opt-level=1' --config 'profile.dev-opt.debug=1' \
  --config 'profile.dev-opt.incremental=true' --config 'profile.dev-opt.lto=false' \
  --config 'profile.dev-opt.codegen-units=256'
# Repeat the same arguments with `cargo test --locked --lib`.
```

The final cutover validation ran the standalone check and the complete library
suite: **102 passed, 0 failed, 0 ignored**. Earlier compilation failures from
unused import qualifications are retained with their corrected reruns in the
IRU-3 execution evidence. The generated standalone lock is retained outside
source control; package content and dependency resolution remain reproducible.

## Patch inventory

### R10: response and credential diagnostics stay redacted

- Files: `src/client.rs`, `src/auth/oauth2.rs`, `src/types.rs`.
- Purpose: keep raw response bodies, header values, server error messages/stacks
  and credential config values out of Error/Debug. Header names-only formatting
  makes disable-header-redaction inert. ErrorModel retains its parsed type,
  code and stack depth, with the fixed message from the old patch.
- Required 0.11 adaptation: opaque serde_json decoder source diagnostics may
  echo response enum/known-field values. Those source payloads are suppressed
  on REST/OAuth redaction paths; ErrorKind/status/retry classification is kept.
  Parsed protocol category facts are deliberately preserved.
- Upstream status: pinned upstream still prints body/context and header values.
- Production dependency: all REST/OAuth errors and response/delegation Debug.
- Exit: IRU-13a replaces the vendored redaction with closed adapter responses.
- Coverage: migrated old error/body secret canaries and header assertions,
  OAuth valid/invalid JSON and full opaque-source diagnostic canaries, and
  DTO credential-value Debug canary. Conflicting upstream header tests now
  assert names-only output even with redaction disabled.
- Validation: standalone REST crate check/tests after this final patch is ready.
