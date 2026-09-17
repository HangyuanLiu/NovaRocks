# NovaRocks patches on top of crates.io iceberg-catalog-rest 0.9.0

Every entry below says *why* the patch exists, because this file's job is to
tell whoever bumps the vendored version which divergences must survive the
bump. Line references are into `src/` of this directory.

- Implemented the Iceberg REST view endpoints on `RestCatalog`:
  create_view / load_view / update_view (commit) / drop_view /
  view_exists / list_views, plus `CreateViewRequest`, `LoadViewResult`,
  `CommitViewRequest` wire types. Upstream 0.9.0 has no view support.

- Added `RestCatalog::stage_create_table`, which uses the catalog's existing
  configured HTTP client, authentication, and FileIO construction while
  sending `stage-create: true` and accepting the specified null
  `metadata-location` response. The result carries initialization updates
  derived from authoritative staged metadata for a single subsequent
  `assert-create` commit.

- Added typed `stage_create_table_typed` and `commit_staged_table_typed`
  variants. They preserve `Conflict`, `KnownNotDispatched`,
  `PossiblyDispatched`, and committed-response-finalization states so a
  durable CTAS saga never classifies REST dispatch certainty from strings or
  the generic `Unexpected` error kind.

- Gave `Catalog::create_table` and `Catalog::load_table` the error kinds their
  status codes prove, instead of the generic `Unexpected`. Upstream 0.9.0 maps
  create's 409 and 404 and load's 404 to `Unexpected`, while `register_table`
  in the same file already maps the identical statuses to `TableAlreadyExists`
  and `NamespaceNotFound` -- so this is an internal inconsistency, not a
  deliberate upstream choice. It matters because a *received* 409 proves the
  create did not happen and a received 404 proves the table is absent: with
  `Unexpected`, a definitively refused create is reported as dispatch-unknown,
  and adjudication can never prove absence, so it can never settle a lost
  response. Now `create_table` returns `TableAlreadyExists` / `NamespaceNotFound`
  and `load_table` returns `TableNotFound`.

- Added an opt-in access-delegation request surface: the
  `x-iceberg-access-delegation: vended-credentials` header constants
  (`catalog.rs:58-64`) and the `*_with_access_delegation` methods that send
  them -- `load_table_with_access_delegation` (`catalog.rs:849`),
  `load_table_deferred_with_access_delegation` (`catalog.rs:866`),
  `create_table_with_access_delegation` (`catalog.rs:836`), and the staged
  variants (`catalog.rs:953`, `catalog.rs:982`). Upstream 0.9.0 never sends
  the header, so a REST server that vends storage credentials returns none and
  the only object-store credentials available are process-global ones -- which
  is the configuration NovaRocks is trying to stop requiring. The header is
  installed *only* by these parallel methods, never by the upstream `Catalog`
  trait implementations; that is the property that lets delegation be adopted
  per catalog without changing the wire shape of any existing call.

- Added `RestAccessDelegation` and `StorageCredentialDelegation`
  (`catalog.rs:390-473`) as the only way to read
  `LoadTableResult::storage_credentials`. They expose the prefix, the
  configuration *key names*, and a value only by exact-key lookup; they are
  deliberately not serializable, expose no raw-map accessor, and their
  hand-written `Debug` prints counts and key names. The point is that a vended
  credential must not be able to reach a log line, a `Debug` output, or an
  error context by accident: without a closed wrapper the response's
  credential map is an ordinary `HashMap<String, String>` that any
  `{:?}` on a surrounding value will print in full.

- Added `DeferredRestTableMaterialization` (`catalog.rs:514-538`) and the
  deferred response types that carry it, so a REST table response can be
  frozen *before* any `FileIO` is constructed and materialized afterwards
  through `materialize_with_file_io`. Upstream builds FileIO from the
  catalog-global storage factory while it decodes the response, which is
  exactly the wrong order for vended credentials: the credentials that
  authorize the storage arrive *inside* that same response. The provider
  (`novarocks/connector/iceberg/src/catalog/rest.rs:153` and `:240`,
  `loaded_table.rs:557`) depends on this ordering -- it converts the
  delegation into its own sealed capability and only then chooses the
  request-scoped FileIO. `commit_staged_table_typed_deferred` and
  `commit_staged_table_typed_with_file_io` (`catalog.rs:1177`, `:1253`) are
  the same inversion for the publish leg, where the commit response proves the
  catalog mutation but carries no new delegation, so the caller must finalize
  it with the lease it already holds. A rebase that keeps only the eager
  `*_with_access_delegation` methods and drops the deferred ones silently
  reintroduces the catalog-global FileIO on the vended path.

- Added `load_credentials_with_access_delegation` (`catalog.rs:909`) and its
  `LoadCredentialsResult` wire type (`types.rs:293-298`). A vended lease
  expires, and renewing it by re-loading the table would re-observe schema and
  snapshot state -- a renewal could then silently move the table an in-flight
  query is already reading. So renewal is a separate closed response with no
  metadata and no FileIO configuration, reached through the already-initialized
  catalog context and its authenticated client, and it returns the same
  redacted wrapper. The caller is
  `novarocks/connector/iceberg/src/loaded_table.rs:484`.

- Added `CatalogConfig::merged_properties` (`types.rs:38-44`) and
  `RestContext.server_properties` (`catalog.rs:367`): the `defaults` plus
  `overrides` the server returned from `/v1/config`, with every user-supplied
  property excluded. Upstream merges server and user configuration into one
  `RestCatalogConfig` immediately, after which no capability gate can
  distinguish a fact the *server* asserted from one a local operator wrote
  into the catalog definition. Recorded honestly: **nothing reads
  `server_properties` today** -- it is populated and kept so that a future
  extension gate cannot be written against the merged map by accident. On
  upgrade, keep it or delete it deliberately; do not "repair" it by pointing
  it at the merged config, which would defeat its only purpose.

- Removed every path that puts a REST response body, a response header value,
  or a server-supplied error message into an `Error` context or a `Debug`
  output. Concretely: the `json` error context on authentication failure
  (`client.rs:166`, `:180`), on response decode failure (`client.rs:292`) and
  on unexpected-status errors (`client.rs:325`) is now a fixed
  `response_body: [REDACTED]`; `deserialize_unexpected_catalog_error` no
  longer reads the body at all; `format_headers_redacted` (`client.rs:300`)
  returns header *names* only; `HttpClient`'s `Debug` prints
  `extra_header_names` (`client.rs:51`); and `ErrorResponse`, `ErrorModel`,
  `LoadTableResult` and `StorageCredential` get hand-written `Debug` impls
  (`types.rs:52`, `:79`, `:260`, `:311`).

  Upstream's redaction is an allow-list of six sensitive header *names*. That
  shape is adequate only while secrets travel in request headers; once the
  catalog vends credentials the secret is in the response **body**, under a
  server-chosen key, and a non-2xx body or an extension header defeats a
  name allow-list entirely. Two canary tests hold the line: `types.rs:483`
  and `client.rs:448`.

  Two consequences to carry forward deliberately. First,
  `REST_CATALOG_PROP_DISABLE_HEADER_REDACTION` is still accepted and still
  threaded through the call sites, but it is now inert -- values are never
  printed regardless (`client.rs:300` takes it as `_disable_redaction`).
  Second, this is a behavior change and not only a logging change:
  `From<ErrorModel> for Error` (`types.rs:90`) now builds the error from the
  fixed string `"REST catalog returned an error"` and records only
  `stack_depth`, so the REST server's own message no longer reaches the user.
  That is intended -- the message is server-controlled text on a path that can
  carry credentials -- but it means an upgrade that drops this patch will look
  like it *improves* diagnostics while reopening the leak.

- Routed `Catalog::create_table` and `Catalog::load_table` through the private
  `create_table_with_stage`, `materialize_table_response` and
  `defer_table_response` helpers instead of each building its own request and
  `Table`. This is not a tidy-up: it is what keeps the delegated and the
  ordinary paths on one request shape and one materialization, with the
  `stage_create` flag and the delegation header as the only two things that
  vary. If a rebase re-inlines the upstream method bodies, the
  `*_with_access_delegation` methods start drifting from the trait methods
  they are supposed to mirror.

- Left over from the fenced-CTAS publication extension (#896), whose own
  PATCH.md entry was removed together with the extension in #969:
  `materialize_ctas_staged_table` (`catalog.rs:1066`),
  `encode_ctas_stage_provider_payload` (`catalog.rs:1108`),
  `encode_ctas_publish_provider_payload` (`catalog.rs:1131`) and
  `encode_ctas_downstream_action` (`catalog.rs:1399`). They encode one
  ordinary REST stage/commit request as a bounded extension payload and
  materialize the staged table a fence-aware server returns, without opening a
  second client or dispatching a second stage request. Recorded here so the
  next upgrade does not have to reconstruct this: **nothing in the repository
  calls them today.** Dropping them during a rebase is safe; carrying them
  forward unexamined is not "preserving a patch".

- Depends on the sibling `iceberg` vendor patch: `StagedTableCreate` and every
  typed staged variant call
  `TableMetadata::staged_create_initialization_updates()`, which is Patch 8 of
  `vendor/iceberg-0.9.0/PATCH.md`. This crate does not compile against a stock
  `iceberg` 0.9.x, so the two vendor directories must be upgraded together.
  `Cargo.toml` additionally registers the `staged_create_probe` integration
  test that exercises the staged path against a live REST fixture.

## Validation

Diff base: upstream `iceberg-catalog-rest` 0.9.0 and 0.9.1 ship a
byte-identical `src/`, so either release can be used as the comparison base
for this directory and the whole `src/` diff is NovaRocks patches. The
vendored `Cargo.toml` keeps `version = "0.9.0"` and `iceberg = "0.9.0"`, and
adds the `staged_create_probe` test target; nothing else in the manifest
diverges.

This extracted crate's manifest resolves `iceberg = "0.9.0"` from crates.io
unless the NovaRocks vendor patch is supplied explicitly. Run from this
directory (`vendor/iceberg-catalog-rest-0.9.0`); `CARGO_TARGET_DIR` is
deliberately unset, so Cargo uses this crate's default `target/` directory:

```bash
env -u CARGO_TARGET_DIR cargo tree \
  --config 'patch.crates-io.iceberg.path="../iceberg-0.9.0"' \
  -p iceberg-catalog-rest

env -u CARGO_TARGET_DIR cargo test \
  --config 'patch.crates-io.iceberg.path="../iceberg-0.9.0"' \
  --lib

source ../../docker/iceberg-rest/runtime/current/env.sh
env -u CARGO_TARGET_DIR cargo test \
  --config 'patch.crates-io.iceberg.path="../iceberg-0.9.0"' \
  --test staged_create_probe test_stage_create_local_fixture_probe -- \
  --ignored --exact --nocapture
```

The `cargo tree` output must identify both crates by their worktree paths,
not a registry source.
