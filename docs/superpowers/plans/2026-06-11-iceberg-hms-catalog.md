# Iceberg Hive Metastore (HMS) Catalog Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `iceberg.catalog.type = hive` (Hive Metastore) as a fully read+write Iceberg catalog kind in NovaRocks standalone mode, delegating the HMS Thrift protocol and the lock+alter_table commit to the official `iceberg-catalog-hms` crate.

**Architecture:** A new `IcebergCatalogKind::Hive` variant whose catalog instance is the iceberg-rust `HmsCatalog` (built via `HmsCatalogBuilder` with NovaRocks' `S3StorageFactory` injected). Because every engine DML/DDL flow already resolves its catalog through `build_iceberg_catalog(entry) -> Arc<dyn iceberg::Catalog>`, adding one dispatch arm lights up SELECT/INSERT/UPDATE/DELETE/MERGE/compaction/MV. HMS behaves like REST for namespace/table operations (a remote catalog service owns the state), so the existing `matches!(entry.kind, Rest)` branch points in `registry.rs` broaden to a shared `uses_remote_catalog()` predicate.

**Tech Stack:** Rust, vendored `iceberg 0.9.0` + `iceberg-catalog-rest 0.9.0`, new `iceberg-catalog-hms 0.9.0` (pulls volo/volo-thrift/pilota), Docker Compose fixture (MinIO + Iceberg REST + Spark + new standalone Hive Metastore), `sql-tests` runner.

**Spec:** `docs/superpowers/specs/2026-06-11-iceberg-hms-catalog-design.md`

---

## Execution Progress / Handoff (as of 2026-06-11, branch `claude/admiring-kalam-30e7bf`)

This plan is **partially executed**. The entire Rust implementation is done and
unit-tested; what remains is the Docker HMS fixture + the two SQL suites + the
final regression. Execution paused to hand off to a different environment for
the Docker/network-heavy steps. Resume with subagent-driven-development.

### DONE (committed)
- **Task 1** — `iceberg-catalog-hms` dependency. **DEVIATION (important):** the crate
  does NOT compile against this repo's vendored `iceberg 0.9.0` (vendored iceberg
  PATCH 6 adds `PrimitiveType::Variant`, breaking the crate's exhaustive match,
  `E0004`). So it is **VENDORED** at `vendor/iceberg-catalog-hms-0.9.0/` with a
  one-arm patch (`Variant → FeatureUnsupported`) + `PATCH.md` + a `[patch.crates-io]`
  entry — same pattern as the existing vendored iceberg crates. (The spec §4.1
  "do not vendor" assumption was wrong; the spec's own contingency — vendor if a
  patch is needed — was triggered.)
- **Tasks 2–6** — `hive` catalog kind end-to-end in `src/connector/iceberg/catalog/registry.rs`:
  `IcebergCatalogKind::Hive`, `hms_uris` field, `uses_remote_catalog()` predicate,
  `hive` parsing + `build_hms_catalog_entry` (thrift:// strip, first-URI, kerberos
  fail-fast), `build_hms_catalog` (+ `with_storage_factory`, buffered/framed), the
  `Hive` dispatcher arm, and the routing smoke test.
  **De-risk gate PASSED:** volo-thrift runs cleanly under `block_on_iceberg`
  (`data_block_on` is already a multi-thread `enable_all` Tokio runtime) — NO
  workaround needed; HMS uses the plain `block_on_iceberg` path like REST.
- **Tasks 7–8** — broadened the ~10 namespace/table `matches!(Rest)` branch points
  to `uses_remote_catalog()` (Rest|Hive) routing through `build_iceberg_catalog`;
  fmt/clippy clean. **REST no-regression proven** (the 5 REST mockito tests pass).
  `views.rs` / `iceberg_view_rewrite.rs` left Rest-only (HMS views out of scope v1).
- **Task 9 (partial)** — image source committed: `docker/iceberg-rest/hms/{Dockerfile,core-site.xml}`.
  **Image NOT built yet** (this is the paused step — slow `docker pull`/`build`).
- **Task 12** — `iceberg-hms` / `iceberg-hms-compatibility` placeholder arm in
  `tests/sql-test-runner/src/config.rs` (+ test). Reviewed.
- **Task 15 (partial)** — `AGENTS.md`/`CLAUDE.md` now list `hive` as a supported
  catalog type. The `docs/guides/iceberg-v3/catalog.md` ❌→✅ flip is **deliberately
  deferred** until the e2e suites pass (so we don't claim verified e2e prematurely).

All Rust changes verified: `cargo test -p novarocks --lib connector::iceberg::catalog::registry`
→ 41 pass; `tests/sql-test-runner` → 89 pass; fmt/clippy clean.

### REMAINING (needs Docker / a running HMS) — exact next steps
1. **Finish Task 9 — build the image** (the paused step). On the new environment:
   ```bash
   docker pull apache/hive:4.0.0
   docker run --rm --entrypoint bash apache/hive:4.0.0 -lc 'ls /opt/hive/lib/hadoop-common-*.jar'
   # If the bundled hadoop is NOT 3.3.6, edit docker/iceberg-rest/hms/Dockerfile's
   # HADOOP_VERSION (and a compatible AWS_SDK_VERSION) to match, THEN:
   docker build -t novarocks/hive-metastore:4.0.0 docker/iceberg-rest/hms
   ```
2. **Task 10** — add the `hms` service to `compose.yml` + `shared.env` (per the Task 10
   section); bring the shared fixture up (do NOT tear it down) and verify the `hms`
   container is Up and the thrift port is reachable.
3. **Task 11** — thread HMS env + readiness through `up.sh`; regenerate and verify
   `NOVAROCKS_ICEBERG_HMS_URI` etc. are exported.
4. **Task 13** — `sql-tests/iceberg-hms` round-trip suite (incl. DELETE/UPDATE — the
   full-parity gate); record (`--record-from target`) + verify.
5. **Task 14** — `sql-tests/iceberg-hms-compatibility` cross-engine suite (Spark HMS
   catalog config + both directions); record + verify.
6. **Task 15 (finish)** — flip the HMS row in `docs/guides/iceberg-v3/catalog.md` to ✅.
7. **Task 16** — full regression (registry tests, iceberg-rest suite for no-regression,
   both HMS suites).

### Environment notes
- The Docker fixture (`docker/iceberg-rest/`) is shared across worktrees; MinIO/REST/Spark
  were already running. NEVER `down.sh --docker`. Add `hms` and `docker compose up -d`.
- `shared.env` is shared — adding `NOVA_ENV_HMS_PORT=9083` etc. there is intended.
- The paused build was handed to the user because of a slow-network `docker pull`/`build`.

---

## Conventions for this plan

- All commands run from the repo root: `/Users/harbor/.claude/worktrees/NovaRocks/admiring-kalam-30e7bf`.
- Build profile for correctness iteration: plain `cargo build` / `cargo test` (debug). Use `--profile dev-opt` only when running SQL suites where query speed matters.
- Commit after every task. Commit messages are English. **Do NOT add `Co-Authored-By: Claude` trailers** (project rule).
- Git push: this repo uses a triangular workflow (push to fork). Do not push during plan execution unless asked; only local commits on the current `claude/admiring-kalam-30e7bf` branch.
- "Run the existing REST unit tests" means the `registry.rs` `mod tests` block (mockito-based) — these guard against regressions when the shared `matches!(Rest)` branches change.

---

## File Structure

**Modified:**
- `Cargo.toml` — add `iceberg-catalog-hms = "0.9.0"` dependency.
- `src/connector/iceberg/catalog/registry.rs` — the entire catalog-side change (enum variant, entry field, predicate, parsing, builder, dispatcher arm, broadened branch points, unit tests). All catalog logic stays in this one file, matching the REST precedent.
- `docker/iceberg-rest/compose.yml` — add `hms` service.
- `docker/iceberg-rest/shared.env` — add HMS port + warehouse + image defaults.
- `docker/iceberg-rest/up.sh` — emit HMS env vars, add HMS readiness wait, add HMS to manifest + printed examples.
- `tests/sql-test-runner/src/config.rs` — `apply_suite_placeholder_defaults` arms for the two new suites.
- `docs/guides/iceberg-v3/catalog.md` — flip the HMS row to supported.
- `CLAUDE.md` / `AGENTS.md` — note `hive` as a supported catalog type.

**Created:**
- `docker/iceberg-rest/hms/Dockerfile` — standalone Hive Metastore image (apache/hive:4.0.0 + hadoop-aws + S3A config).
- `docker/iceberg-rest/hms/core-site.xml` — S3A → MinIO configuration for the metastore.
- `sql-tests/iceberg-hms/` — NovaRocks-only round-trip suite (`init.sql`, `cleanup.sql`, `sql/*.sql`, `result/*.result`, `README.md`).
- `sql-tests/iceberg-hms-compatibility/` — Spark ↔ NovaRocks cross-engine suite (same layout).

**Out of scope (v1 non-goals, enforced by explicit errors / left Rest-only):**
- Kerberos/SASL auth (rejected in `build_hms_catalog_entry`).
- Multi-level namespaces (HMS is single-level database).
- Iceberg views on HMS (`views.rs:42` and `iceberg_view_rewrite.rs:247` stay `Rest`-only; HMS view ops will hit the existing "unsupported" path).
- HA multi-metastore failover (first `hive.metastore.uris` entry only).

---

## Phase 0 — Dependency + de-risk volo/block_on bridging

The single biggest unknown is whether the volo-thrift async client inside `iceberg-catalog-hms` runs correctly under NovaRocks' `block_on_iceberg` (`data_block_on`). De-risk this before building anything else (spec §6, §9).

### Task 1: Add the dependency and confirm it compiles against the patched iceberg

**Files:**
- Modify: `Cargo.toml:28-29` (dependency declarations area, next to `iceberg` / `iceberg-catalog-rest`)

- [ ] **Step 1: Add the dependency**

In `Cargo.toml`, directly below the existing line `iceberg-catalog-rest = "0.9.0"` (line 29), add:

```toml
iceberg-catalog-hms = "0.9.0"
```

Do **not** add a `[patch]` entry for it — its transitive `iceberg ^0.9.0` dependency is already redirected to `vendor/iceberg-0.9.0` by the existing `[patch.crates-io]` block (lines 121-131). The vendored iceberg's visibility patches are additive, so the crate compiles against it.

- [ ] **Step 2: Build to resolve and compile the new dependency tree**

Run: `cargo build 2>&1 | tail -30`
Expected: PASS (compiles). First build is slow — volo/pilota/volo-thrift compile for the first time. If it fails on a version conflict for `iceberg`, confirm the `[patch.crates-io] iceberg` entry covers the version `iceberg-catalog-hms` requests (it requires `^0.9.0`).

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "build: add iceberg-catalog-hms 0.9.0 dependency"
```

### Task 2: Smoke-test the volo client under `block_on_iceberg` (routing + bridging proof)

This test proves three things at once: (a) the dep resolves and links, (b) the volo async client runs to completion (error, not hang/panic) under `data_block_on`, (c) `build_iceberg_catalog` routes a Hive entry to the HMS builder. It points at a closed TCP port so no Docker is needed.

**Files:**
- Modify: `src/connector/iceberg/catalog/registry.rs` (the `#[cfg(test)] mod tests` block near the end, after the existing REST tests around line 3569)

> NOTE: This test references `build_iceberg_catalog` with a `Hive` entry, which does not exist yet. Tasks 3–6 add it. Write this test now as the failing target, then implement Tasks 3–6 to make it pass. If executing strictly task-by-task, mark this test `#[ignore]` until Task 6, then un-ignore it. The recommended flow is to write it here and let Task 6 be the step that turns it green.

- [ ] **Step 1: Write the failing test**

Add to the `mod tests` block in `registry.rs`. Add `IcebergCatalogKind` and `build_iceberg_catalog` to the `use super::{...}` import if not already present (they are):

```rust
    fn hive_props(uris: &str) -> Vec<(String, String)> {
        vec![
            ("type".to_string(), "iceberg".to_string()),
            ("iceberg.catalog.type".to_string(), "hive".to_string()),
            ("hive.metastore.uris".to_string(), uris.to_string()),
            ("warehouse".to_string(), "s3://warehouse/hms".to_string()),
        ]
    }

    /// The Hive entry must route through `build_hms_catalog` (volo-thrift
    /// client) under `block_on_iceberg`. Pointing at a closed port, the catalog
    /// must surface an error (connection refused) WITHOUT hanging or panicking —
    /// proving the async bridge and the dispatcher routing both work.
    #[test]
    fn build_iceberg_catalog_dispatches_hive_kind_and_errors_on_dead_port() {
        let entry = build_catalog_entry("ice_hms", &hive_props("thrift://127.0.0.1:1")).expect("hive entry");
        assert_eq!(entry.kind, IcebergCatalogKind::Hive);

        // Build + force a catalog round-trip. Either build_iceberg_catalog errs
        // (eager connect) or list_namespaces errs (lazy connect); both are fine.
        let result: Result<(), String> = (|| {
            let catalog = build_iceberg_catalog(&entry)?;
            block_on_iceberg(async { catalog.list_namespaces(None).await })
                .map_err(|e| format!("runtime: {e}"))?
                .map(|_| ())
                .map_err(|e| format!("list: {e}"))
        })();
        assert!(result.is_err(), "HMS catalog against a dead port must error, got Ok");
    }
```

You will need `use super::block_on_iceberg;` and `use iceberg::Catalog;` in the test module (the latter is already imported at line 3278).

- [ ] **Step 2: Run it to verify it fails to compile (Hive variant missing)**

Run: `cargo test -p novarocks build_iceberg_catalog_dispatches_hive_kind 2>&1 | tail -20`
Expected: FAIL — compile error, `no variant named Hive found for enum IcebergCatalogKind` (and `hive` rejected by `build_catalog_entry`). This is the target Tasks 3–6 satisfy.

- [ ] **Step 3: (No implementation here.)** Implementation is Tasks 3–6. Do not commit a broken build; this task's checkbox is satisfied once Task 6 turns the test green.

---

## Phase 1 — registry.rs: the catalog kind

### Task 3: Add the `Hive` variant, the `hms_uris` field, and the `uses_remote_catalog()` predicate

**Files:**
- Modify: `src/connector/iceberg/catalog/registry.rs:47-60` (enum), `:62-80` (struct), and add an `impl` method near `:135`.

- [ ] **Step 1: Add the enum variant**

In `enum IcebergCatalogKind` (line 47), after the `Rest` variant (line 59), add:

```rust
    /// `iceberg.catalog.type = hive` — speak Hive Metastore (HMS) Thrift
    /// against an external metastore (`hive.metastore.uris`). Table state lives
    /// in HMS table parameters (`metadata_location`); commits go through the
    /// `iceberg-catalog-hms` crate's lock + alter_table protocol. v1: plaintext
    /// thrift only, single-level namespace.
    Hive,
```

- [ ] **Step 2: Add the `hms_uris` field**

In `struct IcebergCatalogEntry` (line 63), after the `rest_uri` field (line 73), add:

```rust
    /// HMS endpoint in `host:port` form (no `thrift://`) — populated only when
    /// `kind == IcebergCatalogKind::Hive`. None otherwise.
    #[allow(dead_code)]
    pub(crate) hms_uris: Option<String>,
```

This new field must be set in **every** `IcebergCatalogEntry { ... }` literal. Update them now to `hms_uris: None`:
- `build_catalog_entry` Hadoop entry (around line 1400).
- `build_rest_catalog_entry` (around line 1492).
- the test fixtures at `:2884` and any other literal (search `IcebergCatalogEntry {`).

Run to find them all: `grep -n "IcebergCatalogEntry {" src/connector/iceberg/catalog/registry.rs`

- [ ] **Step 3: Add the predicate method**

In `impl IcebergCatalogEntry` (the block starting at line 135), after `is_s3` (line 142), add:

```rust
    /// True when namespace/table state is owned by a remote Iceberg catalog
    /// service (REST server or Hive Metastore) rather than NovaRocks' direct
    /// filesystem / object-store warehouse layout (Hadoop / Memory). These
    /// catalogs route namespace + table operations through the iceberg-rust
    /// `Catalog` trait via `build_iceberg_catalog`.
    pub(crate) fn uses_remote_catalog(&self) -> bool {
        matches!(self.kind, IcebergCatalogKind::Rest | IcebergCatalogKind::Hive)
    }
```

- [ ] **Step 4: Build to verify all literals are updated**

Run: `cargo build 2>&1 | tail -20`
Expected: PASS. If it fails with `missing field hms_uris`, add `hms_uris: None` to the reported literal.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/catalog/registry.rs
git commit -m "feat(iceberg): add Hive catalog kind, hms_uris field, uses_remote_catalog predicate"
```

### Task 4: Parse `iceberg.catalog.type = hive` and build the HMS entry

**Files:**
- Modify: `src/connector/iceberg/catalog/registry.rs:1323-1337` (kind parsing + REST early-return), and add `build_hms_catalog_entry` + a shared `object_store_config_from_props` helper near `build_rest_catalog_entry` (line 1429).
- Test: same file's `mod tests`.

- [ ] **Step 1: Write the failing tests**

Add to `mod tests`:

```rust
    #[test]
    fn build_catalog_entry_accepts_hive_kind_with_uris() {
        let entry = build_catalog_entry(
            "ice_hms",
            &[
                ("type".to_string(), "iceberg".to_string()),
                ("iceberg.catalog.type".to_string(), "hive".to_string()),
                ("hive.metastore.uris".to_string(), "thrift://hms-host:9083".to_string()),
                ("warehouse".to_string(), "s3://warehouse/hms".to_string()),
            ],
        )
        .expect("hive entry");
        assert_eq!(entry.kind, IcebergCatalogKind::Hive);
        // hms_uris is the host:port form with thrift:// stripped.
        assert_eq!(entry.hms_uris.as_deref(), Some("hms-host:9083"));
        assert_eq!(entry.warehouse_uri, "s3://warehouse/hms");
    }

    #[test]
    fn build_catalog_entry_hive_takes_first_uri_and_strips_scheme() {
        let entry = build_catalog_entry(
            "ice_hms",
            &[
                ("iceberg.catalog.type".to_string(), "hive".to_string()),
                ("hive.metastore.uris".to_string(), "thrift://a:9083,thrift://b:9083".to_string()),
            ],
        )
        .expect("hive entry");
        assert_eq!(entry.hms_uris.as_deref(), Some("a:9083"));
    }

    #[test]
    fn build_catalog_entry_rejects_hive_without_uris() {
        let err = build_catalog_entry(
            "ice_hms",
            &[("iceberg.catalog.type".to_string(), "hive".to_string())],
        )
        .map(|_| ())
        .expect_err("uris required");
        assert!(err.contains("hive.metastore.uris"), "{err}");
    }

    #[test]
    fn build_catalog_entry_rejects_hive_kerberos_v1() {
        let err = build_catalog_entry(
            "ice_hms",
            &[
                ("iceberg.catalog.type".to_string(), "hive".to_string()),
                ("hive.metastore.uris".to_string(), "thrift://hms:9083".to_string()),
                ("hive.metastore.sasl.enabled".to_string(), "true".to_string()),
            ],
        )
        .map(|_| ())
        .expect_err("kerberos/sasl rejected in v1");
        assert!(err.contains("plaintext thrift only"), "{err}");
    }
```

Also update the existing `build_catalog_entry_rejects_unknown_catalog_type` test (line 3349): its assertion `err.contains("memory|hadoop|rest")` must become `err.contains("memory|hadoop|rest|hive")` once Step 3 changes the error message.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p novarocks build_catalog_entry_accepts_hive 2>&1 | tail -20`
Expected: FAIL — `hive` is currently rejected with "supports iceberg.catalog.type=memory|hadoop|rest".

- [ ] **Step 3: Add `hive` to the kind parser and the error message**

In `build_catalog_entry`, the `match props.get("iceberg.catalog.type")` block (lines 1323-1333), add a `hive` arm after the `rest` arm (line 1327) and extend the error string:

```rust
        Some(v) if v.eq_ignore_ascii_case("hive") => IcebergCatalogKind::Hive,
        Some(v) => {
            return Err(format!(
                "standalone iceberg catalog supports iceberg.catalog.type=memory|hadoop|rest|hive, got {v}"
            ));
        }
```

Then, right after the REST early-return (lines 1335-1337), add the Hive early-return:

```rust
    if matches!(kind, IcebergCatalogKind::Hive) {
        return build_hms_catalog_entry(&mut props);
    }
```

- [ ] **Step 4: Extract the shared object-store-config helper**

To avoid duplicating REST's S3-config block, add this helper just above `build_rest_catalog_entry` (line 1429). It is the exact block currently inlined at lines 1450-1478:

```rust
/// Build an object-store config from catalog S3 properties, deriving the
/// bucket from an `s3://` / `s3a://` / `oss://` warehouse URI. Shared by the
/// REST and Hive entry builders (both point at object-store warehouses and
/// inject a `StorageFactory` rather than touching a local warehouse path).
fn object_store_config_from_props(
    props: &HashMap<String, String>,
    warehouse: &str,
) -> Option<crate::fs::object_store::ObjectStoreConfig> {
    let raw_props: Vec<(String, String)> = props.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    let s3_factory =
        crate::connector::iceberg::catalog::s3_storage::S3StorageFactory::from_catalog_properties(&raw_props)?;
    let bucket = warehouse
        .strip_prefix("s3://")
        .or_else(|| warehouse.strip_prefix("s3a://"))
        .or_else(|| warehouse.strip_prefix("oss://"))
        .and_then(|rest| rest.split('/').next())
        .unwrap_or_default()
        .to_string();
    Some(crate::fs::object_store::ObjectStoreConfig {
        endpoint: s3_factory.endpoint.clone(),
        bucket,
        root: String::new(),
        access_key_id: s3_factory.access_key_id.clone(),
        access_key_secret: s3_factory.access_key_secret.clone(),
        session_token: None,
        enable_path_style_access: Some(s3_factory.enable_path_style),
        region: Some(s3_factory.region.clone()),
        retry_max_times: Some(3),
        retry_min_delay_ms: Some(100),
        retry_max_delay_ms: Some(2000),
        timeout_ms: Some(30000),
        io_timeout_ms: Some(30000),
    })
}
```

Then replace the inlined `let s3_config = if let Some(s3_factory) = ... { Some(...) } else { None };` block inside `build_rest_catalog_entry` (lines 1448-1479) with:

```rust
    let s3_config = object_store_config_from_props(props, &warehouse);
```

- [ ] **Step 5: Add `build_hms_catalog_entry`**

Add immediately after `build_rest_catalog_entry` (after line 1502):

```rust
/// Build an [`IcebergCatalogEntry`] for `iceberg.catalog.type = hive`.
///
/// Hive Metastore stores each table's current `metadata.json` pointer in the
/// table's HMS parameters. NovaRocks delegates the HMS Thrift protocol and the
/// lock + alter_table commit to the `iceberg-catalog-hms` crate; this function
/// only validates and normalizes catalog properties.
///
/// v1 scope: plaintext thrift, single metastore, single-level namespace.
/// Kerberos/SASL and multi-URI HA are rejected / reduced here (fail fast).
fn build_hms_catalog_entry(
    props: &mut HashMap<String, String>,
) -> Result<IcebergCatalogEntry, String> {
    // v1 = plaintext thrift only. Reject auth-related properties up front.
    for k in props.keys() {
        let lk = k.to_ascii_lowercase();
        if lk.contains("kerberos") || lk.contains("sasl") || lk.contains("keytab") || lk.contains("principal") {
            return Err(format!(
                "hive iceberg catalog v1 supports plaintext thrift only; unsupported auth property `{k}`"
            ));
        }
    }

    let raw_uris = props
        .get("hive.metastore.uris")
        .or_else(|| props.get("iceberg.catalog.hive.metastore.uris"))
        .cloned()
        .ok_or_else(|| {
            "hive iceberg catalog requires `hive.metastore.uris` (e.g. thrift://host:9083)".to_string()
        })?;
    // v1: single metastore — take the first comma-separated URI. HA is a follow-up.
    let first_uri = raw_uris
        .split(',')
        .map(|s| s.trim())
        .find(|s| !s.is_empty())
        .ok_or_else(|| "hive.metastore.uris is empty".to_string())?
        .to_string();
    // HMS_CATALOG_PROP_URI wants `host:port`, not a `thrift://` URI.
    let hms_endpoint = first_uri
        .strip_prefix("thrift://")
        .unwrap_or(&first_uri)
        .to_string();

    let warehouse = props
        .get("iceberg.catalog.warehouse")
        .or_else(|| props.get("warehouse"))
        .or_else(|| props.get("hive.metastore.warehouse.dir"))
        .cloned()
        .unwrap_or_default();

    let s3_config = object_store_config_from_props(props, &warehouse);

    // No local warehouse_path for HMS; placeholder so any legacy hadoop-only
    // path that touches warehouse_path fails loudly instead of corrupting a dir.
    let warehouse_path = PathBuf::from("/__novarocks_hms_catalog_no_local_warehouse__");

    props.insert("type".to_string(), "iceberg".to_string());
    props.insert("iceberg.catalog.type".to_string(), "hive".to_string());
    props.insert("hive.metastore.uris".to_string(), first_uri);
    if !warehouse.is_empty() {
        props.insert("iceberg.catalog.warehouse".to_string(), warehouse.clone());
    }

    Ok(IcebergCatalogEntry {
        kind: IcebergCatalogKind::Hive,
        warehouse_uri: warehouse,
        rest_uri: None,
        hms_uris: Some(hms_endpoint),
        properties: sorted_properties(props),
        s3_config,
        warehouse_path,
        table_cache: Arc::new(std::sync::RwLock::new(HashMap::new())),
        data_files_cache: Arc::new(std::sync::RwLock::new(HashMap::new())),
    })
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p novarocks build_catalog_entry_ 2>&1 | tail -30`
Expected: PASS — all `build_catalog_entry_*` tests including the new hive ones and the still-passing rest/hadoop ones.

- [ ] **Step 7: Commit**

```bash
git add src/connector/iceberg/catalog/registry.rs
git commit -m "feat(iceberg): parse iceberg.catalog.type=hive into an HMS entry"
```

### Task 5: Build the `HmsCatalog` and wire the dispatcher arm

**Files:**
- Modify: `src/connector/iceberg/catalog/registry.rs` — add `build_hms_catalog` after `build_rest_catalog` (line 1571), add the `Hive` arm in `build_iceberg_catalog` (lines 1600-1609).

- [ ] **Step 1: Add `build_hms_catalog`**

Add after `build_rest_catalog` (after line 1571):

```rust
/// Build an Iceberg `HmsCatalog` for an entry whose
/// `kind == IcebergCatalogKind::Hive`. Asynchronous because the volo-thrift
/// client connects during catalog operations; synchronous engine flows go
/// through [`build_iceberg_catalog`], which wraps this with `block_on_iceberg`.
pub(crate) async fn build_hms_catalog(
    entry: &IcebergCatalogEntry,
) -> Result<iceberg_catalog_hms::HmsCatalog, String> {
    use iceberg::CatalogBuilder;
    use iceberg_catalog_hms::{
        HMS_CATALOG_PROP_THRIFT_TRANSPORT, HMS_CATALOG_PROP_URI, HMS_CATALOG_PROP_WAREHOUSE,
        HmsCatalogBuilder, THRIFT_TRANSPORT_BUFFERED, THRIFT_TRANSPORT_FRAMED,
    };

    if !matches!(entry.kind, IcebergCatalogKind::Hive) {
        return Err(format!(
            "build_hms_catalog called on non-Hive entry kind={:?}",
            entry.kind
        ));
    }
    let uri = entry.hms_uris.clone().ok_or_else(|| {
        "hive iceberg catalog entry missing hms_uris (CREATE EXTERNAL CATALOG must set `hive.metastore.uris`)"
            .to_string()
    })?;

    let mut props: HashMap<String, String> = HashMap::new();
    props.insert(HMS_CATALOG_PROP_URI.to_string(), uri);
    if !entry.warehouse_uri.is_empty() {
        props.insert(
            HMS_CATALOG_PROP_WAREHOUSE.to_string(),
            entry.warehouse_uri.clone(),
        );
    }
    // thrift transport: default buffered; framed when hive.metastore.thrift.framed=true.
    let framed = entry
        .properties
        .iter()
        .find(|(k, _)| k == "hive.metastore.thrift.framed")
        .map(|(_, v)| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false);
    props.insert(
        HMS_CATALOG_PROP_THRIFT_TRANSPORT.to_string(),
        if framed {
            THRIFT_TRANSPORT_FRAMED.to_string()
        } else {
            THRIFT_TRANSPORT_BUFFERED.to_string()
        },
    );

    let storage_factory = build_storage_factory_for_entry(entry)?;
    HmsCatalogBuilder::default()
        .with_storage_factory(storage_factory)
        .load("hms", props)
        .await
        .map_err(|e| format!("build HMS iceberg catalog: {e}"))
}
```

> IMPLEMENTATION NOTE: Confirm the exact form of `HMS_CATALOG_PROP_THRIFT_TRANSPORT` / `THRIFT_TRANSPORT_BUFFERED` / `THRIFT_TRANSPORT_FRAMED` against the installed crate (`cargo doc -p iceberg-catalog-hms --open`, or read `~/.cargo/registry/src/*/iceberg-catalog-hms-0.9.0/src/lib.rs`). They are `&str` value constants passed via the props map (verified from the crate docs). If a build error shows they are not `&str`, adjust the `.to_string()` calls accordingly.

- [ ] **Step 2: Add the dispatcher arm**

In `build_iceberg_catalog` (line 1597), the `match entry.kind` currently has `Hadoop | Memory` and `Rest` arms. Add a `Hive` arm:

```rust
        IcebergCatalogKind::Hive => {
            let hms = block_on_iceberg(async { build_hms_catalog(entry).await })??;
            Ok(Arc::new(hms) as Arc<dyn iceberg::Catalog>)
        }
```

- [ ] **Step 3: Build**

Run: `cargo build 2>&1 | tail -20`
Expected: PASS. Fix any const-type mismatch per the implementation note.

- [ ] **Step 4: Commit**

```bash
git add src/connector/iceberg/catalog/registry.rs
git commit -m "feat(iceberg): build HmsCatalog and dispatch Hive kind in build_iceberg_catalog"
```

### Task 6: Turn the Phase-0 routing smoke test green

**Files:**
- Modify: none (code already in place) — run the Task 2 test.

- [ ] **Step 1: Run the routing smoke test**

If you marked it `#[ignore]` in Task 2, remove the attribute now.

Run: `cargo test -p novarocks build_iceberg_catalog_dispatches_hive_kind_and_errors_on_dead_port 2>&1 | tail -20`
Expected: PASS — the Hive entry routes to the HMS builder and the volo client returns an error (connection refused) under `block_on_iceberg` within a second or two, without hanging or panicking.

> IF IT HANGS: the volo client requires a Tokio reactor that `data_block_on` does not provide. Inspect `data_block_on` (search `fn data_block_on` in `src/`). Mitigation: run HMS catalog ops on a dedicated multi-threaded Tokio runtime, or wrap the `.load()` + ops in `tokio::task::spawn_blocking` with their own `Runtime`. Capture the fix as a follow-up note in the spec's §6 and adjust `build_hms_catalog` / the dispatcher accordingly before proceeding.

- [ ] **Step 2: Run the full registry test module to confirm no regression**

Run: `cargo test -p novarocks --lib connector::iceberg::catalog::registry 2>&1 | tail -30`
Expected: PASS — all existing REST/Hadoop tests plus the new Hive tests.

- [ ] **Step 3: Commit (if Task 2 left the test ignored/uncommitted)**

```bash
git add src/connector/iceberg/catalog/registry.rs
git commit -m "test(iceberg): HMS dispatch + block_on bridging smoke test"
```

---

## Phase 2 — registry.rs: route HMS through the remote-catalog branches

HMS is a remote catalog (like REST): namespace and table state live in the metastore, not in NovaRocks' filesystem layout. Broaden the `matches!(entry.kind, Rest)` branch points to `uses_remote_catalog()` and build the catalog via `build_iceberg_catalog` (which now dispatches Rest→RestCatalog, Hive→HmsCatalog). This is behavior-preserving for REST: `build_iceberg_catalog` for a Rest entry does exactly `block_on(build_rest_catalog)`.

### Task 7: Broaden the namespace/table operation branches to `uses_remote_catalog()`

**Files:**
- Modify: `src/connector/iceberg/catalog/registry.rs` functions at lines 219, 259, 304, 378, 463, 524, 639, 682, 839, 869.

Apply each edit below. For the **positive** branches (currently `if matches!(entry.kind, IcebergCatalogKind::Rest)`), change the condition to `if entry.uses_remote_catalog()` AND replace `let catalog = block_on_iceberg(async { build_rest_catalog(entry).await })??;` with `let catalog = build_iceberg_catalog(entry)?;`. Also change "REST" → "iceberg" in the adjacent error strings so HMS errors are not mislabeled.

- [ ] **Step 1: `create_namespace` (line 224)**

Change:
```rust
    if matches!(entry.kind, IcebergCatalogKind::Rest) {
        let namespace = NamespaceIdent::new(ns_name);
        let catalog = block_on_iceberg(async { build_rest_catalog(entry).await })??;
        return block_on_iceberg(async {
            catalog.create_namespace(&namespace, HashMap::new()).await
        })
        .map_err(|e| format!("create REST namespace runtime: {e}"))?
        .map(|_| ())
        .map_err(|e| format!("create REST namespace {namespace}: {e}"));
    }
```
to:
```rust
    if entry.uses_remote_catalog() {
        let namespace = NamespaceIdent::new(ns_name);
        let catalog = build_iceberg_catalog(entry)?;
        return block_on_iceberg(async {
            catalog.create_namespace(&namespace, HashMap::new()).await
        })
        .map_err(|e| format!("create iceberg namespace runtime: {e}"))?
        .map(|_| ())
        .map_err(|e| format!("create iceberg namespace {namespace}: {e}"));
    }
```

- [ ] **Step 2: `namespace_exists` (line 264)**

Change `if matches!(entry.kind, IcebergCatalogKind::Rest) {` to `if entry.uses_remote_catalog() {`, replace the `build_rest_catalog` line with `let catalog = build_iceberg_catalog(entry)?;`, and change the two `format!("... REST namespace ...")` strings to `"... iceberg namespace ..."`.

- [ ] **Step 3: `list_namespaces` (line 305)**

Change `if matches!(entry.kind, IcebergCatalogKind::Rest) {` to `if entry.uses_remote_catalog() {`, replace `let catalog = block_on_iceberg(async { build_rest_catalog(entry).await })??;` with `let catalog = build_iceberg_catalog(entry)?;`, and change the `format!("list REST namespaces ...")` strings to `"list iceberg namespaces ..."`.

- [ ] **Step 4: `drop_namespace` (line 383)**

Same transformation as Step 1, for the `drop_namespace` body. Condition → `entry.uses_remote_catalog()`; catalog → `build_iceberg_catalog(entry)?`; error strings "REST" → "iceberg".

- [ ] **Step 5: `list_tables` (line 468)**

Change `if matches!(entry.kind, IcebergCatalogKind::Rest) {` to `if entry.uses_remote_catalog() {`, replace the `build_rest_catalog` line with `let catalog = build_iceberg_catalog(entry)?;`, error strings "REST" → "iceberg".

- [ ] **Step 6: `drop_table` (line 649)**

Change `if matches!(entry.kind, IcebergCatalogKind::Rest) {` to `if entry.uses_remote_catalog() {`, replace the `build_rest_catalog` line with `let catalog = build_iceberg_catalog(entry)?;`, error strings "REST" → "iceberg".

- [ ] **Step 7: `load_table` (line 701)**

Change `let table = if matches!(entry.kind, IcebergCatalogKind::Rest) {` to `let table = if entry.uses_remote_catalog() {`, replace `let catalog = block_on_iceberg(async { build_rest_catalog(entry).await })??;` with `let catalog = build_iceberg_catalog(entry)?;`. Keep `format_rest_load_table_error` (it normalizes a not-found error to "unknown table: ns.tbl" and works for any `iceberg::Error`).

- [ ] **Step 8: `current_schema_id` (line 847)**

Change `if matches!(entry.kind, IcebergCatalogKind::Rest) {` to `if entry.uses_remote_catalog() {`, replace the `build_rest_catalog` line with `let catalog = build_iceberg_catalog(entry)?;`. Keep `format_rest_load_table_error`.

- [ ] **Step 9: `create_table` namespace pre-create (line 572)**

Change:
```rust
    if !matches!(entry.kind, IcebergCatalogKind::Rest) {
        let _ =
            block_on_iceberg(async { catalog.create_namespace(&namespace, HashMap::new()).await });
    }
```
to:
```rust
    if !entry.uses_remote_catalog() {
        let _ =
            block_on_iceberg(async { catalog.create_namespace(&namespace, HashMap::new()).await });
    }
```
(HMS, like REST, relies on an explicit `CREATE DATABASE`; the suites do this in `init.sql`.)

> LEAVE the `format-version` re-insertion at line 552 as `matches!(entry.kind, IcebergCatalogKind::Rest)`. HMS builds metadata locally via `TableMetadataBuilder::from_table_creation` (the Hadoop path), which honors the typed `format_version` field and rejects the reserved `format-version` property. Adding HMS here would break `create_table`.

- [ ] **Step 10: `insert_rows` register-table guard (line 889)**

Change:
```rust
    if !matches!(entry.kind, IcebergCatalogKind::Rest) {
```
to:
```rust
    if !entry.uses_remote_catalog() {
```
(HMS resolves tables through the metastore `get_table`, like REST — it must NOT register by metadata-location the way the in-memory Hadoop catalog does.)

- [ ] **Step 11: Build + run the full registry test module (REST regression guard)**

Run: `cargo test -p novarocks --lib connector::iceberg::catalog::registry 2>&1 | tail -30`
Expected: PASS — the mockito REST tests still pass (proving the `build_rest_catalog` → `build_iceberg_catalog` swap is behavior-preserving), plus the Hive tests.

- [ ] **Step 12: Confirm views stay Rest-only (HMS view ops unsupported in v1)**

Verify (do NOT change) that `src/connector/iceberg/catalog/views.rs:42` and `src/engine/iceberg_view_rewrite.rs:247` still match only `IcebergCatalogKind::Rest`. A Hive catalog hitting a view operation will take the existing "unsupported" path — the intended v1 behavior.

Run: `grep -n "IcebergCatalogKind::Rest" src/connector/iceberg/catalog/views.rs src/engine/iceberg_view_rewrite.rs`
Expected: both still reference only `Rest`.

- [ ] **Step 13: Commit**

```bash
git add src/connector/iceberg/catalog/registry.rs
git commit -m "feat(iceberg): route Hive catalog through the remote-catalog branch points"
```

### Task 8: clippy + fmt gate for the Rust changes

**Files:** none (verification only).

- [ ] **Step 1: Format**

Run: `cargo fmt`
Then: `git diff --stat`

- [ ] **Step 2: Clippy**

Run: `cargo clippy -p novarocks 2>&1 | tail -30`
Expected: no new warnings introduced by the HMS code. Fix any (e.g. unused import of `build_rest_catalog` if it became unused — it is still used by `build_iceberg_catalog`'s Rest arm, so it should remain).

- [ ] **Step 3: Commit (if fmt/clippy changed anything)**

```bash
git add -A
git commit -m "style(iceberg): fmt + clippy for HMS catalog"
```

---

## Phase 3 — Docker fixture: standalone Hive Metastore

This phase adds a real HMS service to the shared `docker/iceberg-rest` fixture so the catalog can be exercised end-to-end and cross-engine. **This is the highest-risk phase** (HMS image + S3A → MinIO). Build the image and validate connectivity before formalizing suites.

### Task 9: Hive Metastore Docker image (apache/hive:4.0.0 + S3A)

**Files:**
- Create: `docker/iceberg-rest/hms/Dockerfile`
- Create: `docker/iceberg-rest/hms/core-site.xml`

- [ ] **Step 1: Write `docker/iceberg-rest/hms/core-site.xml`**

```xml
<?xml version="1.0"?>
<configuration>
  <property><name>fs.s3a.impl</name><value>org.apache.hadoop.fs.s3a.S3AFileSystem</value></property>
  <property><name>fs.s3a.endpoint</name><value>http://minio:9000</value></property>
  <property><name>fs.s3a.path.style.access</name><value>true</value></property>
  <property><name>fs.s3a.access.key</name><value>admin</value></property>
  <property><name>fs.s3a.secret.key</name><value>admin123</value></property>
  <property><name>fs.s3a.connection.ssl.enabled</name><value>false</value></property>
  <property><name>fs.s3a.aws.credentials.provider</name><value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value></property>
</configuration>
```

- [ ] **Step 2: Write `docker/iceberg-rest/hms/Dockerfile`**

```dockerfile
# Standalone Hive Metastore for the NovaRocks Iceberg test fixture.
# Adds hadoop-aws + the AWS SDK bundle so the metastore can resolve s3a://
# warehouse locations against MinIO. Backed by the image's embedded Derby DB
# (cleared on container restart — fine for tests).
FROM apache/hive:4.0.0

USER root

# IMPORTANT: HADOOP_VERSION must match the hadoop-*.jar already bundled in
# /opt/hive/lib. Verify with:
#   docker run --rm --entrypoint bash apache/hive:4.0.0 -lc 'ls /opt/hive/lib/hadoop-common-*.jar'
# Adjust the ARG below to that exact version before building.
ARG HADOOP_VERSION=3.3.6
ARG AWS_SDK_VERSION=1.12.367

RUN set -eux; \
    cd /opt/hive/lib; \
    curl -fsSL -o "hadoop-aws-${HADOOP_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar"; \
    curl -fsSL -o "aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

COPY core-site.xml /opt/hive/conf/core-site.xml

USER hive
```

- [ ] **Step 3: Determine the correct HADOOP_VERSION and build the image**

Run:
```bash
docker run --rm --entrypoint bash apache/hive:4.0.0 -lc 'ls /opt/hive/lib/hadoop-common-*.jar'
```
Set the `HADOOP_VERSION` ARG in the Dockerfile to the version shown, then:
```bash
docker build -t novarocks/hive-metastore:4.0.0 docker/iceberg-rest/hms
```
Expected: image builds; both jars download.

- [ ] **Step 4: Commit**

```bash
git add docker/iceberg-rest/hms/Dockerfile docker/iceberg-rest/hms/core-site.xml
git commit -m "test(iceberg): standalone Hive Metastore docker image for HMS catalog"
```

### Task 10: Add the `hms` service to compose.yml and shared.env

**Files:**
- Modify: `docker/iceberg-rest/compose.yml`
- Modify: `docker/iceberg-rest/shared.env`

- [ ] **Step 1: Add HMS defaults to `shared.env`**

Append to `docker/iceberg-rest/shared.env`:

```bash
NOVA_ENV_HMS_PORT=9083
NOVA_ENV_SHARED_HMS_WAREHOUSE_URI=s3://warehouse/shared/hms
HMS_IMAGE=novarocks/hive-metastore:4.0.0
```

- [ ] **Step 2: Add the `hms` service to `compose.yml`**

In `docker/iceberg-rest/compose.yml`, add this service after the `rest` service block (before `spark`):

```yaml
  hms:
    image: ${HMS_IMAGE:-novarocks/hive-metastore:4.0.0}
    build:
      context: ./hms
    pull_policy: missing
    depends_on:
      mc:
        condition: service_completed_successfully
    environment:
      SERVICE_NAME: metastore
      AWS_ACCESS_KEY_ID: ${MINIO_ROOT_USER:-admin}
      AWS_SECRET_ACCESS_KEY: ${MINIO_ROOT_PASSWORD:-admin123}
      AWS_REGION: us-east-1
    ports:
      - "${NOVA_ENV_HMS_PORT:-9083}:9083"
    networks:
      iceberg_net:
        aliases:
          - hms
```

- [ ] **Step 3: Bring up the fixture and verify HMS starts and listens**

```bash
source docker/iceberg-rest/runtime/current/env.sh 2>/dev/null || docker/iceberg-rest/up.sh --prepare-only
docker/iceberg-rest/up.sh
docker compose -p "${NOVA_ENV_COMPOSE_PROJECT:-nr-iceberg-rest}" -f docker/iceberg-rest/compose.yml ps
```
Then verify the thrift port accepts connections:
```bash
( exec 3<>/dev/tcp/127.0.0.1/${NOVA_ENV_HMS_PORT:-9083} ) && echo "HMS port open" || echo "HMS port CLOSED"
```
Expected: the `hms` container is `Up` and the port is open. If the container exits, inspect logs:
```bash
docker compose -p "${NOVA_ENV_COMPOSE_PROJECT:-nr-iceberg-rest}" -f docker/iceberg-rest/compose.yml logs hms --tail=120
```
Common failure: metastore schema not initialized — apache/hive:4.0.0 metastore auto-creates the Derby schema on first boot via `SERVICE_NAME=metastore`; if it complains about schema, add `IS_RESUME=true` or an init step per the image docs.

- [ ] **Step 4: Commit**

```bash
git add docker/iceberg-rest/compose.yml docker/iceberg-rest/shared.env
git commit -m "test(iceberg): add hms service to the shared docker fixture"
```

### Task 11: Emit HMS env vars + readiness wait in up.sh

**Files:**
- Modify: `docker/iceberg-rest/up.sh` (anchors below are 1:1 with the current file).

- [ ] **Step 1: Add an `hms_port` variable next to the other ports**

Near lines 123-124 (`configured_rest_port`/`configured_spark_ui_port`) and 140-141 (`rest_port`/`spark_ui_port`), add an HMS port alongside, e.g. after line 141:

```bash
    hms_port="${NOVA_ENV_HMS_PORT:-9083}"
```
and near line 124:
```bash
configured_hms_port="${NOVA_ENV_HMS_PORT:-9083}"
```
Use whichever of these two variables the surrounding code uses to compose `compose.env` and `env.sh` (mirror exactly how `rest_port` is threaded through).

- [ ] **Step 2: Write `NOVA_ENV_HMS_PORT` into `compose.env`**

At the `compose.env` heredoc (around lines 320-321, where `NOVA_ENV_REST_PORT=$rest_port` is written), add:
```bash
NOVA_ENV_HMS_PORT=$hms_port
```

- [ ] **Step 3: Export HMS vars in `env.sh`**

In the `env.sh` export block (around lines 471-496), after `export NOVA_ENV_REST_PORT="$rest_port"` (line 477) add:
```bash
export NOVA_ENV_HMS_PORT="$hms_port"
```
and after `export NOVAROCKS_ICEBERG_REST_URI="$rest_uri"` (line 489) add:
```bash
hms_uri="thrift://127.0.0.1:$hms_port"
export NOVAROCKS_ICEBERG_HMS_URI="$hms_uri"
export NOVA_ENV_SHARED_HMS_WAREHOUSE_URI="${NOVA_ENV_SHARED_HMS_WAREHOUSE_URI:-s3://warehouse/shared/hms}"
```

- [ ] **Step 4: Add an HMS entry to the manifest heredoc**

In the `manifest.json` heredoc (starts line 509), add an `hms` endpoint field mirroring the `rest` field (keep valid JSON — add a comma). Example field:
```
  "hms_uri": "$hms_uri",
  "hms_port": $hms_port,
```

- [ ] **Step 5: Add a TCP readiness wait for HMS**

In the readiness section (around lines 613-628 with `wait_http` for MinIO + REST), add a TCP-based wait (HMS is thrift, not HTTP) after the REST wait (line 628):

```bash
  wait_tcp() {
    local host="$1" port="$2" name="$3" i
    for i in $(seq 1 60); do
      if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then exec 3>&- 3<&-; return 0; fi
      sleep 1
    done
    echo "timed out waiting for $name on $host:$port" >&2
    docker compose --env-file "$compose_env" -p "$compose_project" -f "$compose_file" logs --tail=120 hms >&2
    return 1
  }
  wait_tcp 127.0.0.1 "$hms_port" "Hive Metastore"
```

> NOTE: A TCP-accept check only proves the port is bound, not that the metastore finished schema init. This is acceptable for v1; the suite's `CREATE DATABASE` in `init.sql` is the real readiness gate. A stronger check (a thrift `get_all_databases` ping) is a follow-up.

- [ ] **Step 6: Add the new suites to the printed example commands**

After the `iceberg-compatibility` example lines (596-598 and 664-666), add:
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --config "\$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-hms --mode verify
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --config "\$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-hms-compatibility --mode verify
```

- [ ] **Step 7: Regenerate and verify env exports**

```bash
docker/iceberg-rest/up.sh --prepare-only
source docker/iceberg-rest/runtime/current/env.sh
echo "HMS_PORT=$NOVA_ENV_HMS_PORT HMS_URI=$NOVAROCKS_ICEBERG_HMS_URI WH=$NOVA_ENV_SHARED_HMS_WAREHOUSE_URI"
```
Expected: all three print non-empty values; `NOVAROCKS_ICEBERG_HMS_URI=thrift://127.0.0.1:9083` (or the worktree's port).

- [ ] **Step 8: Commit**

```bash
git add docker/iceberg-rest/up.sh
git commit -m "test(iceberg): emit HMS env + readiness wait in up.sh"
```

---

## Phase 4 — SQL test suites

Suites are auto-discovered from `sql-tests/<name>/sql/` (see `build_suite_configs` in `config.rs`), so creating the directories registers them. The only code change is adding placeholder-default arms in `config.rs`.

### Task 12: Add placeholder-default arms for the HMS suites

**Files:**
- Modify: `tests/sql-test-runner/src/config.rs` — `apply_suite_placeholder_defaults` (line 145).
- Test: same file's `mod tests`.

- [ ] **Step 1: Write the failing test**

Add to `config.rs` `mod tests`:

```rust
    #[test]
    fn hms_suite_defaults_populate_uris_warehouse_and_oss() {
        // Ensure deterministic defaults regardless of ambient env.
        unsafe {
            std::env::remove_var("NOVAROCKS_ICEBERG_HMS_URI");
            std::env::remove_var("NOVA_ENV_SHARED_HMS_WAREHOUSE_URI");
        }
        let mut vars = std::collections::HashMap::new();
        apply_suite_placeholder_defaults(&mut vars, "iceberg-hms");
        assert_eq!(vars.get("iceberg_hms_uris").map(String::as_str), Some("thrift://127.0.0.1:9083"));
        assert_eq!(vars.get("iceberg_hms_warehouse").map(String::as_str), Some("s3://warehouse/shared/hms"));
        assert!(vars.contains_key("oss_ak"));
        assert!(vars.contains_key("oss_endpoint"));
    }
```

> NOTE on `unsafe`/env: match the crate's existing convention for setting env in tests (Rust 2024 `std::env::set_var` is `unsafe`). If other tests in the crate avoid touching env, instead assert only that the keys are present (`vars.contains_key("iceberg_hms_uris")`) so the test is env-independent.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sql-tests hms_suite_defaults 2>&1 | tail -20`
Expected: FAIL — keys absent (no arm yet).

(If the runner crate package name differs, use the path form: `cargo test --manifest-path tests/sql-test-runner/Cargo.toml hms_suite_defaults`.)

- [ ] **Step 3: Add the suite arms**

In `apply_suite_placeholder_defaults` (line 146), add two arms before the `_ => return` arm. Place them so the shared `oss_*` defaults at the bottom of the function still run (do NOT early-return):

```rust
        "iceberg-hms" | "iceberg-hms-compatibility" => {
            insert_placeholder_default(
                variables,
                "iceberg_hms_uris",
                env_or_default("NOVAROCKS_ICEBERG_HMS_URI", "thrift://127.0.0.1:9083"),
            );
            insert_placeholder_default(
                variables,
                "iceberg_hms_warehouse",
                env_or_default(
                    "NOVA_ENV_SHARED_HMS_WAREHOUSE_URI",
                    "s3://warehouse/shared/hms",
                ),
            );
        }
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p sql-tests hms_suite_defaults 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/sql-test-runner/src/config.rs
git commit -m "test(iceberg): HMS suite placeholder defaults in sql-test runner"
```

### Task 13: `iceberg-hms` suite — NovaRocks write→read round trip

**Files:**
- Create: `sql-tests/iceberg-hms/init.sql`, `cleanup.sql`, `README.md`
- Create: `sql-tests/iceberg-hms/sql/iceberg_hms_write_roundtrip.sql`
- Create: `sql-tests/iceberg-hms/result/iceberg_hms_write_roundtrip.result` (generated by record mode)

Prereq: the Phase 1–3 code is committed and the docker fixture (incl. `hms`) is up. A NovaRocks standalone-server must be running against the generated config.

- [ ] **Step 1: Write `init.sql` (catalog + database setup)**

`sql-tests/iceberg-hms/init.sql`:
```sql
CREATE EXTERNAL CATALOG IF NOT EXISTS `iceberg_hms_${suite_uuid0}`
PROPERTIES (
    "type"="iceberg",
    "iceberg.catalog.type"="hive",
    "hive.metastore.uris"="${iceberg_hms_uris}",
    "warehouse"="${iceberg_hms_warehouse}",
    "aws.s3.access_key"="${oss_ak}",
    "aws.s3.secret_key"="${oss_sk}",
    "aws.s3.endpoint"="${oss_endpoint}",
    "aws.s3.region"="us-east-1",
    "aws.s3.enable_path_style_access"="true"
);
```

- [ ] **Step 2: Write `cleanup.sql`**

`sql-tests/iceberg-hms/cleanup.sql` (mirror `sql-tests/iceberg-rest/cleanup.sql` — read it first for the exact DROP pattern):
```sql
DROP CATALOG IF EXISTS `iceberg_hms_${suite_uuid0}`;
```

- [ ] **Step 3: Write the round-trip case (append + overwrite + DELETE + UPDATE)**

`sql-tests/iceberg-hms/sql/iceberg_hms_write_roundtrip.sql` — uses fully-qualified `catalog.db.table` names with `${suite_uuid0}` / `${uuid0}` placeholders and per-query directives (verified against `sql-tests/iceberg-rest/sql/iceberg_rest_insert_select.sql`: there is **no** `-- @catalog=`/`USE`; three-part names are used inline). This single case exercises the full write surface so the HMS lock+alter_table commit is verified for BOTH `create_table` (append) AND `update_table` (DELETE/UPDATE) — the latter being the riskiest path and the reason crate A was chosen:

```sql
-- @order_sensitive=true
-- Validate the Iceberg-on-HMS commit path end to end:
--   CREATE DATABASE / CREATE TABLE / INSERT (append) / DELETE / UPDATE /
--   INSERT OVERWRITE — each followed by a positive count+rows assertion.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_hms_${suite_uuid0}.hms_io_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_hms_${suite_uuid0}.hms_io_db_${uuid0}.t_io_${uuid0} (
  id BIGINT,
  region STRING,
  amount DOUBLE
)
PARTITION BY (region);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_hms_${suite_uuid0}.hms_io_db_${uuid0}.t_io_${uuid0}
VALUES (1, 'us', 10.5), (2, 'us', 20.0), (3, 'eu', 30.25);

-- query 4
-- After 3 appended rows: count = 3.
SELECT COUNT(*) AS n FROM iceberg_hms_${suite_uuid0}.hms_io_db_${uuid0}.t_io_${uuid0};

-- query 5
SELECT id, region, amount
  FROM iceberg_hms_${suite_uuid0}.hms_io_db_${uuid0}.t_io_${uuid0}
  ORDER BY id;

-- query 6
-- @skip_result_check=true
-- DELETE drives update_table: a second HMS alter_table commit with requirements.
DELETE FROM iceberg_hms_${suite_uuid0}.hms_io_db_${uuid0}.t_io_${uuid0} WHERE region = 'eu';

-- query 7
-- After deleting the 'eu' row: count = 2.
SELECT COUNT(*) AS n FROM iceberg_hms_${suite_uuid0}.hms_io_db_${uuid0}.t_io_${uuid0};

-- query 8
-- @skip_result_check=true
-- UPDATE drives the merge-on-read / copy-on-write commit through HMS.
UPDATE iceberg_hms_${suite_uuid0}.hms_io_db_${uuid0}.t_io_${uuid0} SET amount = amount + 1 WHERE id = 1;

-- query 9
-- id=1 amount 10.5 -> 11.5; id=2 unchanged at 20.0.
SELECT id, region, amount
  FROM iceberg_hms_${suite_uuid0}.hms_io_db_${uuid0}.t_io_${uuid0}
  ORDER BY id;

-- query 10
-- @skip_result_check=true
INSERT OVERWRITE iceberg_hms_${suite_uuid0}.hms_io_db_${uuid0}.t_io_${uuid0}
VALUES (999, 'ap', 0.0), (998, 'ap', 1.0);

-- query 11
-- After INSERT OVERWRITE: only the two new rows remain.
SELECT id, region, amount
  FROM iceberg_hms_${suite_uuid0}.hms_io_db_${uuid0}.t_io_${uuid0}
  ORDER BY id;

-- query 12
-- @skip_result_check=true
DROP TABLE iceberg_hms_${suite_uuid0}.hms_io_db_${uuid0}.t_io_${uuid0};

-- query 13
-- @skip_result_check=true
DROP DATABASE iceberg_hms_${suite_uuid0}.hms_io_db_${uuid0};
```

> FULL-PARITY GATE: when recording (Step 5), the recorded result for queries 6–9 MUST show a successful DELETE/UPDATE and the asserted post-mutation counts/rows — NOT an error. If DELETE or UPDATE errors through the HMS catalog, that is a real `update_table`/commit gap to fix before declaring the feature done (do not record an error as the golden).

- [ ] **Step 4: Start standalone-server (if not running) against the generated config**

```bash
source docker/iceberg-rest/runtime/current/env.sh
LOG=/tmp/novarocks-hms-server.log
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
for i in $(seq 1 90); do grep -q '^NOVAROCKS_READY ' "$LOG" && break; sleep 1; done
grep -q '^NOVAROCKS_READY ' "$LOG" || { echo "server not ready"; tail -30 "$LOG"; exit 1; }
```

- [ ] **Step 5: Record the golden result**

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-hms \
  --mode record --record-from target
```
Expected: produces `sql-tests/iceberg-hms/result/iceberg_hms_insert_select.result`. (Project rule: record NovaRocks-only goldens with `--record-from target`, not the default `reference`.)

- [ ] **Step 6: Verify the recorded golden passes**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-hms --mode verify
```
Expected: PASS — `count(*)` returns 4, ordered SELECT returns rows 1..3.

- [ ] **Step 7: Add a README mirroring the iceberg-rest suite**

`sql-tests/iceberg-hms/README.md`: copy `sql-tests/iceberg-rest/README.md` and adapt wording (catalog type hive, HMS service, what the suite proves).

- [ ] **Step 8: Commit**

```bash
git add sql-tests/iceberg-hms
git commit -m "test(iceberg): iceberg-hms NovaRocks write/read round-trip suite"
```

### Task 14: `iceberg-hms-compatibility` suite — Spark ↔ NovaRocks cross-engine

**Files:**
- Modify: `docker/iceberg-rest/spark/` (add an HMS catalog to Spark's config — read the dir first).
- Create: `sql-tests/iceberg-hms-compatibility/init.sql`, `cleanup.sql`, `README.md`, `sql/*.sql`, `result/*.result`.

- [ ] **Step 1: Inspect how Spark catalogs are configured**

Run:
```bash
ls -R docker/iceberg-rest/spark
grep -rn "spark.sql.catalog" docker/iceberg-rest/spark
echo "--- generated spark catalog SQL env ---"
grep -n "SPARK" docker/iceberg-rest/up.sh | head
```
Identify where the REST catalog is registered for Spark (e.g. `spark-defaults.conf` or a generated `NOVAROCKS_ICE_REST_CATALOG_SQL`). The HMS catalog mirrors it with `type=hive`.

- [ ] **Step 2: Add a Spark HMS catalog config**

In Spark's `spark-defaults.conf` (under `docker/iceberg-rest/spark/`), add an Iceberg HiveCatalog pointing at the in-network HMS + MinIO:
```
spark.sql.catalog.hms_catalog                 org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.hms_catalog.type            hive
spark.sql.catalog.hms_catalog.uri             thrift://hms:9083
spark.sql.catalog.hms_catalog.warehouse       s3://warehouse/shared/hms
spark.sql.catalog.hms_catalog.io-impl         org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.hms_catalog.s3.endpoint     http://minio:9000
spark.sql.catalog.hms_catalog.s3.path-style-access  true
```
(Spark uses in-network hostnames `hms`/`minio`; NovaRocks uses host endpoints from `env.sh`. Do not mix the two.)

- [ ] **Step 3: Write `init.sql` + the Spark-writes / NovaRocks-reads case**

`sql-tests/iceberg-hms-compatibility/init.sql` (the NovaRocks-side hive catalog):
```sql
CREATE EXTERNAL CATALOG IF NOT EXISTS `iceberg_hms_compat_${suite_uuid0}`
PROPERTIES (
    "type"="iceberg",
    "iceberg.catalog.type"="hive",
    "hive.metastore.uris"="${iceberg_hms_uris}",
    "warehouse"="${iceberg_hms_warehouse}",
    "aws.s3.access_key"="${oss_ak}",
    "aws.s3.secret_key"="${oss_sk}",
    "aws.s3.endpoint"="${oss_endpoint}",
    "aws.s3.region"="us-east-1",
    "aws.s3.enable_path_style_access"="true"
);
```

`sql-tests/iceberg-hms-compatibility/cleanup.sql`:
```sql
DROP CATALOG IF EXISTS `iceberg_hms_compat_${suite_uuid0}`;
```

`sql-tests/iceberg-hms-compatibility/sql/spark_hms_minio_write_read.sql` — the `shell:` + `spark-sql.sh` mechanism is copied verbatim from `sql-tests/iceberg-compatibility/sql/spark_rest_minio_v3_complex_nested.sql`; only the Spark catalog (`ice_rest` → `hms_catalog`) and the NovaRocks catalog name change. The runner substitutes `${...}` placeholders in the whole case text BEFORE running the shell block, so placeholders work inside the quoted heredoc:

```sql
-- @order_sensitive=true
-- @sequential=true
-- Spark writes an Iceberg table through the Hive Metastore catalog; NovaRocks reads it.

-- query 1
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-hms-write-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
CREATE NAMESPACE IF NOT EXISTS hms_catalog.nr_hms_compat_${suite_uuid0};
DROP TABLE IF EXISTS hms_catalog.nr_hms_compat_${suite_uuid0}.spark_hms_${uuid0};
CREATE TABLE hms_catalog.nr_hms_compat_${suite_uuid0}.spark_hms_${uuid0} (
  id BIGINT,
  region STRING,
  amount DOUBLE
) USING iceberg
TBLPROPERTIES ('format-version' = '2', 'write.format.default' = 'parquet');
INSERT INTO hms_catalog.nr_hms_compat_${suite_uuid0}.spark_hms_${uuid0} VALUES
  (1, 'us', 10.5), (2, 'us', 20.0), (3, 'eu', 30.25);
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 2
SELECT id, region, amount
FROM iceberg_hms_compat_${suite_uuid0}.nr_hms_compat_${suite_uuid0}.spark_hms_${uuid0}
ORDER BY id;

-- query 3
-- @skip_result_check=true
DROP TABLE iceberg_hms_compat_${suite_uuid0}.nr_hms_compat_${suite_uuid0}.spark_hms_${uuid0} FORCE;
```

- [ ] **Step 4: Write the reverse case — NovaRocks writes, Spark reads**

`sql-tests/iceberg-hms-compatibility/sql/nova_hms_minio_write_read.sql`. NovaRocks writes through the hive catalog; Spark reads the same HMS table back. The `@result_contains` substring asserts a value that appears in `spark-sql.sh` stdout regardless of its exact column formatting — adjust the substring to match the real output after the first record run:

```sql
-- @order_sensitive=true
-- @sequential=true
-- NovaRocks writes an Iceberg table through the Hive Metastore catalog; Spark reads it back.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_hms_compat_${suite_uuid0}.nr_hms_w_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_hms_compat_${suite_uuid0}.nr_hms_w_db_${uuid0}.nova_hms_${uuid0} (
  id BIGINT,
  region STRING,
  amount DOUBLE
)
PARTITION BY (region);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_hms_compat_${suite_uuid0}.nr_hms_w_db_${uuid0}.nova_hms_${uuid0}
VALUES (1, 'us', 10.5), (2, 'eu', 20.0);

-- query 4
-- @result_contains=10.5
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-hms-read-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
SELECT id, region, amount
FROM hms_catalog.nr_hms_w_db_${uuid0}.nova_hms_${uuid0}
ORDER BY id;
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"

-- query 5
-- @skip_result_check=true
DROP TABLE iceberg_hms_compat_${suite_uuid0}.nr_hms_w_db_${uuid0}.nova_hms_${uuid0};
```

- [ ] **Step 5: Rebuild Spark image (config baked in) and bring the fixture up**

```bash
docker compose -p "${NOVA_ENV_COMPOSE_PROJECT:-nr-iceberg-rest}" -f docker/iceberg-rest/compose.yml build spark
docker/iceberg-rest/up.sh
```

- [ ] **Step 6: Record + verify**

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-hms-compatibility \
  --mode record --record-from target
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-hms-compatibility --mode verify
```
Expected: both directions PASS — Spark-written rows read by NovaRocks and vice versa, proving real Iceberg-on-HMS interop.

- [ ] **Step 7: README + commit**

`sql-tests/iceberg-hms-compatibility/README.md`: adapt from `sql-tests/iceberg-compatibility/README.md`.

```bash
git add sql-tests/iceberg-hms-compatibility docker/iceberg-rest/spark
git commit -m "test(iceberg): iceberg-hms-compatibility cross-engine suite (Spark <-> NovaRocks)"
```

---

## Phase 5 — Documentation

### Task 15: Flip the HMS capability and note the new catalog type

**Files:**
- Modify: `docs/guides/iceberg-v3/catalog.md`
- Modify: `CLAUDE.md` and `AGENTS.md`

- [ ] **Step 1: Update the catalog capability doc**

In `docs/guides/iceberg-v3/catalog.md`, change the Hive Metastore row from `❌` to `✅` and add a one-line note: read+write supported via `iceberg-catalog-hms`; v1 plaintext thrift only (Kerberos/SASL and multi-level namespaces are follow-ups).

Run first to find the exact line: `grep -n "Hive Metastore\|HMS" docs/guides/iceberg-v3/catalog.md`

- [ ] **Step 2: Update the agent guides**

In both `CLAUDE.md` and `AGENTS.md`, find the §5.3 sentence "supported catalog types are `memory`, `hadoop`, and `rest`" and change it to "`memory`, `hadoop`, `rest`, and `hive`".

Run: `grep -rn "memory.*hadoop.*rest" CLAUDE.md AGENTS.md`

- [ ] **Step 3: Commit**

```bash
git add docs/guides/iceberg-v3/catalog.md CLAUDE.md AGENTS.md
git commit -m "docs(iceberg): mark Hive Metastore catalog supported"
```

### Task 16: Final full-suite regression gate

**Files:** none (verification only).

- [ ] **Step 1: Rust tests + lints**

```bash
cargo fmt --check
cargo clippy -p novarocks 2>&1 | tail -20
cargo test -p novarocks --lib connector::iceberg::catalog::registry 2>&1 | tail -20
```
Expected: clean fmt, no new clippy warnings, all registry tests pass.

- [ ] **Step 2: Re-run the REST suite to prove no regression from the shared-branch refactor**

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-rest --mode verify
```
Expected: PASS — Phase 2's `build_rest_catalog` → `build_iceberg_catalog` swap did not regress REST.

- [ ] **Step 3: Run both HMS suites end-to-end**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-hms --mode verify
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-hms-compatibility --mode verify
```
Expected: both PASS.

- [ ] **Step 4: No commit needed (verification only).** If anything failed, debug per `superpowers:systematic-debugging` before declaring the feature complete.

---

## Risk register (carry into execution)

1. **volo/block_on bridging (Task 6).** If the volo-thrift client hangs under `data_block_on`, isolate HMS ops on a dedicated multi-threaded Tokio runtime or `spawn_blocking`. This is gated first (Phase 0) on purpose.
2. **HMS image S3A (Task 9–10).** `HADOOP_VERSION` must match the jars bundled in `apache/hive:4.0.0`; a mismatch causes `ClassNotFoundException`/`NoSuchMethodError` for S3A. Step 3 of Task 9 verifies the version. Fallback: a metastore image with a Postgres backing DB if Derby + S3A proves flaky.
3. **format-version channel (Task 7 Step 9 note).** HMS uses the typed `format_version` (Hadoop path); do NOT add HMS to the line-552 property re-insertion. The round-trip suite implicitly checks default v2 creation; if v3/row-lineage tables are later needed on HMS, re-verify the format-version path.
4. **thrift transport default.** Defaults to buffered. If the chosen HMS image enables framed transport, set `hive.metastore.thrift.framed=true` in the catalog properties (Task 5 reads it).
5. **HMS readiness (Task 11 Step 5).** TCP-accept ≠ schema-ready. The suite `CREATE DATABASE` is the real gate; flakiness here means adding a thrift `get_all_databases` ping.
