// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! A runner-only, process-local fault entrance for one exact authority.
//! It is compiled out of release builds and requires the harness's private
//! lifecycle-fault directory, a backend index, and a separate explicit opt-in.
//! No SQL, HTTP, or Native protocol can invoke it.

use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use sha2::{Digest, Sha256};

use super::{AcquisitionFailure, AuthorityCapabilityPath, AuthorityShared, StorageAuthorityId};

const ROOT_ENV: &str = "NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_FAULT_DIR";
const BACKEND_INDEX_ENV: &str = "NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_BACKEND_INDEX";
const ENABLE_ENV: &str = "NOVAROCKS_SQL_TEST_STORAGE_AUTHORITY_CLOSE_ENABLE";
static NEXT_REGISTRATION: AtomicU64 = AtomicU64::new(0);

pub(super) struct Scope {
    root: PathBuf,
    backend_index: usize,
}

pub(super) fn configured_scope(
    enabled: Option<&str>,
    root: Option<PathBuf>,
    backend_index: Option<&str>,
) -> Option<Scope> {
    if enabled != Some("1") {
        return None;
    }
    let root = root.filter(|root| root.is_absolute() && root.is_dir())?;
    let backend_index = backend_index?.parse::<usize>().ok()?;
    Some(Scope {
        root,
        backend_index,
    })
}

pub(super) fn start_if_runner_enabled(shared: &Arc<AuthorityShared>) {
    let enabled = std::env::var(ENABLE_ENV).ok();
    let root = std::env::var_os(ROOT_ENV).map(PathBuf::from);
    let backend_index = std::env::var(BACKEND_INDEX_ENV).ok();
    let Some(scope) = configured_scope(enabled.as_deref(), root, backend_index.as_deref()) else {
        return;
    };
    let registration = NEXT_REGISTRATION.fetch_add(1, Ordering::Relaxed);
    let identity_path = scope.root.join(format!(
        "be-{}.storage-authority-{registration}.identity",
        scope.backend_index
    ));
    let key = control_key(&shared.id);
    if let Err(error) = fs::write(&identity_path, identity_record(&shared.id, &key)) {
        tracing::warn!(%error, "cannot register debug storage authority close target");
        return;
    }
    let weak = Arc::downgrade(shared);
    if let Err(error) = std::thread::Builder::new()
        .name("storage-authority-debug-close".into())
        .spawn(move || observe_trigger(scope, identity_path, key, weak))
    {
        tracing::warn!(%error, "cannot start debug storage authority close observer");
    }
}

pub(super) fn control_key(id: &StorageAuthorityId) -> String {
    // The full identity participates, including catalog version and endpoint.
    // Only its digest is written to runner-owned files; an advertised endpoint
    // may contain query parameters and must not be copied into evidence.
    let digest = Sha256::digest(format!("{id:?}").as_bytes());
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("write to String");
    }
    encoded
}

fn identity_record(id: &StorageAuthorityId, key: &str) -> String {
    let mut version = String::with_capacity(64);
    for byte in id.catalog().version().as_bytes() {
        write!(&mut version, "{byte:02x}").expect("write to String");
    }
    let path = match id.capability() {
        AuthorityCapabilityPath::CredentialsEndpoint { .. } => "credentials-endpoint",
        AuthorityCapabilityPath::LoadTableDelegation { .. } => "load-table-delegation",
        AuthorityCapabilityPath::InProcessProvider => "in-process-provider",
        AuthorityCapabilityPath::SeededWithoutRenewal => "seeded-without-renewal",
    };
    let principal = id.principal().map_or_else(
        || "none".to_string(),
        |principal| format!("{}:{}", principal.name(), principal.generation()),
    );
    format!(
        "key={key}\ncatalog={}\nversion={version}\nscope={}\nprincipal={principal}\npath={path}\n",
        id.catalog().catalog_name().as_str(),
        id.scope().as_str(),
    )
}

fn observe_trigger(scope: Scope, identity_path: PathBuf, key: String, weak: Weak<AuthorityShared>) {
    let trigger = scope.root.join(format!(
        "be-{}.storage-authority-close.trigger",
        scope.backend_index
    ));
    let claimed = scope.root.join(format!(
        "be-{}.storage-authority-close.claimed",
        scope.backend_index
    ));
    let confirmed = scope.root.join(format!(
        "be-{}.storage-authority-close.confirmed",
        scope.backend_index
    ));
    let late_confirmed = scope.root.join(format!(
        "be-{}.storage-authority-close.late-confirmed",
        scope.backend_index
    ));
    while let Some(shared) = weak.upgrade() {
        if fs::read_to_string(&trigger)
            .ok()
            .is_some_and(|requested| requested.trim() == key)
            && fs::rename(&trigger, &claimed).is_ok()
        {
            let result = close_if_key(&shared, &key);
            let status = if result { "closed" } else { "already-closed" };
            let before = shared.metrics();
            let temporary = confirmed.with_extension("confirmed.tmp");
            if fs::write(
                &temporary,
                format!(
                    "key={key}\nstatus={status}\nrefreshes_applied={}\nlate_results_discarded={}\n",
                    before.refreshes_applied, before.late_results_discarded
                ),
            )
            .is_ok()
            {
                let _ = fs::rename(temporary, confirmed);
            }
            let _ = fs::remove_file(claimed);
            if result {
                // The runner releases the held HTTP success only after it has
                // read the close confirmation. A second marker records that
                // exact authority's later generation fence, without polling a
                // public endpoint or claiming another consumer's counters.
                while let Some(shared) = weak.upgrade() {
                    let after = shared.metrics();
                    if after.late_results_discarded > before.late_results_discarded {
                        let status = if after.refreshes_applied == before.refreshes_applied {
                            "discarded"
                        } else {
                            "applied-after-close"
                        };
                        let temporary = late_confirmed.with_extension("late-confirmed.tmp");
                        if fs::write(
                            &temporary,
                            format!(
                                "key={key}\nstatus={status}\nrefreshes_applied={}\nlate_results_discarded={}\n",
                                after.refreshes_applied, after.late_results_discarded
                            ),
                        )
                        .is_ok()
                        {
                            let _ = fs::rename(temporary, late_confirmed);
                        }
                        break;
                    }
                    drop(shared);
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
            break;
        }
        drop(shared);
        std::thread::sleep(Duration::from_millis(20));
    }
    let _ = fs::remove_file(identity_path);
}

pub(super) fn close_if_key(shared: &AuthorityShared, requested: &str) -> bool {
    if requested != control_key(&shared.id) {
        return false;
    }
    let mut state = shared.lock_state();
    if state.closed.is_some() {
        return false;
    }
    state.closed = Some(AcquisitionFailure::Denied(
        "runner confirmed storage authority closure".into(),
    ));
    state.generation = state.generation.wrapping_add(1);
    state.material = None;
    state.inflight = None;
    true
}
