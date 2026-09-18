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

//! Local state-machine coverage for the consumer-side storage authority.
//!
//! Every test here drives acquisition through an injected source and a queued
//! executor, so nothing depends on wall-clock sleeps or on a live catalog. The
//! deadlines used are either already elapsed or far in the future, which keeps
//! these cases off the load-sensitive list.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};

use novarocks_spi::connector::{CatalogVersion, ConnectorInstanceId};

use super::*;

/// Runs queued refresh jobs only when the test asks, so every publication
/// point is an explicit step rather than a race.
#[derive(Default)]
struct QueuedExecutor {
    jobs: Mutex<VecDeque<Box<dyn FnOnce() + Send + 'static>>>,
    accepted: AtomicUsize,
}

impl QueuedExecutor {
    fn pending(&self) -> usize {
        self.jobs.lock().expect("queued executor lock").len()
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::Relaxed)
    }

    /// Runs one queued job to completion on this thread.
    fn run_one(&self) -> bool {
        let job = self.jobs.lock().expect("queued executor lock").pop_front();
        match job {
            Some(job) => {
                job();
                true
            }
            None => false,
        }
    }
}

impl RefreshExecutor for QueuedExecutor {
    fn execute(&self, job: Box<dyn FnOnce() + Send + 'static>) {
        self.accepted.fetch_add(1, Ordering::Relaxed);
        self.jobs
            .lock()
            .expect("queued executor lock")
            .push_back(job);
    }
}

/// Hands out programmed outcomes in order, repeating the last one.
struct ScriptedSource {
    outcomes: Mutex<VecDeque<Result<AuthorityMaterial, AcquisitionFailure>>>,
    last: Mutex<Result<AuthorityMaterial, AcquisitionFailure>>,
    calls: AtomicUsize,
}

impl ScriptedSource {
    fn new(outcomes: Vec<Result<AuthorityMaterial, AcquisitionFailure>>) -> Self {
        let last = outcomes
            .last()
            .cloned()
            .unwrap_or(Err(AcquisitionFailure::NoRenewalCapability));
        Self {
            outcomes: Mutex::new(outcomes.into()),
            last: Mutex::new(last),
            calls: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

impl AuthorityMaterialSource for ScriptedSource {
    fn acquire(&self, _deadline: Instant) -> Result<AuthorityMaterial, AcquisitionFailure> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let next = self
            .outcomes
            .lock()
            .expect("scripted source lock")
            .pop_front();
        match next {
            Some(outcome) => outcome,
            None => self.last.lock().expect("scripted source lock").clone(),
        }
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("current-thread runtime")
}

fn catalog(name: &str) -> CatalogHandle {
    CatalogHandle::new(
        ConnectorInstanceId::parse(name).expect("instance id"),
        CatalogVersion::from_bytes([7; 32]),
    )
}

fn prefix(value: &str) -> StorageCredentialScopePrefix {
    StorageCredentialScopePrefix::try_from_normalized(value).expect("scope prefix")
}

fn principal(generation: &str) -> StaticCredentialReference {
    StaticCredentialReference::try_new("vending", generation).expect("credential reference")
}

fn endpoint_capability(url: &str) -> AuthorityCapabilityPath {
    AuthorityCapabilityPath::CredentialsEndpoint {
        principal: principal("g1"),
        endpoint: Arc::from(url),
    }
}

fn endpoint_capability_as(generation: &str, url: &str) -> AuthorityCapabilityPath {
    AuthorityCapabilityPath::CredentialsEndpoint {
        principal: principal(generation),
        endpoint: Arc::from(url),
    }
}

fn identity(url: &str) -> StorageAuthorityId {
    StorageAuthorityId::new(
        catalog("lake"),
        prefix("s3://warehouse/sales/orders/"),
        endpoint_capability(url),
    )
}

fn material(not_after: Instant) -> AuthorityMaterial {
    AuthorityMaterial::new(
        SecretValue::new("ak"),
        SecretValue::new("sk"),
        Some(SecretValue::new("token")),
        not_after,
    )
}

/// A material value distinguishable from `material` by its access key id.
fn renewed_material(not_after: Instant) -> AuthorityMaterial {
    AuthorityMaterial::new(
        SecretValue::new("renewed-ak"),
        SecretValue::new("sk"),
        Some(SecretValue::new("token")),
        not_after,
    )
}

struct Fixture {
    authority: Arc<StorageAuthority>,
    executor: Arc<QueuedExecutor>,
    source: Arc<ScriptedSource>,
}

fn fixture_with(
    id: StorageAuthorityId,
    outcomes: Vec<Result<AuthorityMaterial, AcquisitionFailure>>,
    policy: RefreshPolicy,
) -> Fixture {
    let executor = Arc::new(QueuedExecutor::default());
    let source = Arc::new(ScriptedSource::new(outcomes));
    let authority = Arc::new(StorageAuthority::new(
        id,
        Arc::clone(&source) as Arc<dyn AuthorityMaterialSource>,
        Arc::clone(&executor) as Arc<dyn RefreshExecutor>,
        policy,
    ));
    Fixture {
        authority,
        executor,
        source,
    }
}

fn policy() -> RefreshPolicy {
    RefreshPolicy {
        prefetch_window: Duration::from_secs(300),
        validity_margin: Duration::from_secs(30),
        min_backoff: Duration::from_millis(200),
        max_backoff: Duration::from_secs(5),
    }
}

// ---------------------------------------------------------------------------
// CAD-1 D4: the three states
// ---------------------------------------------------------------------------

#[test]
fn usable_material_outside_the_prefetch_window_never_reaches_the_executor() {
    let now = Instant::now();
    let fixture = fixture_with(identity("https://catalog/credentials"), vec![], policy());
    fixture
        .authority
        .install_material(material(now + Duration::from_secs(3600)));

    let obtained = runtime().block_on(
        fixture
            .authority
            .material_for_request(now, now + Duration::from_secs(10)),
    );

    assert_eq!(
        obtained
            .expect("cached material")
            .access_key_id()
            .expose_secret(),
        "ak"
    );
    assert_eq!(
        fixture.executor.accepted(),
        0,
        "a cache hit must not start any acquisition"
    );
    assert_eq!(fixture.authority.metrics().cache_hits, 1);
}

#[test]
fn prefetch_returns_current_material_and_a_transient_failure_fails_nothing() {
    let now = Instant::now();
    let fixture = fixture_with(
        identity("https://catalog/credentials"),
        vec![Err(AcquisitionFailure::Transient(
            "connection reset".into(),
        ))],
        policy(),
    );
    // Inside the prefetch window but still comfortably usable.
    fixture
        .authority
        .install_material(material(now + Duration::from_secs(120)));

    let obtained = runtime().block_on(
        fixture
            .authority
            .material_for_request(now, now + Duration::from_secs(10)),
    );

    assert!(obtained.is_ok(), "state one must not fail the operation");
    assert_eq!(fixture.executor.accepted(), 1, "prefetch must be started");
    assert_eq!(fixture.authority.metrics().prefetch_started, 1);

    // The refresh now fails transiently. The still-valid material survives.
    assert!(fixture.executor.run_one());
    let after = runtime().block_on(
        fixture
            .authority
            .material_for_request(now, now + Duration::from_secs(10)),
    );
    assert_eq!(
        after
            .expect("material survives a transient prefetch failure")
            .access_key_id()
            .expose_secret(),
        "ak"
    );
    assert_eq!(fixture.authority.metrics().refreshes_failed, 1);
}

#[test]
fn a_confirmed_denial_is_rejected_as_permission_not_retried_as_jitter() {
    let now = Instant::now();
    let fixture = fixture_with(
        identity("https://catalog/credentials"),
        vec![Err(AcquisitionFailure::Denied("token revoked".into()))],
        policy(),
    );

    // No material at all: state two, which has to wait for one acquisition.
    let handle = runtime();
    let first = handle.block_on(async {
        // Register the in-flight request without consuming its outcome.
        let waiter = fixture
            .authority
            .material_for_request(now, now.checked_sub(Duration::from_secs(1)).unwrap_or(now));
        waiter.await
    });
    assert_eq!(
        first.expect_err("no material yet").kind(),
        FileErrorKind::DeadlineExceeded
    );

    assert!(fixture.executor.run_one());

    let second = handle.block_on(
        fixture
            .authority
            .material_for_request(now, now + Duration::from_secs(10)),
    );
    let error = second.expect_err("a denied authority must not hand out material");
    assert_eq!(
        error.kind(),
        FileErrorKind::Permission,
        "a confirmed denial must not be reported as a transient failure"
    );
    assert_eq!(
        fixture.source.calls(),
        1,
        "a denial must not be retried as ordinary jitter"
    );
}

#[test]
fn catalog_unreachable_is_distinguishable_from_denial() {
    // CAD-1 D12 / acceptance 18 is an end-to-end property, but the
    // classification it rests on is decided here.
    let denied = AcquisitionFailure::Denied("revoked".into());
    let unreachable = AcquisitionFailure::CatalogUnreachable("connect timed out".into());
    let id = identity("https://catalog/credentials");

    let denied_error = denied.into_file_error(&id);
    let unreachable_error = unreachable.into_file_error(&id);

    assert_eq!(denied_error.kind(), FileErrorKind::Permission);
    assert_eq!(unreachable_error.kind(), FileErrorKind::Transient);
    assert!(
        unreachable_error
            .to_string()
            .contains("could not reach its catalog"),
        "operators have only the error text to work from: {unreachable_error}"
    );
}

// ---------------------------------------------------------------------------
// CAD-1 D6 / acceptance 5: the shared request belongs to the authority
// ---------------------------------------------------------------------------

#[test]
fn one_waiter_giving_up_does_not_end_the_shared_request() {
    let now = Instant::now();
    let renewed = renewed_material(now + Duration::from_secs(3600));
    let fixture = fixture_with(
        identity("https://catalog/credentials"),
        vec![Ok(renewed)],
        policy(),
    );
    let handle = runtime();

    // Waiter one has no budget left and gives up immediately. It must not take
    // the in-flight request down with it.
    let abandoned = handle.block_on(
        fixture
            .authority
            .material_for_request(now, now.checked_sub(Duration::from_secs(1)).unwrap_or(now)),
    );
    assert_eq!(
        abandoned.expect_err("waiter one gave up").kind(),
        FileErrorKind::DeadlineExceeded
    );
    assert_eq!(
        fixture.executor.pending(),
        1,
        "the request must still be queued after the first waiter gave up"
    );

    // The request completes, and waiter two still gets its result.
    assert!(fixture.executor.run_one());
    let second = handle.block_on(
        fixture
            .authority
            .material_for_request(now, now + Duration::from_secs(10)),
    );
    assert_eq!(
        second.expect("waiter two").access_key_id().expose_secret(),
        "renewed-ak"
    );
    assert_eq!(
        fixture.source.calls(),
        1,
        "the two waiters must share a single acquisition"
    );
}

// ---------------------------------------------------------------------------
// CAD-1 D8 / acceptance 12: a late result cannot reopen a closed authority
// ---------------------------------------------------------------------------

#[test]
fn a_late_success_does_not_restore_a_revoked_authority() {
    let now = Instant::now();
    let fixture = fixture_with(
        identity("https://catalog/credentials"),
        vec![Ok(renewed_material(now + Duration::from_secs(3600)))],
        policy(),
    );
    let handle = runtime();

    // Start a refresh, then observe a revocation while it is still in flight.
    let started = handle.block_on(
        fixture
            .authority
            .material_for_request(now, now.checked_sub(Duration::from_secs(1)).unwrap_or(now)),
    );
    assert_eq!(
        started.expect_err("nothing usable yet").kind(),
        FileErrorKind::DeadlineExceeded
    );
    fixture.authority.close(AcquisitionFailure::Denied(
        "revoked while refreshing".into(),
    ));

    // The earlier refresh now succeeds. It must be discarded.
    assert!(fixture.executor.run_one());

    let after = handle.block_on(
        fixture
            .authority
            .material_for_request(now, now + Duration::from_secs(10)),
    );
    assert_eq!(
        after.expect_err("a closed authority stays closed").kind(),
        FileErrorKind::Permission
    );
    assert_eq!(
        fixture.authority.metrics().refreshes_applied,
        0,
        "a result from a replaced generation must never be applied"
    );
}

#[test]
fn installing_material_into_a_closed_authority_is_ignored() {
    let now = Instant::now();
    let fixture = fixture_with(identity("https://catalog/credentials"), vec![], policy());
    fixture
        .authority
        .close(AcquisitionFailure::Denied("revoked".into()));

    fixture
        .authority
        .install_material(material(now + Duration::from_secs(3600)));

    let obtained = runtime().block_on(
        fixture
            .authority
            .material_for_request(now, now + Duration::from_secs(10)),
    );
    assert_eq!(
        obtained.expect_err("closed").kind(),
        FileErrorKind::Permission
    );
}

// ---------------------------------------------------------------------------
// CAD-1 D7 / acceptance 4: identical material is not an equivalence proof
// ---------------------------------------------------------------------------

#[test]
fn identical_material_with_different_capability_paths_is_not_one_authority() {
    let now = Instant::now();
    let shared = material(now + Duration::from_secs(120));

    let table_a = identity("https://catalog/v1/ns/a/credentials");
    let table_b = identity("https://catalog/v1/ns/b/credentials");
    assert_ne!(
        table_a, table_b,
        "two tables whose refresh endpoints differ are two authorities even when \
         the server happens to vend identical material"
    );

    let fixture_a = fixture_with(
        table_a,
        vec![Ok(renewed_material(now + Duration::from_secs(3600)))],
        policy(),
    );
    let fixture_b = fixture_with(
        table_b,
        vec![Ok(renewed_material(now + Duration::from_secs(3600)))],
        policy(),
    );
    fixture_a.authority.install_material(shared.clone());
    fixture_b.authority.install_material(shared);

    let handle = runtime();
    let _ = handle.block_on(
        fixture_a
            .authority
            .material_for_request(now, now + Duration::from_secs(10)),
    );
    let _ = handle.block_on(
        fixture_b
            .authority
            .material_for_request(now, now + Duration::from_secs(10)),
    );

    assert_eq!(fixture_a.executor.accepted(), 1);
    assert_eq!(
        fixture_b.executor.accepted(),
        1,
        "the second authority must run its own refresh rather than inherit the first one's"
    );
}

#[test]
fn the_catalog_generation_dimension_lives_in_the_identity() {
    // CAD-1 D7 requires the refresh generation to match before an in-flight
    // result may be shared, and D10 places the generation inside CatalogHandle.
    // Nothing else may compare generations separately.
    let scope = prefix("s3://warehouse/sales/orders/");
    let capability = endpoint_capability("https://catalog/credentials");
    let first = StorageAuthorityId::new(
        CatalogHandle::new(
            ConnectorInstanceId::parse("lake").expect("instance id"),
            CatalogVersion::from_bytes([1; 32]),
        ),
        scope.clone(),
        capability.clone(),
    );
    let second = StorageAuthorityId::new(
        CatalogHandle::new(
            ConnectorInstanceId::parse("lake").expect("instance id"),
            CatalogVersion::from_bytes([2; 32]),
        ),
        scope,
        capability,
    );

    assert_ne!(
        first, second,
        "a catalog generation change must produce a different authority identity"
    );
}

#[test]
fn a_different_refreshing_principal_is_a_different_authority() {
    // CAD-1 D1 gives the coordinator and the executor different principals, so
    // the same table reached under each of them is two authorities.
    let scope = prefix("s3://warehouse/sales/orders/");
    let first = StorageAuthorityId::new(
        catalog("lake"),
        scope.clone(),
        endpoint_capability_as("coordinator-g1", "https://catalog/credentials"),
    );
    let second = StorageAuthorityId::new(
        catalog("lake"),
        scope,
        endpoint_capability_as("executor-g1", "https://catalog/credentials"),
    );

    assert_ne!(first, second);
    assert_ne!(
        first.principal(),
        second.principal(),
        "the refreshing identity is the dimension that separates them"
    );
}

#[test]
fn a_seeded_authority_has_no_refreshing_principal() {
    // CAD-1 M1 and CAD-1 D11 both land here: material arrives from elsewhere
    // and there is no path to authenticate a refresh along.
    let id = StorageAuthorityId::new(
        catalog("lake"),
        prefix("s3://warehouse/sales/orders/"),
        AuthorityCapabilityPath::SeededWithoutRenewal,
    );

    assert!(id.principal().is_none());
    assert!(!id.capability().can_renew());
}

// ---------------------------------------------------------------------------
// CAD-1 D11: seeded material with no renewal capability
// ---------------------------------------------------------------------------

#[test]
fn a_seeded_authority_without_renewal_serves_material_then_refuses() {
    let now = Instant::now();
    let id = StorageAuthorityId::new(
        catalog("lake"),
        prefix("s3://warehouse/sales/orders/"),
        AuthorityCapabilityPath::SeededWithoutRenewal,
    );
    let fixture = fixture_with(id, vec![], policy());
    fixture
        .authority
        .install_material(material(now + Duration::from_secs(3600)));

    let served = runtime().block_on(
        fixture
            .authority
            .material_for_request(now, now + Duration::from_secs(10)),
    );
    assert!(served.is_ok(), "seeded material is served like any other");
    assert_eq!(
        fixture.executor.accepted(),
        0,
        "an authority without a renewal path must never start an acquisition"
    );

    // Past expiry there is nothing it can do, and it says so precisely.
    let expired = runtime().block_on(fixture.authority.material_for_request(
        now + Duration::from_secs(4000),
        now + Duration::from_secs(4010),
    ));
    let error = expired.expect_err("expired seeded material");
    assert_eq!(error.kind(), FileErrorKind::Permission);
    assert!(error.to_string().contains("holds no renewal capability"));
}
