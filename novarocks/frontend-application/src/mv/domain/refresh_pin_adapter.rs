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

use crate::mv::domain::application::{MvApplicationError, MvApplicationErrorKind};
use crate::mv::domain::refresh::pin::{RefreshSnapshotPin, RefreshSnapshotPinOccurrence};
use novarocks_mv_application::persistence::exact_revision::persist_exact_connector_revision;
use novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection;
use novarocks_spi::connector::MvStorageObservationPort;
use novarocks_spi::connector::{
    ConnectorControlResolver, ConnectorErrorKind, ConnectorExactSemanticRevision,
    ConnectorRequestContext,
};
use novarocks_sql::compiler::SqlMvRelationOccurrenceId;
use novarocks_types::naming::TableIdentity;
#[cfg(test)]
use std::sync::Arc;

/// Capture refresh pins from the exact connector-control and storage-observation
/// capabilities selected by the caller.  This keeps an EXPLAIN or foreground
/// refresh attempt from reconstructing application state just to re-observe
/// a base table.
pub fn capture_refresh_snapshot_pin_with_ports(
    connector_control: &dyn ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
    projection: &StoredKeyedMvProjection,
    connector_context: &ConnectorRequestContext,
) -> Result<RefreshSnapshotPin, String> {
    let relations = &projection.facts.definition().relation_occurrences;
    let mut entries = Vec::with_capacity(relations.len());
    for relation in relations {
        let base_ref = TableIdentity {
            catalog: relation.catalog_at_binding.clone(),
            namespace: relation.namespace_at_binding.clone(),
            table: relation.relation_at_binding.clone(),
        };
        let (observed, exact_revision) =
            crate::mv::domain::refresh_io::observe_current_refresh_revision_with_ports(
                connector_control,
                storage_observation,
                &base_ref,
                connector_context,
            )?;
        entries.push(finish_pin_occurrence(
            relation.occurrence_id,
            &relation.object_id,
            base_ref,
            observed,
            exact_revision,
        )?);
    }
    RefreshSnapshotPin::try_from_occurrences(entries)
}

/// Await observations through the caller's original capabilities; retaining and
/// driving those capabilities remains the caller's responsibility.
pub async fn capture_refresh_snapshot_pin_with_ports_async(
    connector_control: &dyn ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
    projection: &StoredKeyedMvProjection,
    connector_context: &ConnectorRequestContext,
) -> Result<RefreshSnapshotPin, MvApplicationError> {
    let relations = &projection.facts.definition().relation_occurrences;
    let mut entries = Vec::with_capacity(relations.len());
    for relation in relations {
        let base_ref = TableIdentity {
            catalog: relation.catalog_at_binding.clone(),
            namespace: relation.namespace_at_binding.clone(),
            table: relation.relation_at_binding.clone(),
        };
        let (observed, exact_revision) =
            crate::mv::domain::refresh_io::observe_current_refresh_revision_with_ports_async(
                connector_control,
                storage_observation,
                &base_ref,
                connector_context,
            )
            .await
            .map_err(|error| {
                let kind = if error.kind() == ConnectorErrorKind::Unavailable {
                    MvApplicationErrorKind::Unavailable
                } else {
                    MvApplicationErrorKind::Engine
                };
                MvApplicationError::from_connector(kind, error)
            })?;
        entries.push(
            finish_pin_occurrence(
                relation.occurrence_id,
                &relation.object_id,
                base_ref,
                observed,
                exact_revision,
            )
            .map_err(invalid_pin)?,
        );
    }
    RefreshSnapshotPin::try_from_occurrences(entries).map_err(invalid_pin)
}

fn invalid_pin(message: String) -> MvApplicationError {
    MvApplicationError::new(MvApplicationErrorKind::InvalidRequest, message)
}

pub(crate) fn finish_pin_occurrence(
    occurrence_id: u32,
    expected_object: &novarocks_mv_application::persistence::identity::ObjectIdentity,
    base_ref: TableIdentity,
    observed: crate::mv::domain::storage_observation::MvRefreshBaseObservation,
    exact_revision: ConnectorExactSemanticRevision,
) -> Result<RefreshSnapshotPinOccurrence, String> {
    let snapshot_id = observed.current_snapshot_id().ok_or_else(|| {
        format!(
            "iceberg base table {} has no current snapshot; cannot freeze refresh pin",
            base_ref.fqn()
        )
    })?;
    if exact_revision.object_identity().value().as_ref() != observed.object_id().as_bytes().as_ref()
    {
        return Err(format!(
            "MV refresh occurrence {} observation and exact revision identify different objects",
            occurrence_id,
        ));
    }
    let (persisted_object, _) = persist_exact_connector_revision(&exact_revision)
        .map_err(|error| format!("persist MV refresh source identity: {error}"))?;
    if &persisted_object != expected_object {
        return Err(format!(
            "MV refresh occurrence {} source object changed since D was persisted",
            occurrence_id,
        ));
    }
    RefreshSnapshotPinOccurrence::try_new(
        SqlMvRelationOccurrenceId::new(occurrence_id),
        base_ref,
        snapshot_id,
        observed.object_id().clone(),
        exact_revision,
    )
}

#[cfg(test)]
type AfterCaptureHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
struct AfterCaptureHookRegistration {
    owner: std::thread::ThreadId,
    hook: AfterCaptureHook,
}

#[cfg(test)]
fn lock_after_capture_hook_for_test() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .expect("after_capture_hook test lock")
}

#[cfg(test)]
fn after_capture_hook_slot() -> &'static std::sync::Mutex<Option<AfterCaptureHookRegistration>> {
    static HOOK: std::sync::OnceLock<std::sync::Mutex<Option<AfterCaptureHookRegistration>>> =
        std::sync::OnceLock::new();
    HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn invoke_after_capture_hook() {
    let current_thread = std::thread::current().id();
    let hook = after_capture_hook_slot()
        .lock()
        .expect("after_capture_hook lock")
        .as_ref()
        .and_then(|registration| {
            (registration.owner == current_thread).then(|| Arc::clone(&registration.hook))
        });
    if let Some(hook) = hook {
        hook();
    }
}

/// Test-only race seam.  Production callers must retain every exact base
/// materialization before any post-capture work can observe a newer table
/// generation.
#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) fn invoke_after_capture_hook_for_test() {
    invoke_after_capture_hook();
}

#[cfg(test)]
fn set_after_capture_hook(f: AfterCaptureHook) {
    *after_capture_hook_slot()
        .lock()
        .expect("after_capture_hook lock") = Some(AfterCaptureHookRegistration {
        owner: std::thread::current().id(),
        hook: f,
    });
}

#[cfg(test)]
fn clear_after_capture_hook() {
    *after_capture_hook_slot()
        .lock()
        .expect("after_capture_hook lock") = None;
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) struct AfterCaptureHookGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl AfterCaptureHookGuard {
    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    pub(crate) fn install(hook: Arc<dyn Fn() + Send + Sync>) -> Self {
        let lock = lock_after_capture_hook_for_test();
        set_after_capture_hook(hook);
        Self { _lock: lock }
    }
}

#[cfg(test)]
impl Drop for AfterCaptureHookGuard {
    fn drop(&mut self) {
        clear_after_capture_hook();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn after_capture_hook_round_trip() {
        let _hook_lock = lock_after_capture_hook_for_test();
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag_for_hook = Arc::clone(&flag);
        set_after_capture_hook(Arc::new(move || {
            flag_for_hook.store(true, std::sync::atomic::Ordering::SeqCst);
        }));
        std::thread::spawn(invoke_after_capture_hook)
            .join()
            .expect("invoke hook from non-owner thread");
        assert!(!flag.load(std::sync::atomic::Ordering::SeqCst));
        invoke_after_capture_hook();
        assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
        clear_after_capture_hook();
        flag.store(false, std::sync::atomic::Ordering::SeqCst);
        invoke_after_capture_hook();
        assert!(!flag.load(std::sync::atomic::Ordering::SeqCst));
    }

    fn observation(
        object: &[u8],
        snapshot: Option<i64>,
    ) -> (
        crate::mv::domain::storage_observation::MvRefreshBaseObservation,
        ConnectorExactSemanticRevision,
    ) {
        use novarocks_spi::connector::{
            ConnectorProviderId, ConnectorTableIdentity, ConnectorTableObjectId,
        };
        let context = ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(60),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            1,
            1,
        )
        .unwrap();
        let object =
            ConnectorTableObjectId::try_new(bytes::Bytes::copy_from_slice(object)).unwrap();
        let revision = ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            ConnectorProviderId::parse("iceberg").unwrap(),
            &object,
            snapshot,
        )
        .unwrap();
        let observation =
            crate::mv::domain::storage_observation::MvRefreshBaseObservation::try_new(
                ConnectorTableIdentity {
                    instance_id: novarocks_spi::connector::ConnectorInstanceId::parse(
                        "iceberg.rest",
                    )
                    .unwrap(),
                    namespace: "db".into(),
                    table: "base".into(),
                },
                object,
                snapshot,
                &context,
            )
            .unwrap();
        (observation, revision)
    }

    fn base_ref() -> TableIdentity {
        TableIdentity {
            catalog: "ice".into(),
            namespace: "db".into(),
            table: "base".into(),
        }
    }

    #[test]
    fn common_pin_author_preserves_order_and_exact_revision_after_observation_changes() {
        let (observed, revision) = observation(b"original", Some(17));
        let (persisted, _) = persist_exact_connector_revision(&revision).unwrap();
        let mut entries = Vec::new();
        for occurrence_id in [9, 2] {
            entries.push(
                finish_pin_occurrence(
                    occurrence_id,
                    &persisted,
                    base_ref(),
                    observed.clone(),
                    revision.clone(),
                )
                .unwrap(),
            );
        }
        let pin = RefreshSnapshotPin::try_from_occurrences(entries).unwrap();
        assert!(RefreshSnapshotPin::try_from_occurrences(Vec::new()).is_err());
        assert!(
            RefreshSnapshotPin::try_from_occurrences(vec![
                pin.occurrences()[0].clone(),
                pin.occurrences()[0].clone(),
            ])
            .is_err()
        );
        let (_later_observation, _later_revision) = observation(b"replacement", Some(18));
        assert_eq!(
            pin.occurrences()
                .iter()
                .map(|entry| entry.occurrence_id())
                .collect::<Vec<_>>(),
            vec![
                SqlMvRelationOccurrenceId::new(9),
                SqlMvRelationOccurrenceId::new(2)
            ]
        );
        for entry in pin.occurrences() {
            assert_eq!(entry.snapshot_id(), 17);
            assert_eq!(entry.table_object_id(), observed.object_id());
            assert_eq!(entry.semantic_revision(), &revision);
        }
    }

    #[test]
    fn common_pin_author_keeps_persisted_object_and_snapshot_domain_validation() {
        let (observed, revision) = observation(b"original", Some(17));
        let (_, replacement) = observation(b"replacement", Some(18));
        let (persisted, _) = persist_exact_connector_revision(&revision).unwrap();
        let (foreign_persisted, _) = persist_exact_connector_revision(&replacement).unwrap();
        let mismatch =
            finish_pin_occurrence(9, &persisted, base_ref(), observed.clone(), replacement)
                .unwrap_err();
        assert!(mismatch.contains("observation and exact revision identify different objects"));
        let changed = finish_pin_occurrence(9, &foreign_persisted, base_ref(), observed, revision)
            .unwrap_err();
        assert!(changed.contains("source object changed since D was persisted"));
        let (empty, empty_revision) = observation(b"original", None);
        let missing =
            finish_pin_occurrence(9, &persisted, base_ref(), empty, empty_revision).unwrap_err();
        assert!(missing.contains("has no current snapshot"));
        for message in [mismatch, changed, missing] {
            let error = invalid_pin(message.clone());
            assert_eq!(error.kind(), MvApplicationErrorKind::InvalidRequest);
            assert_eq!(error.message(), message);
            assert_eq!(error.original_failure(), None);
            assert_eq!(error.compile_control_error(), None);
        }
    }
}
