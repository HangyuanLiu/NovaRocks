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

use std::sync::{Arc, Mutex};

use super::MvKeyedProductService;
use crate::activity::{CanonicalMvTarget, MvActivityGateError, MvActivityOwner};
use crate::persistence::definition::CreateMvDefinitionRequest;
use crate::ports::{
    MvCreateCatalogRegistrationPort, MvCreateProviderPort, MvDropCatalogRegistrationPort,
    MvDropProjectionPort, MvDropProviderPort, MvProviderFailure, MvProviderFailureKind,
    MvRefreshExecutionPort, MvRefreshKnownCommittedPort,
};
use crate::process_runtime::ProcessRuntime;
use crate::product::{
    MvCommand, MvCreateCommand, MvCreatedTarget, MvOperationContext, MvProductErrorKind,
    MvProductResult, MvStagedTarget, MvTarget,
};
use crate::publication::{
    MvRefreshPublicationBase, MvRefreshPublicationFinalizationFacts, MvRefreshPublicationIntent,
    MvRefreshPublicationTechnique,
};
use crate::readiness::MvDropReadiness;
use crate::repository::InitialMvRefreshConfiguration;
use crate::scheduler::MvSchedulerConfig;
use crate::state_store_repository::StateStoreMvRepository;
use bytes::Bytes;
use novarocks_query_application::persisted_query_definition::{
    PersistedQueryDefinition, PersistedQueryDialect,
};
use novarocks_spi::connector::{
    ConnectorCommittedVersion, ConnectorTableObjectId, LakePublicationId,
};
use novarocks_state_store_testkit::testing::InMemoryStateStore;
use uuid::Uuid;

#[derive(Default)]
struct CreateEffects {
    events: Mutex<Vec<&'static str>>,
    fail_stage: bool,
    fail_publish: Option<MvProviderFailureKind>,
    fail_projection: bool,
    fail_known_committed_projection: bool,
    drop_absent: bool,
}

impl CreateEffects {
    fn record(&self, event: &'static str) {
        self.events.lock().expect("event lock").push(event);
    }

    fn events(&self) -> Vec<&'static str> {
        self.events.lock().expect("event lock").clone()
    }
}

impl MvCreateProviderPort for CreateEffects {
    fn stage_target(
        &self,
        operation: MvOperationContext,
        command: &MvCommand,
    ) -> Result<MvStagedTarget, MvProviderFailure> {
        self.record("stage");
        let MvCommand::Create(create) = command else {
            return Err(MvProviderFailure::new(
                MvProviderFailureKind::InvalidRequest,
                "CREATE adapter received a non-CREATE command",
            ));
        };
        if self.fail_stage {
            return Err(MvProviderFailure::new(
                MvProviderFailureKind::Unavailable,
                "stage failed",
            ));
        }
        Ok(MvStagedTarget {
            target: create.target.clone(),
            staged_operation_id: operation.operation_id,
        })
    }

    fn publish_staged_target(
        &self,
        _operation: MvOperationContext,
        staged: &MvStagedTarget,
    ) -> Result<MvCreatedTarget, MvProviderFailure> {
        self.record("publish");
        if let Some(kind) = self.fail_publish {
            return Err(MvProviderFailure::new(kind, "publish failed"));
        }
        Ok(MvCreatedTarget {
            target: staged.target.clone(),
            object_id: ConnectorTableObjectId::try_new(Bytes::from_static(b"created-object"))
                .expect("test object ID"),
        })
    }

    fn abort_staged_target(
        &self,
        _operation: MvOperationContext,
        _staged: &MvStagedTarget,
    ) -> Result<(), MvProviderFailure> {
        self.record("abort");
        Ok(())
    }

    fn install_created_projection(
        &self,
        _operation: MvOperationContext,
        _target: &MvCreatedTarget,
    ) -> Result<(), MvProviderFailure> {
        self.record("install");
        if self.fail_projection {
            return Err(MvProviderFailure::new(
                MvProviderFailureKind::Unavailable,
                "projection failed",
            ));
        }
        Ok(())
    }
}

impl MvCreateCatalogRegistrationPort for CreateEffects {
    fn register_target(
        &self,
        _operation: MvOperationContext,
        _target: &MvCreatedTarget,
    ) -> Result<(), MvProviderFailure> {
        self.record("register");
        Ok(())
    }
}

impl MvDropProviderPort for CreateEffects {
    fn drop_target(
        &self,
        _operation: MvOperationContext,
        _target: &MvTarget,
    ) -> Result<(), MvProviderFailure> {
        self.record("drop");
        Ok(())
    }
}

impl MvDropCatalogRegistrationPort for CreateEffects {
    fn unregister_target(
        &self,
        _operation: MvOperationContext,
        _target: &MvTarget,
    ) -> Result<(), MvProviderFailure> {
        self.record("unregister");
        Ok(())
    }
}

struct RefreshExecutionEffects {
    effects: Arc<CreateEffects>,
    published: MvRefreshPublicationFinalizationFacts,
}

struct KnownCommittedEffects {
    effects: Arc<CreateEffects>,
    published: MvRefreshPublicationFinalizationFacts,
}

impl MvRefreshExecutionPort for RefreshExecutionEffects {
    fn execute_refresh(
        self: Box<Self>,
        _target: &MvTarget,
        _attempt: &crate::product::MvRefreshAttemptIdentity,
    ) -> Result<Box<dyn MvRefreshKnownCommittedPort>, MvProviderFailure> {
        Ok(Box::new(KnownCommittedEffects {
            effects: self.effects,
            published: self.published,
        }))
    }
}

impl MvRefreshKnownCommittedPort for KnownCommittedEffects {
    fn finalization_facts(&self) -> &MvRefreshPublicationFinalizationFacts {
        &self.published
    }

    fn project_known_committed(
        self: Box<Self>,
        _target: &MvTarget,
    ) -> Result<(), MvProviderFailure> {
        self.effects.record("project_known_committed");
        if self.effects.fail_known_committed_projection {
            return Err(MvProviderFailure::new(
                MvProviderFailureKind::Unavailable,
                "known-committed projection failed",
            ));
        }
        Ok(())
    }
}

fn finalization_facts(
    target: &MvTarget,
    attempt: &crate::product::MvRefreshAttemptIdentity,
) -> MvRefreshPublicationFinalizationFacts {
    let target_object_id = ConnectorTableObjectId::try_new(Bytes::from_static(b"mv-target-object"))
        .expect("target object ID");
    let intent = MvRefreshPublicationIntent::try_new(
        attempt.publication_id,
        target_object_id.clone(),
        Some(7),
        crate::test_admission::publication_admission(
            target.catalog().unwrap_or("ice"),
            target.namespace(),
            target.name(),
            attempt.publication_id,
            &target_object_id,
        ),
        MvRefreshPublicationTechnique::Full,
        vec![
            MvRefreshPublicationBase::try_new(
                0,
                "ice.db.base".to_string(),
                ConnectorTableObjectId::try_new(Bytes::from_static(b"base-object"))
                    .expect("base object ID"),
                None,
                7,
            )
            .expect("base fact"),
        ],
        "definition-fingerprint".to_string(),
        target.catalog().expect("test target catalog").to_string(),
        target.namespace().to_string(),
        target.name().to_string(),
    )
    .expect("publication intent");
    MvRefreshPublicationFinalizationFacts::try_new(
        intent,
        ConnectorCommittedVersion::try_new(Bytes::from_static(b"mv-publication-version"), Some(42))
            .expect("publication version"),
    )
    .expect("published facts")
}

fn management() -> Arc<crate::management::ManagementEntrance> {
    Arc::new(crate::management::ManagementEntrance::new(
        crate::management::DeploymentOwner::parse("test-deployment").expect("owner"),
        crate::management::ProcessIncarnation::parse("test-process").expect("incarnation"),
    ))
}

async fn repository() -> Arc<StateStoreMvRepository> {
    StateStoreMvRepository::open(
        Arc::new(InMemoryStateStore::new("keyed-service-tests")),
        Default::default(),
    )
    .await
    .expect("open real StateStore MV owner")
}

async fn service_with_refresh_readiness() -> MvKeyedProductService {
    MvKeyedProductService::new_with_keyed_management_continuation(
        MvSchedulerConfig::default(),
        repository().await,
        Arc::<ProcessRuntime<MvTarget, LakePublicationId>>::default(),
        management(),
        Default::default(),
    )
}

// Product effect-order tests use the port's generic witness, never forge
// a native readiness/CAS guard. Real keyed guards are tested by readiness.
struct EffectOrderDropGuard(MvTarget);

impl MvDropProjectionPort<EffectOrderDropGuard> for CreateEffects {
    fn prepare_drop(
        &self,
        _operation: MvOperationContext,
        target: &MvTarget,
        _if_exists: bool,
    ) -> Result<MvDropReadiness<EffectOrderDropGuard>, MvProviderFailure> {
        self.record("prepare_drop");
        Ok(if self.drop_absent {
            MvDropReadiness::AlreadyAbsent
        } else {
            MvDropReadiness::ReadyToDrop(EffectOrderDropGuard(target.clone()))
        })
    }

    fn delete_after_provider_drop(
        &self,
        _operation: MvOperationContext,
        guard: EffectOrderDropGuard,
    ) -> Result<(), MvProviderFailure> {
        assert_eq!(guard.0, create_command().target);
        self.record("delete_projection");
        Ok(())
    }
}

fn create_command() -> MvCreateCommand {
    MvCreateCommand {
        target: MvTarget::from_parts(Some("iceberg"), "db", "mv"),
        if_not_exists: false,
        definition: CreateMvDefinitionRequest {
            query_definition: PersistedQueryDefinition::new(
                "SELECT 1",
                PersistedQueryDialect::StarRocks,
                "iceberg",
                "db",
            )
            .expect("valid persisted query"),
            base_table_refs: Vec::new(),
            primary_key_columns: Vec::new(),
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("iceberg".to_string()),
            target_namespace: Some("db".to_string()),
            target_table: Some("mv".to_string()),
            created_at_ms: 1,
        },
        refresh: InitialMvRefreshConfiguration::default(),
        dependencies: Vec::new(),
    }
}

fn operation() -> MvOperationContext {
    MvOperationContext {
        operation_id: Uuid::nil(),
    }
}

#[test]
fn stopping_product_service_rejects_new_background_activity() {
    let service = MvKeyedProductService::default();
    service.begin_stopping();

    assert!(matches!(
        service.request_activity(
            CanonicalMvTarget::from_parts(Some("ice"), "sales", "mv_orders"),
            MvActivityOwner::ScheduledRefresh,
        ),
        Err(MvActivityGateError::Stopping)
    ));
}

#[test]
fn product_service_retains_the_configured_scheduler_instance() {
    let service = MvKeyedProductService::new(MvSchedulerConfig::new(true, 73, 2, 11, 99));

    assert_eq!(service.scheduler_tick_interval_ms(), 73);
    assert!(service.with_refresh_scheduler(|scheduler| scheduler.enabled()));
    let target = MvTarget::from_parts(Some("ice"), "sales", "mv");
    let projection = crate::persistence::keyed_documents::StoredKeyedMvProjection {
        mv_id: 7,
        facts: crate::persistence::test_support::sample_keyed_projection(target.clone(), Some(11)),
    };
    service.with_refresh_scheduler(|scheduler| {
        scheduler.observe_projection(projection, 1_700_000_061_000);
    });
    let ready = service.with_refresh_scheduler(|scheduler| scheduler.take_ready());
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].projection().mv_id, 7);
    assert_eq!(ready[0].target(), &target);
    assert!(
        service
            .with_refresh_scheduler(|scheduler| scheduler.take_ready())
            .is_empty()
    );
}

#[tokio::test]
async fn serving_product_retains_the_exact_management_authority() {
    let management = management();
    let runtime = Arc::<ProcessRuntime<MvTarget, LakePublicationId>>::default();
    let service = MvKeyedProductService::new_with_keyed_management_continuation(
        MvSchedulerConfig::default(),
        repository().await,
        Arc::clone(&runtime),
        Arc::clone(&management),
        Default::default(),
    );

    assert!(Arc::ptr_eq(
        &service.management_entrance().expect("serving authority"),
        &management,
    ));
    let readiness = service
        .keyed_readiness_service()
        .expect("sole keyed readiness");
    assert!(service.management_continuation().is_some());
    assert!(!runtime.has_active_publications());
    let target = create_command().target;
    let attempt = service.reserve_refresh_attempt();
    let lease = service
        .begin_refresh_publication(&target, &attempt)
        .unwrap();
    assert!(runtime.has_active_publications());
    assert!(readiness.ensure_no_active_publications().is_err());
    drop(lease);
    assert!(!runtime.has_active_publications());
    readiness.ensure_no_active_publications().unwrap();
}

#[test]
fn create_runs_product_effects_in_required_order() {
    let service = MvKeyedProductService::default();
    let effects = CreateEffects::default();

    let result = service
        .create(operation(), create_command(), &effects, &effects)
        .expect("create succeeds");

    assert!(matches!(result, MvProductResult::Created(_)));
    // One atomic point: the target and its documents become visible
    // together. No visible empty table, no post-create descriptor sync.
    assert_eq!(
        effects.events(),
        ["stage", "publish", "install", "register"]
    );
}

#[tokio::test]
async fn known_committed_refresh_projection_is_product_finalization() {
    let service = service_with_refresh_readiness().await;
    let effects = Arc::new(CreateEffects::default());
    let target = MvTarget::from_parts(Some("iceberg"), "db", "mv");
    let attempt = service.reserve_refresh_attempt();
    let published = finalization_facts(&target, &attempt);

    let result = service
        .execute_refresh(
            &target,
            &attempt,
            Box::new(RefreshExecutionEffects {
                effects: Arc::clone(&effects),
                published,
            }),
        )
        .expect("projection succeeds");

    assert!(matches!(result, MvProductResult::Acknowledged));
    assert_eq!(effects.events(), ["project_known_committed"]);
}

#[tokio::test]
async fn known_committed_refresh_rejects_a_different_publication_before_projection() {
    let service = service_with_refresh_readiness().await;
    let effects = Arc::new(CreateEffects::default());
    let target = MvTarget::from_parts(Some("iceberg"), "db", "mv");
    let attempt = service.reserve_refresh_attempt();
    let other_attempt = service.reserve_refresh_attempt();
    let published = finalization_facts(&target, &other_attempt);

    let error = service
        .execute_refresh(
            &target,
            &attempt,
            Box::new(RefreshExecutionEffects {
                effects: Arc::clone(&effects),
                published,
            }),
        )
        .expect_err("different publication identity must be rejected");

    assert_eq!(error.kind(), MvProductErrorKind::Corruption);
    assert!(effects.events().is_empty());
}

#[tokio::test]
async fn product_owns_refresh_publication_admission_and_lease_release() {
    let service = service_with_refresh_readiness().await;
    let readiness = service.keyed_readiness_service().expect("keyed readiness");
    let target = MvTarget::from_parts(Some("iceberg"), "db", "mv");
    let first = service.reserve_refresh_attempt();
    let lease = service
        .begin_refresh_publication(&target, &first)
        .expect("product begins the first publication");
    let second = service.reserve_refresh_attempt();
    assert!(readiness.ensure_no_active_publications().is_err());

    let Err(error) = service.begin_refresh_publication(&target, &second) else {
        panic!("a concurrent publication must be rejected");
    };
    assert_eq!(error.kind(), MvProductErrorKind::Conflict);

    drop(lease);
    readiness.ensure_no_active_publications().unwrap();
    service
        .begin_refresh_publication(&target, &second)
        .expect("dropping the product lease releases the target");
}

#[tokio::test]
async fn known_committed_refresh_keeps_external_commit_when_projection_fails() {
    let service = service_with_refresh_readiness().await;
    let effects = Arc::new(CreateEffects {
        fail_known_committed_projection: true,
        ..Default::default()
    });
    let target = MvTarget::from_parts(Some("iceberg"), "db", "mv");
    let attempt = service.reserve_refresh_attempt();
    let published = finalization_facts(&target, &attempt);

    let error = service
        .execute_refresh(
            &target,
            &attempt,
            Box::new(RefreshExecutionEffects {
                effects: Arc::clone(&effects),
                published,
            }),
        )
        .expect_err("projection finalization fails");

    assert_eq!(
        error.kind(),
        MvProductErrorKind::KnownCommittedFinalizeFailed
    );
    assert_eq!(effects.events(), ["project_known_committed"]);
}

#[test]
fn a_failed_stage_leaves_nothing_to_compensate() {
    let service = MvKeyedProductService::default();
    let effects = CreateEffects {
        fail_stage: true,
        ..Default::default()
    };

    let error = service
        .create(operation(), create_command(), &effects, &effects)
        .expect_err("stage failure ends the statement");

    assert_eq!(error.kind(), MvProductErrorKind::Unavailable);
    assert_eq!(effects.events(), ["stage"], "nothing to abort or drop");
}

#[test]
fn a_proven_unpublished_stage_is_discarded() {
    let service = MvKeyedProductService::default();
    let effects = CreateEffects {
        fail_publish: Some(MvProviderFailureKind::KnownUncommitted),
        ..Default::default()
    };

    let error = service
        .create(operation(), create_command(), &effects, &effects)
        .expect_err("publish failed");

    assert_eq!(error.kind(), MvProductErrorKind::ProviderKnownUncommitted);
    assert_eq!(effects.events(), ["stage", "publish", "abort"]);
}

#[test]
fn an_unknown_publication_keeps_its_stage() {
    let service = MvKeyedProductService::default();
    let effects = CreateEffects {
        fail_publish: Some(MvProviderFailureKind::CommitUnknown),
        ..Default::default()
    };

    let error = service
        .create(operation(), create_command(), &effects, &effects)
        .expect_err("publish outcome is unknown");

    assert_eq!(error.kind(), MvProductErrorKind::CommitUnknown);
    assert_eq!(
        effects.events(),
        ["stage", "publish"],
        "aborting could delete a create that actually succeeded"
    );
}

#[test]
fn a_proven_committed_publication_finalization_failure_keeps_its_stage() {
    let service = MvKeyedProductService::default();
    let effects = CreateEffects {
        fail_publish: Some(MvProviderFailureKind::KnownCommittedFinalizeFailed),
        ..Default::default()
    };
    let error = service
        .create(operation(), create_command(), &effects, &effects)
        .unwrap_err();
    assert_eq!(
        error.kind(),
        MvProductErrorKind::KnownCommittedFinalizeFailed
    );
    assert_eq!(effects.events(), ["stage", "publish"]);
}

#[test]
fn create_marks_projection_failure_after_target_commit() {
    let service = MvKeyedProductService::default();
    let effects = CreateEffects {
        fail_projection: true,
        ..Default::default()
    };

    let error = service
        .create(operation(), create_command(), &effects, &effects)
        .expect_err("projection failed after target commit");

    assert_eq!(
        error.kind(),
        MvProductErrorKind::KnownCommittedFinalizeFailed
    );
    assert_eq!(
        effects.events(),
        ["stage", "publish", "install"],
        "a finalization failure must not undo the published create"
    );
}

#[test]
fn drop_orders_provider_projection_and_catalog_effects() {
    let service = MvKeyedProductService::default();
    let effects = CreateEffects::default();
    let target = MvTarget::from_parts(Some("iceberg"), "db", "mv");

    let result = service
        .drop(operation(), target, false, &effects, &effects, &effects)
        .expect("drop succeeds");

    assert!(matches!(result, MvProductResult::Dropped));
    assert_eq!(
        effects.events(),
        ["prepare_drop", "drop", "delete_projection", "unregister"]
    );
}

#[test]
fn drop_if_exists_absence_emits_no_external_effect() {
    let service = MvKeyedProductService::default();
    let effects = CreateEffects {
        drop_absent: true,
        ..Default::default()
    };
    let target = MvTarget::from_parts(Some("iceberg"), "db", "missing_mv");

    let result = service
        .drop(operation(), target, true, &effects, &effects, &effects)
        .expect("missing IF EXISTS target is acknowledged");

    assert!(matches!(result, MvProductResult::Acknowledged));
    assert_eq!(effects.events(), ["prepare_drop"]);
}
#[test]
fn foreground_and_background_activity_share_the_product_fifo() {
    let service = MvKeyedProductService::default();
    let target = create_command().target;
    let foreground = service
        .acquire_foreground(target.clone(), MvActivityOwner::ManualRefresh, || false)
        .unwrap();
    let mut scheduled = service
        .request_activity(target.clone(), MvActivityOwner::ScheduledRefresh)
        .unwrap();
    let mut maintenance = service
        .request_activity(target, MvActivityOwner::AutomaticMaintenance)
        .unwrap();
    assert!(scheduled.try_acquire().unwrap().is_none());
    assert!(maintenance.try_acquire().unwrap().is_none());
    drop(foreground);
    assert!(maintenance.try_acquire().unwrap().is_none());
    let scheduled_lease = scheduled.try_acquire().unwrap().expect("FIFO head");
    assert!(maintenance.try_acquire().unwrap().is_none());
    drop(scheduled_lease);
    maintenance
        .try_acquire()
        .unwrap()
        .expect("shared FIFO advances");
}

#[test]
fn unit_only_product_refuses_refresh_without_readiness() {
    let service = MvKeyedProductService::default();
    let attempt = service.reserve_refresh_attempt();
    let result = service.begin_refresh_publication(&create_command().target, &attempt);
    assert!(matches!(result, Err(e) if e.kind() == MvProductErrorKind::Unavailable));
}
