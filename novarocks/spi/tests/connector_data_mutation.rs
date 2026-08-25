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

#![cfg(feature = "connector-conformance")]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorCancellation, ConnectorClusterIdentity, ConnectorDataMutation,
    ConnectorDataMutationExecuteRequest, ConnectorDataMutationLease,
    ConnectorDataMutationOperation, ConnectorDataMutationPlan, ConnectorDataMutationPlanSummary,
    ConnectorDataMutationPlanningRequest, ConnectorDataMutationReceipt,
    ConnectorDataMutationReconcileRequest, ConnectorDataMutationSourceScope, ConnectorError,
    ConnectorErrorKind, ConnectorExecutionBindingKey, ConnectorExternalFenceFailure,
    ConnectorExternalFenceGeneration, ConnectorExternalFenceReceipt, ConnectorExternalFenceRequest,
    ConnectorExternalOperationFence, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorInstanceIncarnation, ConnectorListTablesRequest, ConnectorMetadata,
    ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorMutationOperationId,
    ConnectorNamespaceRequest, ConnectorProviderId, ConnectorRequestContext, ConnectorTableHandle,
    ConnectorTableIdentity, ConnectorTableMetadata, ConnectorTableRequest, ConnectorWriteFencing,
    ConnectorWriteOperationId, ConnectorWriteTargetRef, ExternalMutationEffect,
    ExternalMutationEvidence, ExternalMutationFinalization, ExternalMutationOutcome,
    MAX_CONNECTOR_DATA_MUTATION_FILES, MAX_CONNECTOR_DATA_MUTATION_PROVIDER_PAYLOAD_BYTES,
};

struct NeverCancelled;

impl ConnectorCancellation for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

fn context() -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        Arc::new(NeverCancelled),
        16 * 1024 * 1024,
        64 * 1024 * 1024,
    )
    .expect("context")
}

struct FakeMetadata {
    instance_id: ConnectorInstanceId,
}

impl ConnectorMetadata for FakeMetadata {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn namespace_exists(&self, _: ConnectorNamespaceRequest) -> Result<bool, ConnectorError> {
        unreachable!()
    }

    fn table_exists(&self, _: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        unreachable!()
    }

    fn list_tables(
        &self,
        _: ConnectorListTablesRequest,
    ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
        unreachable!()
    }

    fn load_table(
        &self,
        _: ConnectorTableRequest,
    ) -> Result<ConnectorTableMetadata, ConnectorError> {
        unreachable!()
    }
}

#[derive(Clone, Copy)]
enum ExecuteMode {
    Committed,
    Uncommitted,
    Unknown,
    ForeignReceipt,
}

struct FakeDataMutation {
    descriptor: ConnectorInstanceDescriptor,
    key: ConnectorExecutionBindingKey,
    plans: Mutex<HashMap<ConnectorMutationOperationId, ([u8; 32], ConnectorDataMutationPlan)>>,
    mode: Mutex<ExecuteMode>,
    execute_calls: Mutex<usize>,
    /// Every fence this provider was actually asked to publish. A lease that
    /// only remembered a fence locally would leave this empty.
    fence_calls: Mutex<Vec<ConnectorExternalOperationFence>>,
    publishes_fence: bool,
}

impl FakeDataMutation {
    fn new(provider: &str, instance: &str, incarnation: [u8; 16]) -> Arc<Self> {
        Self::build(provider, instance, incarnation, true)
    }

    /// A provider that installs no external fencing capability at all, which is
    /// what the trait default describes.
    fn without_fencing(provider: &str, instance: &str, incarnation: [u8; 16]) -> Arc<Self> {
        Self::build(provider, instance, incarnation, false)
    }

    fn build(
        provider: &str,
        instance: &str,
        incarnation: [u8; 16],
        publishes_fence: bool,
    ) -> Arc<Self> {
        let instance_id = ConnectorInstanceId::parse(instance).expect("instance ID");
        Arc::new(Self {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse(provider).expect("provider ID"),
                instance_id: instance_id.clone(),
            },
            key: ConnectorExecutionBindingKey {
                instance_id,
                incarnation: ConnectorInstanceIncarnation::from_bytes(incarnation),
            },
            plans: Mutex::new(HashMap::new()),
            mode: Mutex::new(ExecuteMode::Committed),
            execute_calls: Mutex::new(0),
            fence_calls: Mutex::new(Vec::new()),
            publishes_fence,
        })
    }

    fn fence_calls(&self) -> Vec<ConnectorExternalOperationFence> {
        self.fence_calls.lock().expect("fence calls").clone()
    }

    fn lease(self: &Arc<Self>) -> ConnectorDataMutationLease {
        ConnectorDataMutationLease::new(
            self.descriptor.clone(),
            self.key.clone(),
            Arc::new(FakeMetadata {
                instance_id: self.key.instance_id.clone(),
            }),
            self.clone(),
            || {},
        )
        .expect("lease")
    }

    fn receipt(
        &self,
        plan: &ConnectorDataMutationPlan,
        descriptor: ConnectorInstanceDescriptor,
    ) -> ConnectorDataMutationReceipt {
        ConnectorDataMutationReceipt::try_new(
            descriptor,
            self.key.incarnation,
            plan.operation_id(),
            plan.operation_kind(),
            plan.request_digest(),
            plan.plan_digest(),
            plan.state_digest(),
            plan.summary(),
            Bytes::from_static(b"secret-receipt"),
        )
        .expect("receipt")
    }

    fn evidence(&self, plan: &ConnectorDataMutationPlan) -> ExternalMutationEvidence {
        ExternalMutationEvidence::try_new(
            1,
            self.descriptor.clone(),
            self.key.incarnation,
            plan.operation_id(),
            plan.operation_kind(),
            Bytes::from_static(b"secret-evidence"),
        )
        .expect("evidence")
    }
}

impl ConnectorDataMutation for FakeDataMutation {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    fn establish_external_fence(
        &self,
        request: ConnectorExternalFenceRequest,
    ) -> Result<ConnectorExternalFenceReceipt, ConnectorError> {
        if !self.publishes_fence {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "this fake provider publishes no external fence marker",
            ));
        }
        request.validate(&self.key)?;
        self.fence_calls
            .lock()
            .expect("fence calls")
            .push(request.fence.clone());
        ConnectorExternalFenceReceipt::try_new(
            &request.fence,
            Bytes::from_static(b"fake-fence-marker"),
        )
    }

    fn plan_mutation(
        &self,
        request: ConnectorDataMutationPlanningRequest,
    ) -> Result<ConnectorDataMutationPlan, ConnectorError> {
        let mut plans = self.plans.lock().expect("plans");
        if let Some((digest, plan)) = plans.get(&request.operation_id()) {
            if digest == &request.request_digest() {
                return Ok(plan.clone());
            }
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "operation request digest conflict",
            ));
        }
        let plan = ConnectorDataMutationPlan::try_new(
            &request,
            [7; 32],
            ConnectorDataMutationPlanSummary::try_new(2, 11, 101).expect("summary"),
            Some(source_scope()),
            Some(
                novarocks_spi::connector::ConnectorDataMutationAddFilesDomain::try_new_caller_managed_stable([9; 32])
                    .expect("domain"),
            ),
            Bytes::from_static(b"secret-plan"),
        )?;
        plans.insert(
            request.operation_id(),
            (request.request_digest(), plan.clone()),
        );
        Ok(plan)
    }

    fn execute(
        &self,
        request: ConnectorDataMutationExecuteRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        *self.execute_calls.lock().expect("calls") += 1;
        let failure = ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Unavailable,
            "scripted external result",
        );
        Ok(match *self.mode.lock().expect("mode") {
            ExecuteMode::Committed => ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                receipt: self.receipt(&request.plan, self.descriptor.clone()),
                finalization: ExternalMutationFinalization::Complete,
            },
            ExecuteMode::Uncommitted => ExternalMutationOutcome::KnownUncommitted { failure },
            ExecuteMode::Unknown => ExternalMutationOutcome::CommitUnknown {
                failure,
                evidence: self.evidence(&request.plan),
            },
            ExecuteMode::ForeignReceipt => ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                receipt: self.receipt(
                    &request.plan,
                    ConnectorInstanceDescriptor {
                        provider_id: self.descriptor.provider_id.clone(),
                        instance_id: ConnectorInstanceId::parse("foreign").expect("foreign"),
                    },
                ),
                finalization: ExternalMutationFinalization::Complete,
            },
        })
    }

    fn reconcile(
        &self,
        request: ConnectorDataMutationReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        let plan = self
            .plans
            .lock()
            .expect("plans")
            .get(&request.operation_id)
            .expect("planned operation")
            .1
            .clone();
        Ok(ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt: self.receipt(&plan, self.descriptor.clone()),
            finalization: ExternalMutationFinalization::Complete,
        })
    }
}

fn planning_request(
    fake: &FakeDataMutation,
    operation_id: ConnectorMutationOperationId,
    source: &str,
) -> ConnectorDataMutationPlanningRequest {
    let table = ConnectorTableHandle::try_new(
        fake.key.instance_id.clone(),
        Bytes::from_static(b"secret-table-handle"),
    )
    .expect("table handle");
    ConnectorDataMutationPlanningRequest::try_new(
        operation_id,
        fake.key.clone(),
        ConnectorDataMutationOperation::register_existing_files(table, source).expect("operation"),
        context(),
    )
    .expect("planning request")
}

fn source_scope() -> ConnectorDataMutationSourceScope {
    ConnectorDataMutationSourceScope::try_new_directory([7; 32]).expect("source scope")
}

fn fence_value(
    fake: &FakeDataMutation,
    operation_id: ConnectorMutationOperationId,
    coordination_attempt: u64,
) -> ConnectorExternalOperationFence {
    ConnectorExternalOperationFence::try_new(
        ConnectorClusterIdentity::derive("data-mutation-fence-test-cluster")
            .expect("cluster identity"),
        ConnectorExternalFenceGeneration::try_new(1, 1, coordination_attempt)
            .expect("fence generation"),
        ConnectorWriteOperationId::from_bytes(operation_id.to_bytes()),
        [4; 16],
        ConnectorTableIdentity {
            instance_id: fake.key.instance_id.clone(),
            namespace: Arc::from("db"),
            table: Arc::from("orders"),
        },
        ConnectorWriteTargetRef::main(),
    )
    .expect("external operation fence")
}

/// Establishing a direct-mutation fence must reach the provider, because the
/// provider is the only party that can publish the marker its own execute later
/// asserts. A lease that recorded the fence locally would report `Fenced` while
/// external truth carried nothing.
#[test]
fn a_direct_mutation_fence_is_published_by_the_provider_before_the_lease_reports_fenced() {
    let fake = FakeDataMutation::new("fake-alpha", "alpha", [1; 16]);
    let lease = fake.lease();
    let operation_id = ConnectorMutationOperationId::from_bytes([9; 16]);
    assert!(
        matches!(
            lease.fencing().expect("fencing"),
            ConnectorWriteFencing::NotFencedByThisPhase { .. }
        ),
        "an operation that never established a fence must say so"
    );

    let fence = fence_value(&fake, operation_id, 2);
    let receipt = lease
        .establish_external_fence(fence.clone(), context())
        .expect("establish the direct mutation fence");
    assert!(receipt.matches(&fence));
    assert_eq!(
        fake.fence_calls(),
        vec![fence.clone()],
        "the lease must ask the provider to publish this exact marker"
    );
    assert_eq!(
        lease.fencing().expect("fencing"),
        ConnectorWriteFencing::Fenced(fence.clone())
    );

    // Monotonicity is preserved and is checked before the provider is
    // contacted: a generation behind the established fence never reaches it.
    let stale = fence_value(&fake, operation_id, 1);
    let error = lease
        .establish_external_fence(stale, context())
        .expect_err("a stale generation must be refused");
    assert_eq!(
        error.external_fence_failure(),
        Some(ConnectorExternalFenceFailure::Stale)
    );
    assert_eq!(fake.fence_calls().len(), 1);

    // A strictly higher generation supersedes, and is published in turn.
    let raised = fence_value(&fake, operation_id, 3);
    lease
        .establish_external_fence(raised.clone(), context())
        .expect("a strictly higher generation must supersede");
    assert_eq!(fake.fence_calls(), vec![fence, raised.clone()]);
    assert_eq!(
        lease.fencing().expect("fencing"),
        ConnectorWriteFencing::Fenced(raised)
    );
}

/// A provider that publishes no marker must leave the operation unfenced.
///
/// This is the failure the local-cell implementation hid: the session reported
/// `Fenced` on a fence no provider had ever seen, and the fenced execute then
/// failed closed at the catalog with nothing to assert.
#[test]
fn a_provider_that_publishes_no_marker_leaves_the_direct_mutation_unfenced() {
    let fake = FakeDataMutation::without_fencing("fake-beta", "beta", [2; 16]);
    let lease = fake.lease();
    let fence = fence_value(&fake, ConnectorMutationOperationId::from_bytes([10; 16]), 1);
    let error = lease
        .establish_external_fence(fence, context())
        .expect_err("a provider without fencing must refuse");
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    assert!(
        matches!(
            lease.fencing().expect("fencing"),
            ConnectorWriteFencing::NotFencedByThisPhase { .. }
        ),
        "a fence the provider never published must not make the lease look fenced"
    );
}

#[test]
fn two_non_iceberg_providers_obey_replay_conflict_and_redaction_contracts() {
    for fake in [
        FakeDataMutation::new("fake-alpha", "alpha", [1; 16]),
        FakeDataMutation::new("fake-beta", "beta", [2; 16]),
    ] {
        let lease = fake.lease();
        let operation_id = ConnectorMutationOperationId::from_bytes([3; 16]);
        let request = planning_request(&fake, operation_id, "s3://bucket/source");
        let plan = lease.plan_mutation(request.clone()).expect("first plan");
        assert_eq!(
            lease.plan_mutation(request).expect("idempotent replay"),
            plan
        );
        let conflict = planning_request(&fake, operation_id, "s3://bucket/changed");
        assert_eq!(
            lease
                .plan_mutation(conflict)
                .expect_err("same operation with another request must fail")
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );

        let debug = format!("{plan:?}");
        assert!(debug.contains("provider_payload_len"));
        assert!(!debug.contains("secret-plan"));
        assert!(!debug.contains("secret-table-handle"));
    }
}

#[test]
fn lease_validates_all_execute_and_reconcile_outcomes() {
    let fake = FakeDataMutation::new("fake-alpha", "alpha", [4; 16]);
    let lease = fake.lease();

    for mode in [ExecuteMode::Committed, ExecuteMode::Uncommitted] {
        *fake.mode.lock().expect("mode") = mode;
        let request = planning_request(&fake, ConnectorMutationOperationId::new(), "s3://x");
        let plan = lease.plan_mutation(request).expect("plan");
        lease
            .execute(
                ConnectorDataMutationExecuteRequest::try_new(
                    plan,
                    lease.fencing().expect("fencing decision"),
                    context(),
                )
                .expect("execute"),
            )
            .expect("valid outcome");
    }

    *fake.mode.lock().expect("mode") = ExecuteMode::Unknown;
    let request = planning_request(&fake, ConnectorMutationOperationId::new(), "s3://unknown");
    let plan = lease.plan_mutation(request).expect("plan");
    let outcome = lease
        .execute(
            ConnectorDataMutationExecuteRequest::try_new(
                plan.clone(),
                lease.fencing().expect("fencing decision"),
                context(),
            )
            .expect("execute"),
        )
        .expect("unknown outcome");
    let ExternalMutationOutcome::CommitUnknown { evidence, .. } = outcome else {
        panic!("expected unknown outcome")
    };
    let reconciled = lease
        .reconcile(
            ConnectorDataMutationReconcileRequest::try_new(&plan, evidence, context())
                .expect("reconcile request"),
        )
        .expect("reconcile");
    assert!(matches!(
        reconciled,
        ExternalMutationOutcome::KnownCommitted { .. }
    ));

    *fake.mode.lock().expect("mode") = ExecuteMode::ForeignReceipt;
    let request = planning_request(&fake, ConnectorMutationOperationId::new(), "s3://foreign");
    let plan = lease.plan_mutation(request).expect("plan");
    assert_eq!(
        lease
            .execute(
                ConnectorDataMutationExecuteRequest::try_new(
                    plan,
                    lease.fencing().expect("fencing decision"),
                    context()
                )
                .expect("execute")
            )
            .expect_err("foreign receipt must be rejected")
            .kind(),
        ConnectorErrorKind::InvalidRequest
    );
}

#[test]
fn bounds_and_exact_generation_fail_closed() {
    assert_eq!(
        ConnectorDataMutationPlanSummary::try_new(MAX_CONNECTOR_DATA_MUTATION_FILES + 1, 0, 0,)
            .expect_err("file bound")
            .kind(),
        ConnectorErrorKind::ResourceExhausted
    );

    let fake = FakeDataMutation::new("fake-beta", "beta", [8; 16]);
    let request = planning_request(&fake, ConnectorMutationOperationId::new(), "s3://bounded");
    assert_eq!(
        ConnectorDataMutationPlan::try_new(
            &request,
            [0; 32],
            ConnectorDataMutationPlanSummary::default(),
            Some(source_scope()),
            Some(
                novarocks_spi::connector::ConnectorDataMutationAddFilesDomain::try_new_caller_managed_stable([9; 32])
                    .expect("domain"),
            ),
            Bytes::from(vec![
                0;
                MAX_CONNECTOR_DATA_MUTATION_PROVIDER_PAYLOAD_BYTES + 1
            ]),
        )
        .expect_err("payload bound")
        .kind(),
        ConnectorErrorKind::ResourceExhausted
    );

    let wrong_key = ConnectorExecutionBindingKey {
        instance_id: fake.key.instance_id.clone(),
        incarnation: ConnectorInstanceIncarnation::from_bytes([9; 16]),
    };
    assert_eq!(
        ConnectorDataMutationLease::new(
            fake.descriptor.clone(),
            wrong_key,
            Arc::new(FakeMetadata {
                instance_id: fake.key.instance_id.clone(),
            }),
            fake,
            || {},
        )
        .err()
        .expect("wrong generation")
        .kind(),
        ConnectorErrorKind::InvalidRequest
    );
}
