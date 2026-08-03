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

use bytes::Bytes;
use novarocks_frontend::connector::{
    ConnectorControlHost, ConnectorControlRetirement, ConnectorControlRetirementSink,
};
use novarocks_spi::connector::{
    ConnectorBeginScanRequest, ConnectorCatalogMutation, ConnectorCatalogMutationReceipt,
    ConnectorCatalogMutationReconcileRequest, ConnectorCatalogMutationRequest,
    ConnectorCatalogMutationResolver, ConnectorCleanupCandidatePageRequest,
    ConnectorCleanupExecuteRequest, ConnectorCleanupFinalizeRequest, ConnectorCleanupMaintenance,
    ConnectorCleanupMaintenanceResolver, ConnectorCleanupPlan, ConnectorCleanupPlanningRequest,
    ConnectorCleanupPrepareRequest, ConnectorCleanupReconcileRequest, ConnectorControlBinding,
    ConnectorControlResolver, ConnectorDataMutation, ConnectorDataMutationExecuteRequest,
    ConnectorDataMutationPlan, ConnectorDataMutationPlanningRequest, ConnectorDataMutationReceipt,
    ConnectorDataMutationReconcileRequest, ConnectorDataMutationResolver,
    ConnectorDistributedRewrite, ConnectorDistributedRewriteAttemptCheckpoint,
    ConnectorDistributedRewriteAttemptDisposition, ConnectorDistributedRewritePlan,
    ConnectorDistributedRewritePlanningRequest, ConnectorDistributedRewriteReceipt,
    ConnectorDistributedRewriteResolver, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionBindingKey, ConnectorExecutionDeclaration, ConnectorExecutionDistribution,
    ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorInstanceIncarnation,
    ConnectorListTablesRequest, ConnectorMetadata, ConnectorMetadataMaintenance,
    ConnectorMetadataMaintenanceExecuteRequest, ConnectorMetadataMaintenancePlan,
    ConnectorMetadataMaintenancePlanningRequest, ConnectorMetadataMaintenanceReceipt,
    ConnectorMetadataMaintenanceReconcileRequest, ConnectorMetadataMaintenanceResolver,
    ConnectorNamespaceRequest, ConnectorProviderId, ConnectorScan, ConnectorScanHandle,
    ConnectorScanPlanning, ConnectorSplitPlanningRequest, ConnectorStatistics,
    ConnectorStatisticsResolver, ConnectorTableHandle, ConnectorTableMetadata,
    ConnectorTableRequest, ConnectorWriteAbortOutcome, ConnectorWriteAbortRequest,
    ConnectorWriteAttemptCompletion, ConnectorWriteCommitRequest, ConnectorWriteControl,
    ConnectorWritePlan, ConnectorWritePlanningRequest, ConnectorWriteReceipt,
    ConnectorWriteReconcileRequest, ExternalMutationOutcome, StatisticsAccuracy,
    StatisticsCoverage, StatisticsDataVersion, StatisticsEvidence, StatisticsEvidenceRevision,
    StatisticsProvenance, StatisticsReadRequest, StatisticsReader,
};

struct TestControl {
    instance_id: ConnectorInstanceId,
    incarnation: ConnectorInstanceIncarnation,
}

impl ConnectorMetadata for TestControl {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn namespace_exists(
        &self,
        _request: ConnectorNamespaceRequest,
    ) -> Result<bool, ConnectorError> {
        Err(unsupported())
    }

    fn table_exists(&self, _request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        Err(unsupported())
    }

    fn list_tables(
        &self,
        _request: ConnectorListTablesRequest,
    ) -> Result<Vec<novarocks_spi::connector::ConnectorTableIdentity>, ConnectorError> {
        Err(unsupported())
    }

    fn load_table(
        &self,
        _request: ConnectorTableRequest,
    ) -> Result<ConnectorTableMetadata, ConnectorError> {
        Err(unsupported())
    }
}

impl ConnectorScanPlanning for TestControl {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn begin_scan(
        &self,
        _table: &ConnectorTableHandle,
        _request: ConnectorBeginScanRequest,
    ) -> Result<ConnectorScan, ConnectorError> {
        Err(unsupported())
    }

    fn plan_splits(
        &self,
        _scan: &ConnectorScanHandle,
        _request: ConnectorSplitPlanningRequest,
    ) -> Result<novarocks_spi::connector::ConnectorSplitPlanningResult, ConnectorError> {
        Err(unsupported())
    }
}

impl ConnectorExecutionDistribution for TestControl {
    fn declaration(
        &self,
        _context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorExecutionDeclaration, ConnectorError> {
        ConnectorExecutionDeclaration::try_new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
                instance_id: self.instance_id.clone(),
            },
            self.incarnation,
            Bytes::from_static(b"binding=default"),
        )
    }
}

fn binding(incarnation: u8) -> ConnectorControlBinding {
    let provider = Arc::new(TestControl {
        instance_id: ConnectorInstanceId::parse("catalog.analytics").expect("instance ID"),
        incarnation: ConnectorInstanceIncarnation::from_bytes([incarnation; 16]),
    });
    ConnectorControlBinding::try_new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
            instance_id: provider.instance_id.clone(),
        },
        provider.incarnation,
        provider.clone(),
        provider.clone(),
        provider,
        None,
    )
    .expect("control binding")
}

struct TestMutation {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
}

impl ConnectorCatalogMutation for TestMutation {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn incarnation(&self) -> ConnectorInstanceIncarnation {
        self.incarnation
    }

    fn execute(
        &self,
        _request: ConnectorCatalogMutationRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
        Err(unsupported())
    }

    fn reconcile(
        &self,
        _request: ConnectorCatalogMutationReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
        Err(unsupported())
    }
}

fn binding_with_mutation(incarnation: u8) -> ConnectorControlBinding {
    let binding = binding(incarnation);
    let descriptor = binding.descriptor().clone();
    let incarnation = binding.incarnation();
    let provider = Arc::new(TestControl {
        instance_id: descriptor.instance_id.clone(),
        incarnation,
    });
    ConnectorControlBinding::try_new(
        descriptor.clone(),
        incarnation,
        provider.clone(),
        provider.clone(),
        provider,
        Some(Arc::new(TestMutation {
            descriptor,
            incarnation,
        })),
    )
    .expect("control binding with mutation")
}

struct TestDataMutation {
    descriptor: ConnectorInstanceDescriptor,
    key: ConnectorExecutionBindingKey,
}

impl ConnectorDataMutation for TestDataMutation {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    fn plan_mutation(
        &self,
        _request: ConnectorDataMutationPlanningRequest,
    ) -> Result<ConnectorDataMutationPlan, ConnectorError> {
        Err(unsupported())
    }

    fn execute(
        &self,
        _request: ConnectorDataMutationExecuteRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        Err(unsupported())
    }

    fn reconcile(
        &self,
        _request: ConnectorDataMutationReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        Err(unsupported())
    }
}

fn binding_with_data_mutation(incarnation_byte: u8) -> ConnectorControlBinding {
    let binding = binding(incarnation_byte);
    let descriptor = binding.descriptor().clone();
    let incarnation = binding.incarnation();
    let key = ConnectorExecutionBindingKey {
        instance_id: descriptor.instance_id.clone(),
        incarnation,
    };
    let provider = Arc::new(TestControl {
        instance_id: descriptor.instance_id.clone(),
        incarnation,
    });
    ConnectorControlBinding::try_new_with_data_mutation(
        descriptor.clone(),
        incarnation,
        provider.clone(),
        provider.clone(),
        provider,
        None,
        Some(Arc::new(TestDataMutation { descriptor, key })),
    )
    .expect("control binding with data mutation")
}

struct TestCleanupMaintenance {
    descriptor: ConnectorInstanceDescriptor,
    key: ConnectorExecutionBindingKey,
}

impl ConnectorCleanupMaintenance for TestCleanupMaintenance {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    fn plan_cleanup(
        &self,
        _request: ConnectorCleanupPlanningRequest,
    ) -> Result<ConnectorCleanupPlan, ConnectorError> {
        Err(unsupported())
    }

    fn prepare_batch(
        &self,
        _request: ConnectorCleanupPrepareRequest,
    ) -> Result<novarocks_spi::connector::PreparedBatch, ConnectorError> {
        Err(unsupported())
    }

    fn execute_batch(
        &self,
        _request: ConnectorCleanupExecuteRequest,
    ) -> Result<novarocks_spi::connector::BatchReceipt, ConnectorError> {
        Err(unsupported())
    }

    fn reconcile_batch(
        &self,
        _request: ConnectorCleanupReconcileRequest,
    ) -> Result<novarocks_spi::connector::BatchReceipt, ConnectorError> {
        Err(unsupported())
    }

    fn read_candidate_page(
        &self,
        _request: ConnectorCleanupCandidatePageRequest,
    ) -> Result<novarocks_spi::connector::CandidatePage, ConnectorError> {
        Err(unsupported())
    }

    fn finalize_terminal(
        &self,
        _request: ConnectorCleanupFinalizeRequest,
    ) -> Result<(), ConnectorError> {
        Err(unsupported())
    }
}

fn binding_with_cleanup_maintenance(incarnation_byte: u8) -> ConnectorControlBinding {
    let binding = binding(incarnation_byte);
    let descriptor = binding.descriptor().clone();
    let incarnation = binding.incarnation();
    let key = ConnectorExecutionBindingKey {
        instance_id: descriptor.instance_id.clone(),
        incarnation,
    };
    let provider = Arc::new(TestControl {
        instance_id: descriptor.instance_id.clone(),
        incarnation,
    });
    ConnectorControlBinding::try_new_with_all_maintenance_capabilities_cleanup_and_staged_create(
        descriptor.clone(),
        incarnation,
        provider.clone(),
        provider.clone(),
        provider,
        None,
        None,
        None,
        None,
        Some(Arc::new(TestCleanupMaintenance { descriptor, key })),
        None,
        None,
        None,
    )
    .expect("control binding with cleanup maintenance")
}

struct TestMetadataMaintenance {
    descriptor: ConnectorInstanceDescriptor,
    key: ConnectorExecutionBindingKey,
}

impl ConnectorMetadataMaintenance for TestMetadataMaintenance {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    fn plan_maintenance(
        &self,
        _request: ConnectorMetadataMaintenancePlanningRequest,
    ) -> Result<ConnectorMetadataMaintenancePlan, ConnectorError> {
        Err(unsupported())
    }

    fn execute(
        &self,
        _request: ConnectorMetadataMaintenanceExecuteRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>, ConnectorError> {
        Err(unsupported())
    }

    fn reconcile(
        &self,
        _request: ConnectorMetadataMaintenanceReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorMetadataMaintenanceReceipt>, ConnectorError> {
        Err(unsupported())
    }
}

fn binding_with_metadata_maintenance(incarnation_byte: u8) -> ConnectorControlBinding {
    let binding = binding(incarnation_byte);
    let descriptor = binding.descriptor().clone();
    let incarnation = binding.incarnation();
    let key = ConnectorExecutionBindingKey {
        instance_id: descriptor.instance_id.clone(),
        incarnation,
    };
    let provider = Arc::new(TestControl {
        instance_id: descriptor.instance_id.clone(),
        incarnation,
    });
    ConnectorControlBinding::try_new_with_all_capabilities_and_metadata_maintenance(
        descriptor.clone(),
        incarnation,
        provider.clone(),
        provider.clone(),
        provider,
        None,
        None,
        Some(Arc::new(TestMetadataMaintenance { descriptor, key })),
        None,
        None,
    )
    .expect("control binding with metadata maintenance")
}

struct TestDistributedRewrite {
    descriptor: ConnectorInstanceDescriptor,
    key: ConnectorExecutionBindingKey,
}

impl ConnectorDistributedRewrite for TestDistributedRewrite {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    fn plan_rewrite(
        &self,
        _request: ConnectorDistributedRewritePlanningRequest,
    ) -> Result<ConnectorDistributedRewritePlan, ConnectorError> {
        Err(unsupported())
    }

    fn activate_rewrite(
        &self,
        _plan: &ConnectorDistributedRewritePlan,
    ) -> Result<(), ConnectorError> {
        Err(unsupported())
    }

    fn checkpoint_attempt(
        &self,
        _plan: &ConnectorDistributedRewritePlan,
        _disposition: ConnectorDistributedRewriteAttemptDisposition,
        _completion: &ConnectorWriteAttemptCompletion,
    ) -> Result<ConnectorDistributedRewriteAttemptCheckpoint, ConnectorError> {
        Err(unsupported())
    }

    fn restore_attempt(
        &self,
        _plan: &ConnectorDistributedRewritePlan,
        _checkpoint: &ConnectorDistributedRewriteAttemptCheckpoint,
    ) -> Result<ConnectorWriteAttemptCompletion, ConnectorError> {
        Err(unsupported())
    }

    fn finalize_rewrite(
        &self,
        _plan: &ConnectorDistributedRewritePlan,
        _receipt: &ConnectorWriteReceipt,
    ) -> Result<ConnectorDistributedRewriteReceipt, ConnectorError> {
        Err(unsupported())
    }
}

struct TestWrite {
    key: ConnectorExecutionBindingKey,
}

impl ConnectorWriteControl for TestWrite {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    fn plan_write(
        &self,
        _request: ConnectorWritePlanningRequest,
    ) -> Result<ConnectorWritePlan, ConnectorError> {
        Err(unsupported())
    }

    fn commit(
        &self,
        _request: ConnectorWriteCommitRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        Err(unsupported())
    }

    fn abort(
        &self,
        _request: ConnectorWriteAbortRequest,
    ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
        Err(unsupported())
    }

    fn reconcile(
        &self,
        _request: ConnectorWriteReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        Err(unsupported())
    }
}

fn binding_with_distributed_rewrite(
    incarnation_byte: u8,
    include_write: bool,
) -> ConnectorControlBinding {
    let binding = binding(incarnation_byte);
    let descriptor = binding.descriptor().clone();
    let incarnation = binding.incarnation();
    let key = ConnectorExecutionBindingKey {
        instance_id: descriptor.instance_id.clone(),
        incarnation,
    };
    let provider = Arc::new(TestControl {
        instance_id: descriptor.instance_id.clone(),
        incarnation,
    });
    let rewrite: Arc<dyn ConnectorDistributedRewrite> = Arc::new(TestDistributedRewrite {
        descriptor: descriptor.clone(),
        key: key.clone(),
    });
    let write = include_write
        .then(|| Arc::new(TestWrite { key: key.clone() }) as Arc<dyn ConnectorWriteControl>);
    ConnectorControlBinding::try_new_with_all_maintenance_capabilities(
        descriptor,
        incarnation,
        provider.clone(),
        provider.clone(),
        provider,
        None,
        None,
        None,
        Some(rewrite),
        write,
        None,
    )
    .expect("control binding with distributed rewrite")
}

struct TestStatistics {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
}

impl StatisticsReader for TestStatistics {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn incarnation(&self) -> ConnectorInstanceIncarnation {
        self.incarnation
    }

    fn read_statistics(
        &self,
        _request: StatisticsReadRequest,
    ) -> Result<StatisticsEvidence, ConnectorError> {
        Ok(StatisticsEvidence {
            data_version: StatisticsDataVersion::try_new(Bytes::from_static(b"data-v1"))?,
            evidence_revision: StatisticsEvidenceRevision::try_new(Bytes::from_static(
                b"evidence-v1",
            ))?,
            coverage: StatisticsCoverage::Full,
            accuracy: StatisticsAccuracy::Exact,
            interval: None,
            provenance: StatisticsProvenance::ProviderArtifact,
            metrics: Default::default(),
        })
    }
}

impl ConnectorStatistics for TestStatistics {}

fn binding_with_statistics(incarnation: u8) -> ConnectorControlBinding {
    let binding = binding(incarnation);
    let descriptor = binding.descriptor().clone();
    let incarnation = binding.incarnation();
    let provider = Arc::new(TestControl {
        instance_id: descriptor.instance_id.clone(),
        incarnation,
    });
    ConnectorControlBinding::try_new_with_statistics(
        descriptor.clone(),
        incarnation,
        provider.clone(),
        provider.clone(),
        provider,
        None,
        Some(Arc::new(TestStatistics {
            descriptor,
            incarnation,
        })),
    )
    .expect("control binding with statistics")
}

#[derive(Default)]
struct RecordingRetirementSink(Mutex<Vec<ConnectorControlRetirement>>);

impl ConnectorControlRetirementSink for RecordingRetirementSink {
    fn retire(&self, retirement: ConnectorControlRetirement) {
        self.0.lock().expect("retirement sink").push(retirement);
    }
}

#[test]
fn lease_drain_dispatches_retirement_to_installed_backends() {
    let host = ConnectorControlHost::new();
    let sink = Arc::new(RecordingRetirementSink::default());
    host.set_retirement_sink(sink.clone());
    let instance_id = ConnectorInstanceId::parse("catalog.analytics").expect("instance ID");
    host.register(binding(7)).expect("register old generation");
    let lease = host.acquire_current(&instance_id).expect("planning lease");
    let old_key = ConnectorExecutionBindingKey {
        instance_id: instance_id.clone(),
        incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
    };
    host.record_installed_backend(&old_key, "127.0.0.1:18080")
        .expect("record ensure acknowledgement");
    host.retire_current(&instance_id)
        .expect("retire old generation");
    assert!(sink.0.lock().expect("retirement sink").is_empty());

    drop(lease);

    let dispatched = sink.0.lock().expect("retirement sink");
    assert_eq!(dispatched.len(), 1);
    assert_eq!(dispatched[0].key, old_key);
    assert_eq!(
        dispatched[0].installed_backends,
        vec![String::from("127.0.0.1:18080")]
    );
    assert!(host.take_ready_retires().expect("retire queue").is_empty());
}

#[test]
fn mutation_lease_fences_retirement_and_missing_capability_is_unsupported() {
    let host = ConnectorControlHost::new();
    let instance_id = ConnectorInstanceId::parse("catalog.analytics").expect("instance ID");
    host.register(binding(7))
        .expect("register no-mutation generation");
    let error = match host.acquire_current_mutation(&instance_id) {
        Ok(_) => panic!("missing capability must fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    host.retire_current(&instance_id)
        .expect("retire no-mutation generation");
    assert_eq!(host.take_ready_retires().expect("retire queue").len(), 1);
    host.register(binding_with_mutation(8))
        .expect("register replacement generation");
    let planning = host.acquire_current(&instance_id).expect("planning lease");
    let mutation = host
        .acquire_current_mutation(&instance_id)
        .expect("mutation lease");
    host.retire_current(&instance_id)
        .expect("retire generation");
    assert!(host.take_ready_retires().expect("retire queue").is_empty());
    drop(planning);
    assert!(host.take_ready_retires().expect("retire queue").is_empty());
    drop(mutation);
    assert_eq!(host.take_ready_retires().expect("retire queue").len(), 1);
}

#[test]
fn data_mutation_lease_requires_the_capability_and_fences_retirement() {
    let host = ConnectorControlHost::new();
    let instance_id = ConnectorInstanceId::parse("catalog.analytics").expect("instance ID");
    host.register(binding(7))
        .expect("register no-data-mutation generation");
    let error = match host.acquire_current_data_mutation(&instance_id) {
        Ok(_) => panic!("missing capability must fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    host.retire_current(&instance_id)
        .expect("retire no-data-mutation generation");
    assert_eq!(host.take_ready_retires().expect("retire queue").len(), 1);

    host.register(binding_with_data_mutation(8))
        .expect("register data-mutation generation");
    let lease = host
        .acquire_current_data_mutation(&instance_id)
        .expect("data-mutation lease");
    assert_eq!(lease.binding_key().incarnation.to_bytes(), [8; 16]);
    assert_eq!(lease.metadata().instance_id(), &instance_id);
    host.retire_current(&instance_id)
        .expect("retire data-mutation generation");
    assert!(host.take_ready_retires().expect("retire queue").is_empty());
    drop(lease);
    assert_eq!(host.take_ready_retires().expect("retire queue").len(), 1);
}

#[test]
fn exact_data_mutation_lease_never_uses_a_replacement_incarnation() {
    let host = ConnectorControlHost::new();
    let instance_id = ConnectorInstanceId::parse("catalog.analytics").expect("instance ID");
    let old_key = ConnectorExecutionBindingKey {
        instance_id: instance_id.clone(),
        incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
    };
    host.register(binding_with_data_mutation(7))
        .expect("register old generation");
    let planning = host.acquire_current(&instance_id).expect("planning lease");
    host.retire_current(&instance_id)
        .expect("retire old generation");

    let error = match host.acquire_current_data_mutation(&instance_id) {
        Ok(_) => panic!("retiring generation must not accept current acquisition"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ConnectorErrorKind::NotFound);

    host.register(binding_with_data_mutation(8))
        .expect("register replacement generation");
    let replacement = host
        .acquire_current_data_mutation(&instance_id)
        .expect("replacement data-mutation lease");
    assert_eq!(replacement.binding_key().incarnation.to_bytes(), [8; 16]);

    let exact_old = host
        .acquire_exact_data_mutation(&old_key)
        .expect("exact retiring generation lease");
    assert_eq!(exact_old.binding_key(), &old_key);

    let unknown_key = ConnectorExecutionBindingKey {
        instance_id: instance_id.clone(),
        incarnation: ConnectorInstanceIncarnation::from_bytes([9; 16]),
    };
    let error = match host.acquire_exact_data_mutation(&unknown_key) {
        Ok(_) => panic!("unknown incarnation must not use the replacement"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ConnectorErrorKind::NotFound);

    drop(planning);
    assert!(host.take_ready_retires().expect("retire queue").is_empty());
    drop(exact_old);
    let ready = host.take_ready_retires().expect("retire queue");
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].key, old_key);

    let error = match host.acquire_exact_data_mutation(&old_key) {
        Ok(_) => panic!("retired generation must not be recreated"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ConnectorErrorKind::NotFound);
    drop(replacement);
}

#[test]
fn metadata_maintenance_lease_requires_the_capability_and_fences_retirement() {
    let host = ConnectorControlHost::new();
    let instance_id = ConnectorInstanceId::parse("catalog.analytics").expect("instance ID");
    host.register(binding(7))
        .expect("register no-metadata-maintenance generation");
    let error = match host.acquire_current_metadata_maintenance(&instance_id) {
        Ok(_) => panic!("missing capability must fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    host.retire_current(&instance_id)
        .expect("retire no-metadata-maintenance generation");
    assert_eq!(host.take_ready_retires().expect("retire queue").len(), 1);

    host.register(binding_with_metadata_maintenance(8))
        .expect("register metadata-maintenance generation");
    let lease = host
        .acquire_current_metadata_maintenance(&instance_id)
        .expect("metadata-maintenance lease");
    assert_eq!(lease.binding_key().incarnation.to_bytes(), [8; 16]);
    assert_eq!(lease.metadata().instance_id(), &instance_id);
    host.retire_current(&instance_id)
        .expect("retire metadata-maintenance generation");
    assert!(host.take_ready_retires().expect("retire queue").is_empty());
    drop(lease);
    assert_eq!(host.take_ready_retires().expect("retire queue").len(), 1);
}

#[test]
fn exact_metadata_maintenance_lease_never_uses_a_replacement_incarnation() {
    let host = ConnectorControlHost::new();
    let instance_id = ConnectorInstanceId::parse("catalog.analytics").expect("instance ID");
    let old_key = ConnectorExecutionBindingKey {
        instance_id: instance_id.clone(),
        incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
    };
    host.register(binding_with_metadata_maintenance(7))
        .expect("register old generation");
    let planning = host.acquire_current(&instance_id).expect("planning lease");
    host.retire_current(&instance_id)
        .expect("retire old generation");

    let error = match host.acquire_current_metadata_maintenance(&instance_id) {
        Ok(_) => panic!("retiring generation must not accept current acquisition"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ConnectorErrorKind::NotFound);

    host.register(binding_with_metadata_maintenance(8))
        .expect("register replacement generation");
    let replacement = host
        .acquire_current_metadata_maintenance(&instance_id)
        .expect("replacement metadata-maintenance lease");
    assert_eq!(replacement.binding_key().incarnation.to_bytes(), [8; 16]);

    let exact_old = host
        .acquire_exact_metadata_maintenance(&old_key)
        .expect("exact retiring generation lease");
    assert_eq!(exact_old.binding_key(), &old_key);

    let unknown_key = ConnectorExecutionBindingKey {
        instance_id: instance_id.clone(),
        incarnation: ConnectorInstanceIncarnation::from_bytes([9; 16]),
    };
    let error = match host.acquire_exact_metadata_maintenance(&unknown_key) {
        Ok(_) => panic!("unknown incarnation must not use the replacement"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ConnectorErrorKind::NotFound);

    drop(planning);
    assert!(host.take_ready_retires().expect("retire queue").is_empty());
    drop(exact_old);
    let ready = host.take_ready_retires().expect("retire queue");
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].key, old_key);

    let error = match host.acquire_exact_metadata_maintenance(&old_key) {
        Ok(_) => panic!("retired generation must not be recreated"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ConnectorErrorKind::NotFound);
    drop(replacement);
}

#[test]
fn cleanup_maintenance_lease_requires_the_capability_and_fences_retirement() {
    let host = ConnectorControlHost::new();
    let instance_id = ConnectorInstanceId::parse("catalog.analytics").expect("instance ID");
    host.register(binding(7))
        .expect("register no-cleanup-maintenance generation");
    let error = match host.acquire_current_cleanup_maintenance(&instance_id) {
        Ok(_) => panic!("missing capability must fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    host.retire_current(&instance_id)
        .expect("retire no-cleanup-maintenance generation");
    assert_eq!(host.take_ready_retires().expect("retire queue").len(), 1);

    host.register(binding_with_cleanup_maintenance(8))
        .expect("register cleanup-maintenance generation");
    let lease = host
        .acquire_current_cleanup_maintenance(&instance_id)
        .expect("cleanup-maintenance lease");
    assert_eq!(lease.binding_key().incarnation.to_bytes(), [8; 16]);
    assert_eq!(lease.metadata().instance_id(), &instance_id);
    host.retire_current(&instance_id)
        .expect("retire cleanup-maintenance generation");
    assert!(host.take_ready_retires().expect("retire queue").is_empty());
    drop(lease);
    assert_eq!(host.take_ready_retires().expect("retire queue").len(), 1);
}

#[test]
fn exact_cleanup_maintenance_lease_never_uses_a_replacement_incarnation() {
    let host = ConnectorControlHost::new();
    let instance_id = ConnectorInstanceId::parse("catalog.analytics").expect("instance ID");
    let old_key = ConnectorExecutionBindingKey {
        instance_id: instance_id.clone(),
        incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
    };
    host.register(binding_with_cleanup_maintenance(7))
        .expect("register old generation");
    let planning = host.acquire_current(&instance_id).expect("planning lease");
    host.retire_current(&instance_id)
        .expect("retire old generation");

    let error = match host.acquire_current_cleanup_maintenance(&instance_id) {
        Ok(_) => panic!("retiring generation must not accept current acquisition"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ConnectorErrorKind::NotFound);

    host.register(binding_with_cleanup_maintenance(8))
        .expect("register replacement generation");
    let replacement = host
        .acquire_current_cleanup_maintenance(&instance_id)
        .expect("replacement cleanup-maintenance lease");
    assert_eq!(replacement.binding_key().incarnation.to_bytes(), [8; 16]);

    let exact_old = host
        .acquire_exact_cleanup_maintenance(&old_key)
        .expect("exact retiring generation lease");
    assert_eq!(exact_old.binding_key(), &old_key);

    let unknown_key = ConnectorExecutionBindingKey {
        instance_id: instance_id.clone(),
        incarnation: ConnectorInstanceIncarnation::from_bytes([9; 16]),
    };
    let error = match host.acquire_exact_cleanup_maintenance(&unknown_key) {
        Ok(_) => panic!("unknown incarnation must not use the replacement"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ConnectorErrorKind::NotFound);

    drop(planning);
    assert!(host.take_ready_retires().expect("retire queue").is_empty());
    drop(exact_old);
    let ready = host.take_ready_retires().expect("retire queue");
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].key, old_key);

    let error = match host.acquire_exact_cleanup_maintenance(&old_key) {
        Ok(_) => panic!("retired generation must not be recreated"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ConnectorErrorKind::NotFound);
    drop(replacement);
}

#[test]
fn distributed_rewrite_lease_requires_both_capabilities_and_fences_retirement() {
    let host = ConnectorControlHost::new();
    let instance_id = ConnectorInstanceId::parse("catalog.analytics").expect("instance ID");
    host.register(binding(7))
        .expect("register no-rewrite generation");
    let error = match host.acquire_current_distributed_rewrite(&instance_id) {
        Ok(_) => panic!("missing rewrite capability must fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    host.retire_current(&instance_id)
        .expect("retire no-rewrite generation");
    assert_eq!(host.take_ready_retires().expect("retire queue").len(), 1);

    host.register(binding_with_distributed_rewrite(8, false))
        .expect("register rewrite-only generation");
    let error = match host.acquire_current_distributed_rewrite(&instance_id) {
        Ok(_) => panic!("missing write capability must fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    host.retire_current(&instance_id)
        .expect("retire rewrite-only generation");
    assert_eq!(host.take_ready_retires().expect("retire queue").len(), 1);

    host.register(binding_with_distributed_rewrite(9, true))
        .expect("register complete generation");
    let lease = host
        .acquire_current_distributed_rewrite(&instance_id)
        .expect("distributed rewrite lease");
    assert_eq!(lease.binding_key().incarnation.to_bytes(), [9; 16]);
    assert_eq!(lease.metadata().instance_id(), &instance_id);
    let writer = lease.derive_write_lease().expect("derived write lease");

    host.retire_current(&instance_id)
        .expect("retire distributed rewrite generation");
    assert!(host.take_ready_retires().expect("retire queue").is_empty());
    drop(lease);
    assert!(host.take_ready_retires().expect("retire queue").is_empty());
    drop(writer);
    assert_eq!(host.take_ready_retires().expect("retire queue").len(), 1);
}

#[test]
fn exact_distributed_rewrite_lease_never_uses_a_replacement_incarnation() {
    let host = ConnectorControlHost::new();
    let instance_id = ConnectorInstanceId::parse("catalog.analytics").expect("instance ID");
    let old_key = ConnectorExecutionBindingKey {
        instance_id: instance_id.clone(),
        incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
    };
    host.register(binding_with_distributed_rewrite(7, true))
        .expect("register old generation");
    let planning = host.acquire_current(&instance_id).expect("planning lease");
    host.retire_current(&instance_id)
        .expect("retire old generation");

    let error = match host.acquire_current_distributed_rewrite(&instance_id) {
        Ok(_) => panic!("retiring generation must not accept current acquisition"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ConnectorErrorKind::NotFound);

    host.register(binding_with_distributed_rewrite(8, true))
        .expect("register replacement generation");
    let replacement = host
        .acquire_current_distributed_rewrite(&instance_id)
        .expect("replacement distributed rewrite lease");
    assert_eq!(replacement.binding_key().incarnation.to_bytes(), [8; 16]);

    let exact_old = host
        .acquire_exact_distributed_rewrite(&old_key)
        .expect("exact retiring generation lease");
    assert_eq!(exact_old.binding_key(), &old_key);

    let unknown_key = ConnectorExecutionBindingKey {
        instance_id: instance_id.clone(),
        incarnation: ConnectorInstanceIncarnation::from_bytes([9; 16]),
    };
    let error = match host.acquire_exact_distributed_rewrite(&unknown_key) {
        Ok(_) => panic!("unknown incarnation must not use the replacement"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ConnectorErrorKind::NotFound);

    drop(planning);
    assert!(host.take_ready_retires().expect("retire queue").is_empty());
    drop(exact_old);
    let ready = host.take_ready_retires().expect("retire queue");
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].key, old_key);

    let error = match host.acquire_exact_distributed_rewrite(&old_key) {
        Ok(_) => panic!("retired generation must not be recreated"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ConnectorErrorKind::NotFound);
    drop(replacement);
}

#[test]
fn statistics_lease_fences_retirement_and_missing_capability_is_unsupported() {
    let host = ConnectorControlHost::new();
    let instance_id = ConnectorInstanceId::parse("catalog.analytics").expect("instance ID");
    host.register(binding(7))
        .expect("register no-statistics generation");
    let error = match host.acquire_current_statistics(&instance_id) {
        Ok(_) => panic!("missing capability must fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    host.retire_current(&instance_id)
        .expect("retire no-statistics generation");
    assert_eq!(host.take_ready_retires().expect("retire queue").len(), 1);

    host.register(binding_with_statistics(8))
        .expect("register replacement generation");
    let lease = host
        .acquire_current_statistics(&instance_id)
        .expect("statistics lease");
    assert!(!lease.supports_collection());
    host.retire_current(&instance_id)
        .expect("retire statistics generation");
    assert!(host.take_ready_retires().expect("retire queue").is_empty());
    drop(lease);
    assert_eq!(host.take_ready_retires().expect("retire queue").len(), 1);
}

fn unsupported() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "test-only control capability",
    )
}
