// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.  The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Provider-neutral orchestration state for distributed table rewrites.
//!
//! The provider freezes its groups before this module is entered.  Core turns
//! those opaque group plans into one sealed C1 write operation, keeps the
//! exact composite lease alive, and records the provider's durable checkpoint
//! for every accepted or superseded attempt.  It intentionally knows neither
//! files, manifests, nor provider report formats.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorBeginScanRequest, ConnectorDistributedRewriteAttemptCheckpoint,
    ConnectorDistributedRewriteAttemptDisposition, ConnectorDistributedRewriteLease,
    ConnectorDistributedRewritePlan, ConnectorDistributedRewriteReceipt, ConnectorError,
    ConnectorErrorKind, ConnectorReadSelector, ConnectorRequestContext,
    ConnectorSplitPlanningRequest, ConnectorTableHandle, ConnectorWriteAbortOutcome,
    ConnectorWriteAttemptCompletion, ConnectorWriteCohortId, ConnectorWriteExecutionId,
    ConnectorWriteOperationId, ConnectorWriteReceipt, ExternalMutationEvidence,
    ExternalMutationOutcome,
};

use crate::query_execution::backend::BackendTopologySnapshot;
use crate::query_execution::contract::{
    ConnectorWriteExecutionRegistration, ConnectorWriteOperationRegistration,
    ConnectorWritePlanningTemplate,
};
use crate::query_execution::outcome::ConnectorWriteCompletion;
use crate::query_execution::preparation::scan::PlannedConnectorRead;
use crate::query_execution::write_operation::ConnectorWriteOperationSession;

/// Plan one frozen source through the scan-planning capability retained by a
/// composite rewrite lease.  The plan is opaque to this module: it has no
/// Iceberg files, catalog client, or provider report decoding.
pub(crate) fn plan_frozen_rewrite_connector_read(
    lease: &ConnectorDistributedRewriteLease,
    topology: &BackendTopologySnapshot,
    source: &ConnectorTableHandle,
    projection: Vec<usize>,
    context: ConnectorRequestContext,
) -> Result<PlannedConnectorRead, ConnectorError> {
    if source.owner() != &lease.binding_key().instance_id {
        return Err(invalid(
            "frozen rewrite source does not belong to the exact rewrite lease",
        ));
    }
    let target_parallelism = NonZeroUsize::new(topology.targets().len()).ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            "distributed rewrite requires at least one live backend",
        )
    })?;
    let batch = ConnectorBatchBudget {
        max_rows: NonZeroUsize::new(4096).expect("rewrite batch rows are nonzero"),
        max_bytes: NonZeroUsize::new(context.max_handle_payload_bytes())
            .expect("validated connector payload budget is nonzero"),
    };
    let scan = lease.planning().begin_scan(
        source,
        ConnectorBeginScanRequest {
            projection,
            static_predicates: Vec::new(),
            selector: ConnectorReadSelector::Current,
            limit: None,
            batch,
            context: context.clone(),
        },
    )?;
    let split_result = lease.planning().plan_splits(
        &scan.handle,
        ConnectorSplitPlanningRequest {
            target_parallelism,
            max_split_bytes: None,
            context: context.clone(),
        },
    )?;
    if split_result
        .splits
        .iter()
        .any(|split| split.owner() != &lease.binding_key().instance_id)
    {
        return Err(invalid(
            "distributed rewrite provider planned a split for another connector instance",
        ));
    }
    Ok(PlannedConnectorRead {
        declaration: lease.execution_declaration(&context)?,
        scan,
        splits: split_result.splits,
        planning_metrics: split_result.metrics,
        static_predicates: Vec::new(),
        predicate_dispositions: Vec::new(),
        residual_predicates: Vec::new(),
        batch,
        // This clone is derived from the composite rewrite lease, so the
        // generic ensure barrier retains the same exact generation without a
        // later current-generation lookup.
        planning_lease: Some(lease.planning_lease()),
        read_session: split_result.session,
    })
}

/// Build the minimal physical source for one opaque frozen rewrite read.
/// Execution preparation replaces this `ConnectorPinned` node exactly once
/// with the `PlannedConnectorRead` above; no normal table lookup may run.
pub(crate) fn frozen_rewrite_scan_physical_plan(
    input_schema: &arrow::datatypes::SchemaRef,
) -> crate::sql::planner::physical::PhysicalPlanNode {
    let mut factory = crate::sql::column_id::ColumnRefFactory::new();
    let mut output_columns = Vec::with_capacity(input_schema.fields().len());
    let mut table_columns = Vec::with_capacity(input_schema.fields().len());
    for field in input_schema.fields() {
        let name = field.name().to_string();
        let data_type = field.data_type().clone();
        let nullable = field.is_nullable();
        let column_id = factory.create(None, name.clone(), data_type.clone(), nullable);
        output_columns.push(crate::sql::analysis::OutputColumn {
            column_id,
            name: name.clone(),
            data_type: data_type.clone(),
            nullable,
            is_internal: false,
        });
        table_columns.push(novarocks_catalog::schema::ColumnDef {
            name,
            data_type,
            nullable,
            write_default: None,
            logical_type: None,
        });
    }
    crate::sql::planner::physical::PhysicalPlanNode {
        kind: crate::sql::planner::physical::PhysicalPlanKind::Scan(
            crate::sql::planner::payload::PlanScanNode {
                database: "__distributed_rewrite".to_string(),
                table: crate::sql::planner::table::TableDef {
                    name: "__connector_frozen_rewrite".to_string(),
                    columns: table_columns,
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: crate::sql::planner::table::ScanSource::ConnectorPinned,
                },
                alias: None,
                columns: output_columns.clone(),
                predicates: Vec::new(),
                required_columns: None,
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            },
        ),
        children: Vec::new(),
        output_columns,
        stats: crate::sql::planner::physical::PhysicalPlanStats {
            output_row_count: 0.0,
            row_count_confidence: crate::sql::planner::physical::PlannerConfidence::Fallback,
            column_statistics: HashMap::new(),
            cost_estimate: None,
            broadcast_decision: None,
        },
        probe_runtime_filters: Vec::new(),
    }
}

/// One-shot injection point for the exact frozen source plan.  Keeping this
/// local to rewrite execution makes a second provider catalog lookup during
/// fragment preparation structurally impossible.
pub(crate) struct FrozenRewriteReadResolver {
    read: Mutex<Option<PlannedConnectorRead>>,
}

impl FrozenRewriteReadResolver {
    pub(crate) fn new(read: PlannedConnectorRead) -> Self {
        Self {
            read: Mutex::new(Some(read)),
        }
    }
}

impl crate::query_execution::preparation::scan::ScanBindingResolver for FrozenRewriteReadResolver {
    fn resolve_scan(
        &self,
        _node_id: i32,
        _scan: &crate::sql::planner::payload::PlanScanNode,
    ) -> Result<Option<crate::query_execution::preparation::scan::ResolvedScanExecution>, String>
    {
        Ok(Some(
            crate::query_execution::preparation::scan::ResolvedScanExecution::ConnectorRead,
        ))
    }

    fn resolve_connector_read(
        &self,
        _node_id: i32,
        _scan: &crate::sql::planner::payload::PlanScanNode,
    ) -> Result<Option<PlannedConnectorRead>, String> {
        self.read
            .lock()
            .map_err(|_| "frozen rewrite connector read lock poisoned".to_string())
            .map(|mut read| read.take())
    }
}

/// One frozen rewrite operation.  A non-empty plan is sealed into C1 before
/// any caller can obtain a cohort execution registration.  Empty plans are a
/// deterministic no-op and deliberately have no writer session.
#[derive(Clone)]
pub struct ConnectorDistributedRewriteSession {
    inner: Arc<ConnectorDistributedRewriteSessionInner>,
}

struct ConnectorDistributedRewriteSessionInner {
    plan: ConnectorDistributedRewritePlan,
    lease: ConnectorDistributedRewriteLease,
    write_session: Option<ConnectorWriteOperationSession>,
    checkpoints: Mutex<BTreeMap<AttemptKey, ConnectorDistributedRewriteAttemptCheckpoint>>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct AttemptKey {
    cohort_id: ConnectorWriteCohortId,
    execution_id: ConnectorWriteExecutionId,
    disposition: u8,
}

impl ConnectorDistributedRewriteSession {
    /// Validate a provider-frozen plan, activate its exact provider service,
    /// and seal all C1 cohorts in one step.  There is no API that adds a
    /// cohort afterwards.
    pub fn try_begin(
        plan: ConnectorDistributedRewritePlan,
        lease: ConnectorDistributedRewriteLease,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        plan.validate()?;
        if plan.owner() != lease.binding_key() {
            return Err(invalid(
                "distributed rewrite plan does not belong to the exact rewrite lease",
            ));
        }

        let write_session = if plan.cohorts().is_empty() {
            None
        } else {
            lease.activate_rewrite(&plan)?;
            let templates = plan
                .cohorts()
                .iter()
                .map(|cohort| {
                    ConnectorWritePlanningTemplate::new_in_cohort(
                        plan.operation_id(),
                        cohort.cohort_id(),
                        plan.target().clone(),
                        cohort.intent(),
                        cohort.input_schema().clone(),
                        cohort.provider_payload().clone(),
                        context.clone(),
                    )
                })
                .collect();
            let registration = ConnectorWriteOperationRegistration::try_new(templates)
                .map_err(|error| invalid(format!("register rewrite cohorts: {error}")))?;
            Some(ConnectorWriteOperationSession::try_begin(
                registration,
                lease.derive_write_lease()?,
            )?)
        };

        Ok(Self {
            inner: Arc::new(ConnectorDistributedRewriteSessionInner {
                plan,
                lease,
                write_session,
                checkpoints: Mutex::new(BTreeMap::new()),
            }),
        })
    }

    pub fn plan(&self) -> &ConnectorDistributedRewritePlan {
        &self.inner.plan
    }

    /// Exact composite lease retained from frozen planning through terminal
    /// commit or abort.  Provider-facing execution may use only this lease to
    /// plan the opaque frozen source.
    pub fn lease(&self) -> &ConnectorDistributedRewriteLease {
        &self.inner.lease
    }

    pub fn operation_id(&self) -> ConnectorWriteOperationId {
        self.inner.plan.operation_id()
    }

    pub fn is_noop(&self) -> bool {
        self.inner.write_session.is_none()
    }

    pub fn write_session(&self) -> Option<&ConnectorWriteOperationSession> {
        self.inner.write_session.as_ref()
    }

    /// Produce the only execution registration for a sealed rewrite cohort.
    /// Calling this before `try_begin` has sealed the full frozen group set is
    /// structurally impossible.
    pub fn execution_registration(
        &self,
        cohort_id: ConnectorWriteCohortId,
    ) -> Result<ConnectorWriteExecutionRegistration, ConnectorError> {
        let session = self
            .inner
            .write_session
            .clone()
            .ok_or_else(|| invalid("distributed rewrite no-op has no staging cohort"))?;
        ConnectorWriteExecutionRegistration::try_new(session, cohort_id)
            .map_err(|error| invalid(format!("register rewrite cohort execution: {error}")))
    }

    /// Record a completed C1 attempt as accepted and persist its opaque report
    /// set through the provider before frontend durable state advances.
    pub fn checkpoint_accepted(
        &self,
        completion: &ConnectorWriteCompletion,
    ) -> Result<ConnectorDistributedRewriteAttemptCheckpoint, ConnectorError> {
        self.checkpoint(
            ConnectorDistributedRewriteAttemptDisposition::Accepted,
            completion,
        )
    }

    /// Move a previously accepted C1 attempt to superseded and checkpoint it
    /// for operation-wide cleanup.  It can never contribute to C1 completeness
    /// after this call.
    pub fn checkpoint_superseded(
        &self,
        completion: &ConnectorWriteCompletion,
    ) -> Result<ConnectorDistributedRewriteAttemptCheckpoint, ConnectorError> {
        let attempt = self.validate_completion(completion)?;
        self.inner
            .write_session
            .as_ref()
            .expect("validated rewrite completion has a write session")
            .supersede_attempt(completion.attachment(), completion.input())?;
        self.persist_checkpoint(
            ConnectorDistributedRewriteAttemptDisposition::Superseded,
            attempt,
        )
    }

    /// Restore one provider-durable attempt only for terminal abort/recovery.
    /// It returns the opaque C1 completion to the caller; this session never
    /// installs it as an accepted staging attempt, so recovery cannot resume
    /// an old execution.
    pub fn restore_for_abort(
        &self,
        checkpoint: &ConnectorDistributedRewriteAttemptCheckpoint,
    ) -> Result<ConnectorWriteAttemptCompletion, ConnectorError> {
        self.validate_checkpoint(checkpoint)?;
        let completion = self
            .inner
            .lease
            .restore_attempt(&self.inner.plan, checkpoint)?;
        if completion.owner() != self.inner.plan.owner()
            || completion.operation_id() != self.operation_id()
            || completion.cohort_id() != checkpoint.cohort_id
            || completion.execution_id() != checkpoint.execution_id
            || completion.digest() != checkpoint.attempt_digest
        {
            return Err(invalid(
                "distributed rewrite restored attempt does not match its checkpoint",
            ));
        }
        self.inner
            .write_session
            .as_ref()
            .expect("non-empty rewrite plan has a write session")
            .restore_for_abort(checkpoint.disposition, completion.clone())?;
        Ok(completion)
    }

    /// Restore the persisted aggregate C1 decision before marker-only
    /// reconcile.  This has no staging path and therefore cannot re-submit a
    /// rewrite that may already have reached the catalog.
    pub fn restore_for_reconcile(&self, aggregate_digest: [u8; 32]) -> Result<(), ConnectorError> {
        self.inner
            .write_session
            .as_ref()
            .ok_or_else(|| invalid("distributed rewrite no-op has no C1 reconcile"))?
            .restore_for_reconcile(aggregate_digest)
    }

    /// Commit every accepted cohort through the same exact C1 control lease.
    pub fn commit(
        &self,
        context: ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        self.inner
            .write_session
            .as_ref()
            .ok_or_else(|| invalid("distributed rewrite no-op has no C1 commit"))?
            .commit(context)
    }

    /// Abort all checkpointed and in-memory staged cohorts through C1.
    pub fn abort(
        &self,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
        self.inner
            .write_session
            .as_ref()
            .ok_or_else(|| invalid("distributed rewrite no-op has no C1 abort"))?
            .abort(context)
    }

    /// Reconcile only after the C1 session made its aggregate commit decision.
    pub fn reconcile(
        &self,
        evidence: ExternalMutationEvidence,
        context: ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        self.inner
            .write_session
            .as_ref()
            .ok_or_else(|| invalid("distributed rewrite no-op has no C1 reconcile"))?
            .reconcile(evidence, context)
    }

    /// Project a known-committed C1 receipt only through the provider that
    /// froze this operation.  Callers must not decode the receipt in core.
    pub fn finalize_committed(
        &self,
        receipt: &ConnectorWriteReceipt,
    ) -> Result<ConnectorDistributedRewriteReceipt, ConnectorError> {
        self.inner.lease.finalize_rewrite(&self.inner.plan, receipt)
    }

    pub fn checkpointed_attempts(
        &self,
    ) -> Result<Vec<ConnectorDistributedRewriteAttemptCheckpoint>, ConnectorError> {
        let checkpoints = self.inner.checkpoints.lock().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "distributed rewrite checkpoint state lock poisoned",
            )
        })?;
        Ok(checkpoints.values().cloned().collect())
    }

    fn checkpoint(
        &self,
        disposition: ConnectorDistributedRewriteAttemptDisposition,
        completion: &ConnectorWriteCompletion,
    ) -> Result<ConnectorDistributedRewriteAttemptCheckpoint, ConnectorError> {
        let attempt = self.validate_completion(completion)?;
        self.persist_checkpoint(disposition, attempt)
    }

    fn validate_completion(
        &self,
        completion: &ConnectorWriteCompletion,
    ) -> Result<ConnectorWriteAttemptCompletion, ConnectorError> {
        let write_session = self.inner.write_session.as_ref().ok_or_else(|| {
            invalid("distributed rewrite no-op cannot checkpoint a staged attempt")
        })?;
        if completion.session().operation_id() != self.operation_id()
            || completion.session().owner() != self.inner.plan.owner()
            || completion.session().sealed().digest() != write_session.sealed().digest()
        {
            return Err(invalid(
                "distributed rewrite completion does not belong to the sealed session",
            ));
        }
        completion.attempt_completion()
    }

    fn persist_checkpoint(
        &self,
        disposition: ConnectorDistributedRewriteAttemptDisposition,
        attempt: ConnectorWriteAttemptCompletion,
    ) -> Result<ConnectorDistributedRewriteAttemptCheckpoint, ConnectorError> {
        let key = AttemptKey {
            cohort_id: attempt.cohort_id(),
            execution_id: attempt.execution_id(),
            disposition: disposition_tag(disposition),
        };
        let checkpoint =
            self.inner
                .lease
                .checkpoint_attempt(&self.inner.plan, disposition, &attempt)?;
        self.validate_checkpoint(&checkpoint)?;
        if checkpoint.cohort_id != key.cohort_id
            || checkpoint.execution_id != key.execution_id
            || checkpoint.attempt_digest != attempt.digest()
        {
            return Err(invalid(
                "distributed rewrite provider returned a foreign attempt checkpoint",
            ));
        }
        let mut checkpoints = self.inner.checkpoints.lock().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "distributed rewrite checkpoint state lock poisoned",
            )
        })?;
        match checkpoints.get(&key) {
            Some(existing) if existing == &checkpoint => Ok(checkpoint),
            Some(_) => Err(invalid(
                "distributed rewrite attempt checkpoint replay changed durable facts",
            )),
            None => {
                checkpoints.insert(key, checkpoint.clone());
                Ok(checkpoint)
            }
        }
    }

    fn validate_checkpoint(
        &self,
        checkpoint: &ConnectorDistributedRewriteAttemptCheckpoint,
    ) -> Result<(), ConnectorError> {
        checkpoint.validate()?;
        if !self
            .inner
            .plan
            .cohorts()
            .iter()
            .any(|cohort| cohort.cohort_id() == checkpoint.cohort_id)
        {
            return Err(invalid(
                "distributed rewrite checkpoint references an unknown cohort",
            ));
        }
        Ok(())
    }
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

const fn disposition_tag(disposition: ConnectorDistributedRewriteAttemptDisposition) -> u8 {
    match disposition {
        ConnectorDistributedRewriteAttemptDisposition::Accepted => 0,
        ConnectorDistributedRewriteAttemptDisposition::Superseded => 1,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorControlBinding, ConnectorDistributedRewrite,
        ConnectorDistributedRewriteCohortPlan, ConnectorDistributedRewritePlanSummary,
        ConnectorDistributedRewritePlanningRequest, ConnectorExecutionBindingKey,
        ConnectorExecutionDeclaration, ConnectorExecutionDistribution, ConnectorInstanceDescriptor,
        ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorMetadata, ConnectorProviderId,
        ConnectorScanPlanning, ConnectorTableHandle, ConnectorWriteCohortId, ConnectorWriteControl,
        ConnectorWritePlan, ConnectorWritePlanningRequest,
    };

    use super::*;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            Arc::new(NeverCancelled),
            1024,
            4096,
        )
        .unwrap()
    }

    struct TestMetadata {
        instance: ConnectorInstanceId,
    }

    struct TestPlanning {
        instance: ConnectorInstanceId,
    }

    impl ConnectorScanPlanning for TestPlanning {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.instance
        }

        fn begin_scan(
            &self,
            _table: &ConnectorTableHandle,
            _request: novarocks_spi::connector::ConnectorBeginScanRequest,
        ) -> Result<novarocks_spi::connector::ConnectorScan, ConnectorError> {
            unreachable!("rewrite session does not plan scans")
        }

        fn plan_splits(
            &self,
            _scan: &novarocks_spi::connector::ConnectorScanHandle,
            _request: novarocks_spi::connector::ConnectorSplitPlanningRequest,
        ) -> Result<novarocks_spi::connector::ConnectorSplitPlanningResult, ConnectorError>
        {
            unreachable!("rewrite session does not plan scans")
        }
    }

    impl ConnectorMetadata for TestMetadata {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.instance
        }
        fn namespace_exists(
            &self,
            _request: novarocks_spi::connector::ConnectorNamespaceRequest,
        ) -> Result<bool, ConnectorError> {
            unreachable!("rewrite session does not load metadata")
        }
        fn table_exists(
            &self,
            _request: novarocks_spi::connector::ConnectorTableRequest,
        ) -> Result<bool, ConnectorError> {
            unreachable!("rewrite session does not load metadata")
        }
        fn list_tables(
            &self,
            _request: novarocks_spi::connector::ConnectorListTablesRequest,
        ) -> Result<Vec<novarocks_spi::connector::ConnectorTableIdentity>, ConnectorError> {
            unreachable!("rewrite session does not load metadata")
        }
        fn load_table(
            &self,
            _request: novarocks_spi::connector::ConnectorTableRequest,
        ) -> Result<novarocks_spi::connector::ConnectorTableMetadata, ConnectorError> {
            unreachable!("rewrite session does not load metadata")
        }
    }

    struct TestRewrite {
        descriptor: ConnectorInstanceDescriptor,
        key: ConnectorExecutionBindingKey,
    }

    impl ConnectorDistributedRewrite for TestRewrite {
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
            unreachable!()
        }
        fn activate_rewrite(
            &self,
            _plan: &ConnectorDistributedRewritePlan,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }
        fn checkpoint_attempt(
            &self,
            _plan: &ConnectorDistributedRewritePlan,
            _disposition: ConnectorDistributedRewriteAttemptDisposition,
            _completion: &ConnectorWriteAttemptCompletion,
        ) -> Result<ConnectorDistributedRewriteAttemptCheckpoint, ConnectorError> {
            unreachable!()
        }
        fn restore_attempt(
            &self,
            _plan: &ConnectorDistributedRewritePlan,
            _checkpoint: &ConnectorDistributedRewriteAttemptCheckpoint,
        ) -> Result<ConnectorWriteAttemptCompletion, ConnectorError> {
            unreachable!()
        }
        fn finalize_rewrite(
            &self,
            _plan: &ConnectorDistributedRewritePlan,
            _receipt: &ConnectorWriteReceipt,
        ) -> Result<ConnectorDistributedRewriteReceipt, ConnectorError> {
            unreachable!()
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
            unreachable!()
        }
        fn commit(
            &self,
            _request: novarocks_spi::connector::ConnectorWriteCommitRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
            unreachable!()
        }
        fn abort(
            &self,
            _request: novarocks_spi::connector::ConnectorWriteAbortRequest,
        ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
            unreachable!()
        }
        fn reconcile(
            &self,
            _request: novarocks_spi::connector::ConnectorWriteReconcileRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
            unreachable!()
        }
    }

    struct TestDistribution {
        descriptor: ConnectorInstanceDescriptor,
        key: ConnectorExecutionBindingKey,
    }
    impl ConnectorExecutionDistribution for TestDistribution {
        fn declaration(
            &self,
            _context: &ConnectorRequestContext,
        ) -> Result<ConnectorExecutionDeclaration, ConnectorError> {
            ConnectorExecutionDeclaration::try_new(
                self.descriptor.clone(),
                self.key.incarnation,
                Bytes::from_static(b"test"),
            )
        }
    }

    fn fixture(
        cohorts: usize,
    ) -> (
        ConnectorDistributedRewritePlan,
        ConnectorDistributedRewriteLease,
    ) {
        let provider = ConnectorProviderId::parse("rewrite-session-test").unwrap();
        let instance = ConnectorInstanceId::parse("rewrite-session-instance").unwrap();
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: provider,
            instance_id: instance.clone(),
        };
        let key = ConnectorExecutionBindingKey {
            instance_id: instance.clone(),
            incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
        };
        let operation_id = ConnectorWriteOperationId::new();
        let table =
            ConnectorTableHandle::try_new(instance.clone(), Bytes::from_static(b"table")).unwrap();
        let request = ConnectorDistributedRewritePlanningRequest::try_new(
            operation_id,
            key.clone(),
            novarocks_spi::connector::ConnectorDistributedRewriteOperation::RewriteDataFiles {
                table: table.clone(),
                rewrite_all: true,
            },
            context(),
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let cohort_plans = (0..cohorts)
            .map(|index| {
                let digest = [u8::try_from(index).unwrap_or_default(); 32];
                ConnectorDistributedRewriteCohortPlan::try_new(
                    ConnectorWriteCohortId::derive(operation_id, b"test", digest).unwrap(),
                    table.clone(),
                    novarocks_spi::connector::ConnectorWriteIntent::RowDelta,
                    schema.clone(),
                    [3; 32],
                    Bytes::from_static(b"group"),
                    digest,
                )
                .unwrap()
            })
            .collect();
        let plan = ConnectorDistributedRewritePlan::try_new(
            &request,
            [1; 32],
            [2; 32],
            ConnectorDistributedRewritePlanSummary {
                groups: cohorts as u64,
                ..Default::default()
            },
            Bytes::from_static(b"plan"),
            cohort_plans,
        )
        .unwrap();
        let rewrite = Arc::new(TestRewrite {
            descriptor: descriptor.clone(),
            key: key.clone(),
        });
        let lease = ConnectorDistributedRewriteLease::new(
            descriptor.clone(),
            key.clone(),
            novarocks_spi::connector::ConnectorControlPlanningLease::new(
                Arc::new(
                    ConnectorControlBinding::try_new(
                        descriptor.clone(),
                        key.incarnation,
                        Arc::new(TestMetadata {
                            instance: key.instance_id.clone(),
                        }),
                        Arc::new(TestPlanning {
                            instance: key.instance_id.clone(),
                        }),
                        Arc::new(TestDistribution {
                            descriptor: descriptor.clone(),
                            key: key.clone(),
                        }),
                        None,
                    )
                    .unwrap(),
                ),
                || {},
            ),
            Arc::new(TestMetadata {
                instance: instance.clone(),
            }),
            Arc::new(TestPlanning { instance }),
            rewrite,
            Arc::new(TestWrite { key: key.clone() }),
            Arc::new(TestDistribution { descriptor, key }),
            || {},
        )
        .unwrap();
        (plan, lease)
    }

    #[test]
    fn seals_every_frozen_cohort_before_execution_registration() {
        let (plan, lease) = fixture(2);
        let cohort_ids = plan
            .cohorts()
            .iter()
            .map(|cohort| cohort.cohort_id())
            .collect::<Vec<_>>();
        let session =
            ConnectorDistributedRewriteSession::try_begin(plan, lease, context()).unwrap();
        assert!(!session.is_noop());
        let sealed = session.write_session().unwrap().sealed();
        assert_eq!(sealed.cohorts().len(), 2);
        for cohort_id in cohort_ids {
            assert_eq!(
                session
                    .execution_registration(cohort_id)
                    .unwrap()
                    .cohort_id(),
                cohort_id
            );
        }
    }

    #[test]
    fn empty_plan_is_noop_without_writer_session() {
        let (plan, lease) = fixture(0);
        let session =
            ConnectorDistributedRewriteSession::try_begin(plan, lease, context()).unwrap();
        assert!(session.is_noop());
        assert!(session.write_session().is_none());
    }
}
