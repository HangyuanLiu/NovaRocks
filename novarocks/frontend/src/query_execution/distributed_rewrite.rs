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

//! Provider-neutral orchestration state for distributed table rewrites.
//!
//! The provider freezes its groups before this module is entered.  Core turns
//! those opaque group plans into one write session whose logical targets stand
//! one-to-one with them, keeps the exact composite lease alive, and commits the
//! union of every group's prepared write set once.  It intentionally knows
//! neither files, manifests, nor provider report formats.

use std::sync::Arc;

use novarocks_spi::connector::write_stack::{ConnectorWriteTargetPlan, WriteTargetOrdinal};
use novarocks_spi::connector::{
    ConnectorDistributedRewriteLease, ConnectorDistributedRewritePlan,
    ConnectorDistributedRewriteReceipt, ConnectorError, ConnectorErrorKind,
    ConnectorRequestContext, ConnectorWriteAbortOutcome, ConnectorWriteCohortId,
    ConnectorWriteOperationId, ConnectorWriteReceipt, ExternalMutationOutcome,
};

use crate::catalog_application::query_bindings::QueryTableBindingStore;
use crate::connector::distributed_rewrite_application::{
    DistributedRewriteApplicationSession, DistributedRewriteSealing, SealedDistributedRewrite,
};
use crate::query_execution::preparation::scan::{QueryPinnedFileSetRead, QueryRewriteGroupRead};
use crate::query_execution::service::QueryExecutionService;
use crate::query_execution::write_session::ConnectorWriteSession;
use novarocks_sql::binding::SqlTableBindingId;
use novarocks_sql::planning::query_execution::{
    FrozenConnectorScanIdentity, FrozenConnectorScanPlan,
};

/// The Frontend-owned maintenance session shape produced by query assembly.
pub(crate) type DistributedRewriteMaintenanceSession =
    DistributedRewriteApplicationSession<ConnectorDistributedRewriteSession>;

impl SealedDistributedRewrite for ConnectorDistributedRewriteSession {
    fn plan(&self) -> &ConnectorDistributedRewritePlan {
        ConnectorDistributedRewriteSession::plan(self)
    }

    fn is_noop(&self) -> bool {
        ConnectorDistributedRewriteSession::is_noop(self)
    }
}

impl DistributedRewriteSealing for QueryExecutionService {
    type Sealed = ConnectorDistributedRewriteSession;

    fn seal_distributed_rewrite(
        &self,
        plan: ConnectorDistributedRewritePlan,
        lease: ConnectorDistributedRewriteLease,
        write_stack: crate::connector::control_host::ConnectorWriteStackLease,
        table: &novarocks_spi::connector::ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<Self::Sealed, String> {
        self.begin_distributed_rewrite_operation_with_lease(
            plan,
            lease,
            write_stack,
            table,
            context,
        )
        .map_err(|error| error.to_string())
    }

    fn seal_noop_distributed_rewrite(
        &self,
        plan: ConnectorDistributedRewritePlan,
        lease: ConnectorDistributedRewriteLease,
    ) -> Result<Self::Sealed, String> {
        ConnectorDistributedRewriteSession::noop(plan, lease).map_err(|error| error.to_string())
    }
}

/// Admit the synthetic source used by one pinned rewrite cohort read.
pub(crate) fn admit_pinned_rewrite_scan_binding(
    bindings: &QueryTableBindingStore,
    input_schema: &arrow::datatypes::SchemaRef,
) -> Result<SqlTableBindingId, String> {
    crate::query_execution::pinned_connector_read::admit_pinned_file_set_scan_binding(
        bindings,
        &frozen_rewrite_identity(),
        input_schema,
    )
}

/// Build the minimal physical source for one pinned rewrite cohort read.
/// Preparation freezes the relation restricted to exactly the files the
/// provider pinned for this cohort; no normal table lookup may run.
pub(crate) fn pinned_rewrite_scan_physical_plan(
    input_schema: &arrow::datatypes::SchemaRef,
    binding: SqlTableBindingId,
) -> FrozenConnectorScanPlan {
    crate::query_execution::pinned_connector_read::pinned_file_set_scan_physical_plan(
        &frozen_rewrite_identity(),
        input_schema,
        binding,
    )
}

pub(crate) fn pinned_rewrite_read_resolver(
    binding: SqlTableBindingId,
    read: QueryPinnedFileSetRead,
) -> crate::query_execution::pinned_connector_read::PinnedFileSetReadResolver {
    crate::query_execution::pinned_connector_read::PinnedFileSetReadResolver::new(
        binding,
        frozen_rewrite_identity(),
        read,
    )
}

/// Admit the synthetic source used by one procedure cohort's group read.
pub(crate) fn admit_rewrite_group_scan_binding(
    bindings: &QueryTableBindingStore,
    input_schema: &arrow::datatypes::SchemaRef,
) -> Result<SqlTableBindingId, String> {
    crate::query_execution::rewrite_group_read::admit_table_execute_scan_binding(
        bindings,
        &frozen_rewrite_identity(),
        input_schema,
    )
}

/// Build the minimal physical source for one procedure cohort's group read.
/// Preparation freezes the relation the group names; no normal table lookup
/// may run.
pub(crate) fn rewrite_group_scan_physical_plan(
    input_schema: &arrow::datatypes::SchemaRef,
    binding: SqlTableBindingId,
) -> FrozenConnectorScanPlan {
    crate::query_execution::rewrite_group_read::table_execute_scan_physical_plan(
        &frozen_rewrite_identity(),
        input_schema,
        binding,
    )
}

pub(crate) fn rewrite_group_read_resolver(
    binding: SqlTableBindingId,
    read: QueryRewriteGroupRead,
) -> crate::query_execution::rewrite_group_read::RewriteGroupReadResolver {
    crate::query_execution::rewrite_group_read::RewriteGroupReadResolver::new(
        binding,
        frozen_rewrite_identity(),
        read,
    )
}

fn frozen_rewrite_identity() -> FrozenConnectorScanIdentity {
    FrozenConnectorScanIdentity::new(
        "__distributed_rewrite",
        "__distributed_rewrite",
        "__connector_frozen_rewrite",
    )
}

/// One frozen rewrite operation.
///
/// The provider freezes its rewrite groups before this module is entered. Core
/// turns them into one write session whose logical targets stand in one-to-one
/// order with those groups, keeps the exact composite lease alive, and commits
/// once at the end.
///
/// An empty plan is a deterministic no-op and deliberately has no write
/// session: there is nothing to write, so there is nothing to commit.
#[derive(Clone)]
pub struct ConnectorDistributedRewriteSession {
    inner: Arc<ConnectorDistributedRewriteSessionInner>,
}

struct ConnectorDistributedRewriteSessionInner {
    plan: ConnectorDistributedRewritePlan,
    lease: ConnectorDistributedRewriteLease,
    write_session: Option<Arc<ConnectorWriteSession>>,
}

impl ConnectorDistributedRewriteSession {
    /// Validate a provider-frozen plan and open one write session covering
    /// every frozen group.
    ///
    /// The provider re-derives its own branches at begin, so this checks that
    /// the target set it sealed agrees with the group set this plan froze. A
    /// disagreement means the table moved between planning and begin, and the
    /// two halves would silently write past each other -- so it fails closed
    /// here rather than at commit.
    /// A plan that froze no group. It writes nothing, so it deliberately has no
    /// write session and never derives a write-stack lease -- there is nothing
    /// for one to admit.
    pub fn noop(
        plan: ConnectorDistributedRewritePlan,
        lease: ConnectorDistributedRewriteLease,
    ) -> Result<Self, ConnectorError> {
        lease.validate_plan(&plan)?;
        if !plan.cohorts().is_empty() {
            return Err(invalid(
                "distributed rewrite plan froze groups, so it is not a no-op",
            ));
        }
        Ok(Self {
            inner: Arc::new(ConnectorDistributedRewriteSessionInner {
                plan,
                lease,
                write_session: None,
            }),
        })
    }

    pub fn try_begin(
        plan: ConnectorDistributedRewritePlan,
        lease: ConnectorDistributedRewriteLease,
        write_stack: crate::connector::control_host::ConnectorWriteStackLease,
        table: &novarocks_spi::connector::ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        lease.validate_plan(&plan)?;

        let write_session = if plan.cohorts().is_empty() {
            None
        } else {
            let write_lease = lease.derive_write_lease()?;
            let session = crate::query_execution::write_session::begin_connector_write_session(
                write_stack,
                &write_lease,
                rewrite_begin_request(table, context).map_err(invalid)?,
            )
            .map_err(invalid)?;
            let sealed = session.expected_targets().len();
            if sealed != plan.cohorts().len() {
                return Err(invalid(format!(
                    "distributed rewrite sealed {sealed} write targets for {} frozen groups",
                    plan.cohorts().len()
                )));
            }
            Some(session)
        };

        Ok(Self {
            inner: Arc::new(ConnectorDistributedRewriteSessionInner {
                plan,
                lease,
                write_session,
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

    pub(crate) fn write_session(&self) -> Option<&Arc<ConnectorWriteSession>> {
        self.inner.write_session.as_ref()
    }

    /// Which logical write target one frozen group writes to.
    ///
    /// The ordinal is the group's position in the frozen plan, which is the
    /// order the session sealed its targets in. Every group has exactly one,
    /// and a cohort id that this plan never froze has none.
    pub(crate) fn write_target_ordinal(
        &self,
        cohort_id: ConnectorWriteCohortId,
    ) -> Result<WriteTargetOrdinal, ConnectorError> {
        rewrite_group_ordinal(&self.inner.plan, cohort_id)
    }

    /// The sealed target this group's query compiles its writer against.
    pub(crate) fn write_target(
        &self,
        cohort_id: ConnectorWriteCohortId,
    ) -> Result<&ConnectorWriteTargetPlan, ConnectorError> {
        let ordinal = self.write_target_ordinal(cohort_id)?;
        self.require_write_session()?
            .targets()
            .iter()
            .find(|target| target.ordinal() == ordinal)
            .ok_or_else(|| invalid("distributed rewrite session did not seal this group's target"))
    }

    /// Take one finished group's prepared write set into the session.
    ///
    /// Each group runs as its own distributed query, so the session collects
    /// their prepared sets and commits the union once. Budgets are charged on
    /// that union, not per group.
    pub(crate) fn accumulate(
        &self,
        prepared: crate::query_execution::write_result::DecodedPreparedWriteSet,
    ) -> Result<(), ConnectorError> {
        self.require_write_session()?.accumulate(prepared)
    }

    /// Commit every accumulated group through the same exact control lease.
    pub fn commit(
        &self,
        context: ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        self.require_write_session()?.finish_accumulated(context)
    }

    /// Abort the whole rewrite through the same control lease.
    pub fn abort(
        &self,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
        self.require_write_session()?.abort(context)
    }

    /// Project a known-committed receipt only through the provider that froze
    /// this operation.  Callers must not decode the receipt in core.
    pub fn finalize_committed(
        &self,
        receipt: &ConnectorWriteReceipt,
    ) -> Result<ConnectorDistributedRewriteReceipt, ConnectorError> {
        self.inner.lease.finalize_rewrite(&self.inner.plan, receipt)
    }

    fn require_write_session(&self) -> Result<&Arc<ConnectorWriteSession>, ConnectorError> {
        self.inner
            .write_session
            .as_ref()
            .ok_or_else(|| invalid("distributed rewrite no-op has no write session"))
    }
}

/// A rewrite republishes the target's own data columns, so its input is the
/// table's schema. It is a data write: `flavor.rs` refuses a row-level delete
/// input for a rewrite, and the provider derives one branch per frozen group
/// from the loaded table rather than from anything named here.
fn rewrite_begin_request(
    table: &novarocks_spi::connector::ConnectorTableMetadata,
    context: ConnectorRequestContext,
) -> Result<novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest, String> {
    use novarocks_spi::connector::{
        ConnectorWriteAdmissionPurpose, ConnectorWriteFieldRequest, ConnectorWriteInputRequest,
        ConnectorWriteIntent, ConnectorWriteTargetRef,
    };

    Ok(
        novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest {
            table: Arc::from(
                format!("{}.{}", table.identity.namespace, table.identity.table).as_str(),
            ),
            target_ref: ConnectorWriteTargetRef::parse("main")
                .map_err(|error| format!("validate rewrite write target ref: {error}"))?,
            intent: ConnectorWriteIntent::Overwrite,
            // A rewrite is arbitrated by the provider's ordinary base-state
            // compare and swap, so it presents as ordinary DML. What makes it
            // a rewrite is the flavor; saying it twice here would let the two
            // disagree.
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            input: ConnectorWriteInputRequest::Data {
                fields: table
                    .schema
                    .fields()
                    .iter()
                    .map(|field| ConnectorWriteFieldRequest::new(field.as_ref().clone()))
                    .collect(),
            },
            base: None,
            flavor: novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::DistributedRewrite,
            context,
        },
    )
}

/// Which logical write target one frozen group writes to.
///
/// The ordinal is the group's position in the frozen plan, which is the order
/// the session sealed its targets in. A cohort id this plan never froze has
/// none, rather than falling through to a neighbour's writer.
fn rewrite_group_ordinal(
    plan: &ConnectorDistributedRewritePlan,
    cohort_id: ConnectorWriteCohortId,
) -> Result<WriteTargetOrdinal, ConnectorError> {
    let position = plan
        .cohorts()
        .iter()
        .position(|cohort| cohort.cohort_id() == cohort_id)
        .ok_or_else(|| invalid("distributed rewrite group is not part of the frozen plan"))?;
    let ordinal = u32::try_from(position)
        .map_err(|_| invalid("distributed rewrite group ordinal space exhausted"))?;
    WriteTargetOrdinal::try_new(ordinal)
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogProperties, CatalogProperty, CatalogProviderKind, CatalogVersion,
        ConnectorCancellation, ConnectorControlBinding, ConnectorDistributedRewrite,
        ConnectorDistributedRewriteCohortPlan, ConnectorDistributedRewritePlanSummary,
        ConnectorDistributedRewritePlanningRequest, ConnectorExecutionDistribution,
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorMetadata,
        ConnectorProviderBinding, ConnectorProviderBindingKey, ConnectorProviderId,
        ConnectorScanPlanning, ConnectorTableHandle, ConnectorWriteActivationIntent,
        ConnectorWriteActivationRequest, ConnectorWriteActivationSource, ConnectorWriteBaseVersion,
        ConnectorWriteCohortId, ConnectorWriteControl, ConnectorWriteFieldBinding,
        ConnectorWriteFieldToken, ConnectorWriteInputShape, ConnectorWriteIntent,
        ConnectorWritePlan, ConnectorWritePlanningRequest, ConnectorWritePreparation,
        ProviderBindingEpoch,
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

    fn catalog_properties(instance: ConnectorInstanceId) -> CatalogProperties {
        CatalogProperties::new(
            CatalogHandle::new(instance, CatalogVersion::from_bytes([3; 32])),
            CatalogProviderKind::Iceberg,
            1,
            vec![
                CatalogProperty::new("warehouse", "s3://rewrite-session").expect("valid warehouse"),
            ],
            Vec::new(),
        )
        .expect("valid catalog properties")
    }

    fn preparation(
        owner: ConnectorProviderBindingKey,
        table: ConnectorTableHandle,
        schema: &arrow::datatypes::SchemaRef,
    ) -> ConnectorWritePreparation {
        let fields = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                ConnectorWriteFieldBinding::new(
                    ConnectorWriteFieldToken::from_bytes([index as u8 + 1; 32]),
                    field.as_ref().clone(),
                )
            })
            .collect();
        ConnectorWritePreparation::try_new(
            owner,
            table,
            novarocks_spi::connector::ConnectorWriteTargetRef::main(),
            ConnectorWriteIntent::Overwrite,
            ConnectorWriteBaseVersion::try_new(Bytes::from_static(b"base")).unwrap(),
            ConnectorWriteInputShape::Data { fields },
            Bytes::from_static(b"prepared"),
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
        key: ConnectorProviderBindingKey,
    }

    impl ConnectorDistributedRewrite for TestRewrite {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn binding_key(&self) -> &ConnectorProviderBindingKey {
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
            plan: &ConnectorDistributedRewritePlan,
            context: ConnectorRequestContext,
        ) -> Result<novarocks_spi::connector::ConnectorWriteActivation, ConnectorError> {
            let source = plan.cohorts().first().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "test rewrite activation requires a cohort",
                )
            })?;
            novarocks_spi::connector::ConnectorWriteActivation::try_new(
                self.key.clone(),
                &ConnectorWriteActivationRequest {
                    operation_id: plan.operation_id(),
                    source: ConnectorWriteActivationSource::Prepared(source.preparation().clone()),
                    intent: ConnectorWriteActivationIntent::Ordinary,
                    context,
                },
                plan.cohorts()
                    .iter()
                    .map(|cohort| (cohort.cohort_id(), cohort.preparation().clone()))
                    .collect(),
            )
        }
        fn checkpoint_attempt(
            &self,
            _plan: &ConnectorDistributedRewritePlan,
            _disposition: novarocks_spi::connector::ConnectorDistributedRewriteAttemptDisposition,
            _completion: &novarocks_spi::connector::ConnectorWriteAttemptCompletion,
        ) -> Result<
            novarocks_spi::connector::ConnectorDistributedRewriteAttemptCheckpoint,
            ConnectorError,
        > {
            unreachable!()
        }
        fn restore_attempt(
            &self,
            _plan: &ConnectorDistributedRewritePlan,
            _checkpoint: &novarocks_spi::connector::ConnectorDistributedRewriteAttemptCheckpoint,
        ) -> Result<novarocks_spi::connector::ConnectorWriteAttemptCompletion, ConnectorError>
        {
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
        key: ConnectorProviderBindingKey,
    }
    impl ConnectorWriteControl for TestWrite {
        fn binding_key(&self) -> &ConnectorProviderBindingKey {
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
        key: ConnectorProviderBindingKey,
    }
    impl ConnectorExecutionDistribution for TestDistribution {
        fn declaration(
            &self,
            _context: &ConnectorRequestContext,
        ) -> Result<ConnectorProviderBinding, ConnectorError> {
            ConnectorProviderBinding::iceberg(
                self.descriptor.instance_id.as_str(),
                self.key.incarnation.to_bytes(),
                "test",
            )
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
            })
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
        let key = ConnectorProviderBindingKey {
            instance_id: instance.clone(),
            incarnation: ProviderBindingEpoch::from_bytes([7; 16]),
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
                    novarocks_spi::connector::ConnectorRewriteCohortRead::DeleteArtifactGroup(
                        novarocks_spi::connector::ConnectorFrozenRewriteGroup::try_new(
                            "db",
                            "orders",
                            "s3://warehouse/db/orders/_rewrite/0199",
                            digest,
                        )
                        .unwrap(),
                    ),
                    schema.clone(),
                    [3; 32],
                    preparation(key.clone(), table.clone(), &schema),
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
            novarocks_spi::connector::ConnectorControlRuntimeId::from_bytes([7; 16]),
            key.incarnation,
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
                    .unwrap()
                    .with_catalog_properties(catalog_properties(instance.clone()))
                    .expect("control binding has catalog execution properties"),
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
    fn every_frozen_group_maps_to_its_own_dense_write_target_ordinal() {
        let (plan, _lease) = fixture(3);
        let ordinals = plan
            .cohorts()
            .iter()
            .map(|cohort| {
                rewrite_group_ordinal(&plan, cohort.cohort_id())
                    .expect("a frozen group has an ordinal")
                    .get()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            ordinals,
            vec![0, 1, 2],
            "a group's ordinal is its position in the frozen plan, which is the order the \
             session seals its targets in"
        );
    }

    #[test]
    fn a_group_the_plan_never_froze_has_no_write_target() {
        let (plan, _lease) = fixture(2);
        let (foreign, _) = fixture(1);
        let foreign_id = foreign.cohorts()[0].cohort_id();
        let error = rewrite_group_ordinal(&plan, foreign_id)
            .expect_err("a foreign group must not resolve to a neighbour's writer");
        assert!(
            error.to_string().contains("not part of the frozen plan"),
            "the refusal must name what it found: {error}"
        );
    }

    #[test]
    fn empty_plan_is_noop_without_writer_session() {
        let (plan, lease) = fixture(0);
        let session = ConnectorDistributedRewriteSession::noop(plan, lease).unwrap();
        assert!(session.is_noop());
        assert!(session.write_session().is_none());
    }

    #[test]
    fn a_plan_with_frozen_groups_is_refused_as_a_noop() {
        let (plan, lease) = fixture(1);
        let Err(error) = ConnectorDistributedRewriteSession::noop(plan, lease) else {
            panic!("a plan that froze a group must open a write session");
        };
        assert!(error.to_string().contains("not a no-op"), "{error}");
    }
}
