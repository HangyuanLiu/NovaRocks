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

//! The frontend-only write session.
//!
//! One `begin_write` on one exact control generation returns the commit handle
//! and the complete set of logical writer recipes the sealed plan may use.
//! Everything else about a distributed write hangs off that: the plan encodes
//! the recipes, the backends execute them, and this session -- and only this
//! session -- can turn the result into an external commit.
//!
//! What it deliberately is not: there is no operation id, no cohort, no
//! execution attempt, no expected-physical-writer manifest, and no report
//! coverage. Those existed because a writer handle used to be bound to a
//! placement. It no longer is, so completeness comes from the execution graph
//! closing rather than from a pre-enumerated identity tree.
//!
//! The terminal decision is single-shot. A session that has committed cannot
//! abort, one that has aborted cannot commit, and the invocation counter makes
//! "the connector was never asked to commit" an assertable fact rather than an
//! inference.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use novarocks_proto_models::connector_write as write_dto;
use novarocks_spi::connector::write_stack::{
    ConnectorPreparedWriteSet, ConnectorWriteBeginRequest, ConnectorWriteFinishRequest,
    ConnectorWriteSessionAbortRequest, ConnectorWriteSessionPlan,
    ConnectorWriteSessionReconcileRequest, ConnectorWriteTargetPlan, PreparedWriteSetLedger,
    UniqueWriterHandleLedger, WriteRowCountAccumulator, WriteTargetOrdinal,
};
use novarocks_spi::connector::{
    CatalogHandle, ConnectorError, ConnectorErrorKind, ConnectorRequestContext,
    ConnectorWriteAbortOutcome, ConnectorWriteReceipt, ExternalMutationEvidence,
    ExternalMutationOutcome,
};

use crate::connector::control_host::ConnectorWriteStackLease;
use crate::native::fragment_encoder::plan::write_dataflow::SealedWriteTargets;
use crate::query_execution::write_result::DecodedPreparedWriteSet;

/// What a session has already decided. Recorded so a second, different
/// decision is refused rather than silently issuing two external effects.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TerminalDecision {
    Committed,
    Aborted,
    /// The commit may or may not have taken effect externally. Neither a
    /// retry nor an abort is safe from here; only reconcile is.
    CommitUnknown,
}

/// One distributed write's frontend session.
pub(crate) struct ConnectorWriteSession {
    lease: ConnectorWriteStackLease,
    plan: ConnectorWriteSessionPlan,
    /// The catalog runtime this session's writers execute against, kept whole
    /// rather than reduced to its handle: the backend leases a catalog from its
    /// properties, and a writer node that named a handle the query never leased
    /// cannot resolve a write runtime on the backend at all.
    catalog_properties: novarocks_spi::connector::CatalogProperties,
    accumulated: Mutex<AccumulatedWriteSet>,
    terminal: Mutex<Option<TerminalDecision>>,
    finish_invocations: AtomicUsize,
}

/// What a session has collected so far across the queries it drives.
///
/// Most writes are one query, but a copy-on-write mutation and a distributed
/// rewrite drive several against one session and commit once at the end. Each
/// query still produces a set that is complete for its own execution graph;
/// what accumulates here is the statement's union.
///
/// The frozen budgets are charged on this union rather than per query. Charging
/// them per query would let a statement hold an unbounded amount before commit
/// while every individual query looked well inside its limit -- and the limits
/// exist to bound exactly what the frontend holds.
#[derive(Default)]
struct AccumulatedWriteSet {
    rows: WriteRowCountAccumulator,
    ledger: PreparedWriteSetLedger,
    fragments: Vec<(WriteTargetOrdinal, Vec<u8>)>,
}

impl ConnectorWriteSession {
    /// Admit the write and freeze its recipes. On return either a session
    /// exists and nothing external has happened yet, or an error was raised and
    /// nothing was started.
    pub(crate) fn begin(
        lease: ConnectorWriteStackLease,
        catalog_properties: novarocks_spi::connector::CatalogProperties,
        request: ConnectorWriteBeginRequest,
    ) -> Result<Self, ConnectorError> {
        let plan = lease.session().begin_write(request)?;
        Ok(Self {
            lease,
            plan,
            catalog_properties,
            accumulated: Mutex::new(AccumulatedWriteSet::default()),
            terminal: Mutex::new(None),
            finish_invocations: AtomicUsize::new(0),
        })
    }

    /// The sealed ordinal set a prepared write set may not exceed.
    pub(crate) fn expected_targets(&self) -> Vec<WriteTargetOrdinal> {
        self.plan.expected_targets()
    }

    pub(crate) fn targets(&self) -> &[ConnectorWriteTargetPlan] {
        self.plan.targets()
    }

    /// How many times this session actually asked the connector to commit.
    ///
    /// The dual barrier's whole point is that some outcomes must leave this at
    /// zero, and "zero" is only meaningful if it is observable.
    pub(crate) fn finish_invocations(&self) -> usize {
        self.finish_invocations.load(Ordering::SeqCst)
    }

    /// Encode every logical recipe once and charge the query's unique-handle
    /// budget.
    ///
    /// A recipe is charged per logical target, not per placement: copying the
    /// same canonical bytes to more backends causes no additional provider
    /// planning, so charging per copy would refuse writes that cost nothing
    /// extra to plan.
    /// The catalog this write executes against, as the backend must materialize
    /// it. It belongs in the query's Init catalog set beside every typed read's.
    pub(crate) const fn catalog_properties(&self) -> &novarocks_spi::connector::CatalogProperties {
        &self.catalog_properties
    }

    pub(crate) fn seal_write_targets(&self) -> Result<SealedWriteTargets, ConnectorError> {
        let encoder = self.lease.handle_encoder();
        let mut ledger = UniqueWriterHandleLedger::new();
        let mut handles = std::collections::BTreeMap::new();
        for target in self.plan.targets() {
            let encoded = encoder
                .encode_writer_handle(target.handle())
                .map_err(|error| {
                    ConnectorError::new(ConnectorErrorKind::Internal, error.to_string())
                })?;
            let canonical = encoder
                .canonical_writer_handle_bytes(target.handle())
                .map_err(|error| {
                    ConnectorError::new(ConnectorErrorKind::Internal, error.to_string())
                })?;
            ledger.charge(target.ordinal(), canonical.len())?;
            if handles.insert(target.ordinal().get(), encoded).is_some() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector write session sealed one logical target twice",
                ));
            }
        }
        Ok(SealedWriteTargets::new(
            self.catalog_properties.handle().clone(),
            handles,
        ))
    }

    /// Turn the canonical fragments the backends reported into provider values
    /// this generation owns.
    fn interpret(
        &self,
        prepared: DecodedPreparedWriteSet,
    ) -> Result<ConnectorPreparedWriteSet, ConnectorError> {
        let row_count = prepared.row_count();
        self.interpret_parts(row_count, prepared.into_fragments())
    }

    /// Turn canonical fragments into provider values this generation owns.
    fn interpret_parts(
        &self,
        row_count: u64,
        fragments: Vec<(WriteTargetOrdinal, Vec<u8>)>,
    ) -> Result<ConnectorPreparedWriteSet, ConnectorError> {
        use novarocks_proto_codec::FieldPath;
        use novarocks_proto_codec::connector_write::ValidatedCommitFragment;
        use prost::Message;

        let decoder = self.lease.fragment_decoder();
        let mut decoded = Vec::with_capacity(fragments.len());
        for (index, (target, bytes)) in fragments.into_iter().enumerate() {
            let raw =
                write_dto::ConnectorCommitFragment::decode(bytes.as_slice()).map_err(|error| {
                    ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        format!(
                            "prepared write set fragment {index} is not a commit fragment: {error}"
                        ),
                    )
                })?;
            // Re-validated at the trust boundary even though the producer and
            // the root already did: a frontend that trusted a backend's
            // validation could not notice a backend that got it wrong.
            let validated = ValidatedCommitFragment::parse(
                raw,
                FieldPath::root("prepared_write_set").index(index),
            )
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::CorruptData, error.to_string())
            })?;
            let fragment = decoder
                .decode_commit_fragment(&validated)
                .map_err(|error| {
                    ConnectorError::new(ConnectorErrorKind::CorruptData, error.to_string())
                })?;
            decoded.push((target, fragment));
        }
        ConnectorPreparedWriteSet::try_new(row_count, decoded, &self.expected_targets())
    }

    /// Collect one query's complete prepared write set into this session.
    ///
    /// A statement that drives several queries against one session -- a
    /// copy-on-write mutation, a distributed rewrite -- calls this once per
    /// query and commits once at the end. Each set is complete for its own
    /// execution graph; what accumulates is the statement's union.
    ///
    /// The frozen budgets are charged here, on the union. Charging them per
    /// query would let a statement hold an unbounded amount before commit while
    /// every individual query looked well inside its limit, and the limits
    /// exist to bound exactly what the frontend holds.
    pub(crate) fn accumulate(
        &self,
        prepared: DecodedPreparedWriteSet,
    ) -> Result<(), ConnectorError> {
        // Refuse after a terminal decision: a set arriving then belongs to work
        // this session already answered for.
        if self.lock_terminal()?.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write session already reached a terminal decision",
            ));
        }
        let mut accumulated = self.lock_accumulated()?;
        accumulated.rows.add(prepared.row_count())?;
        for (target, bytes) in prepared.into_fragments() {
            accumulated.ledger.reserve_fragment(bytes.len())?;
            accumulated.fragments.push((target, bytes));
        }
        Ok(())
    }

    /// Perform the one external commit over everything accumulated so far.
    ///
    /// The caller must already have established BOTH halves of the barrier for
    /// every query it drove: a complete prepared write set each time, and a
    /// lifecycle terminal set in which every participant succeeded. Neither
    /// implies the other, so neither is checked here -- this method exists to be
    /// un-callable until both hold.
    pub(crate) fn finish_accumulated(
        &self,
        context: ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        self.claim_terminal(TerminalDecision::Committed)?;
        let (row_count, fragments) = {
            let mut accumulated = self.lock_accumulated()?;
            (
                accumulated.rows.get(),
                std::mem::take(&mut accumulated.fragments),
            )
        };
        let prepared = self.interpret_parts(row_count, fragments)?;
        self.finish_invocations.fetch_add(1, Ordering::SeqCst);
        let outcome = self
            .lease
            .session()
            .finish_write(ConnectorWriteFinishRequest {
                commit: self.plan.commit_handle(),
                prepared,
                context,
            })?;
        if matches!(outcome, ExternalMutationOutcome::CommitUnknown { .. }) {
            self.record_terminal(TerminalDecision::CommitUnknown);
        }
        Ok(outcome)
    }

    /// The rows accumulated so far. Report them to a client only after the
    /// external commit is known to have succeeded.
    pub(crate) fn accumulated_row_count(&self) -> Result<u64, ConnectorError> {
        Ok(self.lock_accumulated()?.rows.get())
    }

    /// Accumulate one query's set and commit immediately. The shape almost
    /// every write has.
    pub(crate) fn finish(
        &self,
        prepared: DecodedPreparedWriteSet,
        context: ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        self.accumulate(prepared)?;
        self.finish_accumulated(context)
    }

    #[expect(
        dead_code,
        reason = "Retained beside finish_accumulated until every caller moves to the session."
    )]
    fn finish_single(
        &self,
        prepared: DecodedPreparedWriteSet,
        context: ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        self.claim_terminal(TerminalDecision::Committed)?;
        let prepared = self.interpret(prepared)?;
        self.finish_invocations.fetch_add(1, Ordering::SeqCst);
        let outcome = self
            .lease
            .session()
            .finish_write(ConnectorWriteFinishRequest {
                commit: self.plan.commit_handle(),
                prepared,
                context,
            })?;
        if matches!(outcome, ExternalMutationOutcome::CommitUnknown { .. }) {
            self.record_terminal(TerminalDecision::CommitUnknown);
        }
        Ok(outcome)
    }

    /// Release a session that never reached a complete prepared write set.
    pub(crate) fn abort(
        &self,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
        self.claim_terminal(TerminalDecision::Aborted)?;
        self.lease
            .session()
            .abort_write(ConnectorWriteSessionAbortRequest {
                commit: self.plan.commit_handle(),
                context,
            })
    }

    /// Resolve a commit whose external outcome is unknown. Only reachable after
    /// a commit that reported exactly that.
    pub(crate) fn reconcile(
        &self,
        evidence: ExternalMutationEvidence,
        context: ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        {
            let terminal = self.lock_terminal()?;
            if *terminal != Some(TerminalDecision::CommitUnknown) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector write session has no unknown commit outcome to reconcile",
                ));
            }
        }
        self.lease
            .session()
            .reconcile_write(ConnectorWriteSessionReconcileRequest {
                commit: self.plan.commit_handle(),
                evidence,
                context,
            })
    }

    fn lock_accumulated(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, AccumulatedWriteSet>, ConnectorError> {
        self.accumulated.lock().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "connector write session accumulation is poisoned",
            )
        })
    }

    fn claim_terminal(&self, decision: TerminalDecision) -> Result<(), ConnectorError> {
        let mut terminal = self.lock_terminal()?;
        match *terminal {
            None => {
                *terminal = Some(decision);
                Ok(())
            }
            Some(existing) if existing == decision => Ok(()),
            Some(existing) => Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!(
                    "connector write session already reached {existing:?} and cannot also reach {decision:?}"
                ),
            )),
        }
    }

    fn record_terminal(&self, decision: TerminalDecision) {
        if let Ok(mut terminal) = self.terminal.lock() {
            *terminal = Some(decision);
        }
    }

    fn lock_terminal(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, Option<TerminalDecision>>, ConnectorError> {
        self.terminal.lock().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "connector write session terminal state is poisoned",
            )
        })
    }
}

/// Open one distributed write's session on a write stack already pinned to the
/// generation that planned it.
///
/// The catalog identity comes from the same generation's write lease rather
/// than from a fresh catalog resolution: the recipes the session seals are
/// stamped into fragments beside this handle, and a handle from a different
/// incarnation would name a runtime that never admitted them.
pub(crate) fn begin_connector_write_session(
    lease: ConnectorWriteStackLease,
    write_lease: &novarocks_spi::connector::ConnectorWriteLease,
    request: ConnectorWriteBeginRequest,
) -> Result<std::sync::Arc<ConnectorWriteSession>, String> {
    let catalog_properties = write_lease.catalog_properties().cloned().ok_or_else(|| {
        "connector write lease has no immutable catalog runtime identity".to_string()
    })?;
    ConnectorWriteSession::begin(lease, catalog_properties, request)
        .map(std::sync::Arc::new)
        .map_err(|error| format!("begin connector write session: {error}"))
}

/// The external commit of one completed write session, and the rows it made
/// visible.
///
/// `affected_rows` exists only on `KnownCommitted`. That is the whole point of
/// the type: the row count is known as soon as the data plane closes, but
/// reporting it to a client before the commit succeeded would name rows that
/// may never become visible -- and on `CommitUnknown` nobody yet knows whether
/// they did.
pub(crate) struct CommittedWriteSession {
    outcome: ExternalMutationOutcome<ConnectorWriteReceipt>,
    affected_rows: Option<u64>,
}

impl CommittedWriteSession {
    /// The rows a client may be told about, present only after a commit that
    /// is known to have succeeded.
    pub(crate) const fn affected_rows(&self) -> Option<u64> {
        self.affected_rows
    }

    pub(crate) fn into_outcome(self) -> ExternalMutationOutcome<ConnectorWriteReceipt> {
        self.outcome
    }
}

/// Perform the one external commit for a write whose data plane closed and
/// whose execution succeeded, then gate its affected-row count on the result.
pub(crate) fn finish_write_session(
    completion: crate::query_execution::outcome::ConnectorWriteSessionCompletion,
    context: ConnectorRequestContext,
) -> Result<CommittedWriteSession, ConnectorError> {
    let row_count = completion.row_count();
    let (session, prepared) = completion.into_parts();
    let outcome = session.finish(prepared, context)?;
    let affected_rows =
        matches!(outcome, ExternalMutationOutcome::KnownCommitted { .. }).then_some(row_count);
    Ok(CommittedWriteSession {
        outcome,
        affected_rows,
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use novarocks_connector_binding::ConnectorControlWriteBinding;
    use novarocks_proto_codec::connector_write::{
        ConnectorWriteCodecError, ConnectorWriteFragmentDecoder, ConnectorWriteHandleEncoder,
        ValidatedCommitFragment,
    };
    use novarocks_spi::connector::write_stack::{
        ConnectorCommitFragment, ConnectorWriterHandle, MAX_CONNECTOR_UNIQUE_WRITER_HANDLE_BYTES,
        ProviderWriteRuntime, WriteRuntimeAdapter,
    };
    use novarocks_spi::connector::{
        CatalogVersion, ConnectorInstanceDescriptor, ConnectorInstanceId,
        ConnectorProviderBindingKey, ConnectorProviderId,
        ConnectorWriteControl as LegacyWriteControl,
    };

    use super::*;

    // ---- a minimal provider whose only job is to be recoverable -----------

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct FakeCommit;
    #[derive(Clone, Debug, Eq, PartialEq)]
    struct FakeHandle(u32);
    #[derive(Clone, Debug, Eq, PartialEq)]
    struct FakeFragment(u32);

    struct FakeProvider {
        descriptor: ConnectorInstanceDescriptor,
        catalog_handle: CatalogHandle,
    }

    impl ProviderWriteRuntime for FakeProvider {
        type CommitHandle = FakeCommit;
        type WriterHandle = FakeHandle;
        type CommitFragment = FakeFragment;

        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn catalog_handle(&self) -> &CatalogHandle {
            &self.catalog_handle
        }
    }

    fn catalog_handle() -> CatalogHandle {
        CatalogHandle::new(
            ConnectorInstanceId::parse("write_session_unit").expect("instance id"),
            CatalogVersion::from_bytes([3; 32]),
        )
    }

    fn catalog_properties() -> novarocks_spi::connector::CatalogProperties {
        novarocks_spi::connector::CatalogProperties::new(
            catalog_handle(),
            novarocks_spi::connector::CatalogProviderKind::Iceberg,
            1,
            Vec::new(),
            Vec::new(),
        )
        .expect("test catalog properties")
    }

    fn adapter() -> WriteRuntimeAdapter<FakeProvider> {
        let handle = catalog_handle();
        WriteRuntimeAdapter::new(Arc::new(FakeProvider {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("fake").expect("provider id"),
                instance_id: handle.catalog_name().clone(),
            },
            catalog_handle: handle,
        }))
    }

    // ---- the session control under test ----------------------------------

    #[derive(Default)]
    pub(crate) struct Recorded {
        pub(crate) finish: usize,
        pub(crate) abort: usize,
        pub(crate) reconcile: usize,
    }

    struct FakeSession {
        adapter: WriteRuntimeAdapter<FakeProvider>,
        binding_key: ConnectorProviderBindingKey,
        targets: usize,
        recorded: Arc<Mutex<Recorded>>,
        finish_outcome: Mutex<Option<ExternalMutationOutcome<ConnectorWriteReceipt>>>,
    }

    impl novarocks_spi::connector::write_stack::ConnectorWriteControl for FakeSession {
        fn binding_key(&self) -> &ConnectorProviderBindingKey {
            &self.binding_key
        }

        fn begin_write(
            &self,
            _request: ConnectorWriteBeginRequest,
        ) -> Result<ConnectorWriteSessionPlan, ConnectorError> {
            let commit = self.adapter.wrap_commit_handle(FakeCommit);
            let targets = (0..self.targets)
                .map(|index| {
                    let ordinal = WriteTargetOrdinal::try_new(
                        u32::try_from(index).expect("bounded ordinal"),
                    )?;
                    let handle = self.adapter.wrap_writer_handle(FakeHandle(ordinal.get()));
                    Ok(ConnectorWriteTargetPlan::new(
                        ordinal,
                        handle,
                        novarocks_spi::connector::ConnectorWriteInputShape::Data {
                            fields: vec![
                                novarocks_spi::connector::ConnectorWriteFieldBinding::new(
                                    novarocks_spi::connector::ConnectorWriteFieldToken::from_bytes(
                                        [1; 32],
                                    ),
                                    arrow::datatypes::Field::new(
                                        "v",
                                        arrow::datatypes::DataType::Int64,
                                        true,
                                    ),
                                ),
                            ],
                        },
                    ))
                })
                .collect::<Result<Vec<_>, ConnectorError>>()?;
            ConnectorWriteSessionPlan::try_new(commit, targets)
        }

        fn finish_write(
            &self,
            _request: ConnectorWriteFinishRequest<'_>,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
            self.recorded.lock().expect("recorded").finish += 1;
            self.finish_outcome
                .lock()
                .expect("outcome")
                .take()
                .ok_or_else(|| {
                    ConnectorError::new(ConnectorErrorKind::Internal, "no scripted outcome")
                })
        }

        fn abort_write(
            &self,
            _request: ConnectorWriteSessionAbortRequest<'_>,
        ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
            self.recorded.lock().expect("recorded").abort += 1;
            Ok(ConnectorWriteAbortOutcome::KnownUncommitted {
                cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
            })
        }

        fn reconcile_write(
            &self,
            _request: ConnectorWriteSessionReconcileRequest<'_>,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
            self.recorded.lock().expect("recorded").reconcile += 1;
            Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "reconcile outcome is not scripted in this test",
            ))
        }
    }

    // ---- codec facets -----------------------------------------------------

    struct FakeEncoder {
        payload_bytes: usize,
    }

    impl ConnectorWriteHandleEncoder for FakeEncoder {
        fn owner(&self) -> &str {
            "fake"
        }

        fn encode_writer_handle(
            &self,
            _handle: &ConnectorWriterHandle,
        ) -> Result<write_dto::ConnectorWriterHandle, ConnectorWriteCodecError> {
            Ok(write_dto::ConnectorWriterHandle {
                handle: Some(write_dto::connector_writer_handle::Handle::Iceberg(
                    write_dto::IcebergWriterHandle {
                        branch: write_dto::IcebergWriteBranch::Data as i32,
                        table: Some(write_dto::IcebergWriteTableFacts {
                            table_uuid: "u".repeat(self.payload_bytes),
                            ..Default::default()
                        }),
                        output: None,
                        data: None,
                        old_deletes: std::collections::BTreeMap::new(),
                    },
                )),
            })
        }
    }

    struct FakeDecoder {
        adapter: WriteRuntimeAdapter<FakeProvider>,
    }

    impl ConnectorWriteFragmentDecoder for FakeDecoder {
        fn owner(&self) -> &str {
            "fake"
        }

        fn decode_commit_fragment(
            &self,
            _fragment: &ValidatedCommitFragment,
        ) -> Result<ConnectorCommitFragment, ConnectorWriteCodecError> {
            Ok(self.adapter.wrap_commit_fragment(FakeFragment(0)))
        }
    }

    struct UnusedLegacyControl;

    impl LegacyWriteControl for UnusedLegacyControl {
        fn binding_key(&self) -> &ConnectorProviderBindingKey {
            unreachable!("the legacy control is not exercised by the write session")
        }

        fn plan_write(
            &self,
            _request: novarocks_spi::connector::ConnectorWritePlanningRequest,
        ) -> Result<novarocks_spi::connector::ConnectorWritePlan, ConnectorError> {
            unreachable!("the legacy control is not exercised by the write session")
        }

        fn commit(
            &self,
            _request: novarocks_spi::connector::ConnectorWriteCommitRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
            unreachable!("the legacy control is not exercised by the write session")
        }

        fn abort(
            &self,
            _request: novarocks_spi::connector::ConnectorWriteAbortRequest,
        ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
            unreachable!("the legacy control is not exercised by the write session")
        }

        fn reconcile(
            &self,
            _request: novarocks_spi::connector::ConnectorWriteReconcileRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
            unreachable!("the legacy control is not exercised by the write session")
        }
    }

    pub(crate) struct Fixture {
        pub(crate) session: Arc<ConnectorWriteSession>,
        pub(crate) recorded: Arc<Mutex<Recorded>>,
    }

    fn fixture(targets: usize, payload_bytes: usize) -> Fixture {
        fixture_with_outcome(
            targets,
            payload_bytes,
            ExternalMutationOutcome::KnownUncommitted {
                failure: novarocks_spi::connector::ConnectorMutationFailure::new(
                    novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable,
                    "scripted",
                ),
            },
        )
    }

    /// A write session on a scripted control, for tests in the statement flows
    /// that need a real session but no real provider.
    pub(crate) fn fixture_with_outcome(
        targets: usize,
        payload_bytes: usize,
        outcome: ExternalMutationOutcome<ConnectorWriteReceipt>,
    ) -> Fixture {
        let adapter = adapter();
        let binding_key = ConnectorProviderBindingKey {
            instance_id: catalog_handle().catalog_name().clone(),
            incarnation: novarocks_spi::connector::ProviderBindingEpoch::new(),
        };
        let recorded = Arc::new(Mutex::new(Recorded::default()));
        let session_control = Arc::new(FakeSession {
            adapter: adapter.clone(),
            binding_key,
            targets,
            recorded: Arc::clone(&recorded),
            finish_outcome: Mutex::new(Some(outcome)),
        });
        let group = ConnectorControlWriteBinding::new(
            Arc::new(UnusedLegacyControl),
            session_control,
            Arc::new(FakeEncoder { payload_bytes }),
            Arc::new(FakeDecoder { adapter }),
        );
        let lease = ConnectorWriteStackLease::new(
            novarocks_spi::connector::ConnectorControlRuntimeId::new(),
            group,
            || {},
        );
        let session = Arc::new(
            ConnectorWriteSession::begin(lease, catalog_properties(), begin_request())
                .expect("begin write"),
        );
        Fixture { session, recorded }
    }

    fn begin_request() -> ConnectorWriteBeginRequest {
        ConnectorWriteBeginRequest {
            table: Arc::from("db.t"),
            target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::main(),
            intent: novarocks_spi::connector::ConnectorWriteIntent::Append,
            purpose: novarocks_spi::connector::ConnectorWriteAdmissionPurpose::OrdinaryDml,
            input: novarocks_spi::connector::ConnectorWriteInputRequest::Data {
                fields: vec![novarocks_spi::connector::ConnectorWriteFieldRequest::new(
                    arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, true),
                )],
            },
            base: None,
            flavor: novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::Ordinary,
            context: request_context(),
        }
    }

    pub(crate) fn request_context() -> ConnectorRequestContext {
        struct NotCancelled;
        impl novarocks_spi::connector::ConnectorCancellation for NotCancelled {
            fn is_cancelled(&self) -> bool {
                false
            }
        }
        ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(60),
            Arc::new(NotCancelled),
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("request context")
    }

    /// A real canonical commit fragment. The session decodes what it commits,
    /// so a test that fed it arbitrary bytes would never reach the connector.
    fn fragment_bytes(path: &str) -> Vec<u8> {
        use prost::Message;
        write_dto::ConnectorCommitFragment {
            fragment: Some(write_dto::connector_commit_fragment::Fragment::Iceberg(
                write_dto::IcebergCommitFragment {
                    artifact: Some(write_dto::iceberg_commit_fragment::Artifact::DataFile(
                        write_dto::IcebergDataFileArtifact {
                            path: path.to_string(),
                            file_format: write_dto::IcebergFileFormat::Parquet as i32,
                            partition: Some(write_dto::IcebergArtifactPartition {
                                partition_path: String::new(),
                                null_fingerprint: String::new(),
                                partition_spec_id: 0,
                                descriptor: Some(write_dto::IcebergPartitionDescriptor {
                                    values: Vec::new(),
                                }),
                            }),
                            metrics: Some(write_dto::IcebergArtifactMetrics {
                                record_count: 1,
                                file_size_in_bytes: 16,
                                split_offsets: Vec::new(),
                                column_stats: None,
                            }),
                            first_row_id: None,
                        },
                    )),
                },
            )),
        }
        .encode_to_vec()
    }

    pub(crate) fn empty_prepared() -> DecodedPreparedWriteSet {
        DecodedPreparedWriteSet::for_test(0, Vec::new())
    }

    fn evidence() -> ExternalMutationEvidence {
        ExternalMutationEvidence::try_new(
            1,
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("fake").expect("provider id"),
                instance_id: catalog_handle().catalog_name().clone(),
            },
            novarocks_spi::connector::ProviderBindingEpoch::new(),
            novarocks_spi::connector::ConnectorMutationOperationId::new(),
            "write",
            bytes::Bytes::new(),
        )
        .expect("evidence")
    }

    /// Bytes a producer could actually emit, so a test that hands a fragment
    /// to a session exercises the real decode path rather than a stub.
    pub(crate) fn commit_fragment_bytes() -> Vec<u8> {
        use prost::Message;
        write_dto::ConnectorCommitFragment {
            fragment: Some(write_dto::connector_commit_fragment::Fragment::Iceberg(
                write_dto::IcebergCommitFragment {
                    artifact: Some(write_dto::iceberg_commit_fragment::Artifact::DataFile(
                        write_dto::IcebergDataFileArtifact {
                            path: "s3://bucket/db/t/data/new.parquet".to_string(),
                            file_format: write_dto::IcebergFileFormat::Parquet as i32,
                            partition: Some(write_dto::IcebergArtifactPartition {
                                partition_path: String::new(),
                                null_fingerprint: String::new(),
                                partition_spec_id: 0,
                                descriptor: Some(write_dto::IcebergPartitionDescriptor {
                                    values: Vec::new(),
                                }),
                            }),
                            metrics: Some(write_dto::IcebergArtifactMetrics {
                                record_count: 7,
                                file_size_in_bytes: 128,
                                split_offsets: Vec::new(),
                                column_stats: None,
                            }),
                            first_row_id: None,
                        },
                    )),
                },
            )),
        }
        .encode_to_vec()
    }

    pub(crate) fn known_committed() -> ExternalMutationOutcome<ConnectorWriteReceipt> {
        ExternalMutationOutcome::KnownCommitted {
            effect: novarocks_spi::connector::ExternalMutationEffect::Applied,
            receipt: ConnectorWriteReceipt::try_new(bytes::Bytes::from_static(b"receipt"))
                .expect("receipt"),
            finalization: novarocks_spi::connector::ExternalMutationFinalization::Complete,
        }
    }

    pub(crate) fn commit_unknown() -> ExternalMutationOutcome<ConnectorWriteReceipt> {
        ExternalMutationOutcome::CommitUnknown {
            failure: novarocks_spi::connector::ConnectorMutationFailure::new(
                novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable,
                "scripted commit outcome is unknown",
            ),
            evidence: evidence(),
        }
    }

    fn completion(
        session: &Arc<ConnectorWriteSession>,
        row_count: u64,
    ) -> crate::query_execution::outcome::ConnectorWriteSessionCompletion {
        crate::query_execution::outcome::ConnectorWriteSessionCompletion::for_test(
            Arc::clone(session),
            DecodedPreparedWriteSet::for_test(row_count, Vec::new()),
        )
    }

    #[test]
    fn affected_rows_are_reported_only_after_a_known_successful_commit() {
        let fixture = fixture_with_outcome(1, 16, known_committed());
        let committed = finish_write_session(completion(&fixture.session, 7), request_context())
            .expect("finish");

        assert_eq!(committed.affected_rows(), Some(7));
        assert_eq!(fixture.session.finish_invocations(), 1);
        assert_eq!(fixture.recorded.lock().expect("recorded").finish, 1);
    }

    #[test]
    fn a_commit_unknown_outcome_reports_no_affected_rows() {
        // The rows were accepted by every writer, so the count is known -- but
        // whether they became visible is not, and reporting success here would
        // tell a client about rows that may not exist.
        let fixture = fixture_with_outcome(1, 16, commit_unknown());
        let committed = finish_write_session(completion(&fixture.session, 7), request_context())
            .expect("finish");

        assert!(committed.affected_rows().is_none());
        assert!(matches!(
            committed.into_outcome(),
            ExternalMutationOutcome::CommitUnknown { .. }
        ));
        assert_eq!(fixture.session.finish_invocations(), 1);
    }

    #[test]
    fn a_known_uncommitted_commit_reports_no_affected_rows() {
        let fixture = fixture(1, 16);
        let committed = finish_write_session(completion(&fixture.session, 7), request_context())
            .expect("finish");

        assert!(committed.affected_rows().is_none());
        assert!(matches!(
            committed.into_outcome(),
            ExternalMutationOutcome::KnownUncommitted { .. }
        ));
    }

    #[test]
    fn a_session_seals_one_recipe_per_logical_target() {
        let fixture = fixture(3, 16);
        let sealed = fixture
            .session
            .seal_write_targets()
            .expect("sealed targets");
        assert_eq!(sealed.ordinals().collect::<Vec<_>>(), vec![0, 1, 2]);
        assert_eq!(fixture.session.expected_targets().len(), 3);
    }

    #[test]
    fn the_unique_handle_budget_refuses_a_query_whose_recipes_do_not_fit() {
        // Each target's recipe is deliberately enormous, so a handful of
        // logical targets is enough to exceed the whole-query budget.
        let per_handle = 4 * 1024 * 1024;
        let targets = MAX_CONNECTOR_UNIQUE_WRITER_HANDLE_BYTES / per_handle + 1;
        let fixture = fixture(targets, per_handle);
        let error = fixture
            .session
            .seal_write_targets()
            .expect_err("over the unique handle budget");
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    }

    #[test]
    fn a_session_reaches_exactly_one_terminal_decision() {
        let fixture = fixture(1, 16);
        assert_eq!(fixture.session.finish_invocations(), 0);

        let prepared = empty_prepared();
        let _ = fixture.session.finish(prepared, request_context());
        assert_eq!(fixture.session.finish_invocations(), 1);
        assert_eq!(fixture.recorded.lock().expect("recorded").finish, 1);

        // A second, different decision is refused, and the connector is not
        // asked again.
        let error = fixture
            .session
            .abort(request_context())
            .expect_err("abort after commit");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(fixture.recorded.lock().expect("recorded").abort, 0);
    }

    /// The frontend half of the write, composed the way production composes
    /// it: begin, seal the recipes into the plan, read the root result back,
    /// gate on both facts, commit.
    ///
    /// Each piece has its own tests; this one exists because they have to fit
    /// together, and a mismatch between them -- a target the encoder cannot
    /// find, a relation the decoder cannot read, a set the session refuses --
    /// is exactly the kind of defect no single unit test can see.
    #[test]
    fn the_frontend_write_path_composes_from_begin_to_commit() {
        use crate::native::fragment_encoder::plan::write_dataflow::SealedWriteTargets;
        use crate::query_execution::write_barrier::WriteCommitBarrier;

        let fixture = fixture_with_outcome(
            1,
            16,
            ExternalMutationOutcome::KnownUncommitted {
                failure: novarocks_spi::connector::ConnectorMutationFailure::new(
                    novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable,
                    "scripted",
                ),
            },
        );

        // 1. The session seals one recipe per logical target, and the sealed
        //    targets are what the plan encoder consumes.
        let sealed: SealedWriteTargets = fixture
            .session
            .seal_write_targets()
            .expect("sealed targets");
        assert_eq!(sealed.ordinals().collect::<Vec<_>>(), vec![0]);

        // 2. The backends report their fragments through the root relation.
        //    Round-trip a real canonical fragment so the decoder is exercised
        //    against bytes a producer could actually emit.
        let fragment_bytes = {
            use prost::Message;
            write_dto::ConnectorCommitFragment {
                fragment: Some(write_dto::connector_commit_fragment::Fragment::Iceberg(
                    write_dto::IcebergCommitFragment {
                        artifact: Some(write_dto::iceberg_commit_fragment::Artifact::DataFile(
                            write_dto::IcebergDataFileArtifact {
                                path: "s3://bucket/db/t/data/new.parquet".to_string(),
                                file_format: write_dto::IcebergFileFormat::Parquet as i32,
                                partition: Some(write_dto::IcebergArtifactPartition {
                                    partition_path: String::new(),
                                    null_fingerprint: String::new(),
                                    partition_spec_id: 0,
                                    descriptor: Some(write_dto::IcebergPartitionDescriptor {
                                        values: Vec::new(),
                                    }),
                                }),
                                metrics: Some(write_dto::IcebergArtifactMetrics {
                                    record_count: 7,
                                    file_size_in_bytes: 128,
                                    split_offsets: Vec::new(),
                                    column_stats: None,
                                }),
                                first_row_id: None,
                            },
                        )),
                    },
                )),
            }
            .encode_to_vec()
        };
        let prepared = DecodedPreparedWriteSet::for_test(
            7,
            vec![(
                WriteTargetOrdinal::try_new(0).expect("ordinal"),
                fragment_bytes,
            )],
        );

        // 3. Both facts, then and only then the commit.
        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_prepared_write_set(prepared);
        barrier.observe_execution_terminals(true);
        let committable = barrier.into_committable().expect("both facts hold");
        assert_eq!(committable.row_count(), 7);

        let outcome = fixture
            .session
            .finish(committable, request_context())
            .expect("finish reaches the connector");
        assert!(matches!(
            outcome,
            ExternalMutationOutcome::KnownUncommitted { .. }
        ));
        assert_eq!(fixture.session.finish_invocations(), 1);
    }

    /// The same composition, stopped by a failed participant. The connector is
    /// never asked to commit, which is the whole point of splitting the gate.
    #[test]
    fn a_failed_participant_stops_the_composed_path_before_the_connector() {
        use crate::query_execution::write_barrier::WriteCommitBarrier;

        let fixture = fixture(1, 16);
        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_prepared_write_set(empty_prepared());
        barrier.observe_execution_terminals(false);
        assert!(barrier.into_committable().is_err());
        assert_eq!(fixture.session.finish_invocations(), 0);
        assert_eq!(fixture.recorded.lock().expect("recorded").finish, 0);
    }

    /// A copy-on-write mutation and a distributed rewrite drive several
    /// queries against one session and commit once. Each query's set is
    /// complete for its own graph; the statement commits their union.
    #[test]
    fn a_session_commits_the_union_of_every_query_it_drove() {
        let fixture = fixture(1, 16);
        let target = WriteTargetOrdinal::try_new(0).expect("ordinal");

        fixture
            .session
            .accumulate(DecodedPreparedWriteSet::for_test(
                4,
                vec![(target, fragment_bytes("s3://b/a.parquet"))],
            ))
            .expect("first query");
        fixture
            .session
            .accumulate(DecodedPreparedWriteSet::for_test(
                6,
                vec![
                    (target, fragment_bytes("s3://b/b.parquet")),
                    (target, fragment_bytes("s3://b/c.parquet")),
                ],
            ))
            .expect("second query");

        assert_eq!(
            fixture
                .session
                .accumulated_row_count()
                .expect("accumulated rows"),
            10
        );
        assert_eq!(fixture.session.finish_invocations(), 0);

        let _ = fixture.session.finish_accumulated(request_context());
        // One commit for the whole statement, not one per query.
        assert_eq!(fixture.session.finish_invocations(), 1);
        assert_eq!(fixture.recorded.lock().expect("recorded").finish, 1);
    }

    #[test]
    fn the_frozen_budgets_bound_the_union_rather_than_each_query() {
        use novarocks_spi::connector::write_stack::MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES;

        let fixture = fixture(1, 16);
        let target = WriteTargetOrdinal::try_new(0).expect("ordinal");
        // Each query stays far inside the entry budget; together they exceed
        // it. Charging per query would have accepted every one of them.
        let per_query = MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES / 2;
        for _ in 0..2 {
            fixture
                .session
                .accumulate(DecodedPreparedWriteSet::for_test(
                    0,
                    vec![(target, Vec::new()); per_query],
                ))
                .expect("within the union budget");
        }
        let error = fixture
            .session
            .accumulate(DecodedPreparedWriteSet::for_test(
                0,
                vec![(target, Vec::new())],
            ))
            .expect_err("over the union budget");
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    }

    #[test]
    fn a_set_arriving_after_the_terminal_decision_is_refused() {
        let fixture = fixture(1, 16);
        let _ = fixture.session.finish(empty_prepared(), request_context());
        let error = fixture
            .session
            .accumulate(empty_prepared())
            .expect_err("accumulate after terminal");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(fixture.session.finish_invocations(), 1);
    }

    #[test]
    fn reconcile_is_unreachable_until_a_commit_reported_an_unknown_outcome() {
        let fixture = fixture(1, 16);
        let error = fixture
            .session
            .reconcile(evidence(), request_context())
            .expect_err("nothing to reconcile");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(fixture.recorded.lock().expect("recorded").reconcile, 0);
    }
}
