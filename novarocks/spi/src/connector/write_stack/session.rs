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

//! The frontend-only external write session.
//!
//! One `begin_write` atomically returns a frontend-only commit handle and the
//! complete set of logical writer handles the sealed plan may use. There is no
//! separate prepare, activate, or placement-dependent planning step: a writer
//! recipe is a property of the logical target, not of where the plan happens to
//! run.
//!
//! Only the frontend, holding its exact control generation, may finish, abort,
//! or reconcile a write. A backend has no commit handle and no catalog
//! mutation capability at all.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use crate::connector::distributed_rewrite::ConnectorDistributedRewriteShape;
use crate::connector::handle::ConnectorPinnedFileSet;
use crate::connector::row_mutation::{
    ConnectorRowMutationScanBinding, ConnectorRowMutationSelection,
};
use crate::connector::write_stack::prepared::ConnectorPreparedWriteSet;
use crate::connector::write_stack::runtime::{
    ConnectorWriteBinding, ConnectorWriteCommitHandle, ConnectorWriterHandle,
};
use crate::connector::write_stack::target::{WriteTargetOrdinal, validate_dense_target_ordinals};
use crate::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorProviderBindingKey, ConnectorRequestContext,
};
use crate::connector::{
    ConnectorManagedPublicationIntent, ConnectorRowMutationEffect, ConnectorTableHandle,
    ConnectorWriteAbortOutcome, ConnectorWriteAdmissionPurpose, ConnectorWriteBaseVersion,
    ConnectorWriteInputRequest, ConnectorWriteInputShape, ConnectorWriteIntent,
    ConnectorWriteReceipt, ConnectorWriteRouteId, ConnectorWriteTargetRef,
};
use crate::connector::{ConnectorMutationRouteInput, ConnectorWriteFieldToken};
use crate::connector::{ExternalMutationEvidence, ExternalMutationOutcome};

/// The frozen intent a frontend hands to `begin_write`.
///
/// Every fact here is decided before any external write side effect. The
/// provider completes all local and metadata admission inside `begin_write`,
/// so a sealed plan can never discover mid-execution that its write was never
/// admissible.
#[derive(Clone)]
pub struct ConnectorWriteBeginRequest {
    pub table: Arc<str>,
    pub target_ref: ConnectorWriteTargetRef,
    pub intent: ConnectorWriteIntent,
    pub purpose: ConnectorWriteAdmissionPurpose,
    pub input: ConnectorWriteInputRequest,
    pub base: Option<ConnectorWriteBaseVersion>,
    /// What kind of write this is, and the facts only that kind needs.
    pub flavor: ConnectorWriteSessionFlavor,
    pub context: ConnectorRequestContext,
}

/// The write flavors a session admits.
///
/// This selects how the provider plans its logical branches. It is deliberately
/// not a writer identity and carries no operation, cohort, attempt, or
/// placement: two writes of the same flavor against the same table are the same
/// kind of write, and what distinguishes them belongs to whoever owns their
/// external effect.
#[derive(Clone, Debug)]
pub enum ConnectorWriteSessionFlavor {
    /// One logical target writing data.
    Ordinary,
    /// A write into a target the provider has staged but has not registered.
    ///
    /// Every other flavor names its target and lets the provider look it up.
    /// A staged target has no catalog entry to look up -- that is what makes it
    /// staged -- so the caller hands the session the provider-frozen target
    /// facts a catalog load would otherwise have returned. They stay opaque:
    /// this is the same provider-owned table value the staged-create capability
    /// vends, and it names no publication, operation, or attempt.
    StagedCreate(ConnectorTableHandle),
    /// A durable publication whose identity belongs to the upper layer that
    /// owns it. The publication id never reaches a writer recipe, a commit
    /// fragment, or a backend -- but it does reach the snapshot the commit
    /// writes, because the publication fence reads it back off that summary.
    ///
    /// `shape` is declared by the caller and cannot be inferred: an incremental
    /// merge-on-read refresh arrives as a `RowLineage` input with `_file` /
    /// `_pos` identity, which is indistinguishable from an ordinary DML row
    /// mutation by input alone -- and the difference decides whether the commit
    /// is a publication or DML.
    ManagedPublication {
        intent: ConnectorManagedPublicationIntent,
        shape: ConnectorManagedPublicationShape,
    },
    /// A merge-on-read row mutation. The provider decides how many branches
    /// the mutation needs and what each accepts; SQL routes rows to them.
    RowMutation,
    /// A copy-on-write row mutation, which rewrites whole data files.
    ///
    /// It carries the match selection because a copy-on-write session cannot be
    /// planned without it: which files are rewritten, and which rows inside them
    /// were matched, is the materialized result of running the statement's
    /// predicate as a distributed read over the pinned base snapshot. That is a
    /// runtime fact, so no provider-internal freeze can stand in for it -- and
    /// carrying it in the flavor is what makes a session without it
    /// unconstructible rather than merely refused.
    ///
    /// This is a separate flavor from [`Self::RowMutation`] for that reason
    /// alone: the two differ by an input one of them cannot be opened without.
    CopyOnWrite(ConnectorRowMutationSelection),
    /// A rewrite arbitrated by the provider's ordinary base-state compare and
    /// swap rather than by the distributed-write external fence.
    ///
    /// It is a distinct flavor rather than a flag because the difference is not
    /// a tuning knob: a rewrite that took the external fence would serialize
    /// against ordinary DML it does not conflict with, and a DML write that
    /// skipped it would lose the fence's protection.
    ///
    /// It carries the frozen rewrite's shape for the same reason
    /// [`Self::CopyOnWrite`] carries its selection: the provider cuts the same
    /// groups a second time when it seals the session, and which artifacts a
    /// rewrite selected is not a function of the target alone. Reading it off
    /// the writer input instead would make the branch kind a property of what
    /// the caller signed rather than of the operation the frontend froze.
    DistributedRewrite(ConnectorDistributedRewriteShape),
}

/// How a publication's rows reach it.
///
/// This is the branch structure the publication commits, not a tuning knob.
/// The three shapes differ in whether SQL routes change events at all, and in
/// what the commit must supersede.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorManagedPublicationShape {
    /// The publication replaces rows wholesale; SQL sends data rows to one
    /// unrouted branch, and there is no change event to route.
    Data,
    /// The publication applies a change stream that only ever inserts.
    ///
    /// It seals one data branch like [`Self::Data`], but a *routed* one, because
    /// its rows arrive as change events and SQL's change-stream compile requires
    /// every branch to declare which effects it accepts. Nothing is superseded,
    /// so no delete artifact is frozen.
    ///
    /// This is a distinct shape rather than a flag on [`Self::Data`] because the
    /// two produce different plans: one is compiled as an ordinary write, the
    /// other through the change-stream router.
    ///
    /// Note what this does NOT do: an effect no branch accepts is dropped by the
    /// router, not refused, so declaring only Insert does not make "no deletes
    /// can appear" an enforced invariant. It remains the caller's precondition.
    InsertOnlyChangeStream,
    /// The publication applies a change stream that supersedes rows; SQL routes
    /// change events to the branches the provider seals, and the session freezes
    /// the old delete artifacts those branches supersede.
    RowMutation,
}

/// One logical write target and its immutable recipe.
///
/// The same `handle` is copied to every physical writer placement serving this
/// target. The frontend charges the unique-handle budget once per target, not
/// once per copy.
#[derive(Clone, Debug)]
pub struct ConnectorWriteTargetPlan {
    ordinal: WriteTargetOrdinal,
    handle: ConnectorWriterHandle,
    input: ConnectorWriteInputShape,
    route: Option<ConnectorWriteRouteFacts>,
    rewrite_source: Option<ConnectorWriteRewriteSource>,
}

/// What one copy-on-write target must read to produce its rows.
///
/// A copy-on-write branch rewrites whole data files, so its input is not a
/// projection of the statement's source: it re-reads exactly the files this
/// target replaces. The read contract travels with the target because the two
/// are one decision -- a target that replaced one file set while reading
/// another would silently drop or duplicate rows -- and it is provider-owned
/// throughout: the engine passes it to the read side without interpreting it.
#[derive(Clone, Debug)]
pub struct ConnectorWriteRewriteSource {
    source: ConnectorTableHandle,
    /// Exactly the files this target rewrites. Its commit replaces precisely
    /// these, so the read that produces its rows is defined by the same set
    /// rather than by anything re-derived.
    pinned_source: ConnectorPinnedFileSet,
    base_version_digest: [u8; 32],
    scan_schema: SchemaRef,
    scan_bindings: Vec<ConnectorRowMutationScanBinding>,
    match_tokens: Vec<ConnectorWriteFieldToken>,
    written_version_token: Option<ConnectorWriteFieldToken>,
}

impl ConnectorWriteRewriteSource {
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        source: ConnectorTableHandle,
        pinned_source: ConnectorPinnedFileSet,
        base_version_digest: [u8; 32],
        scan_schema: SchemaRef,
        scan_bindings: Vec<ConnectorRowMutationScanBinding>,
        match_tokens: Vec<ConnectorWriteFieldToken>,
        written_version_token: Option<ConnectorWriteFieldToken>,
    ) -> Self {
        Self {
            source,
            pinned_source,
            base_version_digest,
            scan_schema,
            scan_bindings,
            match_tokens,
            written_version_token,
        }
    }

    pub const fn source(&self) -> &ConnectorTableHandle {
        &self.source
    }

    pub const fn pinned_source(&self) -> &ConnectorPinnedFileSet {
        &self.pinned_source
    }

    pub const fn base_version_digest(&self) -> [u8; 32] {
        self.base_version_digest
    }

    pub const fn scan_schema(&self) -> &SchemaRef {
        &self.scan_schema
    }

    pub fn scan_bindings(&self) -> &[ConnectorRowMutationScanBinding] {
        &self.scan_bindings
    }

    pub fn match_tokens(&self) -> &[ConnectorWriteFieldToken] {
        &self.match_tokens
    }

    pub const fn written_version_token(&self) -> Option<ConnectorWriteFieldToken> {
        self.written_version_token
    }
}

/// What SQL needs to route rows to one row-mutation branch.
///
/// These are routing facts, not identity: they say which change events a branch
/// accepts and where in the input row its columns live. The branch's identity is
/// its [`WriteTargetOrdinal`], and its recipe is the opaque writer handle beside
/// it -- neither is derivable from these facts, and none of them reaches a
/// commit fragment.
#[derive(Clone, Debug)]
pub struct ConnectorWriteRouteFacts {
    route_id: ConnectorWriteRouteId,
    accepted_effects: Vec<ConnectorRowMutationEffect>,
    input_ordinals: Vec<ConnectorMutationRouteInput>,
    partition_fields: Vec<ConnectorWriteFieldToken>,
}

impl ConnectorWriteRouteFacts {
    /// A branch that accepts no change event would silently drop every row
    /// routed to it, so an empty effect set is refused.
    pub fn try_new(
        route_id: ConnectorWriteRouteId,
        accepted_effects: Vec<ConnectorRowMutationEffect>,
        input_ordinals: Vec<ConnectorMutationRouteInput>,
        partition_fields: Vec<ConnectorWriteFieldToken>,
    ) -> Result<Self, ConnectorError> {
        if accepted_effects.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "a row-mutation route must accept at least one change event",
            ));
        }
        Ok(Self {
            route_id,
            accepted_effects,
            input_ordinals,
            partition_fields,
        })
    }

    pub const fn route_id(&self) -> ConnectorWriteRouteId {
        self.route_id
    }

    pub fn accepted_effects(&self) -> &[ConnectorRowMutationEffect] {
        &self.accepted_effects
    }

    pub fn input_ordinals(&self) -> &[ConnectorMutationRouteInput] {
        &self.input_ordinals
    }

    pub fn partition_fields(&self) -> &[ConnectorWriteFieldToken] {
        &self.partition_fields
    }
}

impl ConnectorWriteTargetPlan {
    pub const fn new(
        ordinal: WriteTargetOrdinal,
        handle: ConnectorWriterHandle,
        input: ConnectorWriteInputShape,
    ) -> Self {
        Self {
            ordinal,
            handle,
            input,
            route: None,
            rewrite_source: None,
        }
    }

    /// Attach the routing facts of a row-mutation branch.
    pub fn with_route(mut self, route: ConnectorWriteRouteFacts) -> Self {
        self.route = Some(route);
        self
    }

    /// Attach the read contract of a copy-on-write branch.
    pub fn with_rewrite_source(mut self, source: ConnectorWriteRewriteSource) -> Self {
        self.rewrite_source = Some(source);
        self
    }

    /// Present exactly for a row-mutation branch.
    pub const fn route(&self) -> Option<&ConnectorWriteRouteFacts> {
        self.route.as_ref()
    }

    /// Present exactly for a copy-on-write branch that rewrites files.
    pub const fn rewrite_source(&self) -> Option<&ConnectorWriteRewriteSource> {
        self.rewrite_source.as_ref()
    }

    pub const fn ordinal(&self) -> WriteTargetOrdinal {
        self.ordinal
    }

    pub const fn handle(&self) -> &ConnectorWriterHandle {
        &self.handle
    }

    pub const fn input(&self) -> &ConnectorWriteInputShape {
        &self.input
    }
}

/// What `begin_write` returns: the frontend-only commit authority plus the
/// sealed logical target map.
#[derive(Debug)]
pub struct ConnectorWriteSessionPlan {
    commit: ConnectorWriteCommitHandle,
    targets: Vec<ConnectorWriteTargetPlan>,
}

impl ConnectorWriteSessionPlan {
    /// Targets must be dense from zero and must all belong to the same exact
    /// provider generation as the commit handle. A disagreement here means the
    /// session and the plan could name different runtimes, so it fails before
    /// any fragment is encoded.
    pub fn try_new(
        commit: ConnectorWriteCommitHandle,
        targets: Vec<ConnectorWriteTargetPlan>,
    ) -> Result<Self, ConnectorError> {
        let ordinals = targets
            .iter()
            .map(ConnectorWriteTargetPlan::ordinal)
            .collect::<Vec<_>>();
        validate_dense_target_ordinals(&ordinals)?;
        for target in &targets {
            if target.handle().binding() != commit.binding() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector writer handle does not belong to the begin session's generation",
                ));
            }
            target.input().validate()?;
        }
        // Routing is a property of the whole session, not of individual
        // branches: if some branches carry routing facts and others do not, SQL
        // can route rows to part of the write and silently has nowhere to send
        // the rest.
        let routed = targets
            .iter()
            .filter(|target| target.route().is_some())
            .count();
        if routed != 0 && routed != targets.len() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "a connector write session routes either every branch or none",
            ));
        }
        // Two branches sharing a route key would make the router's choice
        // ambiguous, and the loser's rows would vanish.
        let mut seen = std::collections::BTreeSet::new();
        for target in &targets {
            if let Some(route) = target.route()
                && !seen.insert(route.route_id())
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "a connector write session repeats a row-mutation route key",
                ));
            }
        }
        Ok(Self { commit, targets })
    }

    pub const fn binding(&self) -> &ConnectorWriteBinding {
        self.commit.binding()
    }

    pub const fn commit_handle(&self) -> &ConnectorWriteCommitHandle {
        &self.commit
    }

    pub fn targets(&self) -> &[ConnectorWriteTargetPlan] {
        &self.targets
    }

    /// The sealed ordinal set a prepared write set must not exceed.
    pub fn expected_targets(&self) -> Vec<WriteTargetOrdinal> {
        self.targets
            .iter()
            .map(ConnectorWriteTargetPlan::ordinal)
            .collect()
    }

    pub fn into_parts(self) -> (ConnectorWriteCommitHandle, Vec<ConnectorWriteTargetPlan>) {
        (self.commit, self.targets)
    }
}

/// Commit one complete prepared write set.
///
/// The commit handle is borrowed, never moved: a frontend session keeps it for
/// a possible abort or reconcile, and nothing else in the process can take
/// ownership of it.
pub struct ConnectorWriteFinishRequest<'a> {
    pub commit: &'a ConnectorWriteCommitHandle,
    pub prepared: ConnectorPreparedWriteSet,
    pub context: ConnectorRequestContext,
}

/// Release a begin session that never reached a complete prepared write set.
///
/// This is a known-uncommitted path: it may clean up provider-side staging, and
/// it must never report a commit it did not observe.
pub struct ConnectorWriteSessionAbortRequest<'a> {
    pub commit: &'a ConnectorWriteCommitHandle,
    pub context: ConnectorRequestContext,
}

/// Resolve a commit whose external outcome is unknown.
pub struct ConnectorWriteSessionReconcileRequest<'a> {
    pub commit: &'a ConnectorWriteCommitHandle,
    pub evidence: ExternalMutationEvidence,
    pub context: ConnectorRequestContext,
}

/// The frontend-only external write authority of one exact provider
/// generation.
///
/// Every method here mutates, or may mutate, external catalog state. None of
/// them is reachable from a backend role binding.
pub trait ConnectorWriteControl: Send + Sync {
    fn binding_key(&self) -> &ConnectorProviderBindingKey;

    /// Complete all admission and freeze the write recipe. On return either a
    /// session exists and no external effect has happened yet, or an error was
    /// raised and nothing was started.
    fn begin_write(
        &self,
        request: ConnectorWriteBeginRequest,
    ) -> Result<ConnectorWriteSessionPlan, ConnectorError>;

    /// Interpret every commit fragment and perform exactly one external commit.
    fn finish_write(
        &self,
        request: ConnectorWriteFinishRequest<'_>,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError>;

    fn abort_write(
        &self,
        request: ConnectorWriteSessionAbortRequest<'_>,
    ) -> Result<ConnectorWriteAbortOutcome, ConnectorError>;

    fn reconcile_write(
        &self,
        request: ConnectorWriteSessionReconcileRequest<'_>,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::write_stack::adapter::{ProviderWriteRuntime, WriteRuntimeAdapter};
    use crate::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceDescriptor, ConnectorInstanceId,
        ConnectorProviderId, ConnectorWriteFieldBinding, ConnectorWriteFieldToken,
    };

    #[derive(Clone, Debug)]
    struct Value(u32);

    struct FakeProvider {
        descriptor: ConnectorInstanceDescriptor,
        catalog_handle: CatalogHandle,
    }

    impl ProviderWriteRuntime for FakeProvider {
        type CommitHandle = Value;
        type WriterHandle = Value;
        type CommitFragment = Value;

        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn catalog_handle(&self) -> &CatalogHandle {
            &self.catalog_handle
        }
    }

    fn adapter() -> WriteRuntimeAdapter<FakeProvider> {
        let instance_id = ConnectorInstanceId::parse("session_unit").expect("instance id");
        WriteRuntimeAdapter::new(std::sync::Arc::new(FakeProvider {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("fake").expect("provider id"),
                instance_id: instance_id.clone(),
            },
            catalog_handle: CatalogHandle::new(instance_id, CatalogVersion::from_bytes([2; 32])),
        }))
    }

    fn input_shape() -> ConnectorWriteInputShape {
        ConnectorWriteInputShape::Data {
            fields: vec![ConnectorWriteFieldBinding::new(
                ConnectorWriteFieldToken::from_bytes([1; 32]),
                arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, true),
            )],
        }
    }

    fn route(key: u8) -> ConnectorWriteRouteFacts {
        ConnectorWriteRouteFacts::try_new(
            ConnectorWriteRouteId::from_bytes([key; 32]),
            vec![ConnectorRowMutationEffect::Delete],
            Vec::new(),
            Vec::new(),
        )
        .expect("route facts")
    }

    fn target(
        adapter: &WriteRuntimeAdapter<FakeProvider>,
        ordinal: u32,
    ) -> ConnectorWriteTargetPlan {
        ConnectorWriteTargetPlan::new(
            WriteTargetOrdinal::try_new(ordinal).expect("bounded ordinal"),
            adapter.wrap_writer_handle(Value(ordinal)),
            input_shape(),
        )
    }

    #[test]
    fn a_route_that_accepts_nothing_would_silently_drop_its_rows() {
        assert_eq!(
            ConnectorWriteRouteFacts::try_new(
                ConnectorWriteRouteId::from_bytes([1; 32]),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )
            .expect_err("no accepted effects")
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn a_session_routes_every_branch_or_none() {
        let adapter = adapter();
        let commit = adapter.wrap_commit_handle(Value(0));

        // None routed: an ordinary write.
        assert!(ConnectorWriteSessionPlan::try_new(commit, vec![target(&adapter, 0)]).is_ok());

        // All routed: a row mutation.
        let commit = adapter.wrap_commit_handle(Value(0));
        assert!(
            ConnectorWriteSessionPlan::try_new(
                commit,
                vec![
                    target(&adapter, 0).with_route(route(1)),
                    target(&adapter, 1).with_route(route(2)),
                ],
            )
            .is_ok()
        );

        // Half routed: SQL would have nowhere to send the rest.
        let commit = adapter.wrap_commit_handle(Value(0));
        assert_eq!(
            ConnectorWriteSessionPlan::try_new(
                commit,
                vec![
                    target(&adapter, 0).with_route(route(1)),
                    target(&adapter, 1)
                ],
            )
            .expect_err("partially routed")
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn two_branches_cannot_share_a_route_key() {
        let adapter = adapter();
        let commit = adapter.wrap_commit_handle(Value(0));
        assert_eq!(
            ConnectorWriteSessionPlan::try_new(
                commit,
                vec![
                    target(&adapter, 0).with_route(route(1)),
                    target(&adapter, 1).with_route(route(1)),
                ],
            )
            .expect_err("duplicate route key")
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }
}
