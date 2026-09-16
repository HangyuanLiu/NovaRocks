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
    ConnectorMutationMatchContract, ConnectorMutationSelectionFieldRole,
    ConnectorRowMutationScanBinding, ConnectorRowMutationSelection,
    ConnectorRowMutationSelectionOrdinal, selection_effect,
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
use crate::connector::{
    ExternalMutationEvidence, ExternalMutationOutcome, MAX_CONNECTOR_STATISTICS_ARTIFACTS,
    StatisticsArtifactDraft, StatisticsRequiredAggregation,
};

/// Cloneable FE planning facts for collect-on-write statistics on one logical
/// write target.
///
/// This value carries no publication or commit authority and is never encoded
/// into a writer handle. The frontend lowers its ordinary aggregate
/// requirements into the plan; only the resulting resolved generic plan facts
/// cross the native boundary.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct WriteStatisticsContract {
    requirements: Vec<StatisticsRequiredAggregation>,
}

impl WriteStatisticsContract {
    /// Freeze requirements against the exact input shape accepted by this
    /// target. An aggregate input is positional, so every descriptive fact is
    /// checked here before planning rather than trusted independently later.
    pub fn try_new(
        input: &ConnectorWriteInputShape,
        requirements: Vec<StatisticsRequiredAggregation>,
    ) -> Result<Self, ConnectorError> {
        if requirements.len() > MAX_CONNECTOR_STATISTICS_ARTIFACTS {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "write statistics requirement count exceeds the artifact limit",
            ));
        }
        let fields = input.fields();
        let mut identities = std::collections::BTreeSet::new();
        for requirement in &requirements {
            let declared = requirement.input();
            let actual = fields.get(declared.ordinal()).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "write statistics aggregate input ordinal is outside the target input",
                )
            })?;
            let actual = actual.field();
            if declared.name() != actual.name()
                || declared.data_type() != actual.data_type()
                || declared.nullable() != actual.is_nullable()
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "write statistics aggregate input does not exactly match the target field",
                ));
            }
            if !identities.insert(requirement.artifact().clone()) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "write statistics requirements contain a duplicate artifact identity",
                ));
            }
        }
        Ok(Self { requirements })
    }

    pub fn requirements(&self) -> &[StatisticsRequiredAggregation] {
        &self.requirements
    }

    pub const fn is_empty(&self) -> bool {
        self.requirements.is_empty()
    }

    fn validate_against(&self, input: &ConnectorWriteInputShape) -> Result<(), ConnectorError> {
        Self::try_new(input, self.requirements.clone()).map(|_| ())
    }
}

/// One generic statistics artifact produced for one exact logical write
/// target. Target membership is explicit because several physical targets may
/// contribute the same artifact identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteStatisticsArtifact {
    target: WriteTargetOrdinal,
    draft: StatisticsArtifactDraft,
}

impl WriteStatisticsArtifact {
    pub const fn new(target: WriteTargetOrdinal, draft: StatisticsArtifactDraft) -> Self {
        Self { target, draft }
    }

    pub const fn target(&self) -> WriteTargetOrdinal {
        self.target
    }

    pub const fn draft(&self) -> &StatisticsArtifactDraft {
        &self.draft
    }

    pub fn into_parts(self) -> (WriteTargetOrdinal, StatisticsArtifactDraft) {
        (self.target, self.draft)
    }
}

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
    /// It carries the match selection and the exact tokenized contract that
    /// produced it because a copy-on-write session cannot be planned without
    /// either: which files are rewritten, which rows were matched, and where
    /// each writer value came from must remain one provider-owned decision.
    ///
    /// This is a separate flavor from [`Self::RowMutation`] for that reason
    /// alone: the two differ by an input one of them cannot be opened without.
    CopyOnWrite {
        selection: ConnectorRowMutationSelection,
        match_contract: ConnectorMutationMatchContract,
    },
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
    statistics: WriteStatisticsContract,
    route: Option<ConnectorWriteRouteFacts>,
    routing_proof: Option<ConnectorWriteCohortRoutingProof>,
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
    selection_bindings: Arc<[ConnectorWriteSelectionBinding]>,
}

/// The role of a value read from the provider-signed match selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorWriteSelectionBindingRole {
    Identity,
    AfterImage,
}

/// A value whose null representation asks the provider to inherit the value
/// from the physical artifact it is currently writing.
///
/// This is deliberately disjoint from [`ConnectorWriteValueSource::Selection`]:
/// a derived value has no selection token or ordinal that a caller could use
/// to disguise it as an arbitrary identity field.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorWriteProviderDerivedValue {
    Inherit,
}

/// The exact source of one writer occurrence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorWriteValueSource {
    Selection {
        token: ConnectorWriteFieldToken,
        ordinal: u32,
        role: ConnectorWriteSelectionBindingRole,
    },
    ProviderDerived(ConnectorWriteProviderDerivedValue),
}

/// Exact bridge between the independently signed writer and match-selection
/// token spaces. Field names are descriptive only and never participate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorWriteSelectionBinding {
    writer_token: ConnectorWriteFieldToken,
    source: ConnectorWriteValueSource,
}

impl ConnectorWriteSelectionBinding {
    pub const fn new(
        writer_token: ConnectorWriteFieldToken,
        selection_token: ConnectorWriteFieldToken,
        selection_ordinal: u32,
        role: ConnectorWriteSelectionBindingRole,
    ) -> Self {
        Self {
            writer_token,
            source: ConnectorWriteValueSource::Selection {
                token: selection_token,
                ordinal: selection_ordinal,
                role,
            },
        }
    }

    pub const fn provider_derived(
        writer_token: ConnectorWriteFieldToken,
        value: ConnectorWriteProviderDerivedValue,
    ) -> Self {
        Self {
            writer_token,
            source: ConnectorWriteValueSource::ProviderDerived(value),
        }
    }

    pub const fn writer_token(&self) -> ConnectorWriteFieldToken {
        self.writer_token
    }

    pub const fn source(&self) -> ConnectorWriteValueSource {
        self.source
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorWriteCohortRoutingBody {
    Rewrite,
    Append,
}

/// Provider-frozen proof that one target owns exactly these selection rows.
#[derive(Clone, Debug)]
pub struct ConnectorWriteCohortRoutingProof {
    route_id: ConnectorWriteRouteId,
    selection_digest: [u8; 32],
    selection_ordinals: Arc<[ConnectorRowMutationSelectionOrdinal]>,
    body: ConnectorWriteCohortRoutingBody,
}

impl ConnectorWriteCohortRoutingProof {
    pub fn try_new(
        route_id: ConnectorWriteRouteId,
        selection_digest: [u8; 32],
        selection_ordinals: impl Into<Arc<[ConnectorRowMutationSelectionOrdinal]>>,
        body: ConnectorWriteCohortRoutingBody,
    ) -> Result<Self, ConnectorError> {
        let selection_ordinals = selection_ordinals.into();
        if selection_ordinals.is_empty()
            || selection_ordinals.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "COW routing proof ordinals must be non-empty and strictly increasing",
            ));
        }
        Ok(Self {
            route_id,
            selection_digest,
            selection_ordinals,
            body,
        })
    }

    pub const fn route_id(&self) -> ConnectorWriteRouteId {
        self.route_id
    }

    pub const fn selection_digest(&self) -> [u8; 32] {
        self.selection_digest
    }

    pub fn selection_ordinals(&self) -> &[ConnectorRowMutationSelectionOrdinal] {
        &self.selection_ordinals
    }

    pub const fn body(&self) -> ConnectorWriteCohortRoutingBody {
        self.body
    }
}

impl ConnectorWriteRouteFacts {
    /// A branch that accepts no change event would silently drop every row
    /// routed to it, so an empty effect set is refused.
    pub fn try_new(
        route_id: ConnectorWriteRouteId,
        accepted_effects: Vec<ConnectorRowMutationEffect>,
        input_ordinals: Vec<ConnectorMutationRouteInput>,
        partition_fields: Vec<ConnectorWriteFieldToken>,
        selection_bindings: impl Into<Arc<[ConnectorWriteSelectionBinding]>>,
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
            selection_bindings: selection_bindings.into(),
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

    pub fn selection_bindings(&self) -> &[ConnectorWriteSelectionBinding] {
        &self.selection_bindings
    }

    fn validate_against_input(
        &self,
        input: &ConnectorWriteInputShape,
    ) -> Result<(), ConnectorError> {
        let fields = input.fields();
        if self.input_ordinals.len() != fields.len()
            || self
                .input_ordinals
                .iter()
                .zip(&fields)
                .any(|(route, field)| route.token() != field.token())
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation route input token order differs from its target input",
            ));
        }
        if !self.selection_bindings.is_empty()
            && (self.selection_bindings.len() != fields.len()
                || self
                    .selection_bindings
                    .iter()
                    .zip(&fields)
                    .any(|(binding, field)| binding.writer_token() != field.token()))
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "COW selection bindings are incomplete or reordered",
            ));
        }
        Ok(())
    }

    pub fn validate_selection_contract(
        &self,
        input: &ConnectorWriteInputShape,
        contract: &ConnectorMutationMatchContract,
    ) -> Result<(), ConnectorError> {
        self.validate_against_input(input)?;
        if self.selection_bindings.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "COW route omits its provider-signed selection bindings",
            ));
        }
        let fields = input.fields();
        for (binding, writer) in self.selection_bindings.iter().zip(fields) {
            match binding.source() {
                ConnectorWriteValueSource::Selection {
                    token,
                    ordinal,
                    role,
                } => {
                    let selection = contract.selection_field(token).ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "COW route selection binding carries a foreign token",
                        )
                    })?;
                    let role_matches = matches!(
                        (role, selection.role()),
                        (
                            ConnectorWriteSelectionBindingRole::Identity,
                            ConnectorMutationSelectionFieldRole::Identity
                        ) | (
                            ConnectorWriteSelectionBindingRole::AfterImage,
                            ConnectorMutationSelectionFieldRole::AfterImage
                        )
                    );
                    if ordinal != selection.ordinal()
                        || !role_matches
                        || writer.field().data_type() != selection.field().data_type()
                        || (selection.field().is_nullable() && !writer.field().is_nullable())
                    {
                        return Err(ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "COW route selection binding differs from its signed token, role, ordinal, or field",
                        ));
                    }
                }
                ConnectorWriteValueSource::ProviderDerived(
                    ConnectorWriteProviderDerivedValue::Inherit,
                ) if writer.field().is_nullable() => {}
                ConnectorWriteValueSource::ProviderDerived(_) => {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "COW provider-derived writer occurrence must admit the null inherit marker",
                    ));
                }
            }
        }
        Ok(())
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
            statistics: WriteStatisticsContract {
                requirements: Vec::new(),
            },
            route: None,
            routing_proof: None,
            rewrite_source: None,
        }
    }

    pub fn with_statistics_contract(
        mut self,
        statistics: WriteStatisticsContract,
    ) -> Result<Self, ConnectorError> {
        statistics.validate_against(&self.input)?;
        self.statistics = statistics;
        Ok(self)
    }

    /// Attach the routing facts of a row-mutation branch.
    pub fn with_route(mut self, route: ConnectorWriteRouteFacts) -> Self {
        self.route = Some(route);
        self
    }

    pub fn with_routing_proof(mut self, proof: ConnectorWriteCohortRoutingProof) -> Self {
        self.routing_proof = Some(proof);
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

    pub const fn routing_proof(&self) -> Option<&ConnectorWriteCohortRoutingProof> {
        self.routing_proof.as_ref()
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

    pub const fn statistics(&self) -> &WriteStatisticsContract {
        &self.statistics
    }
}

/// What `begin_write` returns: the frontend-only commit authority plus the
/// sealed logical target map.
#[derive(Debug)]
pub struct ConnectorWriteSessionPlan {
    commit: ConnectorWriteCommitHandle,
    targets: Vec<ConnectorWriteTargetPlan>,
    copy_on_write: Option<ConnectorWriteCopyOnWriteRoutingPlan>,
}

#[derive(Clone, Debug)]
pub struct ConnectorWriteCopyOnWriteRoutingPlan {
    selection: ConnectorRowMutationSelection,
    match_contract: ConnectorMutationMatchContract,
}

impl ConnectorWriteCopyOnWriteRoutingPlan {
    pub const fn selection(&self) -> &ConnectorRowMutationSelection {
        &self.selection
    }

    pub const fn match_contract(&self) -> &ConnectorMutationMatchContract {
        &self.match_contract
    }
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
        Self::try_with_copy_on_write(commit, targets, None)
    }

    pub fn try_copy_on_write(
        commit: ConnectorWriteCommitHandle,
        targets: Vec<ConnectorWriteTargetPlan>,
        selection: ConnectorRowMutationSelection,
        match_contract: ConnectorMutationMatchContract,
    ) -> Result<Self, ConnectorError> {
        Self::try_with_copy_on_write(
            commit,
            targets,
            Some(ConnectorWriteCopyOnWriteRoutingPlan {
                selection,
                match_contract,
            }),
        )
    }

    fn try_with_copy_on_write(
        commit: ConnectorWriteCommitHandle,
        targets: Vec<ConnectorWriteTargetPlan>,
        copy_on_write: Option<ConnectorWriteCopyOnWriteRoutingPlan>,
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
            if let Some(route) = target.route() {
                route.validate_against_input(target.input())?;
            }
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
            target.statistics.validate_against(&target.input)?;
            if let Some(route) = target.route()
                && !seen.insert(route.route_id())
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "a connector write session repeats a row-mutation route key",
                ));
            }
        }
        match &copy_on_write {
            Some(routing) => validate_copy_on_write_routing(routing, &targets)?,
            None if targets
                .iter()
                .any(|target| target.routing_proof().is_some()) =>
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "COW routing proof requires its signed selection and match contract",
                ));
            }
            None => {}
        }
        Ok(Self {
            commit,
            targets,
            copy_on_write,
        })
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

    pub const fn copy_on_write(&self) -> Option<&ConnectorWriteCopyOnWriteRoutingPlan> {
        self.copy_on_write.as_ref()
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

fn validate_copy_on_write_routing(
    routing: &ConnectorWriteCopyOnWriteRoutingPlan,
    targets: &[ConnectorWriteTargetPlan],
) -> Result<(), ConnectorError> {
    routing.selection.validate()?;
    routing
        .match_contract
        .validate_selection(&routing.selection)?;
    if targets.is_empty()
        || targets
            .iter()
            .any(|target| target.route().is_none() || target.routing_proof().is_none())
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "COW session requires one route and routing proof per target",
        ));
    }
    let row_count = usize::try_from(routing.selection.row_count()).map_err(|_| {
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "COW selection row count does not fit routing validation memory",
        )
    })?;
    let mut covered = vec![false; row_count];
    for target in targets {
        let route = target.route().expect("checked above");
        route.validate_selection_contract(target.input(), &routing.match_contract)?;
        let proof = target.routing_proof().expect("checked above");
        if proof.route_id() != route.route_id()
            || proof.selection_digest() != routing.selection.digest()
            || matches!(proof.body(), ConnectorWriteCohortRoutingBody::Rewrite)
                != target.rewrite_source().is_some()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "COW routing proof differs from its target, route, or selection",
            ));
        }
        let derived_tokens = route
            .selection_bindings()
            .iter()
            .filter(|binding| {
                matches!(
                    binding.source(),
                    ConnectorWriteValueSource::ProviderDerived(
                        ConnectorWriteProviderDerivedValue::Inherit
                    )
                )
            })
            .map(ConnectorWriteSelectionBinding::writer_token)
            .collect::<Vec<_>>();
        let expected_derived = target
            .rewrite_source()
            .and_then(ConnectorWriteRewriteSource::written_version_token)
            .map(|token| vec![token])
            .unwrap_or_default();
        if derived_tokens != expected_derived {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "COW provider-derived writer source differs from its frozen rewrite role",
            ));
        }
        for ordinal in proof.selection_ordinals() {
            let index = usize::try_from(ordinal.get()).map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "COW routing proof ordinal does not fit this process",
                )
            })?;
            if index >= row_count || covered[index] {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "COW routing proof coverage is out of bounds or overlapping",
                ));
            }
            covered[index] = true;
            let effect = selection_effect(
                &routing.selection,
                *ordinal,
                routing.match_contract.effect_field().target_ordinal(),
            )?;
            if !route.accepted_effects().contains(&effect)
                || matches!(proof.body(), ConnectorWriteCohortRoutingBody::Append)
                    != (effect == ConnectorRowMutationEffect::Insert)
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "COW routing proof does not exactly cover its route effects",
                ));
            }
        }
    }
    if covered.iter().any(|covered| !covered) {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "COW routing proofs do not exactly cover the selection",
        ));
    }
    Ok(())
}

/// Commit one complete prepared write set.
///
/// The commit handle is borrowed, never moved: a frontend session keeps it for
/// a possible abort or reconcile, and nothing else in the process can take
/// ownership of it.
pub struct ConnectorWriteFinishRequest<'a> {
    pub commit: &'a ConnectorWriteCommitHandle,
    pub prepared: ConnectorPreparedWriteSet,
    pub statistics: Vec<WriteStatisticsArtifact>,
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
        ConnectorMutationEffectField, ConnectorMutationSourceField, ConnectorMutationTargetField,
        ConnectorProviderBindingKey, ConnectorProviderId, ConnectorTableHandle,
        ConnectorWriteBaseVersion, ConnectorWriteFieldBinding, ConnectorWriteFieldToken,
        ProviderBindingEpoch,
    };
    use arrow::array::{Int8Array, Int64Array};
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;

    #[derive(Clone, Debug)]
    struct Value;

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
            vec![ConnectorMutationRouteInput::new(
                ConnectorWriteFieldToken::from_bytes([1; 32]),
                0,
            )],
            Vec::new(),
            Vec::new(),
        )
        .expect("route facts")
    }

    fn cow_contract() -> ConnectorMutationMatchContract {
        let instance = ConnectorInstanceId::parse("session_unit").expect("instance");
        let identity = ConnectorMutationSourceField::new(
            ConnectorWriteFieldToken::from_bytes([3; 32]),
            arrow::datatypes::Field::new("same", arrow::datatypes::DataType::Int64, true),
            0,
        );
        ConnectorMutationMatchContract::try_new(
            ConnectorProviderBindingKey {
                instance_id: instance.clone(),
                incarnation: ProviderBindingEpoch::from_bytes([7; 16]),
            },
            ConnectorTableHandle::try_new(instance, bytes::Bytes::from_static(b"table"))
                .expect("table"),
            ConnectorWriteBaseVersion::try_new(bytes::Bytes::from_static(b"base")).expect("base"),
            vec![identity.clone()],
            Vec::new(),
            vec![ConnectorMutationTargetField::new(
                ConnectorWriteFieldToken::from_bytes([4; 32]),
                arrow::datatypes::Field::new("same", arrow::datatypes::DataType::Int64, true),
                1,
            )],
            vec![identity.token()],
            ConnectorMutationEffectField::try_new(
                ConnectorWriteFieldToken::from_bytes([5; 32]),
                arrow::datatypes::Field::new("effect", arrow::datatypes::DataType::Int8, false),
                2,
            )
            .expect("effect"),
        )
        .expect("contract")
    }

    fn cow_input() -> ConnectorWriteInputShape {
        ConnectorWriteInputShape::RowLineage {
            data_fields: vec![ConnectorWriteFieldBinding::new(
                ConnectorWriteFieldToken::from_bytes([1; 32]),
                arrow::datatypes::Field::new("same", arrow::datatypes::DataType::Int64, true),
            )],
            row_identity_fields: vec![ConnectorWriteFieldBinding::new(
                ConnectorWriteFieldToken::from_bytes([2; 32]),
                arrow::datatypes::Field::new("same", arrow::datatypes::DataType::Int64, true),
            )],
        }
    }

    fn cow_selection(effects: &[ConnectorRowMutationEffect]) -> ConnectorRowMutationSelection {
        let contract = cow_contract();
        let schema = Arc::new(Schema::new(vec![
            contract.identity_fields()[0].field().clone(),
            contract.after_fields()[0].field().clone(),
            contract.effect_field().field().clone(),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..effects.len() as i64)),
                Arc::new(Int64Array::from_iter_values(10..10 + effects.len() as i64)),
                Arc::new(Int8Array::from_iter_values(
                    effects.iter().map(|effect| *effect as i8),
                )),
            ],
        )
        .expect("selection batch");
        ConnectorRowMutationSelection::try_new(schema, vec![batch], 1024, 1 << 20)
            .expect("selection")
    }

    fn cow_append_route() -> ConnectorWriteRouteFacts {
        ConnectorWriteRouteFacts::try_new(
            ConnectorWriteRouteId::from_bytes([18; 32]),
            vec![ConnectorRowMutationEffect::Insert],
            vec![ConnectorMutationRouteInput::new(
                ConnectorWriteFieldToken::from_bytes([1; 32]),
                0,
            )],
            Vec::new(),
            vec![ConnectorWriteSelectionBinding::new(
                ConnectorWriteFieldToken::from_bytes([1; 32]),
                ConnectorWriteFieldToken::from_bytes([4; 32]),
                1,
                ConnectorWriteSelectionBindingRole::AfterImage,
            )],
        )
        .expect("append route")
    }

    fn cow_route(ordinals: [u32; 2]) -> ConnectorWriteRouteFacts {
        ConnectorWriteRouteFacts::try_new(
            ConnectorWriteRouteId::from_bytes([8; 32]),
            vec![ConnectorRowMutationEffect::Replace],
            vec![
                ConnectorMutationRouteInput::new(
                    ConnectorWriteFieldToken::from_bytes([1; 32]),
                    ordinals[0],
                ),
                ConnectorMutationRouteInput::new(
                    ConnectorWriteFieldToken::from_bytes([2; 32]),
                    ordinals[1],
                ),
            ],
            Vec::new(),
            vec![
                ConnectorWriteSelectionBinding::new(
                    ConnectorWriteFieldToken::from_bytes([1; 32]),
                    ConnectorWriteFieldToken::from_bytes([4; 32]),
                    1,
                    ConnectorWriteSelectionBindingRole::AfterImage,
                ),
                ConnectorWriteSelectionBinding::new(
                    ConnectorWriteFieldToken::from_bytes([2; 32]),
                    ConnectorWriteFieldToken::from_bytes([3; 32]),
                    0,
                    ConnectorWriteSelectionBindingRole::Identity,
                ),
            ],
        )
        .expect("COW route")
    }

    fn target(
        adapter: &WriteRuntimeAdapter<FakeProvider>,
        ordinal: u32,
    ) -> ConnectorWriteTargetPlan {
        ConnectorWriteTargetPlan::new(
            WriteTargetOrdinal::try_new(ordinal).expect("bounded ordinal"),
            adapter.wrap_writer_handle(Value),
            input_shape(),
        )
    }

    fn statistics_requirement(
        ordinal: usize,
        name: &str,
        data_type: arrow::datatypes::DataType,
        nullable: bool,
        field_id: i32,
    ) -> StatisticsRequiredAggregation {
        StatisticsRequiredAggregation::try_new(
            crate::connector::StatisticsScanColumn::try_new(
                ordinal,
                Arc::<str>::from(name),
                data_type,
                nullable,
            )
            .expect("scan column"),
            "$test_stat",
            crate::connector::StatisticsArtifactIdentity::try_new(vec![field_id], "test/blob")
                .expect("identity"),
        )
        .expect("requirement")
    }

    #[test]
    fn statistics_contract_binds_the_exact_target_field() {
        let input = input_shape();
        let contract = WriteStatisticsContract::try_new(
            &input,
            vec![statistics_requirement(
                0,
                "v",
                arrow::datatypes::DataType::Int64,
                true,
                1,
            )],
        )
        .expect("contract");
        assert_eq!(contract.requirements().len(), 1);

        for mismatch in [
            statistics_requirement(0, "V", arrow::datatypes::DataType::Int64, true, 1),
            statistics_requirement(0, "v", arrow::datatypes::DataType::Int32, true, 1),
            statistics_requirement(0, "v", arrow::datatypes::DataType::Int64, false, 1),
            statistics_requirement(1, "v", arrow::datatypes::DataType::Int64, true, 1),
        ] {
            assert_eq!(
                WriteStatisticsContract::try_new(&input, vec![mismatch])
                    .expect_err("mismatched field")
                    .kind(),
                ConnectorErrorKind::InvalidRequest
            );
        }
    }

    #[test]
    fn statistics_contract_rejects_duplicate_artifact_identity() {
        let input = input_shape();
        let requirement =
            statistics_requirement(0, "v", arrow::datatypes::DataType::Int64, true, 1);
        assert_eq!(
            WriteStatisticsContract::try_new(&input, vec![requirement.clone(), requirement])
                .expect_err("duplicate identity")
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn a_route_that_accepts_nothing_would_silently_drop_its_rows() {
        assert_eq!(
            ConnectorWriteRouteFacts::try_new(
                ConnectorWriteRouteId::from_bytes([1; 32]),
                Vec::new(),
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
        let commit = adapter.wrap_commit_handle(Value);

        // None routed: an ordinary write.
        assert!(ConnectorWriteSessionPlan::try_new(commit, vec![target(&adapter, 0)]).is_ok());

        // All routed: a row mutation.
        let commit = adapter.wrap_commit_handle(Value);
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
        let commit = adapter.wrap_commit_handle(Value);
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
        let commit = adapter.wrap_commit_handle(Value);
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

    #[test]
    fn cow_route_preserves_provider_order_and_repeated_source_occurrences() {
        let input = cow_input();
        let reversed = cow_route([1, 0]);
        reversed
            .validate_selection_contract(&input, &cow_contract())
            .expect("exact token bridge");
        assert_eq!(
            reversed
                .input_ordinals()
                .iter()
                .map(ConnectorMutationRouteInput::input_ordinal)
                .collect::<Vec<_>>(),
            vec![1, 0]
        );

        let repeated = cow_route([0, 0]);
        repeated
            .validate_selection_contract(&input, &cow_contract())
            .expect("two route occurrences may reuse one producer ordinal");
        assert_eq!(repeated.input_ordinals()[0].input_ordinal(), 0);
        assert_eq!(repeated.input_ordinals()[1].input_ordinal(), 0);
    }

    #[test]
    fn cow_selection_bridge_is_token_exact_even_when_names_match() {
        let input = cow_input();
        let contract = cow_contract();
        let missing = ConnectorWriteRouteFacts::try_new(
            ConnectorWriteRouteId::from_bytes([9; 32]),
            vec![ConnectorRowMutationEffect::Replace],
            cow_route([1, 0]).input_ordinals().to_vec(),
            Vec::new(),
            Vec::new(),
        )
        .expect("route");
        assert_eq!(
            missing
                .validate_selection_contract(&input, &contract)
                .expect_err("missing bridge")
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );

        let foreign = ConnectorWriteRouteFacts::try_new(
            ConnectorWriteRouteId::from_bytes([10; 32]),
            vec![ConnectorRowMutationEffect::Replace],
            cow_route([1, 0]).input_ordinals().to_vec(),
            Vec::new(),
            vec![
                ConnectorWriteSelectionBinding::new(
                    ConnectorWriteFieldToken::from_bytes([1; 32]),
                    ConnectorWriteFieldToken::from_bytes([6; 32]),
                    1,
                    ConnectorWriteSelectionBindingRole::AfterImage,
                ),
                cow_route([1, 0]).selection_bindings()[1],
            ],
        )
        .expect("route");
        assert_eq!(
            foreign
                .validate_selection_contract(&input, &contract)
                .expect_err("same field name cannot recover a foreign token")
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );

        let mut tampered_bindings = cow_route([1, 0]).selection_bindings().to_vec();
        tampered_bindings[0] = ConnectorWriteSelectionBinding::new(
            ConnectorWriteFieldToken::from_bytes([1; 32]),
            ConnectorWriteFieldToken::from_bytes([4; 32]),
            0,
            ConnectorWriteSelectionBindingRole::AfterImage,
        );
        let tampered = ConnectorWriteRouteFacts::try_new(
            ConnectorWriteRouteId::from_bytes([13; 32]),
            vec![ConnectorRowMutationEffect::Replace],
            cow_route([1, 0]).input_ordinals().to_vec(),
            Vec::new(),
            tampered_bindings,
        )
        .expect("route");
        assert_eq!(
            tampered
                .validate_selection_contract(&input, &contract)
                .expect_err("tampered selection ordinal")
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn cow_selection_occurrence_can_feed_multiple_writer_occurrences() {
        let input = ConnectorWriteInputShape::Data {
            fields: vec![
                ConnectorWriteFieldBinding::new(
                    ConnectorWriteFieldToken::from_bytes([1; 32]),
                    arrow::datatypes::Field::new("first", arrow::datatypes::DataType::Int64, true),
                ),
                ConnectorWriteFieldBinding::new(
                    ConnectorWriteFieldToken::from_bytes([2; 32]),
                    arrow::datatypes::Field::new("second", arrow::datatypes::DataType::Int64, true),
                ),
            ],
        };
        let route = ConnectorWriteRouteFacts::try_new(
            ConnectorWriteRouteId::from_bytes([11; 32]),
            vec![ConnectorRowMutationEffect::Replace],
            vec![
                ConnectorMutationRouteInput::new(ConnectorWriteFieldToken::from_bytes([1; 32]), 0),
                ConnectorMutationRouteInput::new(ConnectorWriteFieldToken::from_bytes([2; 32]), 0),
            ],
            Vec::new(),
            vec![
                ConnectorWriteSelectionBinding::new(
                    ConnectorWriteFieldToken::from_bytes([1; 32]),
                    ConnectorWriteFieldToken::from_bytes([4; 32]),
                    1,
                    ConnectorWriteSelectionBindingRole::AfterImage,
                ),
                ConnectorWriteSelectionBinding::new(
                    ConnectorWriteFieldToken::from_bytes([2; 32]),
                    ConnectorWriteFieldToken::from_bytes([4; 32]),
                    1,
                    ConnectorWriteSelectionBindingRole::AfterImage,
                ),
            ],
        )
        .expect("route");
        route
            .validate_selection_contract(&input, &cow_contract())
            .expect("one signed selection occurrence may feed two writer occurrences");
    }

    #[test]
    fn provider_derived_value_requires_nullable_inherit_marker() {
        let writer = ConnectorWriteFieldToken::from_bytes([2; 32]);
        let input = |nullable| ConnectorWriteInputShape::RowLineage {
            data_fields: Vec::new(),
            row_identity_fields: vec![ConnectorWriteFieldBinding::new(
                writer,
                arrow::datatypes::Field::new(
                    "new_version",
                    arrow::datatypes::DataType::Int64,
                    nullable,
                ),
            )],
        };
        let route = ConnectorWriteRouteFacts::try_new(
            ConnectorWriteRouteId::from_bytes([12; 32]),
            vec![ConnectorRowMutationEffect::Insert],
            vec![ConnectorMutationRouteInput::new(writer, 0)],
            Vec::new(),
            vec![ConnectorWriteSelectionBinding::provider_derived(
                writer,
                ConnectorWriteProviderDerivedValue::Inherit,
            )],
        )
        .expect("route");
        route
            .validate_selection_contract(&input(true), &cow_contract())
            .expect("nullable writer admits the inherit marker");
        assert_eq!(
            route
                .validate_selection_contract(&input(false), &cow_contract())
                .expect_err("non-null writer cannot carry the inherit marker")
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn cow_session_routing_proof_exactly_covers_selection_by_route_identity() {
        let adapter = adapter();
        let selection = cow_selection(&[
            ConnectorRowMutationEffect::Insert,
            ConnectorRowMutationEffect::Insert,
        ]);
        let route = cow_append_route();
        let proof = ConnectorWriteCohortRoutingProof::try_new(
            route.route_id(),
            selection.digest(),
            vec![
                ConnectorRowMutationSelectionOrdinal::new(0),
                ConnectorRowMutationSelectionOrdinal::new(1),
            ],
            ConnectorWriteCohortRoutingBody::Append,
        )
        .expect("proof");
        let input = ConnectorWriteInputShape::Data {
            fields: vec![cow_input().fields()[0].clone()],
        };
        let target = ConnectorWriteTargetPlan::new(
            WriteTargetOrdinal::try_new(0).expect("ordinal"),
            adapter.wrap_writer_handle(Value),
            input,
        )
        .with_route(route)
        .with_routing_proof(proof);
        ConnectorWriteSessionPlan::try_copy_on_write(
            adapter.wrap_commit_handle(Value),
            vec![target],
            selection,
            cow_contract(),
        )
        .expect("exact routing proof");
    }

    #[test]
    fn cow_session_routing_proof_fails_closed_on_missing_or_wrong_effect_coverage() {
        let adapter = adapter();
        let selection = cow_selection(&[
            ConnectorRowMutationEffect::Insert,
            ConnectorRowMutationEffect::Replace,
        ]);
        let route = cow_append_route();
        let proof = ConnectorWriteCohortRoutingProof::try_new(
            route.route_id(),
            selection.digest(),
            vec![ConnectorRowMutationSelectionOrdinal::new(0)],
            ConnectorWriteCohortRoutingBody::Append,
        )
        .expect("partial proof");
        let input = ConnectorWriteInputShape::Data {
            fields: vec![cow_input().fields()[0].clone()],
        };
        let target = ConnectorWriteTargetPlan::new(
            WriteTargetOrdinal::try_new(0).expect("ordinal"),
            adapter.wrap_writer_handle(Value),
            input,
        )
        .with_route(route)
        .with_routing_proof(proof);
        assert_eq!(
            ConnectorWriteSessionPlan::try_copy_on_write(
                adapter.wrap_commit_handle(Value),
                vec![target],
                selection,
                cow_contract(),
            )
            .expect_err("partial coverage")
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }
}
