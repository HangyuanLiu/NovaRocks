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

//! Negotiating and freezing one query's provider reads.
//!
//! This is the fourth kind of question the compiler asks, and the only one
//! that is not a lookup. The other three ask an owner what is already true;
//! this one asks a provider what it will take on, and then commits it. What it
//! commits is what the plan is built against, so it happens exactly once per
//! scan and cannot be re-asked later with a different answer.
//!
//! Negotiation offers the whole list at once - projection, then filter, then
//! limit - because order is part of the question: a limit means one thing above
//! a filter and another below it. The provider answers each offer in turn, and
//! only an answer that says `Exact` relieves the engine of that work.
//!
//! Freezing yields two things of different kinds, and they leave here by
//! different doors. The static facts become the read's contract and travel with
//! the plan to every worker. The capability to perform the read belongs to this
//! attempt alone: it is deposited the moment it is taken and never appears in a
//! fact, a plan, or anything encoded.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use novarocks_catalog_application::ConnectorControlHost;
use novarocks_physical_plan::{
    ExactInputVersion, NullOrdering, PredicateGuaranteeKind, ProviderColumnReference,
    ProviderReadReference, SortDirection,
};
use novarocks_query_application::preparation::{
    FrozenReadAccess, ProviderReadFactPort, ReadAccessDeposit, ReadAccessSink,
};
use novarocks_spi::connector::{
    CatalogProperties, ConnectorControlPlanningLease, ConnectorControlReadBinding,
    ConnectorPlanningContext, ConnectorReadAttemptAccess, ConnectorReadWireEncoder,
    ConnectorRequestContext,
    read_stack::{
        ConnectorExpression, ConnectorReadColumnBinding, ConnectorReadColumnHandle,
        ConnectorReadConstraint, ConnectorReadDistribution, ConnectorReadMetadata,
        ConnectorReadMetadataKind, ConnectorReadMetadataRequest, ConnectorReadMetadataVersion,
        ConnectorReadNullOrdering, ConnectorReadProperties, ConnectorReadRelationVersion,
        ConnectorReadRequestControl, ConnectorReadSortDirection, ConnectorReadTableHandle,
        ConnectorReadWorkSource, ConnectorSession, Constraint, SchemaTableName, TupleDomain,
        negotiation::{
            ReadFreezeRequest, ReadNegotiation, ReadPushdownDisposition, ReadPushdownOp,
            ReadPushdownOutcome,
        },
        runtime::ConnectorReadAssignment,
    },
};
use novarocks_sql::compiler::{
    ProviderReadColumnFact, ProviderReadColumnNeed, ProviderReadDistribution, ProviderReadFact,
    ProviderReadLimitFact, ProviderReadNeed, ProviderReadOrderingKey, ProviderReadPredicateFact,
    ProviderReadProperties, ProviderReadRelationNeed, ProviderReadRequestBinding,
    ProviderReadStaticContract, ProviderReadVersionNeed,
};
use novarocks_sql::planning::catalog::MetadataTableKind;
use novarocks_types::naming::TableIdentity;

use crate::catalog_application::query_bindings::{QueryFrozenReadInput, QueryTableBindingStore};
use crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor;

/// Variable names offered to a provider for one read's projection.
///
/// They are positional on purpose: the provider is being asked about the
/// request's own dense ordinals, not about anything the query calls a column.
const READ_VARIABLE_PREFIX: &str = "r";

/// What this role keeps for one frozen read.
///
/// A freeze leaves two things behind that the plan itself must not carry, and
/// they are kept together because they are accounted for together: each belongs
/// to exactly one scan occurrence, each is useless without the plan, and the
/// pairing that proves every scan has a capability proves every scan has its
/// encoding too.
///
/// They are spent differently. The capability is released when the attempt
/// ends. The encoding is read once, when the plan is put on the wire, and dies
/// with the plan version.
pub(crate) struct FrozenProviderRead {
    pub(crate) access: ConnectorReadAttemptAccess,
    /// The generation this read was frozen against, held open for as long as
    /// the plan may run. Releasing it earlier would let the provider retire a
    /// generation a frozen read still names.
    pub(crate) generation: ConnectorControlPlanningLease,
    pub(crate) catalog: CatalogProperties,
    pub(crate) encoding: FrozenReadEncoding,
}

/// The provider-side half of one scan's wire form.
///
/// These values exist only while a read is being negotiated: the assignments
/// name provider columns, and the two domains are what the provider said it
/// would and would not enforce. Nothing downstream can recover them from the
/// plan, so they are kept from the moment the read stops being negotiable.
pub(crate) struct FrozenReadEncoding {
    /// The relation this read names, which the plan deliberately does not
    /// carry: a physical plan addresses a provider relation by frozen
    /// reference, not by the name a statement used to reach it.
    pub(crate) identity: TableIdentity,
    /// Each provider column this read projects, paired with how the request
    /// named it. The plan addresses these columns by provider reference; what
    /// a column is *called* is SQL-local and survives only here, so the pairing
    /// is kept rather than the two halves separately - matching them again by
    /// position later is how a rename becomes a mislabelled column.
    pub(crate) columns: Box<[(ProviderColumnReference, ProviderReadColumnNeed)]>,
    pub(crate) relation: novarocks_spi::connector::read_stack::ConnectorReadRelation,
    pub(crate) assignments: Vec<ConnectorReadAssignment>,
    /// The constraint the provider was offered.
    ///
    /// Splits are enumerated against what was offered, not against what the
    /// provider took on: a provider that declined still prunes by whatever it
    /// can, and handing it less than it was asked about would enumerate more
    /// work than the query needs.
    pub(crate) offered_constraint: ConnectorReadConstraint,
    /// What the provider guarantees. The engine may stop evaluating it.
    pub(crate) enforced_predicate: TupleDomain<ConnectorReadColumnHandle>,
    /// What the reader must still apply itself.
    pub(crate) unenforced_predicate: TupleDomain<ConnectorReadColumnHandle>,
    pub(crate) remaining_expression: Option<ConnectorExpression>,
    pub(crate) work_source: ConnectorReadWorkSource,
    pub(crate) encoder: Arc<dyn ConnectorReadWireEncoder>,
}

/// Freezes each scan of one statement with its provider.
pub(crate) struct FrontendProviderReadFacts {
    control: Arc<ConnectorControlHost>,
    bindings: Arc<QueryTableBindingStore>,
    session: ConnectorSession,
    context: ConnectorRequestContext,
    blocking: ConnectorBlockingIoSupervisor,
}

impl FrontendProviderReadFacts {
    pub(crate) const fn new(
        control: Arc<ConnectorControlHost>,
        bindings: Arc<QueryTableBindingStore>,
        session: ConnectorSession,
        context: ConnectorRequestContext,
        blocking: ConnectorBlockingIoSupervisor,
    ) -> Self {
        Self {
            control,
            bindings,
            session,
            context,
            blocking,
        }
    }
}

#[async_trait]
impl ProviderReadFactPort for FrontendProviderReadFacts {
    /// What a freeze leaves this role: the ability to reacquire exactly the
    /// handle that was frozen, for one attempt, and the provider-side half of
    /// that read's wire form. Neither holds a secret and neither offers a way
    /// to choose a different read.
    type Access = FrozenProviderRead;

    async fn resolve_provider_reads(
        &self,
        needs: &[ProviderReadNeed],
        taken: &ReadAccessSink<Self::Access>,
    ) -> Result<Vec<ProviderReadFact>, String> {
        let needs = needs.to_vec();
        let deposits = taken.deposits();
        let control = Arc::clone(&self.control);
        let bindings = Arc::clone(&self.bindings);
        let session = self.session.clone();
        let context = self.context.clone();
        self.blocking
            .spawn_ordinary(move || {
                let mut facts = Vec::with_capacity(needs.len());
                for need in &needs {
                    facts.push(freeze_one_read(
                        need, &control, &bindings, &session, &context, &deposits,
                    )?);
                }
                Ok(facts)
            })
            .finish()
            .await
            .map_err(|error| format!("frontend provider read lane: {error}"))?
    }
}

/// Negotiate and freeze exactly one read.
fn freeze_one_read(
    need: &ProviderReadNeed,
    control: &ConnectorControlHost,
    bindings: &QueryTableBindingStore,
    session: &ConnectorSession,
    context: &ConnectorRequestContext,
    deposits: &ReadAccessDeposit<FrozenProviderRead>,
) -> Result<ProviderReadFact, String> {
    let relation = need.relation();
    let identity = relation_identity(relation);
    let name = format!("{}.{}", identity.namespace, identity.table);
    let materialization = bindings
        .frozen_read_input(need.binding(), frozen_input(relation)?)
        .map_err(|error| format!("provider read of {name}: {error}"))?;
    let read = control
        .typed_read_for_planning_lease(&materialization.planning_lease)
        .map_err(|error| format!("provider read of {name} has no typed read binding: {error}"))?;
    // The control this read is negotiated and frozen through is the same one
    // its attempt access is sealed against. A provider may pin what it
    // resolved while answering -- the sealed access is then a view of that pin
    // -- and asking through a second control would seal against a request that
    // was never asked anything.
    let request_control = request_control_for(&read, context)
        .map_err(|error| format!("provider read of {name}: {error}"))?;
    let metadata = request_control.metadata();
    let table = SchemaTableName::try_new(&identity.namespace, &identity.table)
        .map_err(|error| format!("provider read of {name}: {error}"))?;

    // 1. Freeze the relation family this read names. Admission already
    //    resolved the name, so a provider that now reports nothing means the
    //    pin is gone rather than that the query named an unknown relation.
    let handle = open_relation(metadata.as_ref(), session, &table, relation, &name)?;

    // 2. Bind the requested projection to the provider's own columns. The
    //    request's ordinals are the output authority; the provider is asked for
    //    each column by its own schema spelling.
    let column_bindings = metadata
        .get_column_bindings(session, &handle)
        .map_err(|error| format!("provider read of {name} cannot read its columns: {error}"))?;
    let assignments = assignments_for(need.columns(), &column_bindings, &name)?;
    let columns_by_ordinal = assignments
        .iter()
        .enumerate()
        .map(|(ordinal, assignment)| (assignment.column().clone(), ordinal_of(ordinal)))
        .collect::<BTreeMap<_, _>>();

    // 3. Offer projection, filter and limit as one ordered list. Order is part
    //    of the question, and asking as a list is what lets the provider answer
    //    about the read it will actually perform rather than about three
    //    unrelated fragments of it.
    let offer = offered_ops(need, &assignments, &column_ordinals(&assignments))?;
    let negotiation = ReadNegotiation {
        handle,
        ops: offer.ops.clone(),
    };
    let offered = negotiation.ops.len();
    let negotiated = metadata
        .negotiate(session, &negotiation)
        .map_err(|error| format!("provider read of {name} could not be negotiated: {error}"))?;
    negotiated
        .verify_shape(offered)
        .map_err(|error| format!("provider read of {name} answered the wrong shape: {error}"))?;

    // 4. Commit it. Everything after this point describes a read that is no
    //    longer negotiable.
    let relation_kind = relation.relation_kind();
    let freeze_request = ReadFreezeRequest {
        handle: negotiated.handle.clone(),
        relation_kind,
        expected_input_version: None,
    };
    let frozen = metadata
        .freeze(session, &freeze_request)
        .map_err(|error| format!("provider read of {name} could not be frozen: {error}"))?
        .into_verified(&freeze_request)
        .map_err(|error| format!("provider read of {name} froze the wrong read: {error}"))?;

    // 5. Assemble everything the freeze leaves behind, so that taking the
    //    capability and accounting for it are adjacent: nothing may happen
    //    between sealing one and depositing it.
    let encoder = read.encoder();
    let provider_relation = metadata
        .relation(relation_kind, negotiated.handle.clone())
        .map_err(|error| format!("provider read of {name} has no frozen relation: {error}"))?;
    let (enforced_predicate, unenforced_predicate, remaining_expression) =
        filter_responsibility(&offer, &negotiated.outcomes);
    let (schema, named_columns) =
        column_facts(need.columns(), &assignments, encoder.as_ref(), &name)?;
    let catalog_properties = materialization
        .planning_lease
        .binding()
        .catalog_properties()
        .map_err(|error| format!("provider read of {name} has no catalog properties: {error}"))?
        .clone();

    // 6. Take the capability and account for it in the same breath.
    let access = read
        .seal_attempt_access(&request_control, &negotiated.handle)
        .map_err(|error| {
            format!("provider read of {name} cannot seal its per-attempt access: {error}")
        })?;
    deposits.deposit(
        need.occurrence(),
        FrozenReadAccess {
            binding: need.binding(),
            access: FrozenProviderRead {
                access,
                generation: materialization.planning_lease.clone(),
                catalog: catalog_properties,
                encoding: FrozenReadEncoding {
                    identity: identity.clone(),
                    columns: named_columns,
                    relation: provider_relation.clone(),
                    assignments: assignments.clone(),
                    offered_constraint: offered_constraint(&offer),
                    enforced_predicate,
                    unenforced_predicate,
                    remaining_expression,
                    work_source: work_source_of(relation),
                    encoder: Arc::clone(&encoder),
                },
            },
        },
    );

    // 7. Project the frozen read into the contract the plan is built against.
    let relation_payload = encoder
        .encode_relation_payload(&provider_relation)
        .map_err(|error| format!("provider read of {name} cannot encode its relation: {error}"))?;
    let input_version = ExactInputVersion::try_new(frozen.input_version().as_bytes().to_vec())
        .map_err(|error| format!("provider read of {name} has no exact input version: {error}"))?;
    let contract = ProviderReadStaticContract {
        sql_binding: need.binding(),
        request: ProviderReadRequestBinding::from_need(need),
        read: ProviderReadReference {
            binding: negotiated.handle.binding().clone(),
            input_version,
            relation: relation_payload,
        },
        work_source: work_source_of(relation),
        selection_digest: frozen.selection_digest(),
        schema,
        predicates: predicate_facts(need, &offer, &negotiated.outcomes),
        limit: limit_fact(need, &offer, &negotiated.outcomes),
        provided_properties: read_properties(frozen.properties(), &columns_by_ordinal, &name)?,
        // Artifact inputs belong to reads derived from a sealed artifact, and
        // the coverage a provider publishes here names digests rather than the
        // artifacts themselves. The materialized-view slice that produces such
        // a read is the owner that can name them.
        artifact_inputs: Box::new([]),
        artifact_refs: Box::new([]),
        coverage_evidence: frozen.coverage_evidence().into(),
    };
    ProviderReadFact::negotiated(need, contract).map_err(|error| {
        format!("provider read of {name} answered its own request wrongly: {error}")
    })
}

/// Which admission-frozen input this read names.
fn frozen_input(relation: &ProviderReadRelationNeed) -> Result<QueryFrozenReadInput, String> {
    let version = match relation {
        ProviderReadRelationNeed::Data { version, .. }
        | ProviderReadRelationNeed::FrozenInputSet { version, .. }
        | ProviderReadRelationNeed::Metadata { version, .. } => *version,
        ProviderReadRelationNeed::Delta { .. }
        | ProviderReadRelationNeed::PinnedFileSet { .. }
        | ProviderReadRelationNeed::TableExecute { .. } => {
            return Err(unsupported_family(relation));
        }
    };
    Ok(match version {
        ProviderReadVersionNeed::Current => QueryFrozenReadInput::Current,
        ProviderReadVersionNeed::Snapshot(snapshot_id) => {
            QueryFrozenReadInput::Snapshot(snapshot_id)
        }
    })
}

/// A relation family whose admitted carrier arrives with its own product.
///
/// Each of these is frozen from something the read need does not carry - a
/// pinned file cohort, a change window's endpoints, a rewrite group - and each
/// belongs to a statement family (DML, incremental view maintenance, table
/// maintenance) whose own cutover owns that carrier. Refusing is how a read of
/// the wrong relation stays impossible in the meantime: a change window
/// answered with a table handle would silently read the whole relation.
fn unsupported_family(relation: &ProviderReadRelationNeed) -> String {
    let (family, owner) = match relation {
        ProviderReadRelationNeed::Delta { .. } => ("change window", "incremental view maintenance"),
        ProviderReadRelationNeed::PinnedFileSet { .. } => ("pinned file set", "row mutation"),
        ProviderReadRelationNeed::TableExecute { .. } => ("table execute", "table maintenance"),
        ProviderReadRelationNeed::Data { .. }
        | ProviderReadRelationNeed::FrozenInputSet { .. }
        | ProviderReadRelationNeed::Metadata { .. } => ("relation", "query"),
    };
    format!(
        "provider read of a {family} relation is frozen from a carrier this read request does not \
         name; the {owner} product owns it"
    )
}

fn relation_identity(relation: &ProviderReadRelationNeed) -> &TableIdentity {
    match relation {
        ProviderReadRelationNeed::Data { relation, .. }
        | ProviderReadRelationNeed::FrozenInputSet { relation, .. }
        | ProviderReadRelationNeed::Metadata { relation, .. }
        | ProviderReadRelationNeed::Delta { relation, .. }
        | ProviderReadRelationNeed::PinnedFileSet { relation }
        | ProviderReadRelationNeed::TableExecute { relation } => relation,
    }
}

/// How this read's work reaches a backend.
///
/// Only the provider knows for a metadata relation, and it says so through the
/// plan it returns; an ordinary relation is always enumerated into splits.
const fn work_source_of(relation: &ProviderReadRelationNeed) -> ConnectorReadWorkSource {
    match relation {
        ProviderReadRelationNeed::Metadata { .. } => ConnectorReadWorkSource::WholeRelation,
        _ => ConnectorReadWorkSource::RuntimeSplits,
    }
}

/// Open the relation family this read names.
fn open_relation(
    metadata: &dyn ConnectorReadMetadata,
    session: &ConnectorSession,
    table: &SchemaTableName,
    relation: &ProviderReadRelationNeed,
    name: &str,
) -> Result<ConnectorReadTableHandle, String> {
    match relation {
        ProviderReadRelationNeed::Data { version, .. }
        | ProviderReadRelationNeed::FrozenInputSet { version, .. } => metadata
            .get_table_handle(session, table, relation_version(*version), None)
            .map_err(|error| format!("provider read of {name} cannot be opened: {error}"))?
            .ok_or_else(|| {
                format!("provider read of {name} is no longer resolvable after admission pinned it")
            }),
        ProviderReadRelationNeed::Metadata { kind, version, .. } => {
            let request = ConnectorReadMetadataRequest::try_new(
                metadata_kind(*kind)?,
                metadata_version(*version),
            )
            .map_err(|error| format!("provider read of {name}: {error}"))?;
            let plan = metadata
                .get_system_table_plan_for_request(session, table, &request)
                .map_err(|error| {
                    format!("provider read of {name} cannot be opened as metadata: {error}")
                })?
                .ok_or_else(|| {
                    format!("provider read of {name} is not a metadata relation of this provider")
                })?;
            Ok(plan.into_handle())
        }
        _ => Err(unsupported_family(relation)),
    }
}

const fn relation_version(version: ProviderReadVersionNeed) -> ConnectorReadRelationVersion {
    match version {
        ProviderReadVersionNeed::Current => ConnectorReadRelationVersion::Current,
        ProviderReadVersionNeed::Snapshot(snapshot_id) => {
            ConnectorReadRelationVersion::SnapshotId(snapshot_id)
        }
    }
}

const fn metadata_version(version: ProviderReadVersionNeed) -> ConnectorReadMetadataVersion {
    match version {
        ProviderReadVersionNeed::Current => ConnectorReadMetadataVersion::Current,
        ProviderReadVersionNeed::Snapshot(snapshot_id) => {
            ConnectorReadMetadataVersion::SnapshotId(snapshot_id)
        }
    }
}

/// The provider's own spelling of one metadata relation.
fn metadata_kind(kind: MetadataTableKind) -> Result<ConnectorReadMetadataKind, String> {
    let suffix = match kind {
        MetadataTableKind::Snapshots => "$snapshots",
        MetadataTableKind::History => "$history",
        MetadataTableKind::Refs => "$refs",
        MetadataTableKind::Files => "$files",
        MetadataTableKind::Manifests => "$manifests",
        MetadataTableKind::Partitions => "$partitions",
        MetadataTableKind::Entries => "$entries",
    };
    ConnectorReadMetadataKind::try_new(suffix)
        .map_err(|error| format!("metadata relation kind {suffix}: {error}"))
}

/// Bind each requested ordinal to the provider column it names.
fn assignments_for(
    columns: &[ProviderReadColumnNeed],
    bindings: &[ConnectorReadColumnBinding],
    name: &str,
) -> Result<Vec<ConnectorReadAssignment>, String> {
    let mut assignments = Vec::with_capacity(columns.len());
    for column in columns {
        let binding = unique_binding(bindings, column.name(), name)?;
        let assignment = ConnectorReadAssignment::try_new(
            format!("{READ_VARIABLE_PREFIX}{}", column.ordinal()),
            binding.column().clone(),
            column.connector_type(),
        )
        .map_err(|error| {
            format!(
                "provider read of {name} cannot bind output column '{}': {error}",
                column.name()
            )
        })?;
        assignments.push(assignment);
    }
    Ok(assignments)
}

/// A read may name one provider column more than once; the reverse - one name
/// matching two provider columns - would make every predicate about it
/// ambiguous, and picking either would push down a predicate about the other.
fn unique_binding<'a>(
    bindings: &'a [ConnectorReadColumnBinding],
    column: &str,
    name: &str,
) -> Result<&'a ConnectorReadColumnBinding, String> {
    let mut matched = bindings.iter().filter(|binding| binding.name() == column);
    let first = matched.next().ok_or_else(|| {
        format!("provider read of {name} has no provider column named '{column}'")
    })?;
    if matched.next().is_some() {
        return Err(format!(
            "provider read of {name} matches more than one provider column named '{column}'"
        ));
    }
    Ok(first)
}

fn column_ordinals(
    assignments: &[ConnectorReadAssignment],
) -> BTreeMap<u32, ConnectorReadColumnHandle> {
    assignments
        .iter()
        .enumerate()
        .map(|(ordinal, assignment)| (ordinal_of(ordinal), assignment.column().clone()))
        .collect()
}

fn ordinal_of(ordinal: usize) -> u32 {
    u32::try_from(ordinal).unwrap_or(u32::MAX)
}

/// What was offered, and where each answer will be found.
struct OfferedOps {
    ops: Vec<ReadPushdownOp>,
    filter: Option<usize>,
    limit: Option<usize>,
}

/// Offer the projection, then the filter, then the limit.
fn offered_ops(
    need: &ProviderReadNeed,
    assignments: &[ConnectorReadAssignment],
    columns: &BTreeMap<u32, ConnectorReadColumnHandle>,
) -> Result<OfferedOps, String> {
    let mut ops = vec![ReadPushdownOp::Projection {
        assignments: assignments.to_vec(),
    }];
    let filter = if need.predicates().is_empty() {
        None
    } else {
        ops.push(ReadPushdownOp::Filter {
            constraint: provider_constraint(need.filter(), columns)?,
        });
        Some(ops.len() - 1)
    };
    let limit = need.limit().map(|rows| {
        ops.push(ReadPushdownOp::Limit { rows });
        ops.len() - 1
    });
    Ok(OfferedOps { ops, filter, limit })
}

/// Restate the request's constraint in the provider's own column vocabulary.
///
/// A requested ordinal with no provider column is refused rather than dropped:
/// dropping it would widen what the provider is told to prune by, which reads
/// as a weaker hint but is really a different question than the one asked.
fn provider_constraint(
    filter: &Constraint<u32>,
    columns: &BTreeMap<u32, ConnectorReadColumnHandle>,
) -> Result<ConnectorReadConstraint, String> {
    let mut missing = None;
    let summary = filter.summary().transform_keys(|ordinal| {
        let column = columns.get(ordinal).cloned();
        if column.is_none() {
            missing = Some(*ordinal);
        }
        column
    });
    let mut assignments = BTreeMap::new();
    for (variable, ordinal) in filter.assignments() {
        let column = columns
            .get(ordinal)
            .cloned()
            .ok_or_else(|| format!("read constraint names projection ordinal {ordinal}, which the read does not project"))?;
        assignments.insert(Arc::clone(variable), column);
    }
    if let Some(ordinal) = missing {
        return Err(format!(
            "read constraint names projection ordinal {ordinal}, which the read does not project"
        ));
    }
    ConnectorReadConstraint::try_new(summary, filter.expression().clone(), assignments)
        .map_err(|error| format!("read constraint cannot be offered: {error}"))
}

/// What the provider guaranteed, per predicate occurrence.
///
/// Only `Exact` relieves the engine. A provider that merely prunes has
/// answered about its own work, not about which rows the query returns, so
/// every occurrence stays the engine's to evaluate.
fn predicate_facts(
    need: &ProviderReadNeed,
    offer: &OfferedOps,
    outcomes: &[ReadPushdownOutcome],
) -> Box<[ProviderReadPredicateFact]> {
    let guarantee = offer
        .filter
        .and_then(|index| outcomes.get(index))
        .filter(|outcome| outcome.disposition == ReadPushdownDisposition::Exact)
        .map_or(PredicateGuaranteeKind::PruningOnly, |_| {
            PredicateGuaranteeKind::Exact
        });
    need.predicates()
        .iter()
        .map(|predicate| ProviderReadPredicateFact::new(predicate.occurrence(), guarantee))
        .collect()
}

/// Whether the provider took the limit on, or only used it.
fn limit_fact(
    need: &ProviderReadNeed,
    offer: &OfferedOps,
    outcomes: &[ReadPushdownOutcome],
) -> ProviderReadLimitFact {
    let Some(rows) = need.limit() else {
        return ProviderReadLimitFact::NotRequested;
    };
    let exact = offer
        .limit
        .and_then(|index| outcomes.get(index))
        .is_some_and(|outcome| outcome.disposition == ReadPushdownDisposition::Exact);
    if exact {
        ProviderReadLimitFact::Exact(rows)
    } else {
        ProviderReadLimitFact::Residual(rows)
    }
}

/// What the provider was offered as a filter, if anything.
fn offered_constraint(offer: &OfferedOps) -> ConnectorReadConstraint {
    offer
        .filter
        .and_then(|index| match &offer.ops[index] {
            ReadPushdownOp::Filter { constraint } => Some(constraint.clone()),
            _ => None,
        })
        .unwrap_or_else(|| ConnectorReadConstraint::of_summary(TupleDomain::all()))
}

/// Who evaluates what, after the provider answered the filter.
///
/// Only `Exact` relieves the engine, and that is decided elsewhere - here it
/// decides what the *reader* is told to do, which is a different question with
/// the same answer at only one of the three dispositions.
///
/// - `Exact`: the provider guarantees the filter, so the engine stops
///   evaluating it and the reader applies only what came back as residual.
/// - `PruningOnly`: the provider will use the filter but guarantees nothing, so
///   the engine keeps evaluating it. The reader is still handed the domain,
///   because applying it removes only rows the engine would remove anyway -
///   evaluating a predicate twice costs time, and skipping the pruning costs
///   the entire point of a pruning answer.
/// - `Unsupported`, or no filter offered: the reader is told nothing. An
///   unenforced domain is the reader's own work by contract, and a relation the
///   provider declined may have no reader that applies one - a metadata
///   relation read whole by a single backend opens its page source with no
///   constraint at all, so a predicate parked there would be applied by nobody.
fn filter_responsibility(
    offer: &OfferedOps,
    outcomes: &[ReadPushdownOutcome],
) -> (
    TupleDomain<ConnectorReadColumnHandle>,
    TupleDomain<ConnectorReadColumnHandle>,
    Option<ConnectorExpression>,
) {
    let nothing_for_the_reader = (TupleDomain::all(), TupleDomain::all(), None);
    let Some(index) = offer.filter else {
        return nothing_for_the_reader;
    };
    let ReadPushdownOp::Filter {
        constraint: offered,
    } = &offer.ops[index]
    else {
        return nothing_for_the_reader;
    };
    let Some(answer) = outcomes.get(index) else {
        return nothing_for_the_reader;
    };
    match answer.disposition {
        ReadPushdownDisposition::Unsupported => nothing_for_the_reader,
        ReadPushdownDisposition::PruningOnly => (
            // No guarantee is claimed for any column: the engine evaluates the
            // whole predicate regardless of what the provider skipped.
            TupleDomain::all(),
            offered.summary().clone(),
            None,
        ),
        ReadPushdownDisposition::Exact => {
            let Some(residual) = answer.residual.as_ref() else {
                // A provider that guarantees a filter says what remains, even
                // when nothing does. Without that we cannot tell what it kept,
                // and guessing "nothing remains" would drop rows.
                return nothing_for_the_reader;
            };
            let unenforced = residual.summary().clone();
            // Enforcement is claimed only for a column the provider kept
            // whole. A column handed back partially is covered by its own
            // guarantee for the complement, and by the reader for the rest.
            let enforced = offered
                .summary()
                .filter_columns(|column| unenforced.domain_for(column).is_none());
            let remaining = Some(residual.expression().clone()).filter(|e| !e.is_constant_true());
            (enforced, unenforced, remaining)
        }
    }
}

/// The provider's column identity for each requested ordinal, and the name the
/// request used for it.
fn column_facts(
    columns: &[ProviderReadColumnNeed],
    assignments: &[ConnectorReadAssignment],
    encoder: &dyn ConnectorReadWireEncoder,
    name: &str,
) -> Result<
    (
        Box<[ProviderReadColumnFact]>,
        Box<[(ProviderColumnReference, ProviderReadColumnNeed)]>,
    ),
    String,
> {
    let mut facts = Vec::with_capacity(columns.len());
    let mut named = Vec::with_capacity(columns.len());
    for (column, assignment) in columns.iter().zip(assignments.iter()) {
        let payload = encoder
            .encode_column_payload(assignment.column())
            .map_err(|error| {
                format!(
                    "provider read of {name} cannot encode column '{}': {error}",
                    column.name()
                )
            })?;
        let reference = ProviderColumnReference {
            column_payload: payload,
        };
        facts.push(ProviderReadColumnFact::new(
            column.ordinal(),
            reference.clone(),
            column.engine_type().clone(),
        ));
        named.push((reference, column.clone()));
    }
    Ok((facts.into_boxed_slice(), named.into_boxed_slice()))
}

/// The physical facts the provider guarantees, in the request's own ordinals.
fn read_properties(
    properties: &ConnectorReadProperties<ConnectorReadColumnHandle>,
    ordinals: &BTreeMap<ConnectorReadColumnHandle, u32>,
    name: &str,
) -> Result<ProviderReadProperties, String> {
    let distribution = match properties.distribution() {
        ConnectorReadDistribution::Unconstrained => ProviderReadDistribution::Unconstrained,
        ConnectorReadDistribution::Singleton => ProviderReadDistribution::Singleton,
        ConnectorReadDistribution::RoundRobin => ProviderReadDistribution::RoundRobin,
        // A partitioned read is only usable as a plan property with a proof
        // the provider contract does not carry: a hash read needs its exact
        // partition-count token, a bucket read its ordinal-domain evidence.
        // Claiming the property without them would let the plan skip an
        // exchange on a guarantee nobody made.
        ConnectorReadDistribution::Hash { .. }
        | ConnectorReadDistribution::BucketShuffle { .. } => {
            return Err(format!(
                "provider read of {name} declares a partitioned distribution without the \
                 partition proof a plan property requires"
            ));
        }
    };
    let ordering = properties
        .ordering()
        .iter()
        .map(|key| {
            let request_ordinal = ordinals.get(key.column()).copied().ok_or_else(|| {
                format!("provider read of {name} orders by a column it does not project")
            })?;
            Ok(ProviderReadOrderingKey {
                request_ordinal,
                direction: match key.direction() {
                    ConnectorReadSortDirection::Ascending => SortDirection::Ascending,
                    ConnectorReadSortDirection::Descending => SortDirection::Descending,
                },
                null_ordering: match key.null_ordering() {
                    ConnectorReadNullOrdering::First => NullOrdering::First,
                    ConnectorReadNullOrdering::Last => NullOrdering::Last,
                },
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(ProviderReadProperties {
        distribution,
        ordering: ordering.into_boxed_slice(),
    })
}

/// The request-scoped control one read's attempt access is sealed against.
fn request_control_for(
    read: &ConnectorControlReadBinding,
    context: &ConnectorRequestContext,
) -> Result<ConnectorReadRequestControl, String> {
    let Some(factory) = read.request_factory() else {
        return Ok(ConnectorReadRequestControl::unsupported_attempt_access(
            read.metadata(),
            read.splits(),
        ));
    };
    let planning =
        ConnectorPlanningContext::try_from_request(context.clone().without_attempt_capabilities())
            .map_err(|error| format!("read planning context: {error}"))?;
    factory
        .for_planning(&planning)
        .map_err(|error| format!("read request control: {error}"))
}

#[cfg(test)]
mod tests {
    use novarocks_physical_plan::{ProviderReadOccurrenceId, ValueType};
    use novarocks_spi::connector::read_stack::{ConnectorValueType, TupleDomain};
    use novarocks_sql::binding::SqlTableBindingAllocator;
    use novarocks_sql::compiler::fixtures;
    use novarocks_types::naming::TableIdentity;

    use super::*;

    fn identity() -> TableIdentity {
        TableIdentity::new("iceberg", "db", "orders")
    }

    fn a_binding() -> novarocks_sql::binding::SqlTableBindingId {
        SqlTableBindingAllocator::try_new_for_test(std::num::NonZeroU64::new(1).unwrap())
            .expect("allocator")
            .allocate()
            .expect("binding")
    }

    /// One read of one column, with one predicate and a limit, so both
    /// responsibilities below have something to be decided about.
    fn a_need(limit: Option<u64>) -> ProviderReadNeed {
        let column = fixtures::provider_read_column_need(
            0,
            "order_key",
            ValueType::new(arrow::datatypes::DataType::Int64, false),
            ConnectorValueType::BigInt,
        )
        .expect("column need");
        let predicate = fixtures::provider_read_predicate_need(
            fixtures::provider_predicate_occurrence(0),
            Constraint::of_summary(TupleDomain::all()),
        );
        fixtures::provider_read_need(
            1,
            ProviderReadOccurrenceId::new(1),
            a_binding(),
            ProviderReadRelationNeed::Data {
                relation: identity(),
                version: ProviderReadVersionNeed::Current,
            },
            vec![column],
            vec![predicate],
            limit,
        )
        .expect("provider read need")
    }

    /// The offer layout production builds for this need: projection, then
    /// filter, then limit. Only the positions matter to what is read back, so
    /// the operations themselves stand in for ones carrying provider columns.
    fn offer_of(need: &ProviderReadNeed) -> OfferedOps {
        let mut ops = vec![ReadPushdownOp::Projection {
            assignments: Vec::new(),
        }];
        let filter = (!need.predicates().is_empty()).then(|| {
            ops.push(ReadPushdownOp::Filter {
                constraint: Constraint::of_summary(TupleDomain::all()),
            });
            ops.len() - 1
        });
        let limit = need.limit().map(|rows| {
            ops.push(ReadPushdownOp::Limit { rows });
            ops.len() - 1
        });
        OfferedOps { ops, filter, limit }
    }

    /// One answer per offered operation, in the offered order.
    fn all_answered(
        offer: &OfferedOps,
        filter: ReadPushdownDisposition,
        limit: ReadPushdownDisposition,
    ) -> Vec<ReadPushdownOutcome> {
        let mut outcomes = vec![answered(ReadPushdownDisposition::Exact); offer.ops.len()];
        if let Some(index) = offer.filter {
            outcomes[index] = answered(filter);
        }
        if let Some(index) = offer.limit {
            outcomes[index] = answered(limit);
        }
        outcomes
    }

    fn answered(disposition: ReadPushdownDisposition) -> ReadPushdownOutcome {
        ReadPushdownOutcome {
            disposition,
            residual: None,
        }
    }

    /// The version a read names is the admitted input it reads, and it selects
    /// that input by value alone.
    #[test]
    fn a_read_names_the_input_it_was_admitted_against() {
        assert!(matches!(
            frozen_input(&ProviderReadRelationNeed::Data {
                relation: identity(),
                version: ProviderReadVersionNeed::Current,
            }),
            Ok(QueryFrozenReadInput::Current)
        ));
        assert!(matches!(
            frozen_input(&ProviderReadRelationNeed::FrozenInputSet {
                relation: identity(),
                version: ProviderReadVersionNeed::Snapshot(7),
            }),
            Ok(QueryFrozenReadInput::Snapshot(7))
        ));
    }

    /// A change window is frozen from endpoints this read request does not
    /// name. Opening it as an ordinary table would read the whole relation
    /// instead of the difference between two snapshots, so it is refused.
    #[test]
    fn a_relation_frozen_from_an_unnamed_carrier_is_refused() {
        for relation in [
            ProviderReadRelationNeed::Delta {
                relation: identity(),
                from_snapshot_id: 1,
                to_snapshot_id: 2,
            },
            ProviderReadRelationNeed::PinnedFileSet {
                relation: identity(),
            },
            ProviderReadRelationNeed::TableExecute {
                relation: identity(),
            },
        ] {
            assert!(
                frozen_input(&relation).is_err(),
                "{relation:?} has no admitted input this request names"
            );
        }
    }

    /// Only `Exact` relieves the engine. A provider that prunes has answered
    /// about its own work, not about which rows the query returns, so the
    /// engine keeps evaluating every predicate.
    #[test]
    fn only_an_exact_filter_answer_guarantees_a_predicate() {
        let need = a_need(None);
        let offer = offer_of(&need);
        for (disposition, expected) in [
            (
                ReadPushdownDisposition::Exact,
                PredicateGuaranteeKind::Exact,
            ),
            (
                ReadPushdownDisposition::PruningOnly,
                PredicateGuaranteeKind::PruningOnly,
            ),
            (
                ReadPushdownDisposition::Unsupported,
                PredicateGuaranteeKind::PruningOnly,
            ),
        ] {
            let outcomes = all_answered(&offer, disposition, ReadPushdownDisposition::Exact);
            let facts = predicate_facts(&need, &offer, &outcomes);
            assert_eq!(facts.len(), 1);
            assert_eq!(facts[0].guarantee(), expected, "{disposition:?}");
        }
    }

    /// A filter the provider declined leaves the reader nothing to apply. The
    /// engine evaluates the whole predicate, and a relation read whole by one
    /// backend has no reader that would apply a parked domain anyway.
    #[test]
    fn a_declined_filter_leaves_the_reader_nothing() {
        let need = a_need(None);
        let offer = offer_of(&need);
        for disposition in [
            ReadPushdownDisposition::Unsupported,
            // `Exact` without a residual is the same case: a provider that
            // guarantees a filter says what remains, and without that we
            // cannot tell what it kept.
            ReadPushdownDisposition::Exact,
        ] {
            let outcomes = all_answered(&offer, disposition, ReadPushdownDisposition::Exact);
            let (enforced, unenforced, remaining) = filter_responsibility(&offer, &outcomes);
            assert_eq!(enforced, TupleDomain::all(), "{disposition:?}");
            assert_eq!(unenforced, TupleDomain::all(), "{disposition:?}");
            assert!(remaining.is_none(), "{disposition:?}");
        }
    }

    /// A limit the provider only used stays the engine's to enforce; repeating
    /// a limit is free, and not repeating one the provider never guaranteed
    /// returns rows the query excluded.
    #[test]
    fn a_limit_is_residual_unless_the_provider_took_it_on() {
        let unasked = a_need(None);
        assert!(matches!(
            limit_fact(&unasked, &offer_of(&unasked), &[]),
            ProviderReadLimitFact::NotRequested
        ));
        let need = a_need(Some(10));
        let offer = offer_of(&need);
        let took_it_on = all_answered(
            &offer,
            ReadPushdownDisposition::Exact,
            ReadPushdownDisposition::Exact,
        );
        assert!(matches!(
            limit_fact(&need, &offer, &took_it_on),
            ProviderReadLimitFact::Exact(10)
        ));
        let only_used_it = all_answered(
            &offer,
            ReadPushdownDisposition::Exact,
            ReadPushdownDisposition::PruningOnly,
        );
        assert!(matches!(
            limit_fact(&need, &offer, &only_used_it),
            ProviderReadLimitFact::Residual(10)
        ));
    }
}
