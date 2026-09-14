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

use std::collections::{BTreeMap, BTreeSet};

use novarocks_connector_contract::{
    ConnectorEncodedPayload, ConnectorRowMutationEffect, ConnectorWriteFieldToken,
    ConnectorWriteRouteId, WriteTargetOrdinal,
};

use crate::{
    AggregateBinding, AggregateCallId, ArtifactRefId, EdgeId, ExprArena, ExprId, FragmentId,
    NodeId, NullOrdering, PLAN_CONTRACT_REVISION, PlanVersionId, ProviderColumnReference, Relation,
    RuntimeFilterId, SealedArtifactRef, SealedArtifactSinkSpec, SortDirection, SortExpr, ValueId,
    ValueType,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriterDerivedKind {
    RelationKind,
    AffectedRows,
    CommitFragment,
    ChangeEvent,
    ArtifactReference,
    RelationAuxiliary,
    WriteTargetOrdinal,
    GroupingKey,
}

pub const WRITER_MULTIPLEX_SCHEMA_REVISION: u32 = 1;
pub const ROOT_WRITE_RESULT_SCHEMA_REVISION: u32 = 1;

#[derive(Clone, Debug, PartialEq)]
pub enum ValueOrigin {
    ProviderField {
        scan_node: NodeId,
        field: ProviderColumnReference,
    },
    Expr {
        node: NodeId,
        expr: ExprId,
    },
    NullExtended {
        node: NodeId,
        of: ValueId,
    },
    AggregateState {
        call: AggregateCallId,
        phase: crate::AggregatePhase,
    },
    AggregateResult {
        call: AggregateCallId,
    },
    NodeOutput {
        node: NodeId,
        output_ordinal: u32,
    },
    ExchangeImport {
        edge: EdgeId,
        source_value: ValueId,
    },
    CteImport {
        edge: EdgeId,
        producer_fragment: FragmentId,
        producer_value: ValueId,
    },
    WriterDerived {
        writer_node: NodeId,
        kind: WriterDerivedKind,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct ValueDef {
    pub id: ValueId,
    pub ty: ValueType,
    pub origin: ValueOrigin,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputPort {
    pub node: NodeId,
    /// Ordered occurrences. Repeating the same value is legal and intentional.
    pub columns: Box<[ValueId]>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResultField {
    pub name: Box<str>,
    pub alias: Option<Box<str>>,
    pub value: ValueId,
    pub ty: ValueType,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResultPort {
    pub fragment: FragmentId,
    pub output: OutputPort,
    pub fields: Box<[ResultField]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HashDefinition {
    pub algorithm: novarocks_type_contract::PartitionHashAlgorithm,
}

impl HashDefinition {
    pub const fn native_exchange() -> Self {
        Self {
            algorithm: novarocks_type_contract::PartitionHashAlgorithm::NativeExchangeV1,
        }
    }
}

/// Complete identity and definition of one ordinary hash-partition space.
///
/// The count parameter is independent from a fragment's pipeline DOP. Task
/// assignment instantiates it once from live topology within its admissible
/// domain; the final plan carries no topology-derived count or route.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HashPartitionScheme {
    pub space: novarocks_type_contract::PartitionSpaceId,
    pub count: PartitionCountParameter,
    pub definition: HashDefinition,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PartitionCountParameter {
    pub id: novarocks_type_contract::PartitionCountParameterId,
    pub admissible: PartitionCountDomain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PartitionCountDomain {
    pub min: u32,
    pub max: u32,
    pub requires_power_of_two: bool,
}

impl PartitionCountDomain {
    /// Whether one TaskAssignment-time count is a member of this domain.
    pub const fn admits(self, actual_count: u32) -> bool {
        actual_count >= self.min
            && actual_count <= self.max
            && (!self.requires_power_of_two || actual_count.is_power_of_two())
    }
}

/// Exact evidence for the ordinal domain of a bucket layout.
///
/// The digest binds the complete ordered bucket-ordinal domain supplied by the
/// planner, including ordinals whose current scan selection has no work. This
/// preserves the bucket-to-destination alignment required by bucket shuffle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketOrdinalDomainProof {
    pub first_ordinal: u32,
    pub ordinal_count: u32,
    pub evidence_digest: [u8; 32],
}

/// Complete identity and definition of one bucket partition space.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketPartitionScheme {
    pub space: novarocks_type_contract::PartitionSpaceId,
    pub bucket_count: u32,
    pub hash: novarocks_type_contract::PartitionHashAlgorithm,
    pub layout: novarocks_type_contract::BucketLayoutAlgorithm,
    pub ordinal_domain: BucketOrdinalDomainProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Distribution {
    Unconstrained,
    Singleton,
    RoundRobin,
    Broadcast,
    Hash {
        keys: Box<[ValueId]>,
        scheme: HashPartitionScheme,
    },
    BucketShuffle {
        keys: Box<[ValueId]>,
        scheme: BucketPartitionScheme,
    },
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct OrderingKey {
    pub value: ValueId,
    pub direction: SortDirection,
    pub null_ordering: NullOrdering,
}

/// Whether one logical row occurrence exists on one execution lane or may
/// have been copied onto multiple lanes.
///
/// This is independent from distribution layout. A repartition can change the
/// layout but cannot erase copies that already exist.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowMultiplicity {
    SingleCopy,
    Replicated,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalProperties {
    pub distribution: Distribution,
    pub row_multiplicity: RowMultiplicity,
    pub ordering: Box<[OrderingKey]>,
}

/// Derive the exact properties preserved by a filter.
///
/// A broadcast input remains replica-equivalent only when every replica
/// evaluates the predicate identically.
pub fn derive_filter_output_properties(
    input: &PhysicalProperties,
    predicate_replica_deterministic: bool,
) -> PhysicalProperties {
    let distribution =
        if input.distribution == Distribution::Broadcast && !predicate_replica_deterministic {
            Distribution::Unconstrained
        } else {
            input.distribution.clone()
        };
    PhysicalProperties {
        distribution,
        row_multiplicity: input.row_multiplicity,
        ordering: input.ordering.clone(),
    }
}

/// Derive the exact properties preserved by a projection.
///
/// Partitioning survives only when every partition key is still materialized.
/// Ordering survives as its longest consecutive output prefix. Broadcast
/// replica equivalence additionally requires deterministic expressions.
pub fn derive_project_output_properties(
    input: &PhysicalProperties,
    output_values: &[ValueId],
    expressions_replica_deterministic: bool,
) -> PhysicalProperties {
    let output_values = output_values.iter().copied().collect::<BTreeSet<_>>();
    let distribution = match &input.distribution {
        Distribution::Hash { keys, .. } | Distribution::BucketShuffle { keys, .. }
            if keys.iter().all(|key| output_values.contains(key)) =>
        {
            input.distribution.clone()
        }
        Distribution::Hash { .. } | Distribution::BucketShuffle { .. } => {
            Distribution::Unconstrained
        }
        Distribution::Broadcast if !expressions_replica_deterministic => {
            Distribution::Unconstrained
        }
        distribution => distribution.clone(),
    };
    let ordering = input
        .ordering
        .iter()
        .take_while(|key| output_values.contains(&key.value))
        .copied()
        .collect::<Vec<_>>()
        .into_boxed_slice();
    PhysicalProperties {
        distribution,
        row_multiplicity: input.row_multiplicity,
        ordering,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JoinKind {
    Cross,
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
    NullAwareLeftAnti,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JoinDistribution {
    Colocated,
    Partitioned,
    BroadcastBuild,
    Singleton,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum JoinSide {
    Left,
    Right,
}

impl JoinSide {
    pub const fn input_ordinal(self) -> u32 {
        match self {
            Self::Left => 0,
            Self::Right => 1,
        }
    }

    pub const fn opposite(self) -> Self {
        match self {
            Self::Left => Self::Right,
            Self::Right => Self::Left,
        }
    }
}

/// Closed execution placement modes for a nested-loop join.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NestLoopJoinDistribution {
    /// Both inputs are globally gathered before evaluation.
    Singleton,
    /// The right input is replicated to every partition of the left input.
    BroadcastRight,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggregateCall {
    pub id: AggregateCallId,
    pub binding: AggregateBinding,
    pub arguments: Box<[ExprId]>,
    pub distinct: bool,
    pub order_by: Box<[SortExpr]>,
    pub output: ValueId,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WindowExpression {
    pub expression: ExprId,
    pub output: ValueId,
}

/// One node-owned window partition/order contract shared by every call in the
/// node. Individual `ExprKind::WindowCall` values carry only function-specific
/// arguments, ordering, frame and NULL treatment.
#[derive(Clone, Debug, PartialEq)]
pub struct WindowSpec {
    pub partition_by: Box<[SortExpr]>,
    pub order_by: Box<[SortExpr]>,
    pub expressions: Box<[WindowExpression]>,
}

/// One ordered output occurrence of a table-function node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableFunctionOutput {
    /// An outer-input value retained with the same value identity.
    PassThrough(ValueId),
    /// A value produced by one column of the bound relation result.
    FunctionResult { result_ordinal: u32, value: ValueId },
}

impl TableFunctionOutput {
    pub const fn value(self) -> ValueId {
        match self {
            Self::PassThrough(value) | Self::FunctionResult { value, .. } => value,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SetOperationKind {
    UnionAll,
    Intersect,
    Except,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriterTarget {
    /// Provider-private write handle with an exact public binding header.
    pub handle: ConnectorEncodedPayload,
    pub write_target_ordinal: WriteTargetOrdinal,
    pub input: Box<[ValueId]>,
    pub required_distribution: Distribution,
    pub target_fields: Box<[WriterTargetField]>,
    pub output_schema: WriterRelationSchema,
    pub partial_aggregates: Box<[WriterAggregateCall]>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriterTargetField {
    pub token: ConnectorWriteFieldToken,
    pub input: ValueId,
    pub ty: ValueType,
    pub hidden: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriterRelationFieldRole {
    Kind,
    TargetOrdinal,
    RowCount,
    CommitFragment,
    Auxiliary,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriterRelationField {
    pub value: ValueId,
    pub name: Box<str>,
    pub ty: ValueType,
    pub role: WriterRelationFieldRole,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriterRelationSchema {
    pub revision: u32,
    pub fields: Box<[WriterRelationField]>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriterAggregateCall {
    pub input: ValueId,
    pub binding: AggregateBinding,
    pub output: ValueId,
}

#[derive(Clone, Debug, PartialEq)]
pub enum UnpivotConstant {
    Scalar(ExprId),
    Int32List(Box<[i32]>),
    Utf8Map(Box<[(Box<str>, Box<str>)]>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct UnpivotValueMapping {
    pub input: ValueId,
    pub constants: Box<[UnpivotConstant]>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UnpivotSpec {
    pub passthrough: Box<[(ValueId, ValueId)]>,
    pub value_output: ValueId,
    pub literal_outputs: Box<[ValueId]>,
    pub mappings: Box<[UnpivotValueMapping]>,
    pub max_output_rows: u64,
    pub max_output_bytes: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriterGroupedUnpivotMapping {
    /// Exact writer target whose final aggregate group selects this mapping.
    pub write_target_ordinal: WriteTargetOrdinal,
    pub input: ValueId,
    pub constants: Box<[UnpivotConstant]>,
}

/// Writer-statistics expansion after target-grouped final aggregation.
///
/// This is distinct from an ordinary relational Unpivot: the grouping key
/// selects a target-local mapping set before expansion. Without that key,
/// constants belonging to different write targets would be cross-expanded.
#[derive(Clone, Debug, PartialEq)]
pub struct WriterGroupedUnpivotSpec {
    /// Exact subset of write targets that requested statistics expansion.
    /// Targets absent here still participate in the write and final summary.
    pub statistics_target_ordinals: Box<[WriteTargetOrdinal]>,
    pub grouping_input: ValueId,
    pub grouping_output: ValueId,
    pub passthrough_output: ValueId,
    pub value_output: ValueId,
    pub literal_outputs: Box<[ValueId]>,
    pub mappings: Box<[WriterGroupedUnpivotMapping]>,
    pub max_output_rows: u64,
    pub max_output_bytes: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriterFinishSpec {
    pub expected_target_ordinals: Box<[WriteTargetOrdinal]>,
    pub input_schema: WriterRelationSchema,
    pub output_schema: WriterRelationSchema,
    pub final_aggregates: Box<[WriterAggregateCall]>,
    pub grouped_unpivot: Option<WriterGroupedUnpivotSpec>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct JoinKey {
    pub left: ExprId,
    pub right: ExprId,
    pub null_safe: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PartitionTopNType {
    RowNumber,
    Rank,
    DenseRank,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SortMode {
    Global,
    Analytic {
        partition_by: Box<[SortExpr]>,
    },
    PartitionTopN {
        partition_by: Box<[SortExpr]>,
        limit: u64,
        kind: PartitionTopNType,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopNPhase {
    Single,
    Partial { sequence: crate::TopNSequenceId },
    Final { sequence: crate::TopNSequenceId },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowCountAssertion {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RowCountAssertionSpec {
    Global {
        subject: Box<str>,
        desired_rows: u64,
        comparison: RowCountAssertion,
    },
    PerKeyAtMostOne {
        keys: Box<[ValueId]>,
        labels: Box<[Box<str>]>,
        message: Box<str>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct GroupingOutput {
    pub output: ValueId,
    /// Ordered arguments of GROUPING/GROUPING_ID.
    pub arguments: Box<[ValueId]>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChangeEventSpec {
    pub predicate: Option<ExprId>,
    pub effect: ConnectorRowMutationEffect,
    pub assignments: Box<[(ValueId, Option<ExprId>)]>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChangeStreamRoute {
    pub route_id: ConnectorWriteRouteId,
    pub write_target_ordinal: WriteTargetOrdinal,
    pub accepted_effects: Box<[ConnectorRowMutationEffect]>,
    pub input_mapping: Box<[(ConnectorWriteFieldToken, ValueId)]>,
    pub partition_by: Box<[ValueId]>,
    pub edge: EdgeId,
}

#[derive(Clone, Debug, PartialEq)]
pub enum NodeKind {
    Scan {
        /// Query-local identity of this exact provider-read occurrence.
        ///
        /// Two scans may intentionally share one frozen provider relation and
        /// input version. The occurrence keeps their runtime-private
        /// capabilities distinct without changing provider relation identity.
        occurrence: crate::ProviderReadOccurrenceId,
        relation: Box<Relation>,
        read_budget: ScanReadBudget,
        provider_outputs: Box<[(ProviderColumnReference, ValueId)]>,
        /// Predicates that the engine must evaluate at the scan. This may also
        /// contain an `Exact` provider guarantee when deliberate rechecking is
        /// required, and may contain predicates that were never pushed down.
        residuals: Box<[ExprId]>,
        derived_values: Box<[ValueId]>,
    },
    /// Retains rows for which every predicate holds.
    ///
    /// The conjunct list is held here rather than folded into one expression so
    /// that predicate count never becomes expression depth, and so that
    /// consumers that reason per conjunct - pushdown, residual responsibility,
    /// runtime-filter placement - read the conjuncts directly instead of
    /// re-splitting a tree. Order is evaluation order and short-circuits at the
    /// first `false`, exactly as [`crate::ExprKind::Conjunction`] does.
    Filter {
        predicates: Box<[ExprId]>,
    },
    Project {
        expressions: Box<[(ExprId, ValueId)]>,
    },
    Aggregate {
        group_by: Box<[(ExprId, ValueId)]>,
        calls: Box<[AggregateCall]>,
    },
    HashJoin {
        kind: JoinKind,
        keys: Box<[JoinKey]>,
        build_side: JoinSide,
        distribution: JoinDistribution,
        residual: Option<ExprId>,
        null_extended: Box<[ValueId]>,
    },
    NestLoopJoin {
        kind: JoinKind,
        distribution: NestLoopJoinDistribution,
        predicate: Option<ExprId>,
        null_extended: Box<[ValueId]>,
    },
    Sort {
        order_by: Box<[SortExpr]>,
        mode: SortMode,
    },
    TopN {
        order_by: Box<[SortExpr]>,
        limit: u64,
        offset: u64,
        phase: TopNPhase,
    },
    Limit {
        limit: Option<u64>,
        offset: u64,
    },
    Window(WindowSpec),
    SetOp {
        kind: SetOperationKind,
        input_mappings: Box<[Box<[ValueId]>]>,
    },
    Values {
        rows: Box<[Box<[ExprId]>]>,
    },
    Repeat {
        grouping_sets: Box<[Box<[ValueId]>]>,
        /// Exact replacement for grouping values that can become NULL in at
        /// least one grouping set: `(input, nullable output)`.
        grouping_values: Box<[(ValueId, ValueId)]>,
        grouping_outputs: Box<[GroupingOutput]>,
    },
    Unpivot {
        spec: UnpivotSpec,
    },
    GenerateSeries {
        start: ExprId,
        stop: ExprId,
        step: Option<ExprId>,
    },
    TableFunction {
        function: crate::BoundTableFunction,
        arguments: Box<[ExprId]>,
        outputs: Box<[TableFunctionOutput]>,
        left_outer: bool,
    },
    AssertOneRow(RowCountAssertionSpec),
    ChangeEventExpand {
        events: Box<[ChangeEventSpec]>,
        effect_output: ValueId,
    },
    ExchangeSource {
        edge: EdgeId,
        imports: Box<[(ValueId, ValueId)]>,
    },
    TableWriter {
        target: WriterTarget,
    },
    TableFinish(WriterFinishSpec),
}

/// Explicit per-reader output-batch budget for one scan.
///
/// These values are semantic resource limits in the final plan. They are
/// supplied by planning and never reconstructed by a codec or runtime default.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScanReadBudget {
    pub max_batch_rows: u64,
    pub max_batch_bytes: u64,
}

impl NodeKind {
    pub(crate) fn expression_references(&self, output: &mut Vec<ExprId>) {
        match self {
            Self::Scan {
                relation,
                residuals,
                ..
            } => {
                output.extend(
                    relation
                        .predicate_guarantees()
                        .iter()
                        .map(|guarantee| guarantee.predicate),
                );
                output.extend(residuals.iter().copied());
            }
            Self::Filter { predicates } => output.extend(predicates.iter().copied()),
            Self::Project { expressions } => {
                output.extend(expressions.iter().map(|(expr, _)| *expr));
            }
            Self::Aggregate { group_by, calls } => {
                output.extend(group_by.iter().map(|(expression, _)| *expression));
                for call in calls {
                    output.extend(call.arguments.iter().copied());
                    output.extend(call.order_by.iter().map(|item| item.expr));
                }
            }
            Self::HashJoin { keys, residual, .. } => {
                for key in keys {
                    output.extend([key.left, key.right]);
                }
                output.extend(*residual);
            }
            Self::NestLoopJoin { predicate, .. } => output.extend(*predicate),
            Self::Sort { order_by, mode } => {
                output.extend(order_by.iter().map(|item| item.expr));
                match mode {
                    SortMode::Global => {}
                    SortMode::Analytic { partition_by }
                    | SortMode::PartitionTopN { partition_by, .. } => {
                        output.extend(partition_by.iter().map(|item| item.expr));
                    }
                }
            }
            Self::TopN { order_by, .. } => output.extend(order_by.iter().map(|item| item.expr)),
            Self::Window(spec) => {
                output.extend(spec.partition_by.iter().map(|item| item.expr));
                output.extend(spec.order_by.iter().map(|item| item.expr));
                output.extend(spec.expressions.iter().map(|item| item.expression));
            }
            Self::Values { rows } => {
                for row in rows {
                    output.extend(row.iter().copied());
                }
            }
            Self::GenerateSeries { start, stop, step } => {
                output.extend([*start, *stop]);
                output.extend(*step);
            }
            Self::TableFunction { arguments, .. } => output.extend(arguments.iter().copied()),
            Self::Unpivot { spec } => {
                for mapping in &spec.mappings {
                    for constant in &mapping.constants {
                        if let UnpivotConstant::Scalar(expression) = constant {
                            output.push(*expression);
                        }
                    }
                }
            }
            Self::ChangeEventExpand { events, .. } => {
                for event in events {
                    output.extend(event.predicate);
                    output.extend(event.assignments.iter().filter_map(|(_, expr)| *expr));
                }
            }
            Self::TableFinish(spec) => {
                if let Some(unpivot) = &spec.grouped_unpivot {
                    for mapping in &unpivot.mappings {
                        for constant in &mapping.constants {
                            if let UnpivotConstant::Scalar(expression) = constant {
                                output.push(*expression);
                            }
                        }
                    }
                }
            }
            Self::Limit { .. }
            | Self::SetOp { .. }
            | Self::Repeat { .. }
            | Self::AssertOneRow(_)
            | Self::ExchangeSource { .. }
            | Self::TableWriter { .. } => {}
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalNode {
    pub id: NodeId,
    pub inputs: Box<[NodeId]>,
    pub required_inputs: Box<[PhysicalProperties]>,
    pub output_properties: PhysicalProperties,
    pub output: OutputPort,
    pub kind: NodeKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PipelineDopDomain {
    pub min: u32,
    pub max: u32,
    pub requires_power_of_two: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum FragmentSink {
    Result,
    Stream {
        edge: EdgeId,
    },
    Multicast {
        edges: Box<[EdgeId]>,
    },
    Router {
        effect: ValueId,
        routes: Box<[ChangeStreamRoute]>,
    },
    SealedArtifact(Box<SealedArtifactSinkSpec>),
    Noop,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Fragment {
    id: FragmentId,
    root: NodeId,
    values: BTreeMap<ValueId, ValueDef>,
    expressions: ExprArena,
    nodes: BTreeMap<NodeId, PhysicalNode>,
    sink: FragmentSink,
    dop_domain: PipelineDopDomain,
    runtime_filters: Box<[RuntimeFilterId]>,
}

impl Fragment {
    pub const fn id(&self) -> FragmentId {
        self.id
    }

    pub const fn root(&self) -> NodeId {
        self.root
    }

    pub fn values(&self) -> &BTreeMap<ValueId, ValueDef> {
        &self.values
    }

    pub const fn expressions(&self) -> &ExprArena {
        &self.expressions
    }

    pub fn nodes(&self) -> &BTreeMap<NodeId, PhysicalNode> {
        &self.nodes
    }

    pub const fn sink(&self) -> &FragmentSink {
        &self.sink
    }

    pub const fn dop_domain(&self) -> PipelineDopDomain {
        self.dop_domain
    }

    pub fn runtime_filters(&self) -> &[RuntimeFilterId] {
        &self.runtime_filters
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EdgeKind {
    Stream,
    CteMulticast,
    ChangeStreamRouter,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeSource {
    pub fragment: FragmentId,
    pub projection: Box<[ValueId]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeDestination {
    pub fragment: FragmentId,
    pub node: NodeId,
    /// Ordered `(source value, destination import value)` mapping.
    pub receive_mapping: Box<[(ValueId, ValueId)]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Edge {
    pub id: EdgeId,
    pub kind: EdgeKind,
    pub source: EdgeSource,
    pub destination: EdgeDestination,
    pub partitioning: EdgePartitioning,
}

/// One exchange distribution expressed in both adjacent value domains.
///
/// The sender side names values in the source fragment. The receiver side
/// names the corresponding imported values in the destination fragment.
/// Keeping both forms avoids treating fragment-local `ValueId`s as global.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgePartitioning {
    pub source: Distribution,
    /// Multiplicity before this exchange distributes its source rows.
    pub source_multiplicity: RowMultiplicity,
    pub destination: Distribution,
    /// Multiplicity observed by the receiving fragment.
    pub destination_multiplicity: RowMultiplicity,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CutValue {
    pub value: ValueId,
    pub ty: ValueType,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CutImport {
    pub source: CutValue,
    pub destination: ValueId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangeStreamWriterCutField {
    pub token: ConnectorWriteFieldToken,
    pub source: ValueId,
    pub destination: ValueId,
}

/// Symmetric proof carried on both sides of a change-stream edge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangeStreamWriterCut {
    pub route_id: ConnectorWriteRouteId,
    pub write_target_ordinal: WriteTargetOrdinal,
    pub fields: Box<[ChangeStreamWriterCutField]>,
}

/// One writer relation field expressed in both adjacent fragment domains.
#[derive(Clone, Debug, PartialEq)]
pub struct WriterResultCutField {
    pub source: ValueId,
    pub destination: ValueId,
    pub name: Box<str>,
    pub ty: ValueType,
    pub role: WriterRelationFieldRole,
}

/// Symmetric proof that one stream edge carries one complete writer relation.
#[derive(Clone, Debug, PartialEq)]
pub struct WriterResultCut {
    pub write_target_ordinal: WriteTargetOrdinal,
    pub schema_revision: u32,
    pub fields: Box<[WriterResultCutField]>,
}

/// Complete inbound cut supplied with one independently validated fragment.
#[derive(Clone, Debug, PartialEq)]
pub struct InboundFragmentCut {
    pub edge: EdgeId,
    pub kind: EdgeKind,
    pub source_fragment: FragmentId,
    pub destination_node: NodeId,
    pub imports: Box<[CutImport]>,
    pub partitioning: EdgePartitioning,
    /// Exact provider inputs proven upstream of this edge.
    pub source_bindings: Box<[crate::ArtifactSourceBinding]>,
    /// Whether the upstream row set can contain rows with no provider source.
    pub has_source_free_rows: bool,
    pub change_stream_writer: Option<ChangeStreamWriterCut>,
    pub writer_result: Option<WriterResultCut>,
}

/// Complete outbound cut supplied with one independently validated fragment.
#[derive(Clone, Debug, PartialEq)]
pub struct OutboundFragmentCut {
    pub edge: EdgeId,
    pub kind: EdgeKind,
    pub destination_fragment: FragmentId,
    pub projection: Box<[CutValue]>,
    /// Exact source-to-destination value mapping at the peer boundary.
    pub destination_imports: Box<[CutImport]>,
    pub partitioning: EdgePartitioning,
    /// Exact provider inputs proven upstream of this edge.
    pub source_bindings: Box<[crate::ArtifactSourceBinding]>,
    /// Whether the upstream row set can contain rows with no provider source.
    pub has_source_free_rows: bool,
    pub change_stream_writer: Option<ChangeStreamWriterCut>,
    pub writer_result: Option<WriterResultCut>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FragmentCuts {
    pub inbound: Box<[InboundFragmentCut]>,
    pub outbound: Box<[OutboundFragmentCut]>,
    /// Exact immutable artifacts consumed by this fragment.
    pub artifact_refs: Box<[SealedArtifactRef]>,
    /// Complete static runtime-filter contracts with at least one local endpoint.
    pub runtime_filters: Box<[RuntimeFilter]>,
    /// Exact immutable plan subgraph needed to recompute every attached
    /// runtime-filter equality and scan-lineage proof without the rest of the
    /// query plan.
    pub runtime_filter_proof: RuntimeFilterProofGraph,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeFilterProofGraph {
    pub fragments: Box<[Fragment]>,
    pub edges: Box<[Edge]>,
    /// Complete filter contracts needed to prove blocking-wait closure. This
    /// includes the locally attached filters and any filter attached to a
    /// fragment on their transitive producer build dependency paths.
    pub filters: Box<[RuntimeFilter]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterKind {
    Bloom,
    MinMax,
    InList,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterNullSemantics {
    NeverMatches,
    NullSafeEqual,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeFilterOrderKey {
    pub ty: ValueType,
    pub direction: SortDirection,
    pub null_ordering: NullOrdering,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RuntimeFilterDomain {
    Membership {
        ty: ValueType,
        null_semantics: RuntimeFilterNullSemantics,
    },
    Ordered {
        key: RuntimeFilterOrderKey,
        inclusive: bool,
        comparator: novarocks_type_contract::OrderedComparisonAlgorithm,
    },
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RuntimeFilterContributionKind {
    ValueDomainDelta,
    FinalDomainShard,
    OrderedBoundUpdate,
    FinalOrderedHullShard,
    ProducerClosed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterLifecycle {
    CompleteOnce,
    MonotonicUpdates,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterReduction {
    SetUnion,
    TightenOrderedBound,
    UnionOrderedHull,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterCompletion {
    ProducerClosed,
    FencedCommittedDomain,
}

/// One node in a bounded, non-recursive runtime-filter coverage expression.
/// Composite children are arena indices in canonical strictly increasing
/// order and must precede their parent.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RuntimeFilterCoverageNode {
    Witness(crate::RuntimeFilterWitnessId),
    AllOf { children: Box<[u32]> },
    AnyOf { children: Box<[u32]> },
}

/// Static proof that a runtime-filter completion condition covers its exact
/// producer set. The flat arena keeps untrusted depth away from Rust call
/// stacks during clone, comparison, transport and destruction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterCoverage {
    pub nodes: Box<[RuntimeFilterCoverageNode]>,
    pub root: u32,
}

/// Exact static producer-input progress that closes one producer witness.
///
/// `build_edges` names the producer input frontier: the hash-join build input
/// or the Aggregate TopN input. `non_build_edges` is the remainder of the
/// producer fragment's inbound frontier and is empty for Aggregate TopN.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterProducerProgress {
    /// Complete inbound exchange set under the join build input.
    pub build_edges: Box<[EdgeId]>,
    /// Complete inbound exchange set outside the join build input.
    pub non_build_edges: Box<[EdgeId]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterApplyPoint {
    NodeInput { input_ordinal: u32 },
    NodeOutput,
    ScanSource,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterProducerTarget {
    JoinBuildKey {
        equality: crate::RuntimeFilterEqualityWitnessId,
    },
    AggregateTopNKey {
        group_key_ordinal: u32,
        /// Exact TopN whose monotonic bound is contributed by this
        /// aggregate. The validator replays the parent/key/order proof rather
        /// than trusting planner placement.
        topn: NodeId,
        phase: TopNPhase,
        order_key_ordinal: u32,
        limit: u64,
        offset: u64,
        direction: crate::SortDirection,
        null_ordering: crate::NullOrdering,
    },
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RuntimeFilterArtifactCapability {
    Membership,
    OrderedRange,
    EmptyDomain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LateApplyGranularity {
    Row,
    Batch,
    RowGroup,
    Split,
    File,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterConsumerActivation {
    BlockingSnapshot,
    /// Start without the filter, then install its one final snapshot only for
    /// work that has not crossed the named scheduling boundary.
    StartUnfilteredThenApplyComplete {
        late_apply: LateApplyGranularity,
    },
    /// Remain runnable while installing every monotonic update at the named
    /// scheduling boundary. There is no complete snapshot admission barrier.
    NonBlockingLive {
        late_apply: LateApplyGranularity,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeFilterConsumerTarget {
    JoinProbeKey {
        equality: crate::RuntimeFilterEqualityWitnessId,
    },
    ScanField {
        equality: crate::RuntimeFilterEqualityWitnessId,
        lineage: Box<[RuntimeFilterLineageStep]>,
    },
    AggregateTopNScanField {
        producer: crate::RuntimeFilterWitnessId,
        lineage: Box<[RuntimeFilterLineageStep]>,
    },
}

/// One checked reverse-lineage transition from a join probe key toward an
/// earlier scan field. Each variant names only information that the validator
/// can recompute from the immutable plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterLineageStep {
    FilterPassThrough {
        fragment: FragmentId,
        node: NodeId,
        input_ordinal: u32,
    },
    SortPassThrough {
        fragment: FragmentId,
        node: NodeId,
        input_ordinal: u32,
    },
    ProjectIdentity {
        fragment: FragmentId,
        node: NodeId,
        output_ordinal: u32,
    },
    /// Traverse one exact equality key of an inner hash join. `source_side`
    /// identifies the key currently named by the join output and
    /// `target_side` identifies the input/key followed by the reverse trace.
    JoinEquality {
        fragment: FragmentId,
        node: NodeId,
        key_ordinal: u32,
        source_side: JoinSide,
        target_side: JoinSide,
    },
    AggregateGroupKey {
        fragment: FragmentId,
        node: NodeId,
        group_key_ordinal: u32,
    },
    UnionAllBranch {
        fragment: FragmentId,
        node: NodeId,
        input_ordinal: u32,
        output_ordinal: u32,
    },
    ExchangeMapping {
        edge: EdgeId,
        mapping_ordinal: u32,
    },
}

/// Recomputable proof anchor tying one runtime-filter domain to one exact
/// hash-join equality key. Consumers may filter only the opposite key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeFilterEqualityWitness {
    pub id: crate::RuntimeFilterEqualityWitnessId,
    pub fragment: FragmentId,
    pub join: NodeId,
    pub key_ordinal: u32,
    pub domain_side: JoinSide,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeFilterProducer {
    pub witness: crate::RuntimeFilterWitnessId,
    pub endpoint: RuntimeFilterEndpoint,
    pub apply_point: RuntimeFilterApplyPoint,
    pub contribution_kinds: Box<[RuntimeFilterContributionKind]>,
    pub completion: RuntimeFilterCompletion,
    pub progress: RuntimeFilterProducerProgress,
    pub target: RuntimeFilterProducerTarget,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterEndpoint {
    pub fragment: FragmentId,
    pub node: NodeId,
    pub values: Box<[ValueId]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterConsumer {
    pub endpoint: RuntimeFilterEndpoint,
    pub apply_point: RuntimeFilterApplyPoint,
    pub capabilities: Box<[RuntimeFilterArtifactCapability]>,
    pub activation: RuntimeFilterConsumerActivation,
    pub target: RuntimeFilterConsumerTarget,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeFilterPolicy {
    /// Maximum accepted bytes from one producer contribution.
    pub max_contribution_bytes: u64,
    /// Maximum materialized artifact bytes. Exceeding either byte limit,
    /// exhausting retries, reaching the deadline, or losing a producer closes
    /// the optimization without an artifact. Every consumer then continues
    /// unfiltered; a runtime filter never fails the query or waits forever.
    pub max_artifact_bytes: u64,
    pub deadline_ms: u64,
    pub max_retries: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeFilter {
    pub id: RuntimeFilterId,
    pub kind: RuntimeFilterKind,
    pub domain: RuntimeFilterDomain,
    pub lifecycle: RuntimeFilterLifecycle,
    pub reduction: RuntimeFilterReduction,
    pub availability_coverage: RuntimeFilterCoverage,
    pub terminal_coverage: RuntimeFilterCoverage,
    pub equality_witnesses: Box<[RuntimeFilterEqualityWitness]>,
    pub producers: Box<[RuntimeFilterProducer]>,
    pub consumers: Box<[RuntimeFilterConsumer]>,
    pub policy: RuntimeFilterPolicy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequiredContracts {
    pub plan_contract_revision: u32,
}

impl Default for RequiredContracts {
    fn default() -> Self {
        Self {
            plan_contract_revision: PLAN_CONTRACT_REVISION,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PlanAnnotation {
    pub subject: AnnotationSubject,
    pub key: Box<str>,
    pub value: Box<str>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnnotationSubject {
    Plan,
    Fragment(FragmentId),
    Node(FragmentId, NodeId),
    Value(FragmentId, ValueId),
}

/// Complete immutable final physical plan.
#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalPlan {
    version: PlanVersionId,
    fragments: BTreeMap<FragmentId, Fragment>,
    edges: BTreeMap<EdgeId, Edge>,
    runtime_filters: BTreeMap<RuntimeFilterId, RuntimeFilter>,
    result_port: Option<ResultPort>,
    artifact_refs: BTreeMap<ArtifactRefId, SealedArtifactRef>,
    required: RequiredContracts,
    annotations: Box<[PlanAnnotation]>,
}

impl PhysicalPlan {
    pub const fn version(&self) -> PlanVersionId {
        self.version
    }

    pub fn fragments(&self) -> &BTreeMap<FragmentId, Fragment> {
        &self.fragments
    }

    pub fn edges(&self) -> &BTreeMap<EdgeId, Edge> {
        &self.edges
    }

    pub fn runtime_filters(&self) -> &BTreeMap<RuntimeFilterId, RuntimeFilter> {
        &self.runtime_filters
    }

    pub const fn result_port(&self) -> Option<&ResultPort> {
        self.result_port.as_ref()
    }

    pub fn artifact_refs(&self) -> &BTreeMap<ArtifactRefId, SealedArtifactRef> {
        &self.artifact_refs
    }

    pub const fn required(&self) -> RequiredContracts {
        self.required
    }

    pub fn annotations(&self) -> &[PlanAnnotation] {
        &self.annotations
    }
}

pub(crate) struct PhysicalPlanParts {
    pub version: PlanVersionId,
    pub fragments: BTreeMap<FragmentId, Fragment>,
    pub edges: BTreeMap<EdgeId, Edge>,
    pub runtime_filters: BTreeMap<RuntimeFilterId, RuntimeFilter>,
    pub result_port: Option<ResultPort>,
    pub artifact_refs: BTreeMap<ArtifactRefId, SealedArtifactRef>,
    pub required: RequiredContracts,
    pub annotations: Box<[PlanAnnotation]>,
}

impl From<PhysicalPlanParts> for PhysicalPlan {
    fn from(parts: PhysicalPlanParts) -> Self {
        Self {
            version: parts.version,
            fragments: parts.fragments,
            edges: parts.edges,
            runtime_filters: parts.runtime_filters,
            result_port: parts.result_port,
            artifact_refs: parts.artifact_refs,
            required: parts.required,
            annotations: parts.annotations,
        }
    }
}

pub(crate) struct FragmentParts {
    pub id: FragmentId,
    pub root: NodeId,
    pub values: BTreeMap<ValueId, ValueDef>,
    pub expressions: ExprArena,
    pub nodes: BTreeMap<NodeId, PhysicalNode>,
    pub sink: FragmentSink,
    pub dop_domain: PipelineDopDomain,
    pub runtime_filters: Box<[RuntimeFilterId]>,
}

impl From<FragmentParts> for Fragment {
    fn from(parts: FragmentParts) -> Self {
        Self {
            id: parts.id,
            root: parts.root,
            values: parts.values,
            expressions: parts.expressions,
            nodes: parts.nodes,
            sink: parts.sink,
            dop_domain: parts.dop_domain,
            runtime_filters: parts.runtime_filters,
        }
    }
}
