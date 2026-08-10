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

use arrow::datatypes::DataType;
use std::collections::{BTreeMap, BTreeSet};

use crate::query_execution::preparation::scan::ScanExecutionBindings;
use crate::sql::analysis::TypedExpr;
use crate::sql::planner::distributed::{FragmentId, PlanFragment};
use crate::sql::planner::runtime_filter::contract::{
    ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
    ContributionKind, PlanNodeId, ReductionRequirement, RuntimeFilterLogicalDomain,
};
use crate::sql::planner::runtime_filter::graph::{
    ApplyPoint, ConsumerBindingTarget, ProducerBindingTarget, RuntimeFilterBindingRole,
    RuntimeFilterBindingSpec, RuntimeFilterGraph, RuntimeFilterSemanticScanDomainTarget,
};

#[derive(Clone, Debug)]
pub(crate) struct RuntimeFilterBindingTable {
    fragment_id: FragmentId,
    bindings: BTreeMap<BindingId, PreparedRuntimeFilterBinding>,
}

impl RuntimeFilterBindingTable {
    pub(super) fn empty(fragment_id: FragmentId) -> Self {
        Self {
            fragment_id,
            bindings: BTreeMap::new(),
        }
    }

    pub(crate) const fn fragment_id(&self) -> FragmentId {
        self.fragment_id
    }

    pub(crate) fn bindings(&self) -> impl ExactSizeIterator<Item = &PreparedRuntimeFilterBinding> {
        self.bindings.values()
    }

    pub(crate) fn binding(&self, binding_id: BindingId) -> Option<&PreparedRuntimeFilterBinding> {
        self.bindings.get(&binding_id)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedRuntimeFilterBinding {
    binding_id: BindingId,
    channel_id: ChannelId,
    node_id: PlanNodeId,
    apply_point: ApplyPoint,
    expression: TypedExpr,
    logical_domain: RuntimeFilterLogicalDomain,
    reduction: ReductionRequirement,
    role: PreparedRuntimeFilterBindingRole,
}

impl PreparedRuntimeFilterBinding {
    pub(crate) const fn binding_id(&self) -> BindingId {
        self.binding_id
    }

    pub(crate) const fn channel_id(&self) -> ChannelId {
        self.channel_id
    }

    pub(crate) const fn node_id(&self) -> PlanNodeId {
        self.node_id
    }

    pub(crate) const fn apply_point(&self) -> ApplyPoint {
        self.apply_point
    }

    pub(crate) const fn expression(&self) -> &TypedExpr {
        &self.expression
    }

    pub(crate) const fn logical_domain(&self) -> &RuntimeFilterLogicalDomain {
        &self.logical_domain
    }

    pub(crate) const fn reduction(&self) -> ReductionRequirement {
        self.reduction
    }

    pub(crate) const fn role(&self) -> &PreparedRuntimeFilterBindingRole {
        &self.role
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PreparedRuntimeFilterBindingRole {
    Producer {
        contribution_kinds: BTreeSet<ContributionKind>,
        completion_requirement: CompletionRequirement,
        target: ProducerBindingTarget,
    },
    Consumer {
        capabilities: BTreeSet<ArtifactCapability>,
        activation: ConsumerActivation,
        target: PreparedRuntimeFilterConsumerTarget,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PreparedRuntimeFilterConsumerTarget {
    DirectInput {
        input_ordinal: usize,
    },
    SourceBoundary {
        scan_domain: Option<PreparedRuntimeFilterScanDomainTarget>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PreparedRuntimeFilterScanDomainTarget {
    pub(crate) field_ordinal: u32,
    pub(crate) data_type: DataType,
    pub(crate) nullable: bool,
}

pub(super) fn materialize_runtime_filter_binding_tables(
    graph: &RuntimeFilterGraph,
    fragments: &[PlanFragment],
) -> Result<BTreeMap<FragmentId, RuntimeFilterBindingTable>, String> {
    materialize_runtime_filter_binding_tables_with_scan_bindings(graph, fragments, None)
}

pub(super) fn materialize_runtime_filter_binding_tables_with_scan_bindings(
    graph: &RuntimeFilterGraph,
    fragments: &[PlanFragment],
    scan_bindings: Option<&ScanExecutionBindings>,
) -> Result<BTreeMap<FragmentId, RuntimeFilterBindingTable>, String> {
    let mut tables = BTreeMap::new();
    for fragment in fragments {
        if tables
            .insert(
                fragment.fragment_id,
                RuntimeFilterBindingTable::empty(fragment.fragment_id),
            )
            .is_some()
        {
            return Err(format!(
                "runtime filter binding materialization found duplicate fragment id={}",
                fragment.fragment_id
            ));
        }
    }

    let mut pending = graph
        .bindings()
        .map(|binding| (binding.binding_id, binding))
        .collect::<BTreeMap<_, _>>();

    fn visit(
        graph: &RuntimeFilterGraph,
        scan_bindings: Option<&ScanExecutionBindings>,
        pending: &mut BTreeMap<BindingId, &RuntimeFilterBindingSpec>,
        table: &mut RuntimeFilterBindingTable,
        node: &crate::sql::planner::distributed::DistributedNode,
    ) -> Result<(), String> {
        for binding_id in &node.runtime_filter_binding_ids {
            let Some(binding) = pending.remove(binding_id) else {
                return Err(if graph.binding(*binding_id).is_some() {
                    format!(
                        "runtime filter binding id={} is attached more than once",
                        binding_id.get()
                    )
                } else {
                    format!(
                        "runtime filter attachment references unknown binding id={}",
                        binding_id.get()
                    )
                });
            };
            if binding.location.fragment_id.get() != table.fragment_id
                || binding.location.node_id.get() != node.node_id
            {
                return Err(format!(
                    "runtime filter binding id={} location fragment_id={} node_id={} does not match attachment fragment_id={} node_id={}",
                    binding_id.get(),
                    binding.location.fragment_id.get(),
                    binding.location.node_id.get(),
                    table.fragment_id,
                    node.node_id
                ));
            }
            match (&binding.role, binding.apply_point) {
                (RuntimeFilterBindingRole::Producer(_), ApplyPoint::NodeOutput)
                | (RuntimeFilterBindingRole::Consumer(_), ApplyPoint::NodeInput) => {}
                (RuntimeFilterBindingRole::Producer(_), apply_point) => {
                    return Err(format!(
                        "runtime filter producer binding id={} must use NodeOutput, found {apply_point:?}",
                        binding_id.get()
                    ));
                }
                (RuntimeFilterBindingRole::Consumer(_), apply_point) => {
                    return Err(format!(
                        "runtime filter consumer binding id={} must use NodeInput, found {apply_point:?}",
                        binding_id.get()
                    ));
                }
            }
            let channel = graph.channel(binding.channel_id).ok_or_else(|| {
                format!(
                    "runtime filter binding id={} references unknown channel id={}",
                    binding_id.get(),
                    binding.channel_id.get()
                )
            })?;
            validate_expression_type(binding, &channel.logical_domain)?;
            let role = match &binding.role {
                RuntimeFilterBindingRole::Producer(requirement) => {
                    PreparedRuntimeFilterBindingRole::Producer {
                        contribution_kinds: requirement.contribution_kinds.clone(),
                        completion_requirement: requirement.completion_requirement,
                        target: requirement.target,
                    }
                }
                RuntimeFilterBindingRole::Consumer(requirement) => {
                    PreparedRuntimeFilterBindingRole::Consumer {
                        capabilities: requirement.capabilities.clone(),
                        activation: requirement.activation,
                        target: materialize_consumer_target(
                            *binding_id,
                            binding.location.fragment_id,
                            binding.location.node_id,
                            &requirement.target,
                            scan_bindings,
                        )?,
                    }
                }
            };
            let prepared = PreparedRuntimeFilterBinding {
                binding_id: *binding_id,
                channel_id: binding.channel_id,
                node_id: binding.location.node_id,
                apply_point: binding.apply_point,
                expression: binding.expression.clone(),
                logical_domain: channel.logical_domain.clone(),
                reduction: channel.reduction_requirement,
                role,
            };
            if table.bindings.insert(*binding_id, prepared).is_some() {
                return Err(format!(
                    "runtime filter binding id={} materialized more than once",
                    binding_id.get()
                ));
            }
        }
        for child in &node.children {
            visit(graph, scan_bindings, pending, table, child)?;
        }
        Ok(())
    }

    for fragment in fragments {
        let table = tables
            .get_mut(&fragment.fragment_id)
            .expect("table was initialized from the same fragment list");
        visit(graph, scan_bindings, &mut pending, table, &fragment.root)?;
    }
    if let Some((binding_id, binding)) = pending.first_key_value() {
        return Err(format!(
            "runtime filter graph binding id={} at fragment_id={} node_id={} has no node attachment",
            binding_id.get(),
            binding.location.fragment_id.get(),
            binding.location.node_id.get()
        ));
    }
    Ok(tables)
}

fn materialize_consumer_target(
    binding_id: BindingId,
    fragment_id: crate::sql::planner::runtime_filter::contract::PlanFragmentId,
    node_id: PlanNodeId,
    target: &ConsumerBindingTarget,
    scan_bindings: Option<&ScanExecutionBindings>,
) -> Result<PreparedRuntimeFilterConsumerTarget, String> {
    match target {
        ConsumerBindingTarget::DirectInput { input_ordinal } => {
            Ok(PreparedRuntimeFilterConsumerTarget::DirectInput {
                input_ordinal: *input_ordinal,
            })
        }
        ConsumerBindingTarget::SourceBoundary { scan_domain } => {
            let scan_domain = scan_domain
                .as_ref()
                .map(|target| {
                    materialize_scan_domain_target(
                        binding_id,
                        fragment_id,
                        node_id,
                        target,
                        scan_bindings,
                    )
                })
                .transpose()?;
            Ok(PreparedRuntimeFilterConsumerTarget::SourceBoundary { scan_domain })
        }
    }
}

fn materialize_scan_domain_target(
    binding_id: BindingId,
    fragment_id: crate::sql::planner::runtime_filter::contract::PlanFragmentId,
    node_id: PlanNodeId,
    target: &RuntimeFilterSemanticScanDomainTarget,
    scan_bindings: Option<&ScanExecutionBindings>,
) -> Result<PreparedRuntimeFilterScanDomainTarget, String> {
    let scan_bindings = scan_bindings.ok_or_else(|| {
        format!(
            "runtime filter binding id={} has a scan-domain semantic target without pinned scan bindings",
            binding_id.get()
        )
    })?;
    let binding = scan_bindings.binding(node_id.get()).ok_or_else(|| {
        format!(
            "runtime filter binding id={} scan-domain target has no pinned scan binding for node_id={}",
            binding_id.get(),
            node_id.get()
        )
    })?;
    let read = scan_bindings
        .connector_read(fragment_id.get(), node_id.get())
        .ok_or_else(|| {
            format!(
                "runtime filter binding id={} scan-domain target requires a pinned connector read for fragment_id={} node_id={}",
                binding_id.get(),
                fragment_id.get(),
                node_id.get()
            )
        })?;
    let physical = binding
        .physical_columns
        .iter()
        .filter(|column| column.planner.column_id == target.column_id)
        .collect::<Vec<_>>();
    let [physical] = physical.as_slice() else {
        return Err(format!(
            "runtime filter binding id={} scan-domain target column id {} does not resolve to exactly one pinned physical scan output",
            binding_id.get(),
            target.column_id
        ));
    };
    if physical.planner.data_type != target.data_type
        || physical.planner.nullable != target.nullable
        || physical.source.data_type != target.data_type
        || physical.source.nullable != target.nullable
    {
        return Err(format!(
            "runtime filter binding id={} scan-domain target column '{}' type/nullability drifted from its pinned scan binding",
            binding_id.get(),
            physical.source.name
        ));
    }
    let output_matches = read
        .scan
        .output_schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| field.name().eq_ignore_ascii_case(&physical.source.name))
        .collect::<Vec<_>>();
    let [(output_ordinal, output)] = output_matches.as_slice() else {
        return Err(format!(
            "runtime filter binding id={} scan-domain target source column '{}' does not resolve to exactly one pinned connector output",
            binding_id.get(),
            physical.source.name
        ));
    };
    if output.data_type() != &target.data_type || output.is_nullable() != target.nullable {
        return Err(format!(
            "runtime filter binding id={} scan-domain target source column '{}' type/nullability drifted from pinned connector output",
            binding_id.get(),
            physical.source.name
        ));
    }
    let field_ordinal = *read.provider_field_ordinals.get(*output_ordinal).ok_or_else(|| {
        format!(
            "runtime filter binding id={} scan-domain target connector output ordinal {} has no pinned provider ordinal",
            binding_id.get(), output_ordinal
        )
    })?;
    Ok(PreparedRuntimeFilterScanDomainTarget {
        field_ordinal,
        data_type: target.data_type.clone(),
        nullable: target.nullable,
    })
}

fn validate_expression_type(
    binding: &RuntimeFilterBindingSpec,
    domain: &RuntimeFilterLogicalDomain,
) -> Result<(), String> {
    let matches = match domain {
        RuntimeFilterLogicalDomain::Membership { value_type, .. } => {
            value_type == &binding.expression.data_type
        }
        RuntimeFilterLogicalDomain::OrderedBound(order) => {
            order.keys.len() == 1 && order.keys[0].data_type == binding.expression.data_type
        }
    };
    if matches {
        Ok(())
    } else {
        Err(format!(
            "runtime filter binding id={} expression type {:?} does not match channel domain",
            binding.binding_id.get(),
            binding.expression.data_type
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::analysis::{ExprKind, LiteralValue};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::distributed::{
        DataPartition, DataSink, DistributedNode, DistributedNodeKind, PlanFragment,
    };
    use crate::sql::planner::payload::PlanValuesNode;
    use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};
    use crate::sql::planner::runtime_filter::comparator::comparator_digest_for_plan;
    use crate::sql::planner::runtime_filter::contract::{
        ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
        ContributionKind, CoverageWitnessId, LateApplyGranularity, NullOrder, NullSemantics,
        OrderContract, OrderKeyContract, PlanFragmentId, PlanNodeId, ReductionRequirement,
        RuntimeFilterLifecycle, RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement,
        SortDirection,
    };
    use crate::sql::planner::runtime_filter::coverage::Coverage;
    use crate::sql::planner::runtime_filter::graph::{
        ApplyPoint, ConsumerBindingTarget, ConsumerRequirement, PlanLocation, ProducerRequirement,
        RuntimeFilterBindingRole, RuntimeFilterBindingSpec, RuntimeFilterChannelSpec,
        RuntimeFilterGraph, RuntimeFilterSemanticScanDomainTarget,
    };

    fn expression(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn node(fragment_id: FragmentId, node_id: i32, ids: Vec<BindingId>) -> DistributedNode {
        DistributedNode {
            node_id,
            fragment_id,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: ids,
            children: Vec::new(),
            stats: PhysicalPlanStats {
                output_row_count: 0.0,
                row_count_confidence: PlannerConfidence::Fallback,
                column_statistics: Default::default(),
                cost_estimate: None,
                broadcast_decision: None,
            },
            payload: DistributedNodeKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns: Vec::new(),
            }),
        }
    }

    fn node_with_children(
        fragment_id: FragmentId,
        node_id: i32,
        ids: Vec<BindingId>,
        children: Vec<DistributedNode>,
    ) -> DistributedNode {
        let mut node = node(fragment_id, node_id, ids);
        node.children = children;
        node
    }

    fn fragment(fragment_id: FragmentId, root: DistributedNode) -> PlanFragment {
        PlanFragment {
            fragment_id,
            root,
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Noop,
            output_exprs: None,
            output_columns: Vec::new(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        }
    }

    fn channel(
        channel_id: ChannelId,
        logical_domain: RuntimeFilterLogicalDomain,
        reduction_requirement: ReductionRequirement,
    ) -> RuntimeFilterChannelSpec {
        let (lifecycle, allowed_contribution_kinds, required_consumer_capabilities) =
            match (&logical_domain, reduction_requirement) {
                (RuntimeFilterLogicalDomain::Membership { .. }, ReductionRequirement::SetUnion) => {
                    (
                        RuntimeFilterLifecycle::CompleteOnce,
                        BTreeSet::from([
                            ContributionKind::ValueDomainDelta,
                            ContributionKind::ProducerClosed,
                        ]),
                        BTreeSet::from([
                            ArtifactCapability::Membership,
                            ArtifactCapability::EmptyDomain,
                        ]),
                    )
                }
                (
                    RuntimeFilterLogicalDomain::OrderedBound(_),
                    ReductionRequirement::TightenOrderedBound,
                ) => (
                    RuntimeFilterLifecycle::MonotonicUpdates,
                    BTreeSet::from([
                        ContributionKind::OrderedBoundUpdate,
                        ContributionKind::ProducerClosed,
                    ]),
                    BTreeSet::from([ArtifactCapability::OrderedRange]),
                ),
                (
                    RuntimeFilterLogicalDomain::OrderedBound(_),
                    ReductionRequirement::MergeTopKSummary(_),
                ) => (
                    RuntimeFilterLifecycle::MonotonicUpdates,
                    BTreeSet::from([
                        ContributionKind::TopKSummary,
                        ContributionKind::ProducerClosed,
                    ]),
                    BTreeSet::from([ArtifactCapability::OrderedRange]),
                ),
                _ => panic!("test channel contract must be semantically compatible"),
            };
        RuntimeFilterChannelSpec {
            channel_id,
            logical_domain,
            lifecycle,
            availability_coverage: Coverage::Leaf(CoverageWitnessId::new(1)),
            terminal_coverage: Coverage::Leaf(CoverageWitnessId::new(1)),
            reduction_requirement,
            allowed_contribution_kinds,
            required_consumer_capabilities,
            policy: RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 1,
                max_artifact_bytes: 1,
                deadline_ms: 1,
                max_retries: 0,
            },
        }
    }

    fn membership_channel(channel_id: ChannelId) -> RuntimeFilterChannelSpec {
        channel(
            channel_id,
            RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NeverMatches,
            },
            ReductionRequirement::SetUnion,
        )
    }

    fn producer_binding(
        binding_id: BindingId,
        channel_id: ChannelId,
        fragment_id: u32,
        node_id: i32,
    ) -> RuntimeFilterBindingSpec {
        producer_binding_with_kinds(
            binding_id,
            channel_id,
            fragment_id,
            node_id,
            BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
        )
    }

    fn producer_binding_with_kinds(
        binding_id: BindingId,
        channel_id: ChannelId,
        fragment_id: u32,
        node_id: i32,
        contribution_kinds: BTreeSet<ContributionKind>,
    ) -> RuntimeFilterBindingSpec {
        RuntimeFilterBindingSpec {
            binding_id,
            channel_id,
            coverage_witness_id: None,
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(fragment_id),
                node_id: PlanNodeId::new(node_id),
            },
            expression: expression(i64::from(binding_id.get())),
            apply_point: ApplyPoint::NodeOutput,
            role: RuntimeFilterBindingRole::Producer(ProducerRequirement {
                contribution_kinds,
                completion_requirement: CompletionRequirement::ProducerClosed,
                target: ProducerBindingTarget::JoinBuildKey { ordinal: 0 },
            }),
        }
    }

    fn consumer_binding(
        binding_id: BindingId,
        channel_id: ChannelId,
        fragment_id: u32,
        node_id: i32,
    ) -> RuntimeFilterBindingSpec {
        RuntimeFilterBindingSpec {
            binding_id,
            channel_id,
            coverage_witness_id: None,
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(fragment_id),
                node_id: PlanNodeId::new(node_id),
            },
            expression: expression(i64::from(binding_id.get())),
            apply_point: ApplyPoint::NodeInput,
            role: RuntimeFilterBindingRole::Consumer(ConsumerRequirement {
                capabilities: BTreeSet::from([
                    ArtifactCapability::Membership,
                    ArtifactCapability::EmptyDomain,
                ]),
                activation: ConsumerActivation::NonBlockingLive {
                    late_apply: LateApplyGranularity::Batch,
                },
                target: ConsumerBindingTarget::SourceBoundary { scan_domain: None },
            }),
        }
    }

    fn graph_with(
        channels: Vec<RuntimeFilterChannelSpec>,
        bindings: Vec<RuntimeFilterBindingSpec>,
    ) -> RuntimeFilterGraph {
        let mut graph = RuntimeFilterGraph::default();
        for channel in channels {
            graph.insert_channel(channel).unwrap();
        }
        for binding in bindings {
            graph.insert_binding(binding).unwrap();
        }
        graph
    }

    #[test]
    fn preparation_projects_every_graph_binding_exactly_once() {
        let channel_id = ChannelId::new(9);
        let binding_id = BindingId::new(7);
        let consumer_id = BindingId::new(8);
        let graph = graph_with(
            vec![membership_channel(channel_id)],
            vec![
                producer_binding(binding_id, channel_id, 1, 10),
                consumer_binding(consumer_id, channel_id, 1, 11),
            ],
        );

        let tables = materialize_runtime_filter_binding_tables(
            &graph,
            &[fragment(
                1,
                node_with_children(
                    1,
                    10,
                    vec![binding_id],
                    vec![node(1, 11, vec![consumer_id])],
                ),
            )],
        )
        .unwrap();

        assert_eq!(tables[&1].bindings().len(), 2);
        assert_eq!(
            tables[&1].bindings().next().unwrap().binding_id(),
            binding_id
        );
        let producer = tables[&1].binding(binding_id).unwrap();
        assert_eq!(producer.channel_id(), channel_id);
        assert_eq!(producer.node_id(), PlanNodeId::new(10));
        assert_eq!(producer.apply_point(), ApplyPoint::NodeOutput);
        assert!(matches!(
            producer.role(),
            PreparedRuntimeFilterBindingRole::Producer {
                completion_requirement: CompletionRequirement::ProducerClosed,
                ..
            }
        ));
        let consumer = tables[&1].binding(consumer_id).unwrap();
        assert_eq!(consumer.node_id(), PlanNodeId::new(11));
        assert_eq!(consumer.apply_point(), ApplyPoint::NodeInput);
        assert!(matches!(
            consumer.role(),
            PreparedRuntimeFilterBindingRole::Consumer {
                activation: ConsumerActivation::NonBlockingLive {
                    late_apply: LateApplyGranularity::Batch,
                },
                ..
            }
        ));
        assert_eq!(consumer.expression().data_type, DataType::Int64);

        let duplicate_error = materialize_runtime_filter_binding_tables(
            &graph,
            &[fragment(
                1,
                node_with_children(1, 10, vec![binding_id], vec![node(1, 11, vec![binding_id])]),
            )],
        )
        .unwrap_err();
        assert!(
            duplicate_error.contains("binding id=7 is attached more than once"),
            "{duplicate_error}"
        );
    }

    #[test]
    fn scan_domain_semantic_target_requires_pinned_scan_bindings() {
        let channel_id = ChannelId::new(9);
        let producer_id = BindingId::new(7);
        let consumer_id = BindingId::new(8);
        let mut consumer = consumer_binding(consumer_id, channel_id, 1, 11);
        let RuntimeFilterBindingRole::Consumer(requirement) = &mut consumer.role else {
            unreachable!("fixture is a consumer");
        };
        requirement.target = ConsumerBindingTarget::SourceBoundary {
            scan_domain: Some(RuntimeFilterSemanticScanDomainTarget {
                column_id: ColumnId::new_for_test(4),
                data_type: DataType::Int64,
                nullable: false,
            }),
        };
        let graph = graph_with(
            vec![membership_channel(channel_id)],
            vec![producer_binding(producer_id, channel_id, 1, 10), consumer],
        );

        let error = materialize_runtime_filter_binding_tables(
            &graph,
            &[fragment(
                1,
                node_with_children(
                    1,
                    10,
                    vec![producer_id],
                    vec![node(1, 11, vec![consumer_id])],
                ),
            )],
        )
        .expect_err("scan-domain target cannot use an unpinned provider read");
        assert!(error.contains("without pinned scan bindings"), "{error}");
    }

    #[test]
    fn preparation_partitions_bindings_by_fragment_and_sorts_by_binding_id() {
        let channel_id = ChannelId::new(9);
        let ids = [BindingId::new(30), BindingId::new(10), BindingId::new(20)];
        let graph = graph_with(
            vec![membership_channel(channel_id)],
            vec![
                producer_binding(ids[0], channel_id, 2, 20),
                producer_binding(ids[1], channel_id, 1, 11),
                producer_binding(ids[2], channel_id, 1, 12),
            ],
        );
        let fragments = vec![
            fragment(
                2,
                node_with_children(2, 20, vec![ids[0]], vec![node(2, 21, Vec::new())]),
            ),
            fragment(
                1,
                node_with_children(
                    1,
                    10,
                    Vec::new(),
                    vec![node(1, 12, vec![ids[2]]), node(1, 11, vec![ids[1]])],
                ),
            ),
            fragment(3, node(3, 30, Vec::new())),
        ];

        let tables = materialize_runtime_filter_binding_tables(&graph, &fragments).unwrap();

        assert_eq!(tables.keys().copied().collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_eq!(
            tables[&1]
                .bindings()
                .map(PreparedRuntimeFilterBinding::binding_id)
                .collect::<Vec<_>>(),
            vec![ids[1], ids[2]]
        );
        assert_eq!(tables[&2].bindings().next().unwrap().binding_id(), ids[0]);
        assert_eq!(tables[&3].fragment_id(), 3);
        assert!(tables[&3].is_empty());
    }

    #[test]
    fn preparation_rejects_attachment_without_graph_binding() {
        let error = materialize_runtime_filter_binding_tables(
            &RuntimeFilterGraph::default(),
            &[fragment(1, node(1, 10, vec![BindingId::new(7)]))],
        )
        .unwrap_err();
        assert!(error.contains("unknown binding id=7"), "{error}");
    }

    #[test]
    fn preparation_rejects_graph_binding_without_attachment() {
        let channel_id = ChannelId::new(9);
        let graph = graph_with(
            vec![membership_channel(channel_id)],
            vec![producer_binding(BindingId::new(7), channel_id, 1, 10)],
        );
        let error = materialize_runtime_filter_binding_tables(
            &graph,
            &[fragment(1, node(1, 10, Vec::new()))],
        )
        .unwrap_err();
        assert!(error.contains("binding id=7"), "{error}");
        assert!(error.contains("has no node attachment"), "{error}");
    }

    #[test]
    fn preparation_rejects_wrong_fragment_node_role_and_apply_point() {
        let channel_id = ChannelId::new(9);
        let binding_id = BindingId::new(7);
        let cases = [
            (
                producer_binding(binding_id, channel_id, 2, 10),
                "does not match attachment fragment_id=1 node_id=10",
            ),
            (
                producer_binding(binding_id, channel_id, 1, 11),
                "does not match attachment fragment_id=1 node_id=10",
            ),
            (
                {
                    let mut binding = producer_binding(binding_id, channel_id, 1, 10);
                    binding.apply_point = ApplyPoint::NodeInput;
                    binding
                },
                "producer binding id=7 must use NodeOutput",
            ),
            (
                {
                    let mut binding = consumer_binding(binding_id, channel_id, 1, 10);
                    binding.apply_point = ApplyPoint::NodeOutput;
                    binding
                },
                "consumer binding id=7 must use NodeInput",
            ),
        ];
        for (binding, expected) in cases {
            let graph = graph_with(vec![membership_channel(channel_id)], vec![binding]);
            let error = materialize_runtime_filter_binding_tables(
                &graph,
                &[fragment(1, node(1, 10, vec![binding_id]))],
            )
            .unwrap_err();
            assert!(
                error.contains(expected),
                "expected {expected:?}, got {error:?}"
            );
        }
    }
}
