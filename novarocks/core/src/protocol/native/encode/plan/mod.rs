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

use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU32,
    sync::Arc,
};

#[cfg(test)]
use std::collections::BTreeMap;

use arrow::datatypes::{DataType, Field};
use iceberg::spec::{ListType, MapType, NestedField, PrimitiveType, StructType, Type};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use super::expr::{encode_expr, encode_sort_items, encode_window_frame};
use crate::connector::iceberg::scan_model as iceberg_scan_model;
use crate::coordinator::prepare::PreparedFragmentSet;
use crate::coordinator::prepare::runtime_filter_binding::{
    PreparedReductionContract, PreparedRuntimeFilterBinding, PreparedRuntimeFilterBindingRole,
    PreparedRuntimeFilterContract, RuntimeFilterBindingTable,
};
use crate::coordinator::prepare::scan::{
    ResolvedScanBinding, ResolvedScanColumnKind, ResolvedScanExecution, ScanExecutionBindings,
};
use crate::proto::{common, plan};
use crate::runtime_filter::model::contract::{
    ArtifactCapability, ComparatorDigest, CompletionFenceKind, CompletionRequirement,
    ConsumerActivation, ContributionKind, LateApplyGranularity, NullOrder, OrderContract,
    OrderKeyContract, SortDirection, TopKSummaryRequirement,
};
use crate::runtime_filter::model::graph::ApplyPoint;
use crate::runtime_filter::port::artifact::ArtifactMembershipSchema;
use crate::runtime_filter::port::ordered_bound::{
    OrderContractDigest, RuntimeOrderContract, RuntimeOrderKey,
};
use crate::runtime_filter::port::topk_summary::{
    RuntimeTopKSummaryContract, TopKSummaryContractDigest,
};
use crate::sql::analysis::OutputColumn as AnalysisOutputColumn;
// Consumed only by `#[cfg(test)]` encoder fixtures (the production write/router
// encoding reads finalized planner types, not these analysis constructors).
use crate::catalog::schema::{ColumnDefault, SqlType, validate_column_default};
use crate::connector::scan_model::starrocks::{
    StarRocksColumnSchemaDescriptor, StarRocksKeysTypeDescriptor, StarRocksTabletSchemaDescriptor,
};
use crate::sql::common::{ChangeStreamBranchKind, JoinKind};
use crate::sql::planner::distributed::write::sink::IcebergWriteInputBinding;
use crate::sql::planner::distributed::write::sink::{
    IcebergWriteFileCompression, IcebergWriteSinkMode, IcebergWriteSinkSpec,
};
use crate::sql::planner::distributed::{
    DataPartition, DataSink, DistributedNode, DistributedNodeKind, DistributedPlan, ExchangeFlavor,
    ExchangeReceiver, FragmentEdge, FragmentEdgeKind, FragmentEdgeOutputCatalog,
    FragmentStreamKind, NodeExecutionColumn, NodeOutputCatalog, PartitionKind, PlanFragment,
    WriteContractCatalog,
};
use crate::sql::planner::payload::PlanRowCountAssertion;
use crate::sql::planner::physical::{
    AggMode, HashSource, JoinDistribution, JoinExecutionMode, PhysicalPlanKind, PlanSetOpKind,
    RedistributeMode, TopNPhase,
};
use crate::sql::planner::table as table_model;
use crate::types::native_proto::encode_type;

use output::{apply_sealed_node_output_columns, encode_output_columns};

mod output;
mod topology;

#[cfg(not(test))]
type ContextRef<'a, T> = &'a T;
#[cfg(test)]
type ContextRef<'a, T> = Option<&'a T>;

pub(super) struct NativePlanEncodeContext<'a> {
    pub(super) scan_bindings: ContextRef<'a, ScanExecutionBindings>,
    /// The sealed node-output contract. The encoder reads each covered node's
    /// (join / scan / set-op / sort) execution output from here rather than
    /// re-deriving or repairing it. `None` only in bare-node encoder unit tests
    /// that have no sealed plan; those rely on the payload columns encoded by
    /// `encode_physical_node`, which is the same data the catalog is built from.
    pub(super) node_outputs: ContextRef<'a, NodeOutputCatalog>,
    /// The sealed fragment-output / stream-edge-projection contract (CGO-9C
    /// Task 2). The encoder maps each fragment's finalized output columns and each
    /// stream edge's finalized sender/receiver projection from here instead of
    /// re-deriving a stream schema or patching the exchange receiver. `None` only
    /// in bare-node/bare-fragment encoder unit tests that have no sealed plan.
    pub(super) fragment_edge_outputs: ContextRef<'a, FragmentEdgeOutputCatalog>,
    /// The sealed Iceberg write output / change-stream router partition contract
    /// (CGO-9C Task 3). The encoder maps each Iceberg write fragment's finalized
    /// output expressions and target output schema, and each change-stream router
    /// branch's finalized partition, from here instead of synthesizing the write
    /// output or reconstructing a partition from `output_partition_ordinals`.
    /// `None` only in bare-node/bare-fragment encoder unit tests, which never
    /// encode a write or router fragment.
    pub(super) write_contracts: ContextRef<'a, WriteContractCatalog>,
    pub(super) runtime_filter_bindings: ContextRef<'a, PreparedFragmentSet>,
}

impl<'a> NativePlanEncodeContext<'a> {
    fn complete(
        src: &'a DistributedPlan,
        scan_bindings: &'a ScanExecutionBindings,
        runtime_filter_bindings: ContextRef<'a, PreparedFragmentSet>,
    ) -> Self {
        Self {
            #[cfg(not(test))]
            scan_bindings,
            #[cfg(test)]
            scan_bindings: Some(scan_bindings),
            #[cfg(not(test))]
            node_outputs: src.node_outputs(),
            #[cfg(test)]
            node_outputs: Some(src.node_outputs()),
            #[cfg(not(test))]
            fragment_edge_outputs: src.fragment_edge_outputs(),
            #[cfg(test)]
            fragment_edge_outputs: Some(src.fragment_edge_outputs()),
            #[cfg(not(test))]
            write_contracts: src.write_contracts(),
            #[cfg(test)]
            write_contracts: Some(src.write_contracts()),
            runtime_filter_bindings,
        }
    }
}

#[cfg(not(test))]
fn optional_context_ref<T>(value: &T) -> Option<&T> {
    Some(value)
}

#[cfg(test)]
fn optional_context_ref<T>(value: Option<&T>) -> Option<&T> {
    value
}

fn required_context_ref<'a, T>(
    value: ContextRef<'a, T>,
    missing: impl FnOnce() -> String,
) -> Result<&'a T, String> {
    #[cfg(not(test))]
    {
        let _ = missing;
        Ok(value)
    }
    #[cfg(test)]
    {
        value.ok_or_else(missing)
    }
}

#[cfg(test)]
pub(super) fn encode_distributed_plan(
    src: &DistributedPlan,
    scan_bindings: &ScanExecutionBindings,
) -> Result<plan::DistributedPlan, String> {
    let prepared = crate::coordinator::prepare::prepared_fragment_set_for_native_encode_test(src)?;
    encode_distributed_plan_from_prepared(src, scan_bindings, &prepared)
}

pub(super) fn encode_distributed_plan_from_prepared(
    src: &DistributedPlan,
    scan_bindings: &ScanExecutionBindings,
    prepared: &PreparedFragmentSet,
) -> Result<plan::DistributedPlan, String> {
    #[cfg(not(test))]
    let runtime_filter_bindings = prepared;
    #[cfg(test)]
    let runtime_filter_bindings = Some(prepared);
    encode_distributed_plan_with_context_inner(
        src,
        NativePlanEncodeContext::complete(src, scan_bindings, runtime_filter_bindings),
    )
}

#[cfg(test)]
pub(super) fn encode_distributed_plan_with_context(
    src: &DistributedPlan,
    ctx: NativePlanEncodeContext<'_>,
) -> Result<plan::DistributedPlan, String> {
    let runtime_filter_bindings = required_context_ref(ctx.runtime_filter_bindings, || {
        "native distributed plan encoding requires prepared runtime filter binding tables"
            .to_string()
    })?;
    encode_distributed_plan_with_context_inner(
        src,
        NativePlanEncodeContext {
            scan_bindings: ctx.scan_bindings,
            node_outputs: Some(src.node_outputs()),
            fragment_edge_outputs: Some(src.fragment_edge_outputs()),
            write_contracts: Some(src.write_contracts()),
            runtime_filter_bindings: Some(runtime_filter_bindings),
        },
    )
}

fn encode_distributed_plan_with_context_inner(
    src: &DistributedPlan,
    ctx: NativePlanEncodeContext<'_>,
) -> Result<plan::DistributedPlan, String> {
    // The sealed node-output contract of `src` is authoritative for every covered
    // node, so bind it here: all fragment/node encoding then reads each covered
    // node's execution output from it instead of re-deriving or repairing it,
    // regardless of how the incoming context was constructed.
    let mut fragments = src
        .fragments()
        .iter()
        .map(|fragment| topology::encode_plan_fragment_with_context(fragment, &ctx))
        .collect::<Result<Vec<_>, _>>()?;
    topology::attach_stream_sinks(src, &mut fragments, &ctx)?;
    Ok(plan::DistributedPlan {
        fragments,
        root_fragment_id: src.root_fragment_id(),
        edges: src
            .edges()
            .iter()
            .map(topology::encode_fragment_edge)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

pub(crate) fn encode_data_partition(src: &DataPartition) -> Result<plan::DataPartition, String> {
    topology::encode_data_partition(src)
}

pub(super) fn encode_runtime_filter_binding_table(
    enclosing_fragment_id: crate::sql::planner::distributed::FragmentId,
    table: &RuntimeFilterBindingTable,
) -> Result<plan::RuntimeFilterBindingTable, String> {
    if table.fragment_id() != enclosing_fragment_id {
        return Err(format!(
            "native runtime filter binding table fragment mismatch: enclosing_fragment_id={enclosing_fragment_id} table_fragment_id={}",
            table.fragment_id()
        ));
    }
    let mut previous_binding_id = None;
    let mut bindings = Vec::with_capacity(table.bindings().len());
    for binding in table.bindings() {
        let binding_id = binding.binding_id().get();
        if previous_binding_id.is_some_and(|previous| previous >= binding_id) {
            return Err(format!(
                "native runtime filter binding table is not strictly ordered by binding id: previous={previous_binding_id:?} current={binding_id}"
            ));
        }
        previous_binding_id = Some(binding_id);
        bindings.push(encode_runtime_filter_binding(binding)?);
    }
    Ok(plan::RuntimeFilterBindingTable {
        fragment_id: table.fragment_id(),
        bindings,
    })
}

fn encode_runtime_filter_binding(
    binding: &PreparedRuntimeFilterBinding,
) -> Result<plan::RuntimeFilterBinding, String> {
    let contract = encode_runtime_filter_contract(binding)?;
    let reduction = encode_runtime_filter_reduction(binding)?;
    let role = Some(match binding.role() {
        PreparedRuntimeFilterBindingRole::Producer {
            contribution_kinds,
            completion_requirement,
        } => plan::runtime_filter_binding::Role::Producer(plan::RuntimeFilterProducerRole {
            contribution_kinds: contribution_kinds
                .iter()
                .copied()
                .map(encode_runtime_filter_contribution_kind)
                .collect(),
            completion_requirement: encode_runtime_filter_completion(*completion_requirement),
        }),
        PreparedRuntimeFilterBindingRole::Consumer {
            capabilities,
            activation,
        } => plan::runtime_filter_binding::Role::Consumer(plan::RuntimeFilterConsumerRole {
            capabilities: capabilities
                .iter()
                .copied()
                .map(encode_runtime_filter_capability)
                .collect(),
            activation: Some(encode_runtime_filter_activation(*activation)),
        }),
    });
    Ok(plan::RuntimeFilterBinding {
        binding_id: binding.binding_id().get(),
        channel_id: binding.channel_id().get(),
        node_id: binding.node_id().get(),
        apply_point: encode_runtime_filter_apply_point(binding.apply_point()),
        expression: Some(encode_expr(binding.expression())?),
        contract: Some(contract),
        reduction: Some(reduction),
        role,
    })
}

fn encode_runtime_filter_contract(
    binding: &PreparedRuntimeFilterBinding,
) -> Result<plan::RuntimeFilterContract, String> {
    use plan::runtime_filter_contract::Kind;

    let kind = match binding.contract() {
        PreparedRuntimeFilterContract::Membership {
            canonical_schema,
            schema_digest,
        } => Kind::Membership(encode_runtime_filter_membership_contract(
            binding.binding_id().get(),
            canonical_schema,
            schema_digest.bytes(),
        )?),
        PreparedRuntimeFilterContract::Ordered {
            keys,
            comparator_digest,
            order_contract_digest,
        } => Kind::Ordered(encode_runtime_filter_ordered_contract(
            binding.binding_id().get(),
            keys,
            *comparator_digest,
            *order_contract_digest,
        )?),
    };
    Ok(plan::RuntimeFilterContract { kind: Some(kind) })
}

fn encode_runtime_filter_ordered_contract(
    binding_id: u32,
    keys: &[RuntimeOrderKey],
    comparator_digest: ComparatorDigest,
    order_contract_digest: OrderContractDigest,
) -> Result<plan::RuntimeFilterOrderedContract, String> {
    if keys.is_empty() {
        return Err(format!(
            "native runtime filter binding id={binding_id} has no canonical order keys"
        ));
    }
    let plan_contract = OrderContract {
        keys: keys
            .iter()
            .map(|key| OrderKeyContract {
                data_type: key.data_type().clone(),
                direction: key.direction(),
                null_order: key.null_order(),
            })
            .collect(),
        inclusive: true,
        comparator_digest,
    };
    let canonical = RuntimeOrderContract::try_from_plan(&plan_contract).map_err(|error| {
        format!(
            "native runtime filter binding id={binding_id} has a noncanonical ordered contract: {error:?}"
        )
    })?;
    if canonical.digest() != order_contract_digest {
        return Err(format!(
            "native runtime filter binding id={binding_id} order contract digest does not match typed keys"
        ));
    }
    Ok(plan::RuntimeFilterOrderedContract {
        keys: keys
            .iter()
            .map(|key| {
                Ok(plan::RuntimeFilterOrderKey {
                    r#type: Some(encode_type(key.data_type())?),
                    direction: encode_runtime_filter_sort_direction(key.direction()),
                    null_order: encode_runtime_filter_null_order(key.null_order()),
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
        comparator_digest: comparator_digest.get().to_vec(),
        order_contract_digest: order_contract_digest.bytes().to_vec(),
    })
}

fn encode_runtime_filter_membership_contract(
    binding_id: u32,
    canonical_schema: &[u8],
    schema_digest: [u8; 32],
) -> Result<plan::RuntimeFilterMembershipContract, String> {
    if canonical_schema.is_empty() {
        return Err(format!(
            "native runtime filter binding id={binding_id} has an empty canonical membership schema"
        ));
    }
    let canonical = ArtifactMembershipSchema::view(canonical_schema).map_err(|error| {
        format!(
            "native runtime filter binding id={binding_id} has a noncanonical membership schema: {error:?}"
        )
    })?;
    if canonical.digest().bytes() != schema_digest {
        return Err(format!(
            "native runtime filter binding id={binding_id} membership schema digest does not match canonical bytes"
        ));
    }
    Ok(plan::RuntimeFilterMembershipContract {
        canonical_schema: canonical_schema.to_vec(),
        schema_digest: schema_digest.to_vec(),
    })
}

fn encode_runtime_filter_reduction(
    binding: &PreparedRuntimeFilterBinding,
) -> Result<plan::RuntimeFilterReductionContract, String> {
    use plan::runtime_filter_reduction_contract::Kind;

    let kind = match binding.reduction() {
        PreparedReductionContract::SetUnion => Kind::SetUnion(true),
        PreparedReductionContract::TightenOrderedBound => Kind::TightenOrderedBound(true),
        PreparedReductionContract::MergeTopKSummary { k, contract_digest } => {
            let PreparedRuntimeFilterContract::Ordered {
                keys,
                comparator_digest,
                ..
            } = binding.contract()
            else {
                return Err(format!(
                    "native runtime filter binding id={} has TopK reduction without an ordered contract",
                    binding.binding_id().get()
                ));
            };
            Kind::MergeTopkSummary(encode_runtime_filter_topk_reduction(
                binding.binding_id().get(),
                keys,
                *comparator_digest,
                *k,
                *contract_digest,
            )?)
        }
    };
    Ok(plan::RuntimeFilterReductionContract { kind: Some(kind) })
}

fn encode_runtime_filter_topk_reduction(
    binding_id: u32,
    keys: &[RuntimeOrderKey],
    comparator_digest: ComparatorDigest,
    k: NonZeroU32,
    contract_digest: TopKSummaryContractDigest,
) -> Result<plan::RuntimeFilterTopKReduction, String> {
    let order = OrderContract {
        keys: keys
            .iter()
            .map(|key| OrderKeyContract {
                data_type: key.data_type().clone(),
                direction: key.direction(),
                null_order: key.null_order(),
            })
            .collect(),
        inclusive: true,
        comparator_digest,
    };
    let requirement =
        TopKSummaryRequirement::try_new(k.get()).expect("prepared TopK K is nonzero by type");
    let canonical = RuntimeTopKSummaryContract::try_from_plan(&order, requirement).map_err(
        |error| {
            format!(
                "native runtime filter binding id={binding_id} has a noncanonical TopK contract: {error:?}"
            )
        },
    )?;
    if canonical.digest() != contract_digest {
        return Err(format!(
            "native runtime filter binding id={binding_id} TopK digest does not match typed order keys and K"
        ));
    }
    Ok(plan::RuntimeFilterTopKReduction {
        k: k.get(),
        contract_digest: contract_digest.bytes().to_vec(),
    })
}

fn encode_runtime_filter_apply_point(value: ApplyPoint) -> i32 {
    match value {
        ApplyPoint::NodeInput => i32::from(plan::RuntimeFilterApplyPoint::NodeInput),
        ApplyPoint::NodeOutput => i32::from(plan::RuntimeFilterApplyPoint::NodeOutput),
    }
}

fn encode_runtime_filter_contribution_kind(value: ContributionKind) -> i32 {
    match value {
        ContributionKind::ValueDomainDelta => {
            i32::from(plan::RuntimeFilterContributionKind::ValueDomainDelta)
        }
        ContributionKind::FinalDomainShard => {
            i32::from(plan::RuntimeFilterContributionKind::FinalDomainShard)
        }
        ContributionKind::OrderedBoundUpdate => {
            i32::from(plan::RuntimeFilterContributionKind::OrderedBoundUpdate)
        }
        ContributionKind::TopKSummary => {
            i32::from(plan::RuntimeFilterContributionKind::TopkSummary)
        }
        ContributionKind::ProducerClosed => {
            i32::from(plan::RuntimeFilterContributionKind::ProducerClosed)
        }
    }
}

fn encode_runtime_filter_completion(value: CompletionRequirement) -> i32 {
    match value {
        CompletionRequirement::ProducerClosed => {
            i32::from(plan::RuntimeFilterCompletionRequirement::ProducerClosed)
        }
        CompletionRequirement::FencedFinalDomain(CompletionFenceKind::CommittedDomainFrozen) => {
            i32::from(plan::RuntimeFilterCompletionRequirement::FencedCommittedDomainFrozen)
        }
    }
}

fn encode_runtime_filter_capability(value: ArtifactCapability) -> i32 {
    match value {
        ArtifactCapability::Membership => {
            i32::from(plan::RuntimeFilterArtifactCapability::Membership)
        }
        ArtifactCapability::OrderedRange => {
            i32::from(plan::RuntimeFilterArtifactCapability::OrderedRange)
        }
        ArtifactCapability::EmptyDomain => {
            i32::from(plan::RuntimeFilterArtifactCapability::EmptyDomain)
        }
    }
}

fn encode_runtime_filter_activation(
    value: ConsumerActivation,
) -> plan::RuntimeFilterConsumerActivation {
    use plan::runtime_filter_consumer_activation::Kind;

    plan::RuntimeFilterConsumerActivation {
        kind: Some(match value {
            ConsumerActivation::BlockingSnapshot => Kind::BlockingSnapshot(true),
            ConsumerActivation::NonBlockingLive { late_apply } => {
                Kind::NonBlockingLive(encode_runtime_filter_late_apply(late_apply))
            }
        }),
    }
}

fn encode_runtime_filter_late_apply(value: LateApplyGranularity) -> i32 {
    match value {
        LateApplyGranularity::Row => i32::from(plan::RuntimeFilterLateApplyGranularity::Row),
        LateApplyGranularity::Batch => i32::from(plan::RuntimeFilterLateApplyGranularity::Batch),
        LateApplyGranularity::RowGroup => {
            i32::from(plan::RuntimeFilterLateApplyGranularity::RowGroup)
        }
        LateApplyGranularity::Split => i32::from(plan::RuntimeFilterLateApplyGranularity::Split),
        LateApplyGranularity::File => i32::from(plan::RuntimeFilterLateApplyGranularity::File),
    }
}

fn encode_runtime_filter_sort_direction(value: SortDirection) -> i32 {
    match value {
        SortDirection::Ascending => i32::from(plan::RuntimeFilterSortDirection::Ascending),
        SortDirection::Descending => i32::from(plan::RuntimeFilterSortDirection::Descending),
    }
}

fn encode_runtime_filter_null_order(value: NullOrder) -> i32 {
    match value {
        NullOrder::First => i32::from(plan::RuntimeFilterNullOrder::First),
        NullOrder::Last => i32::from(plan::RuntimeFilterNullOrder::Last),
    }
}

#[cfg(test)]
pub(crate) fn encode_node(src: &DistributedNode) -> Result<plan::DistributedNode, String> {
    encode_node_with_context(
        src,
        &NativePlanEncodeContext {
            scan_bindings: None,
            node_outputs: None,
            fragment_edge_outputs: None,
            write_contracts: None,
            runtime_filter_bindings: None,
        },
    )
}

pub(super) fn encode_node_with_context(
    src: &DistributedNode,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::DistributedNode, String> {
    let children = src
        .children
        .iter()
        .map(|child| encode_node_with_context(child, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    let payload = match &src.payload {
        DistributedNodeKind::Exchange(exchange) => {
            // A stream-edge receiver carries exactly the edge's finalized
            // projection (the planner's authoritative reconciliation of this
            // receiver against what its source fragment sends); the encoder maps
            // it 1:1 here instead of a later exchange-receiver patch. A receiver
            // that is not a finalized stream-edge target (a CTE-multicast or
            // change-stream-router receiver) keeps its declared columns.
            let output_columns = optional_context_ref(ctx.fragment_edge_outputs)
                .and_then(|catalog| catalog.stream_edge_projection(src.fragment_id, src.node_id))
                .unwrap_or(&exchange.output_columns);
            plan::distributed_node::Payload::Exchange(encode_exchange_receiver(
                exchange,
                output_columns,
            )?)
        }
        other => {
            let physical = crate::sql::planner::distributed::distributed_kind_to_physical(other);
            plan::distributed_node::Payload::Physical(encode_physical_node(
                &physical,
                src.node_id,
                ctx,
            )?)
        }
    };
    let mut node = plan::DistributedNode {
        node_id: src.node_id,
        fragment_id: src.fragment_id,
        tuple_ids: src.tuple_ids.clone(),
        nullable_tuple_ids: src.nullable_tuple_ids.clone(),
        limit: src.limit,
        runtime_filter_binding_ids: src
            .runtime_filter_binding_ids
            .iter()
            .map(|binding_id| binding_id.get())
            .collect(),
        children,
        payload: Some(payload),
    };
    apply_sealed_node_output_columns(&mut node, src, ctx)?;
    Ok(node)
}

#[cfg(test)]
pub(super) fn encoded_physical_variant_names_for_test() -> &'static [&'static str] {
    &[
        "Scan",
        "Filter",
        "Project",
        "Sort",
        "Limit",
        "Values",
        "Repeat",
        "Window",
        "GenerateSeries",
        "TableFunction",
        "AssertOneRow",
        "TopN",
        "HashAggregate",
        "HashJoin",
        "NestLoopJoin",
        "SetOp",
        "ChangeEventExpand",
        "CTEAnchor",
        "CTEProduce",
        "CTEConsume",
        "Redistribute",
    ]
}

fn encode_physical_node(
    src: &PhysicalPlanKind,
    node_id: i32,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::PlanNode, String> {
    use plan::plan_node::Kind;

    let (output_columns, kind) = match src {
        PhysicalPlanKind::Scan(node) => (
            encode_output_columns(&node.columns)?,
            Kind::Scan(encode_scan_node(node, node_id, ctx)?),
        ),
        PhysicalPlanKind::Filter(node) => (
            Vec::new(),
            Kind::Filter(plan::FilterNode {
                predicate: Some(encode_expr(&node.predicate)?),
            }),
        ),
        PhysicalPlanKind::Project(node) => (
            Vec::new(),
            Kind::Project(plan::ProjectNode {
                items: node
                    .items
                    .iter()
                    .map(|item| {
                        Ok(plan::ProjectItem {
                            expr: Some(encode_expr(&item.expr)?),
                            output_name: item.output_name.clone(),
                            output_column_id: item.output_column_id.0,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                output_qualifier: node.output_qualifier.clone(),
            }),
        ),
        PhysicalPlanKind::Sort(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::Sort(plan::SortNode {
                items: encode_sort_items(&node.items)?,
                analytic_partition_by: encode_exprs(&node.analytic_partition_by)?,
                output_columns: encode_output_columns(&node.output_columns)?,
                offset: node.offset,
                partition_limit: node.partition_limit.map(usize_to_u64),
                topn_type: node.topn_type.map(encode_sort_topn_type),
            }),
        ),
        PhysicalPlanKind::Limit(node) => (
            Vec::new(),
            Kind::Limit(plan::LimitNode {
                limit: node.limit,
                offset: node.offset,
            }),
        ),
        PhysicalPlanKind::Values(node) => (
            encode_output_columns(&node.columns)?,
            Kind::Values(plan::ValuesNode {
                rows: node
                    .rows
                    .iter()
                    .map(|row| {
                        Ok(plan::ExprList {
                            values: encode_exprs(row)?,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                columns: encode_output_columns(&node.columns)?,
            }),
        ),
        PhysicalPlanKind::Repeat(node) => (
            Vec::new(),
            Kind::Repeat(plan::RepeatNode {
                repeat_column_ref_list: node
                    .repeat_column_ref_list
                    .iter()
                    .map(|values| plan::StringList {
                        values: values.clone(),
                    })
                    .collect(),
                repeat_column_ref_ids: node
                    .repeat_column_ref_ids
                    .iter()
                    .map(|values| plan::UInt32List {
                        values: values.iter().map(|id| id.0).collect(),
                    })
                    .collect(),
                grouping_ids: node.grouping_ids.clone(),
                all_rollup_columns: node.all_rollup_columns.clone(),
                all_rollup_column_ids: node.all_rollup_column_ids.iter().map(|id| id.0).collect(),
                grouping_key_aliases: node
                    .grouping_key_aliases
                    .iter()
                    .map(|(first, second)| plan::StringPair {
                        first: first.clone(),
                        second: second.clone(),
                    })
                    .collect(),
                grouping_fn_args: node
                    .grouping_fn_args
                    .iter()
                    .map(|(name, values)| plan::NamedStringList {
                        name: name.clone(),
                        values: values.clone(),
                    })
                    .collect(),
                grouping_fn_arg_ids: node
                    .grouping_fn_arg_ids
                    .iter()
                    .map(|values| plan::UInt32List {
                        values: values.iter().map(|id| id.0).collect(),
                    })
                    .collect(),
                grouping_fn_ids: node
                    .grouping_fn_ids
                    .iter()
                    .map(|(name, value)| plan::NamedUInt32 {
                        name: name.clone(),
                        value: value.0,
                    })
                    .collect(),
                virtual_tuple_id: node.virtual_tuple_id,
            }),
        ),
        PhysicalPlanKind::Window(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::Window(plan::WindowNode {
                window_exprs: node
                    .window_exprs
                    .iter()
                    .map(|expr| {
                        Ok(plan::WindowExpr {
                            name: expr.name.clone(),
                            args: encode_exprs(&expr.args)?,
                            distinct: expr.distinct,
                            partition_by: encode_exprs(&expr.partition_by)?,
                            order_by: encode_sort_items(&expr.order_by)?,
                            window_frame: expr
                                .window_frame
                                .as_ref()
                                .map(encode_window_frame)
                                .transpose()?,
                            result_type: Some(encode_type(&expr.result_type)?),
                            output_name: expr.output_name.clone(),
                            output_column_id: expr.output_column_id.0,
                            ignore_nulls: expr.ignore_nulls,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                output_columns: encode_output_columns(&node.output_columns)?,
            }),
        ),
        PhysicalPlanKind::GenerateSeries(node) => (
            Vec::new(),
            Kind::GenerateSeries(plan::GenerateSeriesNode {
                start: node.start,
                end: node.end,
                step: node.step,
                column_name: node.column_name.clone(),
                alias: node.alias.clone(),
                output_column_id: node.output_column_id.0,
            }),
        ),
        PhysicalPlanKind::TableFunction(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::TableFunction(plan::TableFunctionNode {
                function_name: node.function_name.clone(),
                args: encode_exprs(&node.args)?,
                output_columns: encode_output_columns(&node.output_columns)?,
                alias: node.alias.clone(),
                is_left_join: node.is_left_join,
            }),
        ),
        PhysicalPlanKind::AssertOneRow(node) => (
            Vec::new(),
            Kind::AssertOneRow(plan::AssertOneRowNode {
                subquery_text: node.subquery_text.clone(),
                desired_num_rows: node.desired_num_rows,
                assertion: encode_row_count_assertion(node.assertion),
                group_key_column_ids: node
                    .group_key_column_ids
                    .iter()
                    .map(|column_id| column_id.0)
                    .collect(),
                group_key_labels: node.group_key_labels.clone(),
                keyed_message_prefix: node.keyed_message_prefix.clone(),
            }),
        ),
        PhysicalPlanKind::TopN(node) => (
            Vec::new(),
            Kind::Topn(plan::TopNNode {
                items: encode_sort_items(&node.items)?,
                limit: node.limit,
                offset: node.offset,
                phase: encode_topn_phase(node.phase),
                is_split: node.is_split,
            }),
        ),
        PhysicalPlanKind::HashAggregate(node) => {
            // Baseline raw layout/output columns straight from the physical payload.
            // In a sealed plan `apply_sealed_node_output_columns` overwrites both the
            // node output columns and this `output_layout`/`output_columns` from the
            // finalized aggregate contract (which applies the per-mode intermediate
            // aggregate-state types). This raw form only stands in the bare-node
            // encoder unit tests that have no sealed plan; the intermediate-type
            // determination is owned by the planner (`finalize_hash_aggregate_wire`).
            let raw_output_columns = if node.output_columns.is_empty() {
                node.output_layout.full_output_columns()
            } else {
                node.output_columns.clone()
            };
            (
                encode_output_columns(&raw_output_columns)?,
                Kind::HashAggregate(plan::HashAggregateNode {
                    mode: encode_agg_mode(node.mode),
                    group_by: encode_exprs(&node.group_by)?,
                    aggregates: node
                        .aggregates
                        .iter()
                        .map(|call| {
                            Ok(plan::PlanAggregateCall {
                                name: call.name.clone(),
                                args: encode_exprs(&call.args)?,
                                distinct: call.distinct,
                                result_type: Some(encode_type(&call.result_type)?),
                                order_by: encode_sort_items(&call.order_by)?,
                                output_column_id: call.output_column_id.0,
                            })
                        })
                        .collect::<Result<Vec<_>, String>>()?,
                    is_merge: node.is_merge.clone(),
                    output_layout: Some(plan::AggregateOutputLayout {
                        group_key_columns: encode_output_columns(
                            &node.output_layout.group_key_columns,
                        )?,
                        aggregate_columns: encode_output_columns(
                            &node.output_layout.aggregate_columns,
                        )?,
                    }),
                    output_columns: encode_output_columns(&raw_output_columns)?,
                }),
            )
        }
        PhysicalPlanKind::HashJoin(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::HashJoin(plan::HashJoinNode {
                join_type: encode_join_kind(node.join_type),
                eq_conditions: node
                    .eq_conditions
                    .iter()
                    .map(|cond| {
                        Ok(plan::HashJoinEqCondition {
                            left: Some(encode_expr(&cond.left)?),
                            right: Some(encode_expr(&cond.right)?),
                            null_safe: cond.null_safe,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                other_condition: node.other_condition.as_ref().map(encode_expr).transpose()?,
                distribution: encode_join_distribution(&node.distribution),
                execution_mode: node.execution_mode.map(encode_join_execution_mode),
            }),
        ),
        PhysicalPlanKind::NestLoopJoin(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::NestLoopJoin(plan::NestLoopJoinNode {
                join_type: encode_join_kind(node.join_type),
                condition: node.condition.as_ref().map(encode_expr).transpose()?,
            }),
        ),
        PhysicalPlanKind::SetOp(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::SetOp(plan::SetOpNode {
                kind: encode_set_op_kind(node.kind),
                output_columns: encode_output_columns(&node.output_columns)?,
                child_output_columns: node
                    .child_output_columns
                    .iter()
                    .map(|columns| {
                        Ok(plan::OutputColumnList {
                            columns: encode_output_columns(columns)?,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
            }),
        ),
        PhysicalPlanKind::ChangeEventExpand(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::ChangeEventExpand(plan::ChangeEventExpandNode {
                events: node
                    .events
                    .iter()
                    .map(|event| {
                        Ok(plan::DistributedChangeEventSpec {
                            predicate: event.predicate.as_ref().map(encode_expr).transpose()?,
                            branch_kind: encode_change_stream_branch_kind(event.branch_kind),
                            assignments: event
                                .assignments
                                .iter()
                                .map(|assignment| {
                                    Ok(plan::DistributedChangeEventOutputExpr {
                                        output_column_id: assignment.output_column_id.0,
                                        expr: assignment
                                            .expr
                                            .as_ref()
                                            .map(encode_expr)
                                            .transpose()?,
                                    })
                                })
                                .collect::<Result<Vec<_>, String>>()?,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                output_columns: encode_output_columns(&node.output_columns)?,
                change_op_column_id: node.change_op_column_id.0,
                data_route_column_id: node.data_route_column_id.map(|id| id.0),
            }),
        ),
        PhysicalPlanKind::CTEAnchor(node) => (
            Vec::new(),
            Kind::CteAnchor(plan::CteAnchorNode {
                cte_id: node.cte_id,
            }),
        ),
        PhysicalPlanKind::CTEProduce(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::CteProduce(plan::CteProduceNode {
                cte_id: node.cte_id,
                output_columns: encode_output_columns(&node.output_columns)?,
            }),
        ),
        PhysicalPlanKind::CTEConsume(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::CteConsume(plan::CteConsumeNode {
                cte_id: node.cte_id,
                alias: node.alias.clone(),
                output_columns: encode_output_columns(&node.output_columns)?,
                producer_column_ids: node.producer_column_ids.iter().map(|id| id.0).collect(),
            }),
        ),
        PhysicalPlanKind::Redistribute(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::Redistribute(plan::RedistributeNode {
                mode: Some(encode_redistribute_mode(&node.mode)),
                partition_exprs: encode_exprs(&node.partition_exprs)?,
                output_columns: encode_output_columns(&node.output_columns)?,
            }),
        ),
    };

    Ok(plan::PlanNode {
        output_columns,
        kind: Some(kind),
    })
}

fn encode_row_count_assertion(assertion: PlanRowCountAssertion) -> i32 {
    match assertion {
        PlanRowCountAssertion::Eq => plan::RowCountAssertion::Eq as i32,
        PlanRowCountAssertion::Ne => plan::RowCountAssertion::Ne as i32,
        PlanRowCountAssertion::Lt => plan::RowCountAssertion::Lt as i32,
        PlanRowCountAssertion::Le => plan::RowCountAssertion::Le as i32,
        PlanRowCountAssertion::Gt => plan::RowCountAssertion::Gt as i32,
        PlanRowCountAssertion::Ge => plan::RowCountAssertion::Ge as i32,
    }
}

fn encode_scan_node(
    src: &crate::sql::planner::payload::PlanScanNode,
    node_id: i32,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::ScanNode, String> {
    let binding = scan_binding_for_source(node_id, &src.table.source, ctx)?;
    let columns = match binding {
        Some(binding) => encode_bound_scan_output_columns(src, binding)?,
        None => encode_output_columns(&src.columns)?,
    };
    let required_columns = binding.map_or_else(
        || src.required_columns.clone().unwrap_or_default(),
        |binding| encode_bound_required_columns(src, binding),
    );
    Ok(plan::ScanNode {
        database: src.database.clone(),
        table: Some(encode_table_def_with_context(
            &src.table,
            Some(node_id),
            Some(&src.columns),
            binding,
            ctx,
        )?),
        alias: src.alias.clone(),
        columns,
        predicates: encode_exprs(&src.predicates)?,
        required_columns,
        dict_columns: Vec::new(),
        variant_columns: src
            .variant_columns
            .iter()
            .map(|column| {
                Ok(plan::ScanVariantColumn {
                    source_column_id: column.source_column_id.0,
                    source_column: column.source_column.clone(),
                    synthetic_column_id: column.synthetic_column_id.0,
                    synthetic_column: column.synthetic_column.clone(),
                    canonical_path: column.canonical_path.clone(),
                    requested_type: Some(encode_type(&column.requested_type)?),
                    strict: column.strict,
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
        mv_rewritten_from: src.mv_rewritten_from.clone(),
    })
}

fn encode_bound_scan_output_columns(
    src: &crate::sql::planner::payload::PlanScanNode,
    binding: &ResolvedScanBinding,
) -> Result<Vec<common::OutputColumn>, String> {
    let physical_by_planner_id = binding
        .physical_columns
        .iter()
        .map(|column| (column.planner.column_id, column))
        .collect::<HashMap<_, _>>();
    let synthetic_ids = src
        .variant_columns
        .iter()
        .map(|column| column.synthetic_column_id)
        .collect::<HashSet<_>>();
    let mut encoded = Vec::with_capacity(src.columns.len());
    let mut seen_physical_ids = HashSet::new();
    for column in &src.columns {
        if let Some(bound) = physical_by_planner_id.get(&column.column_id) {
            encoded.push(encode_bound_scan_output_column(bound)?);
            seen_physical_ids.insert(column.column_id);
        } else if synthetic_ids.contains(&column.column_id) {
            encoded.push(encode_output_column(column)?);
        }
    }
    for bound in &binding.physical_columns {
        if seen_physical_ids.insert(bound.planner.column_id) {
            encoded.push(encode_bound_scan_output_column(bound)?);
        }
    }
    Ok(encoded)
}

fn encode_bound_required_columns(
    src: &crate::sql::planner::payload::PlanScanNode,
    binding: &ResolvedScanBinding,
) -> Vec<String> {
    let mut required = binding
        .required_reads
        .iter()
        .map(|read| read.source.name.clone())
        .collect::<Vec<_>>();
    for variant in &src.variant_columns {
        let required_by_planner = src.required_columns.as_ref().is_none_or(|columns| {
            columns
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&variant.synthetic_column))
        });
        if required_by_planner
            && !required
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&variant.synthetic_column))
        {
            required.push(variant.synthetic_column.clone());
        }
    }
    required
}

fn encode_bound_scan_output_column(
    column: &crate::coordinator::prepare::scan::ResolvedScanColumn,
) -> Result<common::OutputColumn, String> {
    Ok(common::OutputColumn {
        column_id: column.planner.column_id.0,
        name: column.source.name.clone(),
        r#type: Some(encode_type(&column.source.data_type)?),
        nullable: column.source.nullable,
        is_internal: column.planner.is_internal,
    })
}

/// Encode an exchange receiver. `output_columns` is the receiver's finalized
/// wire schema: for a stream-edge target it is the planner's reconciled edge
/// projection (kept equal to what the sender sends); otherwise it is the
/// receiver's own declared columns.
fn encode_exchange_receiver(
    src: &ExchangeReceiver,
    output_columns: &[AnalysisOutputColumn],
) -> Result<plan::ExchangeReceiver, String> {
    Ok(plan::ExchangeReceiver {
        partition_type: encode_edge_partition_type(&src.partition),
        partition_exprs: encode_exprs(&src.partition.exprs)?,
        source_fragment_id: src.source_fragment_id,
        output_columns: encode_output_columns(output_columns)?,
        output_qualifier: src.output_qualifier.clone(),
        flavor: Some(encode_exchange_flavor(&src.flavor)?),
    })
}

fn encode_exchange_flavor(src: &ExchangeFlavor) -> Result<plan::ExchangeFlavor, String> {
    use plan::exchange_flavor::Kind;

    Ok(plan::ExchangeFlavor {
        kind: Some(match src {
            ExchangeFlavor::Distribution => Kind::Distribution(true),
            ExchangeFlavor::LimitOffset { limit, offset } => {
                Kind::LimitOffset(plan::LimitOffsetFlavor {
                    limit: *limit,
                    offset: *offset,
                })
            }
            ExchangeFlavor::TopNSplit {
                items,
                limit,
                offset,
            } => Kind::TopnSplit(plan::TopNSplitFlavor {
                items: encode_sort_items(items)?,
                limit: *limit,
                offset: *offset,
            }),
            ExchangeFlavor::CteMulticast {
                cte_id,
                receive_producer_column_ids,
            } => Kind::CteMulticast(plan::CteMulticastFlavor {
                cte_id: *cte_id,
                receive_producer_column_ids: receive_producer_column_ids
                    .iter()
                    .map(|id| id.0)
                    .collect(),
            }),
        }),
    })
}

fn encode_table_def_with_context(
    src: &table_model::TableDef,
    scan_node_id: Option<i32>,
    scan_columns: Option<&[AnalysisOutputColumn]>,
    binding: Option<&ResolvedScanBinding>,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::TableDef, String> {
    let (columns, metadata_columns) = match binding {
        Some(binding) if scan_source_requires_resolved_binding(&src.source) => {
            resolved_binding_table_columns(binding)
        }
        Some(binding) => merged_bound_table_columns(src, scan_columns.unwrap_or_default(), binding),
        None => (
            src.columns.clone(),
            src.iceberg_row_lineage_metadata_columns.clone(),
        ),
    };
    Ok(plan::TableDef {
        name: src.name.clone(),
        columns: columns
            .iter()
            .map(encode_column_def)
            .collect::<Result<Vec<_>, _>>()?,
        iceberg_row_lineage_metadata_columns: metadata_columns
            .iter()
            .map(encode_column_def)
            .collect::<Result<Vec<_>, _>>()?,
        source: Some(encode_scan_source(&src.source, scan_node_id, binding, ctx)?),
    })
}

fn scan_source_requires_resolved_binding(source: &table_model::ScanSource) -> bool {
    matches!(
        source,
        table_model::ScanSource::IcebergDeltaTable { .. }
            | table_model::ScanSource::IcebergVersionTable { .. }
            | table_model::ScanSource::IcebergMvTargetState(_)
            | table_model::ScanSource::IcebergMvTargetLocator(_)
    )
}

fn resolved_binding_table_columns(
    binding: &ResolvedScanBinding,
) -> (
    Vec<crate::catalog::schema::ColumnDef>,
    Vec<crate::catalog::schema::ColumnDef>,
) {
    let mut columns = Vec::new();
    let mut metadata_columns = Vec::new();
    let mut seen = HashSet::new();

    for bound in &binding.physical_columns {
        if !seen.insert(bound.source.name.to_ascii_lowercase()) {
            continue;
        }
        match bound.kind {
            ResolvedScanColumnKind::PhysicalTableColumn => columns.push(bound.source.clone()),
            ResolvedScanColumnKind::IcebergMetadataColumn => {
                metadata_columns.push(bound.source.clone())
            }
        }
    }
    for read in &binding.required_reads {
        if seen.insert(read.source.name.to_ascii_lowercase()) {
            columns.push(read.source.clone());
        }
    }

    (columns, metadata_columns)
}

fn merged_bound_table_columns(
    src: &table_model::TableDef,
    scan_columns: &[AnalysisOutputColumn],
    binding: &ResolvedScanBinding,
) -> (
    Vec<crate::catalog::schema::ColumnDef>,
    Vec<crate::catalog::schema::ColumnDef>,
) {
    let mut columns = src.columns.clone();
    let mut metadata_columns = src.iceberg_row_lineage_metadata_columns.clone();
    for bound in &binding.physical_columns {
        let target = match bound.kind {
            ResolvedScanColumnKind::PhysicalTableColumn => &mut columns,
            ResolvedScanColumnKind::IcebergMetadataColumn => &mut metadata_columns,
        };
        let planner_source_name = scan_columns
            .iter()
            .find(|column| column.column_id == bound.planner.column_id)
            .map(|column| column.name.as_str());
        overlay_bound_column(
            target,
            &bound.planner.name,
            planner_source_name,
            &bound.source,
        );
    }
    for read in &binding.required_reads {
        if replace_column_by_name(&mut columns, &read.source)
            || replace_column_by_name(&mut metadata_columns, &read.source)
        {
            continue;
        }
        columns.push(read.source.clone());
    }
    (columns, metadata_columns)
}

fn overlay_bound_column(
    columns: &mut Vec<crate::catalog::schema::ColumnDef>,
    planner_name: &str,
    planner_source_name: Option<&str>,
    source: &crate::catalog::schema::ColumnDef,
) {
    if let Some(index) = columns.iter().position(|column| {
        column.name.eq_ignore_ascii_case(planner_name)
            || planner_source_name.is_some_and(|name| column.name.eq_ignore_ascii_case(name))
            || column.name.eq_ignore_ascii_case(&source.name)
    }) {
        columns[index] = source.clone();
    } else {
        columns.push(source.clone());
    }
}

fn replace_column_by_name(
    columns: &mut [crate::catalog::schema::ColumnDef],
    source: &crate::catalog::schema::ColumnDef,
) -> bool {
    let Some(column) = columns
        .iter_mut()
        .find(|column| column.name.eq_ignore_ascii_case(&source.name))
    else {
        return false;
    };
    *column = source.clone();
    true
}

fn encode_column_def(src: &crate::catalog::schema::ColumnDef) -> Result<plan::ColumnDef, String> {
    Ok(plan::ColumnDef {
        name: src.name.clone(),
        data_type: Some(encode_type(&src.data_type)?),
        nullable: src.nullable,
        write_default_json: src
            .write_default
            .as_ref()
            .map(|literal| encode_column_write_default_json(src, literal))
            .transpose()?,
        logical_type: src.logical_type.as_ref().map(encode_sql_type).transpose()?,
    })
}

fn encode_column_write_default_json(
    column: &crate::catalog::schema::ColumnDef,
    value: &ColumnDefault,
) -> Result<String, String> {
    validate_column_default(value)?;
    let iceberg_type = iceberg_type_for_column_def(column)?;
    let normalized_value;
    let value = match (value, &iceberg_type) {
        (
            ColumnDefault::TimestamptzMicros { micros_since_epoch },
            Type::Primitive(PrimitiveType::Timestamp),
        ) => {
            normalized_value = ColumnDefault::TimestampMicros {
                micros_since_epoch: *micros_since_epoch,
            };
            &normalized_value
        }
        (
            ColumnDefault::TimestamptzNanos { nanos_since_epoch },
            Type::Primitive(PrimitiveType::TimestampNs),
        ) => {
            normalized_value = ColumnDefault::TimestampNanos {
                nanos_since_epoch: *nanos_since_epoch,
            };
            &normalized_value
        }
        _ => value,
    };
    crate::connector::iceberg::default_value::column_default_to_iceberg_literal(
        value,
        &iceberg_type,
    )
    .and_then(|literal| {
        literal
            .try_into_json(&iceberg_type)
            .map(|json| json.to_string())
            .map_err(|err| err.to_string())
    })
    .map_err(|err| {
        format!(
            "encode write_default_json for column `{}` as {:?}: {err}",
            column.name, iceberg_type
        )
    })
}

fn iceberg_type_for_column_def(column: &crate::catalog::schema::ColumnDef) -> Result<Type, String> {
    if let Some(logical_type) = column.logical_type.as_ref() {
        let mut next_field_id = 1;
        return crate::connector::iceberg::catalog::registry::iceberg_type_for_sql_type(
            logical_type,
            &mut next_field_id,
        );
    }
    iceberg_type_for_arrow_data_type(&column.data_type)
}

fn iceberg_type_for_arrow_data_type(data_type: &DataType) -> Result<Type, String> {
    if let Some(primitive) = iceberg_primitive_type_for_arrow_data_type(data_type)? {
        return Ok(Type::Primitive(primitive));
    }

    match data_type {
        DataType::Struct(fields) => Ok(Type::Struct(StructType::new(
            fields
                .iter()
                .map(|field| iceberg_nested_field_for_arrow_field(field.as_ref()))
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        DataType::List(element) | DataType::LargeList(element) => Ok(Type::List(ListType::new(
            iceberg_nested_field_for_arrow_field(element.as_ref())?,
        ))),
        DataType::Map(entries, _sorted) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(format!(
                    "native plan MAP entries field must be Struct, got {:?}",
                    entries.data_type()
                ));
            };
            if fields.len() != 2 {
                return Err(format!(
                    "native plan MAP entries Struct must have 2 fields, got {}",
                    fields.len()
                ));
            }
            Ok(Type::Map(MapType::new(
                iceberg_nested_field_for_arrow_field(fields[0].as_ref())?,
                iceberg_nested_field_for_arrow_field(fields[1].as_ref())?,
            )))
        }
        other => Err(format!(
            "native plan cannot encode write_default_json for Arrow type {other:?} without a logical Iceberg type"
        )),
    }
}

fn iceberg_primitive_type_for_arrow_data_type(
    data_type: &DataType,
) -> Result<Option<PrimitiveType>, String> {
    Ok(Some(match data_type {
        DataType::Boolean => PrimitiveType::Boolean,
        DataType::Int8 | DataType::Int16 | DataType::Int32 => PrimitiveType::Int,
        DataType::Int64 => PrimitiveType::Long,
        DataType::Float32 => PrimitiveType::Float,
        DataType::Float64 => PrimitiveType::Double,
        DataType::Utf8 | DataType::LargeUtf8 => PrimitiveType::String,
        DataType::Binary | DataType::LargeBinary => PrimitiveType::Binary,
        DataType::Date32 => PrimitiveType::Date,
        DataType::Time64(arrow::datatypes::TimeUnit::Microsecond) => PrimitiveType::Time,
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => PrimitiveType::Timestamp,
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
            PrimitiveType::TimestampNs
        }
        DataType::Decimal128(precision, scale) => {
            let scale = u32::try_from(*scale).map_err(|_| {
                format!("Decimal128 negative scale {scale} is not supported by Iceberg defaults")
            })?;
            PrimitiveType::Decimal {
                precision: u32::from(*precision),
                scale,
            }
        }
        _ => return Ok(None),
    }))
}

fn iceberg_nested_field_for_arrow_field(
    field: &Field,
) -> Result<iceberg::spec::NestedFieldRef, String> {
    let field_id = arrow_field_id(field)?;
    let field_type = iceberg_type_for_arrow_data_type(field.data_type())?;
    Ok(Arc::new(NestedField::new(
        field_id,
        field.name(),
        field_type,
        !field.is_nullable(),
    )))
}

fn arrow_field_id(field: &Field) -> Result<i32, String> {
    let raw = field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .ok_or_else(|| {
            format!(
                "native plan field {} is missing parquet field id metadata",
                field.name()
            )
        })?;
    raw.parse::<i32>().map_err(|err| {
        format!(
            "native plan field {} has invalid parquet field id {raw}: {err}",
            field.name()
        )
    })
}

fn scan_binding_for_source<'a>(
    node_id: i32,
    source: &table_model::ScanSource,
    ctx: &'a NativePlanEncodeContext<'_>,
) -> Result<Option<&'a ResolvedScanBinding>, String> {
    let binding =
        optional_context_ref(ctx.scan_bindings).and_then(|bindings| bindings.binding(node_id));
    let required = scan_source_requires_resolved_binding(source);
    if required && binding.is_none() {
        return Err(match source {
            table_model::ScanSource::IcebergDeltaTable {
                from_snapshot_id,
                to_snapshot_id,
                ..
            } => format!(
                "native scan encoder missing prepared binding for node_id={node_id} source={} from_snapshot_id={from_snapshot_id} to_snapshot_id={to_snapshot_id}",
                scan_source_kind(source)
            ),
            _ => format!(
                "native scan encoder missing prepared binding for node_id={node_id} source={}",
                scan_source_kind(source)
            ),
        });
    }
    let Some(binding) = binding else {
        return Ok(None);
    };
    if binding.node_id != node_id {
        return Err(format!(
            "native scan encoder binding node mismatch: requested node_id={node_id}, binding node_id={}",
            binding.node_id
        ));
    }
    let valid_execution = match source {
        table_model::ScanSource::IcebergDeltaTable { .. } => {
            matches!(binding.execution, ResolvedScanExecution::IcebergDelta(_))
        }
        table_model::ScanSource::IcebergDataFiles { .. }
        | table_model::ScanSource::IcebergVersionTable { .. }
        | table_model::ScanSource::IcebergMvTargetState(_)
        | table_model::ScanSource::IcebergMvTargetLocator(_) => {
            matches!(binding.execution, ResolvedScanExecution::IcebergFiles(_))
        }
        table_model::ScanSource::IcebergMetadataTable { .. }
        | table_model::ScanSource::StarRocks { .. } => false,
    };
    if !valid_execution {
        return Err(format!(
            "native scan encoder execution variant mismatch for node_id={node_id} source={}: binding={}",
            scan_source_kind(source),
            resolved_execution_kind(&binding.execution)
        ));
    }
    Ok(Some(binding))
}

fn scan_source_kind(source: &table_model::ScanSource) -> &'static str {
    match source {
        table_model::ScanSource::StarRocks { .. } => "StarRocks",
        table_model::ScanSource::IcebergDataFiles { .. } => "IcebergDataFiles",
        table_model::ScanSource::IcebergMetadataTable { .. } => "IcebergMetadataTable",
        table_model::ScanSource::IcebergDeltaTable { .. } => "IcebergDeltaTable",
        table_model::ScanSource::IcebergVersionTable { .. } => "IcebergVersionTable",
        table_model::ScanSource::IcebergMvTargetState(_) => "IcebergMvTargetState",
        table_model::ScanSource::IcebergMvTargetLocator(_) => "IcebergMvTargetLocator",
    }
}

fn resolved_execution_kind(execution: &ResolvedScanExecution) -> &'static str {
    match execution {
        ResolvedScanExecution::IcebergFiles(_) => "IcebergFiles",
        ResolvedScanExecution::IcebergDelta(_) => "IcebergDelta",
    }
}

fn encode_scan_source(
    src: &table_model::ScanSource,
    scan_node_id: Option<i32>,
    binding: Option<&ResolvedScanBinding>,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::ScanSource, String> {
    use plan::scan_source::Kind;

    if let Some(ResolvedScanExecution::IcebergFiles(files)) =
        binding.map(|binding| &binding.execution)
    {
        return Ok(plan::ScanSource {
            kind: Some(Kind::IcebergDataFiles(plan::IcebergDataFiles {
                table: Some(encode_iceberg_table_info(&files.table)?),
                files: files
                    .files
                    .iter()
                    .map(encode_iceberg_data_file_info)
                    .collect::<Result<Vec<_>, _>>()?,
                cloud_properties: files.cloud_properties.clone().into_iter().collect(),
                binding: match files.binding {
                    iceberg_scan_model::IcebergDataFileBinding::CurrentSnapshot => {
                        plan::IcebergDataFileBinding::CurrentSnapshot as i32
                    }
                    iceberg_scan_model::IcebergDataFileBinding::ExplicitFiles => {
                        plan::IcebergDataFileBinding::ExplicitFiles as i32
                    }
                },
            })),
        });
    }

    Ok(plan::ScanSource {
        kind: Some(match src {
            table_model::ScanSource::StarRocks { .. } => {
                let node_id = scan_node_id.ok_or_else(|| {
                    "StarRocks table source is only valid on a native ScanNode".to_string()
                })?;
                let descriptor = optional_context_ref(ctx.scan_bindings)
                    .and_then(|bindings| bindings.starrocks_source(node_id))
                    .ok_or_else(|| {
                        format!(
                            "StarRocks ScanNode node_id={node_id} missing native source descriptor"
                        )
                    })?;
                Kind::StarrocksTable(plan::StarRocksTableSource {
                    catalog_name: descriptor.catalog_name.clone(),
                    db_id: descriptor.db_id,
                    table_id: descriptor.table_id,
                    schema_id: descriptor.schema_id,
                    storage_columns: descriptor
                        .storage_columns
                        .iter()
                        .map(|column| plan::StarRocksColumnStorageMeta {
                            name: column.name.clone(),
                            unique_id: column.unique_id,
                            default_value: column.default_value.clone(),
                        })
                        .collect(),
                    current_schema: Some(encode_starrocks_tablet_schema(
                        &descriptor.tablet_schema,
                    )),
                })
            }
            table_model::ScanSource::IcebergDataFiles {
                table,
                files,
                cloud_properties,
                binding,
            } => Kind::IcebergDataFiles(plan::IcebergDataFiles {
                table: Some(encode_iceberg_table_info(table)?),
                files: files
                    .iter()
                    .map(encode_iceberg_data_file_info)
                    .collect::<Result<Vec<_>, _>>()?,
                cloud_properties: cloud_properties.clone().into_iter().collect(),
                binding: match binding {
                    iceberg_scan_model::IcebergDataFileBinding::CurrentSnapshot => {
                        plan::IcebergDataFileBinding::CurrentSnapshot as i32
                    }
                    iceberg_scan_model::IcebergDataFileBinding::ExplicitFiles => {
                        plan::IcebergDataFileBinding::ExplicitFiles as i32
                    }
                },
            }),
            table_model::ScanSource::IcebergMetadataTable {
                table,
                metadata_table_type,
                serialized_table,
                cloud_properties,
                metadata_payload,
            } => Kind::IcebergMetadataTable(plan::IcebergMetadataTable {
                table: Some(encode_iceberg_table_info(table)?),
                metadata_table_type: encode_iceberg_metadata_table_type(metadata_table_type),
                serialized_table: serialized_table.clone(),
                cloud_properties: cloud_properties.clone().into_iter().collect(),
                metadata_payload: metadata_payload.clone(),
            }),
            table_model::ScanSource::IcebergDeltaTable {
                table,
                from_snapshot_id,
                to_snapshot_id,
            } => {
                let Some(ResolvedScanExecution::IcebergDelta(delta)) =
                    binding.map(|binding| &binding.execution)
                else {
                    return Err(format!(
                        "native scan encoder missing prepared IcebergDelta binding for node_id={}",
                        scan_node_id
                            .map(|node_id| node_id.to_string())
                            .unwrap_or_else(|| "<none>".to_string())
                    ));
                };

                Kind::IcebergDeltaTable(plan::IcebergDeltaTable {
                    table: Some(encode_iceberg_table_info(table)?),
                    from_snapshot_id: *from_snapshot_id,
                    to_snapshot_id: *to_snapshot_id,
                    delta_plan: Some(
                        super::iceberg_delta_scan::encode_iceberg_delta_scan_plan_native(
                            &delta.runtime_plan,
                        )?,
                    ),
                })
            }
            table_model::ScanSource::IcebergVersionTable { table, snapshot_id } => {
                Kind::IcebergVersionTable(plan::IcebergVersionTable {
                    table: Some(encode_iceberg_table_info(table)?),
                    snapshot_id: *snapshot_id,
                })
            }
            table_model::ScanSource::IcebergMvTargetState(scan) => {
                Kind::IcebergMvTargetState(plan::IcebergMvTargetState {
                    catalog: scan.catalog.clone(),
                    database: scan.database.clone(),
                    table: scan.table.clone(),
                    target_table_uuid: scan.target_table_uuid.clone(),
                    target_snapshot_id: scan.target_snapshot_id,
                    aggregate_state_layout_version: u32::from(scan.aggregate_state_layout_version),
                    columns: scan
                        .columns
                        .iter()
                        .map(encode_column_def)
                        .collect::<Result<Vec<_>, _>>()?,
                    group_key_names: scan.group_key_names.clone(),
                    aggregate_state_names: scan.aggregate_state_names.clone(),
                    physical_column_names: scan.physical_column_names.clone(),
                    row_id_column_name: scan.row_id_column_name.clone(),
                    row_filter: Some(encode_mv_target_state_row_filter(&scan.row_filter)),
                    partition_constraint: match scan.partition_constraint {
                        table_model::IcebergMvTargetStatePartitionConstraint::Unpartitioned => {
                            plan::IcebergMvTargetStatePartitionConstraint::Unpartitioned as i32
                        }
                        table_model::IcebergMvTargetStatePartitionConstraint::AffectedPartitionAllowListRequired => {
                            plan::IcebergMvTargetStatePartitionConstraint::AffectedPartitionAllowListRequired as i32
                        }
                    },
                })
            }
            table_model::ScanSource::IcebergMvTargetLocator(scan) => {
                Kind::IcebergMvTargetLocator(plan::IcebergMvTargetLocator {
                    catalog: scan.catalog.clone(),
                    database: scan.database.clone(),
                    table: scan.table.clone(),
                    target_table_uuid: scan.target_table_uuid.clone(),
                    target_snapshot_id: scan.target_snapshot_id,
                    apply_key_column: scan.apply_key_column.clone(),
                    branch_id_column: scan.branch_id_column.clone(),
                })
            }
        }),
    })
}

fn encode_starrocks_tablet_schema(
    schema: &StarRocksTabletSchemaDescriptor,
) -> plan::StarRocksTabletSchema {
    plan::StarRocksTabletSchema {
        schema_id: schema.schema_id,
        keys_type: match schema.keys_type {
            StarRocksKeysTypeDescriptor::Duplicate => {
                plan::StarRocksKeysType::StarrocksKeysTypeDuplicate as i32
            }
            StarRocksKeysTypeDescriptor::Unique => {
                plan::StarRocksKeysType::StarrocksKeysTypeUnique as i32
            }
            StarRocksKeysTypeDescriptor::Aggregate => {
                plan::StarRocksKeysType::StarrocksKeysTypeAggregate as i32
            }
            StarRocksKeysTypeDescriptor::Primary => {
                plan::StarRocksKeysType::StarrocksKeysTypePrimary as i32
            }
        },
        num_short_key_columns: schema.num_short_key_columns,
        sort_key_idxes: schema.sort_key_idxes.clone(),
        sort_key_unique_ids: schema.sort_key_unique_ids.clone(),
        columns: schema
            .columns
            .iter()
            .map(encode_starrocks_column_schema)
            .collect(),
    }
}

fn encode_starrocks_column_schema(
    column: &StarRocksColumnSchemaDescriptor,
) -> plan::StarRocksColumnSchema {
    plan::StarRocksColumnSchema {
        unique_id: column.unique_id,
        name: column.name.clone(),
        physical_type: column.physical_type.clone(),
        is_key: Some(column.is_key),
        aggregation: column.aggregation.clone(),
        nullable: Some(column.nullable),
        default_value: column.default_value.clone(),
        precision: column.precision,
        scale: column.scale,
        visible: Some(column.visible),
        children: column
            .children
            .iter()
            .map(encode_starrocks_column_schema)
            .collect(),
    }
}

fn encode_mv_target_state_row_filter(
    src: &table_model::IcebergMvTargetStateRowFilter,
) -> plan::IcebergMvTargetStateRowFilter {
    use plan::iceberg_mv_target_state_row_filter::Kind;

    plan::IcebergMvTargetStateRowFilter {
        kind: Some(match src {
            table_model::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name,
                branch_scope,
            } => Kind::DeltaInputRowIds(plan::DeltaInputRowIdsFilter {
                row_id_column_name: row_id_column_name.clone(),
                branch_scope: branch_scope.as_ref().map(|scope| plan::BranchScope {
                    branch_id_column_name: scope.branch_id_column_name.clone(),
                    branch_id: scope.branch_id,
                }),
            }),
        }),
    }
}

fn encode_iceberg_table_info(
    src: &iceberg_scan_model::IcebergTableInfo,
) -> Result<plan::IcebergTableInfo, String> {
    Ok(plan::IcebergTableInfo {
        catalog: src.catalog.clone(),
        namespace: src.namespace.clone(),
        table: src.table.clone(),
        table_uuid: src.table_uuid.clone(),
        current_snapshot_id: src.current_snapshot_id,
        schema_id: src.schema_id,
        location: src.location.clone(),
        schema: Some(encode_iceberg_schema_def(&src.schema)?),
        serialized_metadata: src.serialized_metadata.clone(),
        serialized_metadata_rows: src.serialized_metadata_rows.clone(),
    })
}

fn encode_iceberg_schema_def(
    src: &iceberg_scan_model::IcebergSchemaDef,
) -> Result<plan::IcebergSchemaDef, String> {
    Ok(plan::IcebergSchemaDef {
        fields: src
            .fields
            .iter()
            .map(encode_iceberg_schema_field)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn encode_iceberg_schema_field(
    src: &iceberg_scan_model::IcebergSchemaFieldDef,
) -> Result<plan::IcebergSchemaFieldDef, String> {
    Ok(plan::IcebergSchemaFieldDef {
        field_id: src.field_id,
        name: src.name.clone(),
        initial_default_json: encode_iceberg_schema_default_json(
            "initial_default",
            src.initial_default_json.as_ref(),
            src.initial_default.as_ref(),
        )?,
        write_default_json: encode_iceberg_schema_default_json(
            "write_default",
            src.write_default_json.as_ref(),
            src.write_default.as_ref(),
        )?,
        children: src
            .children
            .iter()
            .map(encode_iceberg_schema_field)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn encode_iceberg_schema_default_json(
    label: &str,
    precomputed_json: Option<&String>,
    literal: Option<&iceberg::spec::Literal>,
) -> Result<Option<String>, String> {
    if let Some(json) = precomputed_json {
        return Ok(Some(json.clone()));
    }
    literal
        .map(super::iceberg_literal_json::serialize_iceberg_literal_json)
        .transpose()
        .map_err(|err| format!("encode Iceberg schema {label} JSON: {err}"))
}

fn encode_iceberg_data_file_info(
    src: &iceberg_scan_model::IcebergDataFileInfo,
) -> Result<plan::IcebergDataFileInfo, String> {
    Ok(plan::IcebergDataFileInfo {
        path: src.path.clone(),
        size: src.size,
        row_count: src.row_count,
        column_stats: src
            .column_stats
            .as_ref()
            .map(|stats| plan::IcebergColumnStatsMap {
                entries: stats
                    .iter()
                    .map(|(name, stats)| (name.clone(), encode_iceberg_column_stats(stats)))
                    .collect::<HashMap<_, _>>(),
            }),
        partition_spec_id: src.partition_spec_id,
        partition_key: src.partition_key.clone(),
        first_row_id: src.first_row_id,
        data_sequence_number: src.data_sequence_number,
        ivm_change_op: src.ivm_change_op.map(i32::from),
        included_positions: src
            .included_positions
            .as_ref()
            .map(|values| plan::Int64List {
                values: values.clone(),
            }),
        delete_files: src
            .delete_files
            .iter()
            .map(encode_iceberg_delete_file_info)
            .collect(),
        manifest_path: src.manifest_path.clone(),
        partition_values: src
            .partition_values
            .iter()
            .map(encode_iceberg_partition_field_value)
            .collect(),
    })
}

fn encode_iceberg_column_stats(
    src: &iceberg_scan_model::IcebergColumnStats,
) -> plan::IcebergColumnStats {
    plan::IcebergColumnStats {
        null_count: src.null_count,
        value_count: src.value_count,
        column_size: src.column_size,
        lower_bound: src.lower_bound.clone(),
        upper_bound: src.upper_bound.clone(),
    }
}

fn encode_iceberg_delete_file_info(
    src: &iceberg_scan_model::IcebergDeleteFileInfo,
) -> plan::IcebergDeleteFileInfo {
    plan::IcebergDeleteFileInfo {
        path: src.path.clone(),
        file_format: match src.file_format {
            iceberg_scan_model::IcebergDeleteFileFormat::Parquet => {
                plan::IcebergDeleteFileFormat::Parquet as i32
            }
            iceberg_scan_model::IcebergDeleteFileFormat::Puffin => {
                plan::IcebergDeleteFileFormat::Puffin as i32
            }
        },
        file_content: match src.file_content {
            iceberg_scan_model::IcebergDeleteFileContent::Position => {
                plan::IcebergDeleteFileContent::Position as i32
            }
            iceberg_scan_model::IcebergDeleteFileContent::Equality => {
                plan::IcebergDeleteFileContent::Equality as i32
            }
        },
        length: src.length,
        content_offset: src.content_offset,
        content_size_in_bytes: src.content_size_in_bytes,
        sequence_number: src.sequence_number,
        partition_spec_id: src.partition_spec_id,
        partition_key: src.partition_key.clone(),
        equality_column_names: src.equality_column_names.clone(),
        equality_field_ids: src.equality_field_ids.clone(),
    }
}

fn encode_iceberg_partition_field_value(
    src: &iceberg_scan_model::IcebergPartitionFieldValue,
) -> plan::IcebergPartitionFieldValue {
    plan::IcebergPartitionFieldValue {
        source_column: src.source_column.clone(),
        field_name: src.field_name.clone(),
        transform: src.transform.clone(),
        value: src.value.as_ref().map(encode_iceberg_partition_value),
    }
}

fn encode_iceberg_partition_value(
    src: &iceberg_scan_model::IcebergPartitionValue,
) -> plan::IcebergPartitionValue {
    use plan::iceberg_partition_value::Value;

    plan::IcebergPartitionValue {
        value: Some(match src {
            iceberg_scan_model::IcebergPartitionValue::Boolean(value) => Value::BoolValue(*value),
            iceberg_scan_model::IcebergPartitionValue::Int32(value) => Value::Int32Value(*value),
            iceberg_scan_model::IcebergPartitionValue::Int64(value) => Value::Int64Value(*value),
            iceberg_scan_model::IcebergPartitionValue::Float(value) => Value::FloatValue(*value),
            iceberg_scan_model::IcebergPartitionValue::Double(value) => Value::DoubleValue(*value),
            iceberg_scan_model::IcebergPartitionValue::String(value) => {
                Value::StringValue(value.clone())
            }
            iceberg_scan_model::IcebergPartitionValue::Binary(value) => {
                Value::BinaryValue(value.clone())
            }
        }),
    }
}

fn encode_iceberg_write_sink_spec(
    src: &IcebergWriteSinkSpec,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::IcebergWriteSinkSpec, String> {
    Ok(plan::IcebergWriteSinkSpec {
        mode: encode_iceberg_write_sink_mode(src.mode),
        target_table_id: src.target_table_id,
        target_table: Some(encode_table_def_with_context(
            &src.target_table,
            None,
            None,
            None,
            ctx,
        )?),
        iceberg: Some(encode_iceberg_table_info(&src.iceberg)?),
        target_columns: src
            .target_columns
            .iter()
            .map(encode_column_def)
            .collect::<Result<Vec<_>, _>>()?,
        table_location: src.table_location.clone(),
        data_location: src.data_location.clone(),
        target_partition_spec_id: src.target_partition_spec_id,
        cloud_properties: src.cloud_properties.clone().into_iter().collect(),
        file_format: src.file_format.clone(),
        compression: match src.compression {
            IcebergWriteFileCompression::Snappy => plan::IcebergWriteFileCompression::Snappy as i32,
        },
        position_delete_output_descriptor: src
            .position_delete_output_descriptor
            .as_ref()
            .map(encode_position_delete_descriptor)
            .transpose()?,
    })
}

fn encode_iceberg_write_input_binding(
    src: &IcebergWriteInputBinding,
) -> plan::IcebergWriteInputBinding {
    use plan::iceberg_write_input_binding::Kind;

    plan::IcebergWriteInputBinding {
        kind: Some(match src {
            IcebergWriteInputBinding::RootOutputByOrdinal => Kind::RootOutputByOrdinal(true),
            IcebergWriteInputBinding::OutputOrdinals(values) => {
                Kind::OutputOrdinals(plan::UInt64List {
                    values: values.iter().map(|value| usize_to_u64(*value)).collect(),
                })
            }
        }),
    }
}

fn encode_position_delete_descriptor(
    src: &crate::connector::iceberg::position_delete_descriptor::PositionDeleteDescriptorInput,
) -> Result<plan::PositionDeleteDescriptorInput, String> {
    Ok(plan::PositionDeleteDescriptorInput {
        file_path: Some(encode_position_delete_output_field(&src.file_path)?),
        pos: Some(encode_position_delete_output_field(&src.pos)?),
        partition_source_fields: src
            .partition_source_fields
            .iter()
            .map(encode_position_delete_partition_source_field)
            .collect::<Result<Vec<_>, _>>()?,
        target_partition_spec_id: src.target_partition_spec_id,
    })
}

fn encode_position_delete_output_field(
    src: &crate::connector::iceberg::position_delete_descriptor::PositionDeleteOutputField,
) -> Result<plan::PositionDeleteOutputField, String> {
    Ok(plan::PositionDeleteOutputField {
        output_expr_index: usize_to_u64(src.output_expr_index),
        name: src.name.clone(),
        data_type: Some(encode_type(&src.data_type)?),
        field_id: src.field_id,
    })
}

fn encode_position_delete_partition_source_field(
    src: &crate::connector::iceberg::position_delete_descriptor::PositionDeletePartitionSourceField,
) -> Result<plan::PositionDeletePartitionSourceField, String> {
    Ok(plan::PositionDeletePartitionSourceField {
        output_expr_index: usize_to_u64(src.output_expr_index),
        source_column_name: src.source_column_name.clone(),
        partition_field_name: src.partition_field_name.clone(),
        transform_expr: src.transform_expr.clone(),
        source_field_id: src.source_field_id,
        data_type: Some(encode_type(&src.data_type)?),
    })
}

fn encode_output_column(
    src: &crate::sql::analysis::OutputColumn,
) -> Result<common::OutputColumn, String> {
    Ok(common::OutputColumn {
        column_id: src.column_id.0,
        name: src.name.clone(),
        r#type: Some(encode_type(&src.data_type)?),
        nullable: src.nullable,
        is_internal: src.is_internal,
    })
}

fn encode_exprs(
    src: &[crate::sql::analysis::TypedExpr],
) -> Result<Vec<crate::proto::expr::Expr>, String> {
    src.iter().map(encode_expr).collect()
}

fn encode_sql_type(src: &SqlType) -> Result<common::TypeDesc, String> {
    use common::type_desc::Kind;

    Ok(common::TypeDesc {
        kind: Some(match src {
            SqlType::Array(element) => Kind::List(Box::new(common::ListType {
                element: Some(Box::new(encode_sql_type(element)?)),
            })),
            SqlType::Map(key, value) => Kind::Map(Box::new(common::MapType {
                key: Some(Box::new(encode_sql_type(key)?)),
                value: Some(Box::new(encode_sql_type(value)?)),
            })),
            SqlType::Struct(fields) => Kind::Strct(common::StructType {
                fields: fields
                    .iter()
                    .map(|(name, ty)| {
                        Ok(common::StructField {
                            name: name.clone(),
                            r#type: Some(encode_sql_type(ty)?),
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
            }),
            other => Kind::Scalar(sql_scalar_type(other)?),
        }),
    })
}

fn sql_scalar_type(src: &SqlType) -> Result<common::ScalarType, String> {
    use common::PrimitiveType;

    let (primitive, precision, scale, time_unit) = match src {
        SqlType::TinyInt => (PrimitiveType::Tinyint, None, None, None),
        SqlType::SmallInt => (PrimitiveType::Smallint, None, None, None),
        SqlType::Int => (PrimitiveType::Int, None, None, None),
        SqlType::BigInt => (PrimitiveType::Bigint, None, None, None),
        SqlType::LargeInt => (PrimitiveType::Largeint, None, None, None),
        SqlType::Float => (PrimitiveType::Float, None, None, None),
        SqlType::Double => (PrimitiveType::Double, None, None, None),
        SqlType::Decimal { precision, scale } => (
            PrimitiveType::Decimal128,
            Some(i32::from(*precision)),
            Some(i32::from(*scale)),
            None,
        ),
        SqlType::String => (PrimitiveType::Varchar, None, None, None),
        SqlType::Json => (PrimitiveType::Json, None, None, None),
        SqlType::Binary => (PrimitiveType::Varbinary, None, None, None),
        SqlType::Bitmap => (PrimitiveType::Bitmap, None, None, None),
        SqlType::Hll => (PrimitiveType::Hll, None, None, None),
        SqlType::Boolean => (PrimitiveType::Boolean, None, None, None),
        SqlType::Date => (PrimitiveType::Date, None, None, None),
        SqlType::DateTime => (PrimitiveType::Datetime, None, None, None),
        SqlType::DateTimeNs => (PrimitiveType::Datetime, None, None, Some(3)),
        SqlType::Time => (PrimitiveType::Time, None, None, None),
        SqlType::Variant => (PrimitiveType::Variant, None, None, None),
        SqlType::Array(_) | SqlType::Map(_, _) | SqlType::Struct(_) => {
            return Err("nested SqlType cannot be encoded as scalar TypeDesc".to_string());
        }
    };
    Ok(common::ScalarType {
        r#type: primitive as i32,
        len: None,
        precision,
        scale,
        time_unit,
    })
}

fn encode_edge_partition_type(src: &DataPartition) -> i32 {
    match src.kind {
        PartitionKind::Unpartitioned => plan::PartitionType::Unpartitioned as i32,
        PartitionKind::Random => plan::PartitionType::Random as i32,
        PartitionKind::Hash => plan::PartitionType::Hash as i32,
    }
}

fn encode_join_kind(src: JoinKind) -> i32 {
    match src {
        JoinKind::Inner => plan::JoinKind::Inner as i32,
        JoinKind::LeftOuter => plan::JoinKind::LeftOuter as i32,
        JoinKind::RightOuter => plan::JoinKind::RightOuter as i32,
        JoinKind::FullOuter => plan::JoinKind::FullOuter as i32,
        JoinKind::Cross => plan::JoinKind::Cross as i32,
        JoinKind::LeftSemi => plan::JoinKind::LeftSemi as i32,
        JoinKind::RightSemi => plan::JoinKind::RightSemi as i32,
        JoinKind::LeftAnti => plan::JoinKind::LeftAnti as i32,
        JoinKind::RightAnti => plan::JoinKind::RightAnti as i32,
        JoinKind::NullAwareLeftAnti => plan::JoinKind::NullAwareLeftAnti as i32,
    }
}

fn encode_join_distribution(src: &JoinDistribution) -> i32 {
    match src {
        JoinDistribution::Unknown => plan::JoinDistribution::Unknown as i32,
        JoinDistribution::Shuffle => plan::JoinDistribution::Shuffle as i32,
        JoinDistribution::Broadcast => plan::JoinDistribution::Broadcast as i32,
        JoinDistribution::Colocate => plan::JoinDistribution::Colocate as i32,
    }
}

fn encode_join_execution_mode(src: JoinExecutionMode) -> i32 {
    match src {
        JoinExecutionMode::Broadcast => plan::JoinExecutionMode::Broadcast as i32,
        JoinExecutionMode::Partitioned => plan::JoinExecutionMode::Partitioned as i32,
        JoinExecutionMode::Colocate => plan::JoinExecutionMode::Colocate as i32,
    }
}

fn encode_agg_mode(src: AggMode) -> i32 {
    match src {
        AggMode::Single => plan::AggMode::Single as i32,
        AggMode::Local => plan::AggMode::Local as i32,
        AggMode::Global => plan::AggMode::Global as i32,
        AggMode::DistinctGlobal => plan::AggMode::DistinctGlobal as i32,
        AggMode::DistinctLocal => plan::AggMode::DistinctLocal as i32,
    }
}

fn encode_topn_phase(src: TopNPhase) -> i32 {
    match src {
        TopNPhase::Partial => plan::TopNPhase::TopnPhasePartial as i32,
        TopNPhase::Final => plan::TopNPhase::TopnPhaseFinal as i32,
    }
}

fn encode_set_op_kind(src: PlanSetOpKind) -> i32 {
    match src {
        PlanSetOpKind::UnionAll => plan::PlanSetOpKind::UnionAll as i32,
        PlanSetOpKind::UnionDistinct => plan::PlanSetOpKind::UnionDistinct as i32,
        PlanSetOpKind::Intersect => plan::PlanSetOpKind::Intersect as i32,
        PlanSetOpKind::Except => plan::PlanSetOpKind::Except as i32,
    }
}

fn encode_change_stream_branch_kind(src: ChangeStreamBranchKind) -> i32 {
    match src {
        ChangeStreamBranchKind::DeleteDv => plan::ChangeStreamBranchKind::DeleteDv as i32,
        ChangeStreamBranchKind::ReuseData => plan::ChangeStreamBranchKind::ReuseData as i32,
        ChangeStreamBranchKind::FreshData => plan::ChangeStreamBranchKind::FreshData as i32,
    }
}

fn encode_sort_topn_type(src: crate::exec::node::sort::SortTopNType) -> i32 {
    match src {
        crate::exec::node::sort::SortTopNType::RowNumber => {
            plan::SortTopNType::SortTopnTypeRowNumber as i32
        }
        crate::exec::node::sort::SortTopNType::Rank => plan::SortTopNType::SortTopnTypeRank as i32,
        crate::exec::node::sort::SortTopNType::DenseRank => {
            plan::SortTopNType::SortTopnTypeDenseRank as i32
        }
    }
}

fn encode_hash_source(src: HashSource) -> i32 {
    match src {
        HashSource::ShuffleAgg => plan::HashSource::ShuffleAgg as i32,
        HashSource::ShuffleJoin => plan::HashSource::ShuffleJoin as i32,
    }
}

fn encode_redistribute_mode(src: &RedistributeMode) -> plan::RedistributeMode {
    use plan::redistribute_mode::Mode;

    plan::RedistributeMode {
        mode: Some(match src {
            RedistributeMode::Gather => Mode::Gather(true),
            RedistributeMode::Hash { cols, source } => Mode::Hash(plan::RedistributeHash {
                cols: cols.iter().map(|id| id.0).collect(),
                source: encode_hash_source(*source),
            }),
            RedistributeMode::Broadcast => Mode::Broadcast(true),
        }),
    }
}

fn encode_iceberg_metadata_table_type(
    src: &crate::connector::iceberg::IcebergMetadataTableType,
) -> i32 {
    match src {
        crate::connector::iceberg::IcebergMetadataTableType::Files => {
            plan::IcebergMetadataTableType::Files as i32
        }
        crate::connector::iceberg::IcebergMetadataTableType::Manifests => {
            plan::IcebergMetadataTableType::Manifests as i32
        }
        crate::connector::iceberg::IcebergMetadataTableType::LogicalIcebergMetadata => {
            plan::IcebergMetadataTableType::LogicalIcebergMetadata as i32
        }
        crate::connector::iceberg::IcebergMetadataTableType::Snapshots => {
            plan::IcebergMetadataTableType::Snapshots as i32
        }
        crate::connector::iceberg::IcebergMetadataTableType::History => {
            plan::IcebergMetadataTableType::History as i32
        }
        crate::connector::iceberg::IcebergMetadataTableType::Refs => {
            plan::IcebergMetadataTableType::Refs as i32
        }
        crate::connector::iceberg::IcebergMetadataTableType::Partitions => {
            plan::IcebergMetadataTableType::Partitions as i32
        }
    }
}

fn encode_iceberg_write_sink_mode(src: IcebergWriteSinkMode) -> i32 {
    match src {
        IcebergWriteSinkMode::Data => plan::IcebergWriteSinkMode::Data as i32,
        IcebergWriteSinkMode::RowLineageData => plan::IcebergWriteSinkMode::RowLineageData as i32,
        IcebergWriteSinkMode::PositionDeletes => plan::IcebergWriteSinkMode::PositionDeletes as i32,
        IcebergWriteSinkMode::DeletionVectors => plan::IcebergWriteSinkMode::DeletionVectors as i32,
        IcebergWriteSinkMode::EqualityDeletes => plan::IcebergWriteSinkMode::EqualityDeletes as i32,
    }
}

fn usize_to_u64(value: usize) -> u64 {
    value as u64
}

fn usize_to_u32(value: usize) -> Result<u32, String> {
    u32::try_from(value).map_err(|_| format!("value {value} does not fit in u32"))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod legacy_tests {
    use std::collections::HashMap;

    use super::tests::two_fragment_stream_plan_for_test;
    use super::*;
    use crate::coordinator::prepare::scan::IcebergDeltaScanRuntimePlan;
    use crate::coordinator::prepare::scan::{
        ResolvedIcebergDeltaScan, ResolvedIcebergFileScan, ResolvedReadColumn, ResolvedReadReason,
        ResolvedScanBinding, ResolvedScanColumn, ResolvedScanColumnKind, ResolvedScanExecution,
        ScanExecutionBindings,
    };
    use crate::runtime_filter::model::graph::RuntimeFilterGraph;
    use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::distributed::DataPartition;
    use crate::sql::planner::physical::runtime_filter::{
        RuntimeFilterBuildIntent, RuntimeFilterProbeIntent,
    };
    use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};
    use arrow::datatypes::{DataType, TimeUnit};

    fn empty_scan_bindings() -> &'static ScanExecutionBindings {
        Box::leak(Box::new(ScanExecutionBindings::default()))
    }

    fn prepared_runtime_filter_bindings(plan: &DistributedPlan) -> &'static PreparedFragmentSet {
        Box::leak(Box::new(
            crate::coordinator::prepare::prepared_fragment_set_for_native_encode_test(plan)
                .expect("materialize native encoder test binding tables"),
        ))
    }

    #[test]
    fn full_plan_encoding_requires_prepared_runtime_filter_binding_tables() {
        let plan = iceberg_delta_distributed_plan_for_test();
        let error = encode_distributed_plan_with_context(
            &plan,
            NativePlanEncodeContext {
                scan_bindings: None,
                node_outputs: None,
                fragment_edge_outputs: None,
                write_contracts: None,
                runtime_filter_bindings: None,
            },
        )
        .expect_err("full-plan encoding without prepared RF binding tables must fail");

        assert_eq!(
            error,
            "native distributed plan encoding requires prepared runtime filter binding tables"
        );
    }

    fn encode_write_default_json_for_test(
        data_type: DataType,
        value: ColumnDefault,
    ) -> Result<Option<String>, String> {
        encode_column_def(&crate::catalog::schema::ColumnDef {
            name: "defaulted".to_string(),
            data_type,
            nullable: true,
            write_default: Some(value),
            logical_type: None,
        })
        .map(|column| column.write_default_json)
    }

    fn field_with_iceberg_id(
        id: i32,
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> Arc<Field> {
        Arc::new(
            Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )])),
        )
    }

    #[test]
    fn column_write_default_json_preserves_primitive_and_temporal_lexical_bytes() {
        let cases = [
            (
                "boolean",
                DataType::Boolean,
                ColumnDefault::Boolean(true),
                "true",
            ),
            ("integer", DataType::Int32, ColumnDefault::Int32(-7), "-7"),
            (
                "decimal",
                DataType::Decimal128(10, 2),
                ColumnDefault::Decimal {
                    unscaled: 999,
                    precision: 10,
                    scale: 2,
                },
                "\"9.99\"",
            ),
            (
                "date",
                DataType::Date32,
                ColumnDefault::Date {
                    days_since_epoch: 0,
                },
                "\"1970-01-01\"",
            ),
            (
                "time",
                DataType::Time64(TimeUnit::Microsecond),
                ColumnDefault::TimeMicros {
                    micros_since_midnight: 0,
                },
                "\"00:00:00\"",
            ),
            (
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                ColumnDefault::TimestampMicros {
                    micros_since_epoch: 1_234_567,
                },
                "\"1970-01-01T00:00:01.234567\"",
            ),
            (
                "timestamptz-normalized",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                ColumnDefault::TimestamptzMicros {
                    micros_since_epoch: 1_234_567,
                },
                "\"1970-01-01T00:00:01.234567\"",
            ),
            (
                "timestamp-ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                ColumnDefault::TimestampNanos {
                    nanos_since_epoch: 1_234_567_890,
                },
                "\"1970-01-01T00:00:01.234567890\"",
            ),
            (
                "binary",
                DataType::Binary,
                ColumnDefault::Binary(vec![0x00, 0x0f, 0x10, 0xff]),
                "\"0f10ff\"",
            ),
        ];

        for (name, data_type, literal, expected) in cases {
            assert_eq!(
                encode_write_default_json_for_test(data_type, literal)
                    .unwrap_or_else(|error| panic!("encode {name} write default: {error}"))
                    .as_deref(),
                Some(expected),
                "case={name}"
            );
        }
    }

    #[test]
    fn column_write_default_json_preserves_empty_and_nested_collection_lexical_bytes() {
        let empty_list_type =
            DataType::List(field_with_iceberg_id(1, "element", DataType::Int32, true));
        assert_eq!(
            encode_write_default_json_for_test(empty_list_type, ColumnDefault::Array(Vec::new()),)
                .unwrap()
                .as_deref(),
            Some("[]")
        );

        let empty_map_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        field_with_iceberg_id(2, "key", DataType::Utf8, false),
                        field_with_iceberg_id(3, "value", DataType::Int32, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        assert_eq!(
            encode_write_default_json_for_test(empty_map_type, ColumnDefault::Map(Vec::new()),)
                .unwrap()
                .as_deref(),
            Some(r#"{"keys":[],"values":[]}"#)
        );

        let list_type = DataType::List(field_with_iceberg_id(11, "element", DataType::Int32, true));
        let map_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        field_with_iceberg_id(13, "key", DataType::Utf8, false),
                        field_with_iceberg_id(14, "value", DataType::Int32, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let nested_type = DataType::Struct(
            vec![
                field_with_iceberg_id(10, "items", list_type, true),
                field_with_iceberg_id(12, "attributes", map_type, true),
            ]
            .into(),
        );
        let nested_literal = ColumnDefault::Struct(vec![
            (
                "items".to_string(),
                ColumnDefault::Array(vec![ColumnDefault::Int32(1), ColumnDefault::Null]),
            ),
            (
                "attributes".to_string(),
                ColumnDefault::Map(vec![
                    (
                        ColumnDefault::String("first".to_string()),
                        ColumnDefault::Int32(2),
                    ),
                    (
                        ColumnDefault::String("second".to_string()),
                        ColumnDefault::Null,
                    ),
                ]),
            ),
        ]);
        assert_eq!(
            encode_write_default_json_for_test(nested_type, nested_literal)
                .unwrap()
                .as_deref(),
            Some(r#"{"10":[1,null],"12":{"keys":["first","second"],"values":[2,null]}}"#)
        );
    }

    #[test]
    fn column_write_default_json_preserves_non_finite_as_legacy_null() {
        let cases = [
            (
                "float-nan",
                DataType::Float32,
                ColumnDefault::Float32 { bits: 0x7fc0_1234 },
            ),
            (
                "float-positive-infinity",
                DataType::Float32,
                ColumnDefault::Float32 {
                    bits: f32::INFINITY.to_bits(),
                },
            ),
            (
                "float-negative-infinity",
                DataType::Float32,
                ColumnDefault::Float32 {
                    bits: f32::NEG_INFINITY.to_bits(),
                },
            ),
            (
                "double-nan",
                DataType::Float64,
                ColumnDefault::Float64 {
                    bits: 0x7ff8_0000_0000_1234,
                },
            ),
            (
                "double-positive-infinity",
                DataType::Float64,
                ColumnDefault::Float64 {
                    bits: f64::INFINITY.to_bits(),
                },
            ),
            (
                "double-negative-infinity",
                DataType::Float64,
                ColumnDefault::Float64 {
                    bits: f64::NEG_INFINITY.to_bits(),
                },
            ),
        ];

        for (name, data_type, literal) in cases {
            assert_eq!(
                encode_write_default_json_for_test(data_type, literal)
                    .unwrap_or_else(|error| panic!("encode {name} write default: {error}"))
                    .as_deref(),
                Some("null"),
                "case={name}"
            );
        }
    }

    #[test]
    fn column_write_default_json_preserves_uuid_and_fixed_unsupported_errors() {
        let cases = [
            (
                DataType::FixedSizeBinary(16),
                ColumnDefault::Uuid(0x0011_2233_4455_6677_8899_aabb_ccdd_eeff_u128.to_be_bytes()),
                "native plan cannot encode write_default_json for Arrow type FixedSizeBinary(16) without a logical Iceberg type",
            ),
            (
                DataType::FixedSizeBinary(4),
                ColumnDefault::Fixed {
                    size: 4,
                    bytes: vec![0x00, 0x7f, 0x80, 0xff],
                },
                "Arrow-to-native TypeDesc conversion does not support data type FixedSizeBinary(4)",
            ),
        ];

        for (data_type, literal, expected) in cases {
            assert_eq!(
                encode_write_default_json_for_test(data_type, literal).unwrap_err(),
                expected
            );
        }
    }

    #[test]
    fn native_encoder_round_trips_all_binding_roles_contracts_and_locations() {
        let probe_expr = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(1),
                qualifier: Some("probe".to_string()),
                column: "k".to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        };
        let build_expr = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(2),
                qualifier: Some("build".to_string()),
                column: "k".to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        };
        let probe_output = vec![output_column(1, "probe", DataType::Int64)];
        let build_output = vec![output_column(2, "build", DataType::Int64)];
        let probe = crate::sql::planner::physical::PhysicalPlanNode {
            kind: PhysicalPlanKind::Values(crate::sql::planner::payload::PlanValuesNode {
                rows: Vec::new(),
                columns: probe_output.clone(),
            }),
            children: Vec::new(),
            output_columns: probe_output,
            stats: stats(),
            probe_runtime_filters: vec![RuntimeFilterProbeIntent {
                filter_id: 41,
                probe_expr: probe_expr.clone(),
            }],
        };
        let build = crate::sql::planner::physical::PhysicalPlanNode {
            kind: PhysicalPlanKind::Values(crate::sql::planner::payload::PlanValuesNode {
                rows: Vec::new(),
                columns: build_output.clone(),
            }),
            children: Vec::new(),
            output_columns: build_output,
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let physical = crate::sql::planner::physical::PhysicalPlanNode {
            kind: PhysicalPlanKind::HashJoin(Box::new(
                crate::sql::planner::physical::PhysicalHashJoinNode {
                    join_type: JoinKind::Inner,
                    eq_conditions: vec![
                        crate::sql::planner::physical::PhysicalHashJoinEqCondition {
                            left: probe_expr.clone(),
                            right: build_expr.clone(),
                            null_safe: false,
                        },
                    ],
                    other_condition: None,
                    distribution: JoinDistribution::Broadcast,
                    execution_mode: Some(JoinExecutionMode::Broadcast),
                    build_runtime_filters: vec![RuntimeFilterBuildIntent {
                        filter_id: 41,
                        build_expr,
                        probe_expr,
                        expr_order: 0,
                        execution_mode: JoinExecutionMode::Broadcast,
                    }],
                    output_columns: vec![
                        output_column(1, "probe", DataType::Int64),
                        output_column(2, "build", DataType::Int64),
                    ],
                },
            )),
            children: vec![probe, build],
            output_columns: vec![
                output_column(1, "probe", DataType::Int64),
                output_column(2, "build", DataType::Int64),
            ],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };

        let distributed =
            crate::sql::planner::distributed::build::build_distributed_plan(&physical)
                .expect("build Graph-owned RF plan");
        assert_eq!(distributed.runtime_filter_graph().channel_count(), 1);
        let prepared = crate::coordinator::prepare::prepare_fragments(
            &distributed,
            &crate::connector::ConnectorRegistry::new(),
            None,
        )
        .expect("prepare Graph-owned RF projection");
        let encoded = encode_distributed_plan_with_context(
            &distributed,
            NativePlanEncodeContext {
                scan_bindings: Some(prepared.scan_bindings()),
                node_outputs: None,
                fragment_edge_outputs: None,
                write_contracts: None,
                runtime_filter_bindings: Some(&prepared),
            },
        )
        .expect("encode Graph-owned RF plan");
        let root = encoded.fragments[0].root.as_ref().expect("encoded root");
        assert!(!root.runtime_filter_binding_ids.is_empty());
        assert!(!root.children[0].runtime_filter_binding_ids.is_empty());
        let Some(plan::distributed_node::Payload::Physical(physical)) = root.payload.as_ref()
        else {
            panic!("expected physical HashJoin root");
        };
        let Some(plan::plan_node::Kind::HashJoin(_join)) = physical.kind.as_ref() else {
            panic!("expected HashJoin payload");
        };

        let bundle = crate::protocol::native::encode::bundle::encode_native_fragment_bundle(
            &distributed,
            &prepared,
        )
        .expect("encode prepared fragment binding tables");
        let encoded_binding_count = bundle
            .fragments_in_id_order()
            .map(|(_, fragment)| {
                fragment
                    .runtime_filter_bindings
                    .as_ref()
                    .expect("every fragment owns an explicit binding table")
                    .bindings
                    .len()
            })
            .sum::<usize>();
        assert_eq!(
            encoded_binding_count,
            distributed.runtime_filter_graph().binding_count(),
            "the encoder must materialize every prepared binding exactly once"
        );

        for (_, fragment) in bundle.fragments_in_id_order() {
            let bindings = &fragment
                .runtime_filter_bindings
                .as_ref()
                .expect("every fragment owns an explicit binding table")
                .bindings;
            assert!(
                bindings
                    .windows(2)
                    .all(|pair| pair[0].binding_id < pair[1].binding_id),
                "each fragment-local table must use deterministic binding-id order"
            );
        }
        let encoded_bindings = bundle
            .fragments_in_id_order()
            .flat_map(|(_, fragment)| {
                fragment
                    .runtime_filter_bindings
                    .as_ref()
                    .expect("every fragment owns an explicit binding table")
                    .bindings
                    .iter()
            })
            .collect::<Vec<_>>();
        let mut producer_count = 0;
        let mut consumer_count = 0;
        for binding in &encoded_bindings {
            let source = distributed
                .runtime_filter_graph()
                .binding(crate::runtime_filter::model::contract::BindingId::new(
                    binding.binding_id,
                ))
                .expect("encoded binding originates in the sealed graph");
            assert_eq!(binding.channel_id, source.channel_id.get());
            assert_eq!(binding.node_id, source.location.node_id.get());
            assert_eq!(
                binding.apply_point,
                encode_runtime_filter_apply_point(source.apply_point)
            );
            assert!(binding.expression.is_some());
            let Some(plan::runtime_filter_contract::Kind::Membership(contract)) = binding
                .contract
                .as_ref()
                .and_then(|contract| contract.kind.as_ref())
            else {
                panic!("broadcast join fixture must encode membership contracts");
            };
            assert!(!contract.canonical_schema.is_empty());
            assert_eq!(contract.schema_digest.len(), 32);
            assert!(matches!(
                binding
                    .reduction
                    .as_ref()
                    .and_then(|reduction| reduction.kind.as_ref()),
                Some(plan::runtime_filter_reduction_contract::Kind::SetUnion(
                    true
                ))
            ));
            match (binding.role.as_ref().expect("binding role"), &source.role) {
                (
                    plan::runtime_filter_binding::Role::Producer(role),
                    crate::runtime_filter::model::graph::RuntimeFilterBindingRole::Producer(
                        source_role,
                    ),
                ) => {
                    producer_count += 1;
                    assert_eq!(
                        role.contribution_kinds,
                        source_role
                            .contribution_kinds
                            .iter()
                            .copied()
                            .map(encode_runtime_filter_contribution_kind)
                            .collect::<Vec<_>>()
                    );
                    assert_eq!(
                        role.completion_requirement,
                        encode_runtime_filter_completion(source_role.completion_requirement)
                    );
                }
                (
                    plan::runtime_filter_binding::Role::Consumer(role),
                    crate::runtime_filter::model::graph::RuntimeFilterBindingRole::Consumer(
                        source_role,
                    ),
                ) => {
                    consumer_count += 1;
                    assert_eq!(
                        role.capabilities,
                        source_role
                            .capabilities
                            .iter()
                            .copied()
                            .map(encode_runtime_filter_capability)
                            .collect::<Vec<_>>()
                    );
                    assert_eq!(
                        role.activation,
                        Some(encode_runtime_filter_activation(source_role.activation))
                    );
                }
                _ => panic!("encoded binding role must match the sealed graph role"),
            }
        }
        assert_eq!((producer_count, consumer_count), (1, 1));

        fn collect_binding_ids(node: &plan::DistributedNode, ids: &mut Vec<u32>) {
            ids.extend_from_slice(&node.runtime_filter_binding_ids);
            for child in &node.children {
                collect_binding_ids(child, ids);
            }
        }
        let mut attached_ids = Vec::new();
        for (_, fragment) in bundle.fragments_in_id_order() {
            collect_binding_ids(
                fragment.root.as_ref().expect("fragment root"),
                &mut attached_ids,
            );
        }
        attached_ids.sort_unstable();
        assert_eq!(
            attached_ids,
            encoded_bindings
                .iter()
                .map(|binding| binding.binding_id)
                .collect::<Vec<_>>(),
            "sealed node binding attachments must round-trip with the table"
        );

        let second = crate::protocol::native::encode::bundle::encode_native_fragment_bundle(
            &distributed,
            &prepared,
        )
        .expect("deterministic second encoding");
        for (fragment_id, first_fragment) in bundle.fragments_in_id_order() {
            assert_eq!(
                first_fragment.runtime_filter_bindings,
                second
                    .get(fragment_id)
                    .expect("same prepared fragment set")
                    .runtime_filter_bindings
            );
        }

        let (&fragment_id, prepared_fragment) = prepared
            .fragment_ids()
            .iter()
            .find_map(|fragment_id| {
                prepared
                    .fragment(*fragment_id)
                    .filter(|fragment| !fragment.runtime_filter_bindings().is_empty())
                    .map(|fragment| (fragment_id, fragment))
            })
            .expect("fixture has a nonempty binding table");
        let mismatch = encode_runtime_filter_binding_table(
            fragment_id
                .checked_add(100)
                .expect("small fixture fragment id"),
            prepared_fragment.runtime_filter_bindings(),
        )
        .expect_err("enclosing fragment mismatch must fail");
        assert!(mismatch.contains("fragment mismatch"), "{mismatch}");
    }

    #[test]
    fn native_encoder_rejects_noncanonical_membership_digest() {
        let schema = crate::runtime_filter::port::artifact::ArtifactMembershipSchema::new(
            &DataType::Int64,
            crate::runtime_filter::model::contract::NullSemantics::NeverMatches,
        )
        .expect("canonical membership schema");
        let error =
            encode_runtime_filter_membership_contract(7, schema.canonical_bytes(), [0xAB; 32])
                .expect_err("digest drift must fail before encoding");
        assert_eq!(
            error,
            "native runtime filter binding id=7 membership schema digest does not match canonical bytes"
        );
    }

    fn canonical_order_contract_for_encoder_test() -> OrderContract {
        let keys = vec![
            OrderKeyContract {
                data_type: DataType::Int64,
                direction: SortDirection::Descending,
                null_order: NullOrder::First,
            },
            OrderKeyContract {
                data_type: DataType::Utf8,
                direction: SortDirection::Ascending,
                null_order: NullOrder::Last,
            },
        ];
        OrderContract {
            comparator_digest:
                crate::runtime_filter::port::ordered_bound::comparator_digest_for_test(
                    &keys,
                    crate::runtime_filter::port::ordered_bound::COMPARATOR_ALGORITHM_VERSION,
                ),
            keys,
            inclusive: true,
        }
    }

    #[test]
    fn native_encoder_preserves_ordered_and_topk_contracts() {
        let order = canonical_order_contract_for_encoder_test();
        let runtime_order = RuntimeOrderContract::try_from_plan(&order).expect("canonical order");
        let encoded_order = encode_runtime_filter_ordered_contract(
            19,
            runtime_order.keys(),
            runtime_order.plan_comparator_digest(),
            runtime_order.digest(),
        )
        .expect("encode canonical ordered contract");
        assert_eq!(encoded_order.keys.len(), 2);
        assert_eq!(
            encoded_order.keys[0].r#type,
            Some(encode_type(&DataType::Int64).expect("encode Int64"))
        );
        assert_eq!(
            encoded_order.keys[0].direction,
            i32::from(plan::RuntimeFilterSortDirection::Descending)
        );
        assert_eq!(
            encoded_order.keys[0].null_order,
            i32::from(plan::RuntimeFilterNullOrder::First)
        );
        assert_eq!(
            encoded_order.keys[1].r#type,
            Some(encode_type(&DataType::Utf8).expect("encode Utf8"))
        );
        assert_eq!(
            encoded_order.keys[1].direction,
            i32::from(plan::RuntimeFilterSortDirection::Ascending)
        );
        assert_eq!(
            encoded_order.keys[1].null_order,
            i32::from(plan::RuntimeFilterNullOrder::Last)
        );
        assert_eq!(
            encoded_order.comparator_digest,
            runtime_order.plan_comparator_digest().get()
        );
        assert_eq!(
            encoded_order.order_contract_digest,
            runtime_order.digest().bytes()
        );

        let requirement = TopKSummaryRequirement::try_new(13).expect("nonzero K");
        let runtime_topk = RuntimeTopKSummaryContract::try_from_plan(&order, requirement)
            .expect("canonical TopK contract");
        let encoded_topk = encode_runtime_filter_topk_reduction(
            19,
            runtime_order.keys(),
            runtime_order.plan_comparator_digest(),
            runtime_topk.k(),
            runtime_topk.digest(),
        )
        .expect("encode canonical TopK reduction");
        assert_eq!(encoded_topk.k, 13);
        assert_eq!(encoded_topk.contract_digest, runtime_topk.digest().bytes());
    }

    #[test]
    fn native_encoder_rejects_corrupt_ordered_and_topk_digests() {
        let order = canonical_order_contract_for_encoder_test();
        let runtime_order = RuntimeOrderContract::try_from_plan(&order).expect("canonical order");
        let ordered_error = encode_runtime_filter_ordered_contract(
            23,
            runtime_order.keys(),
            runtime_order.plan_comparator_digest(),
            crate::runtime_filter::port::ordered_bound::OrderContractDigest::from_bytes_for_codec(
                [0xA5; 32],
            ),
        )
        .expect_err("corrupt order digest must fail");
        assert_eq!(
            ordered_error,
            "native runtime filter binding id=23 order contract digest does not match typed keys"
        );

        let canonical_topk = RuntimeTopKSummaryContract::try_from_plan(
            &order,
            TopKSummaryRequirement::try_new(13).expect("nonzero K"),
        )
        .expect("canonical TopK contract");
        let topk_error = encode_runtime_filter_topk_reduction(
            23,
            runtime_order.keys(),
            runtime_order.plan_comparator_digest(),
            TopKSummaryRequirement::try_new(14)
                .expect("different nonzero K")
                .k(),
            canonical_topk.digest(),
        )
        .expect_err("digest for a different K must fail");
        assert_eq!(
            topk_error,
            "native runtime filter binding id=23 TopK digest does not match typed order keys and K"
        );
    }

    #[test]
    fn native_encoder_emits_explicit_empty_fragment_table() {
        let distributed = two_fragment_stream_plan_for_test();
        assert!(distributed.runtime_filter_graph().is_empty());
        let prepared = crate::coordinator::prepare::prepare_fragments(
            &distributed,
            &crate::connector::ConnectorRegistry::new(),
            None,
        )
        .expect("prepare no-runtime-filter plan");
        let bundle = crate::protocol::native::encode::bundle::encode_native_fragment_bundle(
            &distributed,
            &prepared,
        )
        .expect("encode explicit empty tables");
        for (fragment_id, fragment) in bundle.fragments_in_id_order() {
            let table = fragment
                .runtime_filter_bindings
                .as_ref()
                .expect("empty table is explicit, never absent");
            assert_eq!(table.fragment_id, fragment_id);
            assert!(table.bindings.is_empty());
        }
    }

    #[test]
    fn encoded_join_output_maps_reconciled_children_not_stale_payload() {
        // The join payload lists a stale id (999) that neither child produces --
        // the divergence a marker/anti join or a pruned probe scan creates. The
        // sealed node-output contract reconciles the join against its children,
        // and the encoder maps that contract 1:1, so the encoded join emits
        // [1, 2], not the stale [1, 2, 999]. Were the reconciliation missing, the
        // encoder (now a pure map of the contract) would emit 999 and the BE sink
        // would fail with "output_columns slot id 999 not found in chunk schema".
        let left = output_column(1, "l_k", DataType::Int64);
        let right = output_column(2, "r_k", DataType::Int64);
        let plan = crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
            fragments: vec![PlanFragment {
                fragment_id: 0,
                root: DistributedNode {
                    node_id: 1,
                    fragment_id: 0,
                    tuple_ids: vec![1],
                    nullable_tuple_ids: Vec::new(),
                    limit: -1,
            runtime_filter_binding_ids: Vec::new(),
                    children: vec![
                        DistributedNode {
                            node_id: 2,
                            fragment_id: 0,
                            tuple_ids: vec![2],
                            nullable_tuple_ids: Vec::new(),
                            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
                            children: Vec::new(),
                            stats: stats(),
                            payload: DistributedNodeKind::Values(
                                crate::sql::planner::payload::PlanValuesNode {
                                    rows: Vec::new(),
                                    columns: vec![left.clone()],
                                },
                            ),
                        },
                        DistributedNode {
                            node_id: 3,
                            fragment_id: 0,
                            tuple_ids: vec![3],
                            nullable_tuple_ids: Vec::new(),
                            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
                            children: Vec::new(),
                            stats: stats(),
                            payload: DistributedNodeKind::Values(
                                crate::sql::planner::payload::PlanValuesNode {
                                    rows: Vec::new(),
                                    columns: vec![right.clone()],
                                },
                            ),
                        },
                    ],
                    stats: stats(),
                    payload: DistributedNodeKind::HashJoin(Box::new(
                        crate::sql::planner::physical::PhysicalHashJoinNode {
                            join_type: JoinKind::Inner,
                            eq_conditions: Vec::new(),
                            other_condition: None,
                            distribution: JoinDistribution::Unknown,
                            execution_mode: None,
                            build_runtime_filters: Vec::new(),
                            output_columns: vec![
                                left.clone(),
                                right.clone(),
                                output_column(999, "stale", DataType::Int64),
                            ],
                        },
                    )),
                },
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::Result,
                output_exprs: None,
                output_columns: vec![left.clone(), right.clone()],
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            root_fragment_id: 0,
            runtime_filter_graph: RuntimeFilterGraph::default(),
            edges: Vec::new(),
        };

        let encoded =
            encode_distributed_plan(&plan, empty_scan_bindings()).expect("encode native plan");
        let root = encoded.fragments[0].root.as_ref().expect("root");
        let Some(plan::distributed_node::Payload::Physical(physical)) = root.payload.as_ref()
        else {
            panic!("expected physical join root");
        };
        assert_eq!(
            physical
                .output_columns
                .iter()
                .map(|column| (column.column_id, column.name.as_str()))
                .collect::<Vec<_>>(),
            vec![(1, "l_k"), (2, "r_k")],
            "the encoder maps the reconciled contract, dropping the stale id 999"
        );
    }

    #[test]
    fn iceberg_delta_table_encoder_consumes_prepared_binding_payload() {
        use crate::protocol::native::encode::plan;

        let plan = iceberg_delta_distributed_plan_for_test();
        let source_column = crate::catalog::schema::ColumnDef {
            name: "physical_order_id".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        };
        let mut plan =
            crate::sql::planner::distributed::test_support::draft_builder_from_plan(&plan);
        root_scan_for_test(&mut plan)
            .table
            .columns
            .push(column_def_for_test(
                "stale_unprojected",
                DataType::Utf8,
                true,
            ));
        let plan = plan.seal().expect("seal prepared delta fixture");
        let hidden_equality_column = column_def_for_test("tenant_id", DataType::Int64, false);
        let mut bindings = ScanExecutionBindings::default();
        bindings
            .insert_binding(ResolvedScanBinding {
                node_id: 10,
                execution: ResolvedScanExecution::IcebergDelta(ResolvedIcebergDeltaScan {
                    runtime_plan: IcebergDeltaScanRuntimePlan {
                        table_location: "s3://prepared/orders".to_string(),
                        data_columns: Vec::new(),
                        cloud_properties: BTreeMap::from([(
                            "endpoint".to_string(),
                            "http://prepared-minio".to_string(),
                        )]),
                        change_files: Vec::new(),
                        delete_side: None,
                    },
                }),
                physical_columns: vec![ResolvedScanColumn {
                    planner: output_column(1, "bound_order_id", DataType::Int64),
                    source: source_column.clone(),
                    kind: ResolvedScanColumnKind::PhysicalTableColumn,
                }],
                required_reads: vec![
                    ResolvedReadColumn {
                        planner_column_id: Some(ColumnId::new_for_test(1)),
                        source: source_column,
                        reason: ResolvedReadReason::PlannerRequiredOrOutput,
                    },
                    ResolvedReadColumn {
                        planner_column_id: None,
                        source: hidden_equality_column,
                        reason: ResolvedReadReason::EqualityDeleteKey,
                    },
                ],
            })
            .expect("insert prepared delta binding");

        let encoded = plan::encode_distributed_plan_with_context(
            &plan,
            plan::NativePlanEncodeContext {
                scan_bindings: Some(&bindings),
                node_outputs: None,
                fragment_edge_outputs: None,
                write_contracts: None,
                runtime_filter_bindings: Some(prepared_runtime_filter_bindings(&plan)),
            },
        )
        .expect("encode prepared delta binding");

        let root = encoded.fragments[0].root.as_ref().expect("encoded root");
        let Some(crate::proto::plan::distributed_node::Payload::Physical(physical)) =
            root.payload.as_ref()
        else {
            panic!("expected physical root");
        };
        let Some(crate::proto::plan::plan_node::Kind::Scan(scan)) = physical.kind.as_ref() else {
            panic!("expected scan root");
        };
        assert_eq!(scan.columns[0].name, "physical_order_id");
        assert_eq!(
            scan.required_columns,
            vec!["physical_order_id", "tenant_id"]
        );
        let table = scan.table.as_ref().expect("bound table");
        assert_eq!(
            table
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["physical_order_id", "tenant_id"]
        );
        assert!(table.iceberg_row_lineage_metadata_columns.is_empty());
        let Some(crate::proto::plan::scan_source::Kind::IcebergDeltaTable(delta)) = table
            .source
            .as_ref()
            .and_then(|source| source.kind.as_ref())
        else {
            panic!("expected encoded delta source");
        };
        let runtime = delta.delta_plan.as_ref().expect("prepared runtime payload");
        assert_eq!(runtime.table_location, "s3://prepared/orders");
        assert_eq!(
            runtime.cloud_properties.get("endpoint").map(String::as_str),
            Some("http://prepared-minio")
        );
    }

    #[test]
    fn ordinary_iceberg_binding_preserves_existing_encoding() {
        let plan = iceberg_delta_distributed_plan_for_test();
        let mut plan =
            crate::sql::planner::distributed::test_support::draft_builder_from_plan(&plan);
        let scan = root_scan_for_test(&mut plan);
        scan.table.columns.push(column_def_for_test(
            "unprojected_payload",
            DataType::Utf8,
            true,
        ));
        let table = iceberg_table_info_for_test();
        scan.table.source = table_model::ScanSource::IcebergDataFiles {
            table: table.clone(),
            files: Vec::new(),
            cloud_properties: BTreeMap::from([("region".to_string(), "test".to_string())]),
            binding: iceberg_scan_model::IcebergDataFileBinding::CurrentSnapshot,
        };
        scan.required_columns = Some(vec!["order_id".to_string()]);
        let plan = plan.seal().expect("seal ordinary Iceberg fixture");

        let without_binding = encode_distributed_plan(&plan, empty_scan_bindings())
            .expect("encode ordinary Iceberg scan");
        let mut bindings = ScanExecutionBindings::default();
        bindings
            .insert_binding(file_binding_for_test(
                10,
                table,
                iceberg_scan_model::IcebergDataFileBinding::CurrentSnapshot,
                vec![bound_column_for_test(
                    1,
                    "order_id",
                    "order_id",
                    ResolvedScanColumnKind::PhysicalTableColumn,
                )],
                vec![bound_read_for_test(Some(1), "order_id")],
            ))
            .expect("insert ordinary Iceberg binding");
        let with_binding = encode_distributed_plan_with_context(
            &plan,
            NativePlanEncodeContext {
                scan_bindings: Some(&bindings),
                node_outputs: None,
                fragment_edge_outputs: None,
                write_contracts: None,
                runtime_filter_bindings: Some(prepared_runtime_filter_bindings(&plan)),
            },
        )
        .expect("encode ordinary Iceberg binding");

        assert_eq!(with_binding, without_binding);
    }

    #[test]
    fn refresh_file_bindings_drive_source_projection_metadata_and_hidden_reads() {
        let refresh_sources = [
            table_model::ScanSource::IcebergVersionTable {
                table: iceberg_table_info_for_test(),
                snapshot_id: 1,
            },
            table_model::ScanSource::IcebergMvTargetLocator(
                table_model::IcebergMvTargetLocatorScan {
                    catalog: "ice".to_string(),
                    database: "db".to_string(),
                    table: "orders".to_string(),
                    target_table_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
                    target_snapshot_id: Some(1),
                    apply_key_column: "bound_order_id".to_string(),
                    branch_id_column: None,
                },
            ),
            table_model::ScanSource::IcebergMvTargetState(table_model::IcebergMvTargetStateScan {
                catalog: "ice".to_string(),
                database: "db".to_string(),
                table: "orders".to_string(),
                target_table_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
                target_snapshot_id: Some(1),
                aggregate_state_layout_version: 1,
                columns: Vec::new(),
                group_key_names: vec!["bound_order_id".to_string()],
                aggregate_state_names: Vec::new(),
                physical_column_names: vec!["bound_order_id".to_string()],
                row_id_column_name: "bound_order_id".to_string(),
                row_filter: table_model::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                    row_id_column_name: "bound_order_id".to_string(),
                    branch_scope: None,
                },
                partition_constraint:
                    table_model::IcebergMvTargetStatePartitionConstraint::Unpartitioned,
            }),
        ];

        for source in refresh_sources {
            let plan = iceberg_delta_distributed_plan_for_test();
            let mut plan =
                crate::sql::planner::distributed::test_support::draft_builder_from_plan(&plan);
            let scan = root_scan_for_test(&mut plan);
            scan.table.source = source;
            scan.table.columns = vec![
                column_def_for_test("stale", DataType::Utf8, true),
                column_def_for_test("stale_unprojected", DataType::Utf8, true),
            ];
            scan.columns = vec![
                output_column(1, "stale", DataType::Utf8),
                output_column(2, "stale_meta", DataType::Int64),
            ];
            let plan = plan.seal().expect("seal refresh-source fixture");

            let mut resolved_table = iceberg_table_info_for_test();
            resolved_table.current_snapshot_id = Some(1);
            resolved_table.location = "s3://resolved/orders".to_string();
            resolved_table.schema.fields[0].name = "physical_order_id".to_string();
            resolved_table
                .schema
                .fields
                .push(iceberg_scan_model::IcebergSchemaFieldDef {
                    field_id: 2,
                    name: "tenant_id".to_string(),
                    initial_default: None,
                    write_default: None,
                    initial_default_json: None,
                    write_default_json: None,
                    children: Vec::new(),
                });
            let mut bindings = ScanExecutionBindings::default();
            bindings
                .insert_binding(file_binding_for_test(
                    10,
                    resolved_table,
                    iceberg_scan_model::IcebergDataFileBinding::ExplicitFiles,
                    vec![
                        ResolvedScanColumn {
                            planner: output_column(1, "bound_order_id", DataType::Int64),
                            source: column_def_for_test(
                                "physical_order_id",
                                DataType::Int64,
                                false,
                            ),
                            kind: ResolvedScanColumnKind::PhysicalTableColumn,
                        },
                        ResolvedScanColumn {
                            planner: output_column(2, "bound_file", DataType::Utf8),
                            source: column_def_for_test("_file", DataType::Utf8, false),
                            kind: ResolvedScanColumnKind::IcebergMetadataColumn,
                        },
                    ],
                    vec![
                        bound_read_for_test(Some(1), "physical_order_id"),
                        ResolvedReadColumn {
                            planner_column_id: None,
                            source: column_def_for_test("tenant_id", DataType::Int64, false),
                            reason: ResolvedReadReason::EqualityDeleteKey,
                        },
                    ],
                ))
                .expect("insert refresh file binding");

            let encoded = encode_distributed_plan_with_context(
                &plan,
                NativePlanEncodeContext {
                    scan_bindings: Some(&bindings),
                    node_outputs: None,
                    fragment_edge_outputs: None,
                    write_contracts: None,
                    runtime_filter_bindings: Some(prepared_runtime_filter_bindings(&plan)),
                },
            )
            .expect("encode refresh binding");
            let scan = encoded_root_scan_for_test(&encoded);
            assert_eq!(
                scan.columns
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect::<Vec<_>>(),
                vec!["physical_order_id", "_file"]
            );
            assert_eq!(
                scan.required_columns,
                vec!["physical_order_id", "tenant_id"]
            );
            let table = scan.table.as_ref().expect("bound table");
            assert_eq!(
                table
                    .columns
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect::<Vec<_>>(),
                vec!["physical_order_id", "tenant_id"],
                "resolver-required sources must encode only binding-owned physical columns and hidden reads"
            );
            assert_eq!(
                table
                    .iceberg_row_lineage_metadata_columns
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect::<Vec<_>>(),
                vec!["_file"]
            );
            let Some(crate::proto::plan::scan_source::Kind::IcebergDataFiles(files)) = table
                .source
                .as_ref()
                .and_then(|source| source.kind.as_ref())
            else {
                panic!("refresh source must encode as resolved IcebergDataFiles");
            };
            assert_eq!(
                files.table.as_ref().expect("resolved table").location,
                "s3://resolved/orders"
            );
            assert_eq!(
                files.binding,
                crate::proto::plan::IcebergDataFileBinding::ExplicitFiles as i32
            );
            let (read_columns, variants) = crate::lower::novarocks::scan_read_binding_for_test(
                scan,
                files.table.as_ref().expect("resolved table"),
                &scan.columns,
            )
            .expect("lower bound refresh read plan");
            assert!(
                read_columns.iter().any(|column| column == "tenant_id"),
                "native lowering must resolve hidden equality key from TableDef"
            );
            assert!(variants.is_empty());
        }
    }

    #[test]
    fn required_bindings_reject_missing_node_and_execution_variant_mismatch() {
        let plan = iceberg_delta_distributed_plan_for_test();
        let missing = encode_distributed_plan_with_context(
            &plan,
            NativePlanEncodeContext {
                scan_bindings: Some(&ScanExecutionBindings::default()),
                node_outputs: None,
                fragment_edge_outputs: None,
                write_contracts: None,
                runtime_filter_bindings: Some(prepared_runtime_filter_bindings(&plan)),
            },
        )
        .expect_err("delta source without prepared binding must fail");
        assert!(missing.contains("node_id=10"), "{missing}");
        assert!(missing.contains("IcebergDeltaTable"), "{missing}");
        assert!(missing.contains("from_snapshot_id=1"), "{missing}");
        assert!(missing.contains("to_snapshot_id=2"), "{missing}");

        let mut wrong_node = ScanExecutionBindings::default();
        wrong_node
            .insert_binding(delta_binding_for_test(11))
            .expect("insert binding for wrong node");
        let err = encode_distributed_plan_with_context(
            &plan,
            NativePlanEncodeContext {
                scan_bindings: Some(&wrong_node),
                node_outputs: None,
                fragment_edge_outputs: None,
                write_contracts: None,
                runtime_filter_bindings: Some(prepared_runtime_filter_bindings(&plan)),
            },
        )
        .expect_err("binding at another node id must not be reused");
        assert!(err.contains("node_id=10"), "{err}");

        let mut wrong_execution = ScanExecutionBindings::default();
        wrong_execution
            .insert_binding(file_binding_for_test(
                10,
                iceberg_table_info_for_test(),
                iceberg_scan_model::IcebergDataFileBinding::ExplicitFiles,
                vec![bound_column_for_test(
                    1,
                    "order_id",
                    "order_id",
                    ResolvedScanColumnKind::PhysicalTableColumn,
                )],
                vec![bound_read_for_test(Some(1), "order_id")],
            ))
            .expect("insert wrong execution variant");
        let err = encode_distributed_plan_with_context(
            &plan,
            NativePlanEncodeContext {
                scan_bindings: Some(&wrong_execution),
                node_outputs: None,
                fragment_edge_outputs: None,
                write_contracts: None,
                runtime_filter_bindings: Some(prepared_runtime_filter_bindings(&plan)),
            },
        )
        .expect_err("delta source with file binding must fail");
        assert!(err.contains("execution variant mismatch"), "{err}");
        assert!(err.contains("IcebergFiles"), "{err}");
    }

    #[test]
    fn binding_encoder_preserves_variant_synthetic_output_and_required_name() {
        let plan = iceberg_delta_distributed_plan_for_test();
        let mut plan =
            crate::sql::planner::distributed::test_support::draft_builder_from_plan(&plan);
        let scan = root_scan_for_test(&mut plan);
        let mut table = iceberg_table_info_for_test();
        table.schema.fields[0].name = "v".to_string();
        scan.table.columns = vec![column_def_for_test("v", DataType::LargeBinary, false)];
        scan.table.source = table_model::ScanSource::IcebergDataFiles {
            table: table.clone(),
            files: Vec::new(),
            cloud_properties: BTreeMap::new(),
            binding: iceberg_scan_model::IcebergDataFileBinding::ExplicitFiles,
        };
        scan.columns = vec![
            output_column(1, "v", DataType::LargeBinary),
            OutputColumn {
                column_id: ColumnId::new_for_test(2),
                name: "__nr_var_v_0".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: true,
            },
        ];
        scan.required_columns = Some(vec!["__nr_var_v_0".to_string()]);
        scan.variant_columns = vec![crate::sql::common::ScanVariantColumn {
            source_column_id: ColumnId::new_for_test(1),
            source_column: "v".to_string(),
            synthetic_column_id: ColumnId::new_for_test(2),
            synthetic_column: "__nr_var_v_0".to_string(),
            canonical_path: "$.a.b".to_string(),
            requested_type: DataType::Int64,
            strict: true,
        }];
        let plan = plan.seal().expect("seal variant fixture");
        let mut bindings = ScanExecutionBindings::default();
        bindings
            .insert_binding(file_binding_for_test(
                10,
                table,
                iceberg_scan_model::IcebergDataFileBinding::ExplicitFiles,
                vec![ResolvedScanColumn {
                    planner: output_column(1, "v", DataType::LargeBinary),
                    source: column_def_for_test("v", DataType::LargeBinary, false),
                    kind: ResolvedScanColumnKind::PhysicalTableColumn,
                }],
                Vec::new(),
            ))
            .expect("insert variant binding");

        let encoded = encode_distributed_plan_with_context(
            &plan,
            NativePlanEncodeContext {
                scan_bindings: Some(&bindings),
                node_outputs: None,
                fragment_edge_outputs: None,
                write_contracts: None,
                runtime_filter_bindings: Some(prepared_runtime_filter_bindings(&plan)),
            },
        )
        .expect("encode bound VARIANT scan");
        let scan = encoded_root_scan_for_test(&encoded);
        assert_eq!(
            scan.columns
                .iter()
                .map(|column| (column.column_id, column.name.as_str()))
                .collect::<Vec<_>>(),
            vec![(1, "v"), (2, "__nr_var_v_0")]
        );
        assert_eq!(scan.required_columns, vec!["__nr_var_v_0"]);
        assert_eq!(scan.variant_columns[0].synthetic_column_id, 2);
        let table = scan.table.as_ref().expect("bound table");
        let Some(crate::proto::plan::scan_source::Kind::IcebergDataFiles(files)) = table
            .source
            .as_ref()
            .and_then(|source| source.kind.as_ref())
        else {
            panic!("variant binding must encode as IcebergDataFiles");
        };
        let (read_columns, variants) = crate::lower::novarocks::scan_read_binding_for_test(
            scan,
            files.table.as_ref().expect("resolved table"),
            &scan.columns[1..],
        )
        .expect("lower encoded bound VARIANT scan");
        assert_eq!(read_columns, vec!["v"]);
        assert_eq!(variants, vec![(1, 2)]);
    }

    fn root_scan_for_test(
        plan: &mut crate::sql::planner::distributed::test_support::DistributedPlanDraftBuilder,
    ) -> &mut crate::sql::planner::payload::PlanScanNode {
        let DistributedNodeKind::Scan(scan) = &mut plan.fragments_mut()[0].root.payload else {
            panic!("expected root scan");
        };
        scan
    }

    fn encoded_root_scan_for_test(plan: &plan::DistributedPlan) -> &plan::ScanNode {
        let root = plan.fragments[0].root.as_ref().expect("encoded root");
        let Some(plan::distributed_node::Payload::Physical(physical)) = root.payload.as_ref()
        else {
            panic!("expected physical root");
        };
        let Some(plan::plan_node::Kind::Scan(scan)) = physical.kind.as_ref() else {
            panic!("expected scan root");
        };
        scan
    }

    fn file_binding_for_test(
        node_id: i32,
        table: iceberg_scan_model::IcebergTableInfo,
        file_binding: iceberg_scan_model::IcebergDataFileBinding,
        physical_columns: Vec<ResolvedScanColumn>,
        required_reads: Vec<ResolvedReadColumn>,
    ) -> ResolvedScanBinding {
        ResolvedScanBinding {
            node_id,
            execution: ResolvedScanExecution::IcebergFiles(ResolvedIcebergFileScan {
                table,
                files: Vec::new(),
                cloud_properties: BTreeMap::from([("region".to_string(), "test".to_string())]),
                binding: file_binding,
            }),
            physical_columns,
            required_reads,
        }
    }

    fn delta_binding_for_test(node_id: i32) -> ResolvedScanBinding {
        ResolvedScanBinding {
            node_id,
            execution: ResolvedScanExecution::IcebergDelta(ResolvedIcebergDeltaScan {
                runtime_plan: IcebergDeltaScanRuntimePlan {
                    table_location: "s3://prepared/orders".to_string(),
                    data_columns: Vec::new(),
                    cloud_properties: BTreeMap::new(),
                    change_files: Vec::new(),
                    delete_side: None,
                },
            }),
            physical_columns: vec![bound_column_for_test(
                1,
                "order_id",
                "order_id",
                ResolvedScanColumnKind::PhysicalTableColumn,
            )],
            required_reads: vec![bound_read_for_test(Some(1), "order_id")],
        }
    }

    fn bound_column_for_test(
        id: u32,
        planner_name: &str,
        source_name: &str,
        kind: ResolvedScanColumnKind,
    ) -> ResolvedScanColumn {
        ResolvedScanColumn {
            planner: output_column(id, planner_name, DataType::Int64),
            source: column_def_for_test(source_name, DataType::Int64, false),
            kind,
        }
    }

    fn bound_read_for_test(planner_id: Option<u32>, source_name: &str) -> ResolvedReadColumn {
        ResolvedReadColumn {
            planner_column_id: planner_id.map(ColumnId::new_for_test),
            source: column_def_for_test(source_name, DataType::Int64, false),
            reason: if planner_id.is_some() {
                ResolvedReadReason::PlannerRequiredOrOutput
            } else {
                ResolvedReadReason::EqualityDeleteKey
            },
        }
    }

    fn column_def_for_test(
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> crate::catalog::schema::ColumnDef {
        crate::catalog::schema::ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
            write_default: None,
            logical_type: None,
        }
    }

    fn iceberg_delta_distributed_plan_for_test() -> DistributedPlan {
        let output_columns = vec![output_column(1, "order_id", DataType::Int64)];
        crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
            fragments: vec![PlanFragment {
                fragment_id: 0,
                root: DistributedNode {
                    node_id: 10,
                    fragment_id: 0,
                    tuple_ids: vec![10],
                    nullable_tuple_ids: Vec::new(),
                    limit: -1,
            runtime_filter_binding_ids: Vec::new(),
                    children: Vec::new(),
                    stats: stats(),
                    payload: DistributedNodeKind::Scan(
                        crate::sql::planner::payload::PlanScanNode {
                            database: "db".to_string(),
                            table: iceberg_delta_table_for_test(),
                            alias: None,
                            columns: output_columns.clone(),
                            predicates: Vec::new(),
                            required_columns: None,
                            variant_columns: Vec::new(),
                            mv_rewritten_from: None,
                        },
                    ),
                },
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::Result,
                output_exprs: None,
                output_columns,
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            root_fragment_id: 0,
            runtime_filter_graph: RuntimeFilterGraph::default(),
            edges: Vec::new(),
        }
    }

    fn iceberg_delta_table_for_test() -> table_model::TableDef {
        table_model::TableDef {
            name: "orders".to_string(),
            columns: vec![crate::catalog::schema::ColumnDef {
                name: "order_id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: table_model::ScanSource::IcebergDeltaTable {
                table: iceberg_table_info_for_test(),
                from_snapshot_id: 1,
                to_snapshot_id: 2,
            },
        }
    }

    fn iceberg_table_info_for_test() -> iceberg_scan_model::IcebergTableInfo {
        iceberg_scan_model::IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "orders".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            current_snapshot_id: Some(2),
            schema_id: 1,
            location: "file:///warehouse/orders".to_string(),
            schema: iceberg_scan_model::IcebergSchemaDef {
                fields: vec![iceberg_scan_model::IcebergSchemaFieldDef {
                    field_id: 1,
                    name: "order_id".to_string(),
                    initial_default: None,
                    write_default: None,
                    initial_default_json: None,
                    write_default_json: None,
                    children: Vec::new(),
                }],
            },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    #[test]
    fn stream_sink_derives_generate_series_source_schema() {
        let plan = two_fragment_generate_series_stream_plan_for_test();

        let encoded =
            encode_distributed_plan(&plan, empty_scan_bindings()).expect("encode native plan");

        let source = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 1)
            .expect("source fragment");
        let Some(plan::data_sink::Kind::DataStream(sink)) =
            source.sink.as_ref().and_then(|sink| sink.kind.as_ref())
        else {
            panic!("expected DataStream sink");
        };
        assert_eq!(sink.output_columns, vec![7]);

        let target = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 0)
            .expect("target fragment");
        let receiver = target.root.as_ref().expect("target root");
        let Some(plan::distributed_node::Payload::Exchange(exchange)) = receiver.payload.as_ref()
        else {
            panic!("expected Exchange receiver");
        };
        assert_eq!(
            exchange
                .output_columns
                .iter()
                .map(|column| (column.column_id, column.name.as_str(), column.nullable))
                .collect::<Vec<_>>(),
            vec![(7, "generate_series", false)]
        );
    }

    fn two_fragment_generate_series_stream_plan_for_test() -> DistributedPlan {
        let output_columns = vec![output_column(7, "generate_series", DataType::Int64)];
        crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
            fragments: vec![
                PlanFragment {
                    fragment_id: 1,
                    root: DistributedNode {
                        node_id: 10,
                        fragment_id: 1,
                        tuple_ids: vec![10],
                        nullable_tuple_ids: Vec::new(),
                        limit: -1,
            runtime_filter_binding_ids: Vec::new(),
                        children: Vec::new(),
                        stats: stats(),
                        payload: DistributedNodeKind::GenerateSeries(
                            crate::sql::planner::payload::PlanGenerateSeriesNode {
                                start: 1,
                                end: 3,
                                step: 1,
                                column_name: "generate_series".to_string(),
                                alias: None,
                                output_column_id: ColumnId::new_for_test(7),
                            },
                        ),
                    },
                    data_partition: DataPartition::unpartitioned(),
                    output_partition: DataPartition::unpartitioned(),
                    sink: DataSink::Noop,
                    output_exprs: None,
                    output_columns: Vec::new(),
                    cte_id: None,
                    cte_exchange_nodes: Vec::new(),
                },
                PlanFragment {
                    fragment_id: 0,
                    root: DistributedNode {
                        node_id: 20,
                        fragment_id: 0,
                        tuple_ids: vec![20],
                        nullable_tuple_ids: Vec::new(),
                        limit: -1,
            runtime_filter_binding_ids: Vec::new(),
                        children: Vec::new(),
                        stats: stats(),
                        payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                            partition: DataPartition::unpartitioned(),
                            source_fragment_id: 1,
                            output_columns,
                            output_qualifier: None,
                            flavor: ExchangeFlavor::Distribution,
                        }),
                    },
                    data_partition: DataPartition::unpartitioned(),
                    output_partition: DataPartition::unpartitioned(),
                    sink: DataSink::Result,
                    output_exprs: None,
                    output_columns: Vec::new(),
                    cte_id: None,
                    cte_exchange_nodes: Vec::new(),
                },
            ],
            root_fragment_id: 0,
            runtime_filter_graph: RuntimeFilterGraph::default(),
            edges: vec![FragmentEdge {
                source_fragment_id: 1,
                target_fragment_id: 0,
                target_exchange_node_id: 20,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::Stream,
                output_slot_ids: vec![7],
            }],
        }
    }

    fn output_column(id: u32, name: &str, data_type: DataType) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type,
            nullable: false,
            is_internal: false,
        }
    }

    fn stats() -> PhysicalPlanStats {
        PhysicalPlanStats {
            output_row_count: 1.0,
            row_count_confidence: PlannerConfidence::Exact,
            column_statistics: Default::default(),
            cost_estimate: None,
            broadcast_decision: None,
        }
    }
}
