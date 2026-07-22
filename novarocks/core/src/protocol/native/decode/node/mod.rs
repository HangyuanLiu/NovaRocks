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

//! Native protocol plan-node decoding.

mod aggregate;
mod assert;
mod change_event_expand;
mod common;
mod exchange;
mod filter;
mod generate_series;
mod hash_join;
mod limit;
mod nestloop_join;
mod project;
mod redistribute;
mod repeat;
mod set_op;
mod sort;
mod table_function;
mod topn;
mod values;
mod window;

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use self::common::*;

use super::layout::Layout;
use super::runtime_filter::{
    DecodedBindingRole, DecodedConsumerBindingTarget, DecodedRuntimeFilterBinding,
    DecodedRuntimeFilterContract, DecodedRuntimeFilterReduction, NativeRuntimeFilterDecodeLedger,
};
use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::expr::ExprArena;
use crate::exec::fragment::program::{FragmentNodeId, ScanAssignmentKind};
use crate::exec::node::join::{JoinRuntimeFilterExecution, NativeJoinRuntimeFilterProducerSpec};
use crate::exec::node::limit::LimitNode;
use crate::exec::node::runtime_filter::{
    NativeRuntimeFilterConsumerNode, NativeRuntimeFilterConsumerSpec, NativeRuntimeFilterContract,
    NativeRuntimeFilterReduction,
};
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::proto::{novarocks, plan};
use crate::protocol::common::error::FieldPath;
use crate::runtime::exchange::ExchangeKey;
use crate::runtime::fragment::instance::{
    ExchangeInputAssignment, ExchangeInputAssignments, FragmentInstanceId, ScanAssignments,
};
use crate::runtime::query_context::QueryId;
use crate::runtime::query_options::QueryOptions;
#[cfg(test)]
use crate::runtime::scan_range::ScanRange;
use crate::runtime::scan_range::ScanRangeParams;

#[derive(Clone, Debug)]
pub(crate) struct DecodedNode {
    pub node: ExecNode,
    pub layout: Layout,
    pub output_schema: ChunkSchemaRef,
}

#[derive(Clone, Debug)]
pub(crate) struct NativePlanDecodeContext {
    exchange_inputs: ExchangeInputAssignments,
    scan_assignments: ScanAssignments,
    query_options: Option<QueryOptions>,
    connectors: Option<Arc<crate::connector::ConnectorRegistry>>,
    query_id: Option<QueryId>,
    fragment_instance_id: FragmentInstanceId,
}

impl Default for NativePlanDecodeContext {
    fn default() -> Self {
        Self {
            exchange_inputs: ExchangeInputAssignments::default(),
            scan_assignments: ScanAssignments::default(),
            query_options: None,
            connectors: None,
            query_id: None,
            fragment_instance_id: FragmentInstanceId::new(crate::common::types::UniqueId {
                hi: 0,
                lo: 0,
            }),
        }
    }
}

impl NativePlanDecodeContext {
    pub(crate) fn from_parts(
        exchange_inputs: ExchangeInputAssignments,
        scan_assignments: ScanAssignments,
        query_options: QueryOptions,
        connectors: Arc<crate::connector::ConnectorRegistry>,
        query_id: QueryId,
        fragment_instance_id: FragmentInstanceId,
    ) -> Self {
        Self {
            exchange_inputs,
            scan_assignments,
            query_options: Some(query_options),
            connectors: Some(connectors),
            query_id: Some(query_id),
            fragment_instance_id,
        }
    }

    pub(crate) fn from_native(
        root: &plan::DistributedNode,
        instance_params: &novarocks::InstanceParams,
        query_options: Option<QueryOptions>,
        connectors: Arc<crate::connector::ConnectorRegistry>,
        query_id: QueryId,
        fragment_instance_id: FragmentInstanceId,
    ) -> Result<Self, super::NativeFragmentDecodeError> {
        let scan_kinds =
            collect_scan_assignment_kinds(root, FieldPath::root("plan_fragment").field("root"))?;
        let scan_path = FieldPath::root("instance_params").field("per_node_scan_ranges");
        let mut scan_assignments = BTreeMap::new();
        let mut scan_keys = instance_params
            .per_node_scan_ranges
            .keys()
            .copied()
            .collect::<Vec<_>>();
        scan_keys.sort_unstable();
        for raw_node_id in scan_keys {
            let wire_ranges = &instance_params.per_node_scan_ranges[&raw_node_id];
            let node_path = scan_path.clone().map_key(raw_node_id.to_string());
            let node_id = FragmentNodeId::new(raw_node_id);
            let kind = scan_kinds.get(&node_id).copied().ok_or_else(|| {
                super::NativeFragmentDecodeError::inconsistent(
                    node_path.clone(),
                    format!(
                        "native InstanceParams assigns scan ranges to unknown scan node_id={raw_node_id}"
                    ),
                )
            })?;
            let mut ranges = Vec::with_capacity(wire_ranges.ranges.len());
            for (index, wire_range) in wire_ranges.ranges.iter().enumerate() {
                ranges.push(super::instance::decode_scan_range_params_at(
                    wire_range,
                    node_path.clone().field("ranges").index(index),
                )?);
            }
            scan_assignments.insert(node_id, (kind, ranges));
        }
        let scan_assignments = ScanAssignments::try_new(scan_assignments)
            .map_err(|error| super::NativeFragmentDecodeError::inconsistent(scan_path, error))?;

        let exchange_path = FieldPath::root("instance_params").field("per_exch_num_senders");
        let mut exchange_inputs = BTreeMap::new();
        let mut exchange_keys = instance_params
            .per_exch_num_senders
            .keys()
            .copied()
            .collect::<Vec<_>>();
        exchange_keys.sort_unstable();
        for raw_node_id in exchange_keys {
            let raw_sender_count = instance_params.per_exch_num_senders[&raw_node_id];
            let sender_count = usize::try_from(raw_sender_count)
                .ok()
                .and_then(NonZeroUsize::new)
                .ok_or_else(|| {
                    super::NativeFragmentDecodeError::out_of_range(
                        exchange_path.clone().map_key(raw_node_id.to_string()),
                        format!(
                            "native InstanceParams per_exch_num_senders node_id={raw_node_id} must be positive, got {raw_sender_count}"
                        ),
                    )
                })?;
            exchange_inputs.insert(
                FragmentNodeId::new(raw_node_id),
                ExchangeInputAssignment::new(sender_count),
            );
        }

        Ok(Self {
            exchange_inputs: ExchangeInputAssignments::new(exchange_inputs),
            scan_assignments,
            query_options,
            connectors: Some(connectors),
            query_id: Some(query_id),
            fragment_instance_id,
        })
    }

    #[cfg(test)]
    pub(crate) fn with_exchange_sender_count(mut self, key: ExchangeKey, count: usize) -> Self {
        let count = NonZeroUsize::new(count).expect("test sender count must be positive");
        self.fragment_instance_id = FragmentInstanceId::new(crate::common::types::UniqueId {
            hi: key.finst_id_hi,
            lo: key.finst_id_lo,
        });
        self.exchange_inputs = ExchangeInputAssignments::new(BTreeMap::from([(
            FragmentNodeId::new(key.node_id),
            ExchangeInputAssignment::new(count),
        )]));
        self
    }

    #[cfg(test)]
    pub(crate) fn with_scan_ranges(
        mut self,
        node_id: i32,
        ranges: Vec<novarocks::ScanRangeParams>,
    ) -> Self {
        let ranges = ranges
            .iter()
            .map(super::decode_scan_range_params)
            .collect::<Result<Vec<_>, _>>()
            .expect("decode test scan ranges");
        let kind = match ranges.first().map(|params| &params.range) {
            Some(ScanRange::File(_)) => ScanAssignmentKind::File,
            #[cfg(feature = "compat")]
            Some(ScanRange::BrokerFile(_)) => {
                panic!("native test scan assignment cannot contain a StarRocks broker-file range")
            }
            #[cfg(feature = "compat")]
            Some(ScanRange::SchemaSelection(_)) => {
                panic!("native test scan assignment cannot contain a StarRocks schema selection")
            }
            Some(ScanRange::StarRocksTablet(_)) => ScanAssignmentKind::StarRocksTablet,
            None => panic!("test scan assignment requires at least one range"),
        };
        self.scan_assignments = ScanAssignments::try_new(BTreeMap::from([(
            FragmentNodeId::new(node_id),
            (kind, ranges),
        )]))
        .expect("build test scan assignment");
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_query_options(mut self, query_options: Option<QueryOptions>) -> Self {
        self.query_options = query_options;
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_connector_registry(
        mut self,
        connectors: Arc<crate::connector::ConnectorRegistry>,
    ) -> Self {
        self.connectors = Some(connectors);
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_query_id(mut self, query_id: QueryId) -> Self {
        self.query_id = Some(query_id);
        self
    }

    pub(crate) fn scan_ranges(
        &self,
        node_id: i32,
    ) -> Result<&[ScanRangeParams], super::error::NativeFragmentLeafDecodeError> {
        self.scan_assignments
            .get(&FragmentNodeId::new(node_id))
            .map(|assignment| assignment.ranges())
            .ok_or_else(|| {
                super::error::NativeFragmentLeafDecodeError::at_field(
                    crate::protocol::common::error::ProtocolErrorKind::MissingField,
                    "scan_ranges",
                    format!("native ScanNode node_id={node_id} missing scan ranges"),
                )
            })
    }

    pub(crate) fn query_options(&self) -> Option<&QueryOptions> {
        self.query_options.as_ref()
    }

    #[cfg(feature = "compat")]
    pub(crate) fn query_id(&self) -> Option<QueryId> {
        self.query_id
    }

    pub(crate) fn connectors(
        &self,
    ) -> Result<&crate::connector::ConnectorRegistry, super::error::NativeFragmentLeafDecodeError>
    {
        self.connectors.as_deref().ok_or_else(|| {
            super::error::NativeFragmentLeafDecodeError::at_field(
                crate::protocol::common::error::ProtocolErrorKind::MissingField,
                "connector_registry",
                "native ScanNode requires ConnectorRegistry in NativePlanDecodeContext",
            )
        })
    }

    fn exchange_input(
        &self,
        node_id: i32,
    ) -> Result<(ExchangeKey, usize), super::error::NativeFragmentLeafDecodeError> {
        let assignment = self
            .exchange_inputs
            .get(&FragmentNodeId::new(node_id))
            .ok_or_else(|| {
                super::error::NativeFragmentLeafDecodeError::at_field(
                    crate::protocol::common::error::ProtocolErrorKind::MissingField,
                    "exchange_inputs",
                    format!("ExchangeReceiver missing sender count for node_id {node_id}"),
                )
            })?;
        let fragment_instance_id = self.fragment_instance_id.get();
        Ok((
            ExchangeKey {
                finst_id_hi: fragment_instance_id.hi,
                finst_id_lo: fragment_instance_id.lo,
                node_id,
            },
            assignment.sender_count().get(),
        ))
    }
}

pub(super) fn collect_scan_assignment_kinds(
    root: &plan::DistributedNode,
    root_path: FieldPath,
) -> Result<BTreeMap<FragmentNodeId, ScanAssignmentKind>, super::NativeFragmentDecodeError> {
    fn visit(
        node: &plan::DistributedNode,
        path: FieldPath,
        assignments: &mut BTreeMap<FragmentNodeId, ScanAssignmentKind>,
    ) -> Result<(), super::NativeFragmentDecodeError> {
        if let Some(plan::distributed_node::Payload::Physical(physical)) = node.payload.as_ref()
            && let Some(plan::plan_node::Kind::Scan(scan)) = physical.kind.as_ref()
        {
            let scan_path = path
                .clone()
                .field("payload")
                .field("physical")
                .field("scan");
            let table = scan.table.as_ref().ok_or_else(|| {
                super::NativeFragmentDecodeError::missing(
                    scan_path.clone().field("table"),
                    format!("native ScanNode node_id={} requires table", node.node_id),
                )
            })?;
            let source = table.source.as_ref().ok_or_else(|| {
                super::NativeFragmentDecodeError::missing(
                    scan_path.clone().field("table").field("source"),
                    format!("native ScanNode node_id={} requires source", node.node_id),
                )
            })?;
            let source = source.kind.as_ref().ok_or_else(|| {
                super::NativeFragmentDecodeError::missing(
                    scan_path
                        .clone()
                        .field("table")
                        .field("source")
                        .field("kind"),
                    format!(
                        "native ScanNode node_id={} requires source kind",
                        node.node_id
                    ),
                )
            })?;
            let kind = match source {
                plan::scan_source::Kind::StarrocksTable(_) => ScanAssignmentKind::StarRocksTablet,
                _ => ScanAssignmentKind::File,
            };
            if assignments
                .insert(FragmentNodeId::new(node.node_id), kind)
                .is_some()
            {
                return Err(super::NativeFragmentDecodeError::inconsistent(
                    path.clone().field("node_id"),
                    format!("native plan has duplicate scan node_id={}", node.node_id),
                ));
            }
        }
        for (index, child) in node.children.iter().enumerate() {
            visit(
                child,
                path.clone().field("children").index(index),
                assignments,
            )?;
        }
        Ok(())
    }

    let mut assignments = BTreeMap::new();
    visit(root, root_path, &mut assignments)?;
    Ok(assignments)
}

#[allow(dead_code)]
pub(crate) fn decode_node(
    node: &plan::DistributedNode,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, super::NativeFragmentDecodeError> {
    decode_node_inner(
        node,
        FieldPath::root("plan_fragment").field("root"),
        arena,
        ctx,
        None,
    )
}

pub(crate) fn decode_node_with_runtime_filters(
    node: &plan::DistributedNode,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
    ledger: &mut NativeRuntimeFilterDecodeLedger,
) -> Result<DecodedNode, super::NativeFragmentDecodeError> {
    decode_node_inner(
        node,
        FieldPath::root("plan_fragment").field("root"),
        arena,
        ctx,
        Some(ledger),
    )
}

fn decode_node_inner(
    node: &plan::DistributedNode,
    path: FieldPath,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
    mut ledger: Option<&mut NativeRuntimeFilterDecodeLedger>,
) -> Result<DecodedNode, super::NativeFragmentDecodeError> {
    validate_distributed_node_children(node, path.clone())?;
    let mut children = Vec::with_capacity(node.children.len());
    for (index, child) in node.children.iter().enumerate() {
        children.push(decode_node_inner(
            child,
            path.clone().field("children").index(index),
            arena,
            ctx,
            ledger.as_deref_mut(),
        )?);
    }

    let attached = ledger
        .as_deref()
        .map(|ledger| {
            ledger.peek_attached(
                &node.runtime_filter_binding_ids,
                node.node_id,
                node.fragment_id,
            )
        })
        .transpose()
        .map_err(|error| error.into_native(path.clone()))?
        .unwrap_or_default();
    let direct_inputs = children
        .iter()
        .map(|child| (child.layout.clone(), child.output_schema.clone()))
        .collect::<Vec<_>>();
    let (consumer_bindings, producer_bindings): (Vec<_>, Vec<_>) = attached
        .into_iter()
        .partition(|binding| matches!(binding.role, DecodedBindingRole::Consumer { .. }));
    if !children.is_empty() {
        attach_direct_input_consumers(
            node.node_id,
            &consumer_bindings,
            &mut children,
            arena,
            path.clone().field("runtime_filter_binding_ids"),
        )?;
    }

    let payload = node.payload.as_ref().ok_or_else(|| {
        super::NativeFragmentDecodeError::missing(
            path.clone().field("payload"),
            format!("DistributedNode node_id={} requires payload", node.node_id),
        )
    })?;
    let mut lowered = match payload {
        plan::distributed_node::Payload::Physical(physical) => lower_physical_node(
            node,
            physical,
            path.clone().field("payload").field("physical"),
            path.clone(),
            children,
            arena,
            ctx,
        ),
        plan::distributed_node::Payload::Exchange(exchange) => exchange::lower_exchange_receiver(
            node,
            exchange,
            path.clone().field("payload").field("exchange"),
            children,
            arena,
            ctx,
        ),
    }?;
    if children_are_absent(node) && !consumer_bindings.is_empty() {
        attach_leaf_consumers(node, &consumer_bindings, &mut lowered, arena, path.clone())?;
    }
    if !producer_bindings.is_empty() {
        attach_hash_join_producers(
            node,
            &producer_bindings,
            &direct_inputs,
            &mut lowered,
            arena,
            path.clone(),
        )?;
    }
    let lowered = apply_distributed_limit_if_needed(node, lowered, path.clone())?;
    if let Some(ledger) = ledger {
        ledger
            .commit_consumed_many(&node.runtime_filter_binding_ids)
            .map_err(|error| error.into_native(path))?;
    }
    Ok(lowered)
}

fn validate_distributed_node_children(
    node: &plan::DistributedNode,
    node_path: FieldPath,
) -> Result<(), super::NativeFragmentDecodeError> {
    let actual = node.children.len();
    let Some(payload) = node.payload.as_ref() else {
        return Ok(());
    };
    match payload {
        plan::distributed_node::Payload::Exchange(_) => {
            require_exact_children(node_path, "ExchangeReceiver", 0, actual)
        }
        plan::distributed_node::Payload::Physical(physical) => {
            let Some(kind) = physical.kind.as_ref() else {
                return Ok(());
            };
            match kind {
                plan::plan_node::Kind::Values(_) => {
                    require_exact_children(node_path, "ValuesNode", 0, actual)
                }
                plan::plan_node::Kind::Project(_) => {
                    require_exact_children(node_path, "ProjectNode", 1, actual)
                }
                plan::plan_node::Kind::Filter(_) => {
                    require_exact_children(node_path, "FilterNode", 1, actual)
                }
                plan::plan_node::Kind::Limit(_) => {
                    require_exact_children(node_path, "LimitNode", 1, actual)
                }
                plan::plan_node::Kind::Sort(_) => {
                    require_exact_children(node_path, "SortNode", 1, actual)
                }
                plan::plan_node::Kind::Topn(_) => {
                    require_exact_children(node_path, "TopNNode", 1, actual)
                }
                plan::plan_node::Kind::SetOp(_) => {
                    require_min_children(node_path, "SetOpNode", 2, actual)
                }
                plan::plan_node::Kind::AssertOneRow(_) => {
                    require_exact_children(node_path, "AssertOneRowNode", 1, actual)
                }
                plan::plan_node::Kind::Scan(_) => {
                    require_exact_children(node_path, "ScanNode", 0, actual)
                }
                plan::plan_node::Kind::HashAggregate(_) => {
                    require_exact_children(node_path, "HashAggregateNode", 1, actual)
                }
                plan::plan_node::Kind::HashJoin(_) => {
                    require_exact_children(node_path, "HashJoinNode", 2, actual)
                }
                plan::plan_node::Kind::NestLoopJoin(_) => {
                    require_exact_children(node_path, "NestLoopJoinNode", 2, actual)
                }
                plan::plan_node::Kind::Window(_) => {
                    require_exact_children(node_path, "WindowNode", 1, actual)
                }
                plan::plan_node::Kind::Repeat(_) => {
                    require_exact_children(node_path, "RepeatNode", 1, actual)
                }
                plan::plan_node::Kind::GenerateSeries(_) => {
                    require_exact_children(node_path, "GenerateSeriesNode", 0, actual)
                }
                plan::plan_node::Kind::TableFunction(_) => {
                    require_exact_children(node_path, "TableFunctionNode", 1, actual)
                }
                plan::plan_node::Kind::ChangeEventExpand(_) => {
                    require_exact_children(node_path, "ChangeEventExpandNode", 1, actual)
                }
                plan::plan_node::Kind::Redistribute(_) => {
                    require_exact_children(node_path, "RedistributeNode", 1, actual)
                }
                plan::plan_node::Kind::Decode(_)
                | plan::plan_node::Kind::CteAnchor(_)
                | plan::plan_node::Kind::CteProduce(_)
                | plan::plan_node::Kind::CteConsume(_) => Ok(()),
            }
        }
    }
}

fn children_are_absent(node: &plan::DistributedNode) -> bool {
    node.children.is_empty()
}

fn attach_direct_input_consumers(
    owner_node_id: i32,
    bindings: &[DecodedRuntimeFilterBinding],
    children: &mut [DecodedNode],
    arena: &mut ExprArena,
    path: FieldPath,
) -> Result<(), super::NativeFragmentDecodeError> {
    let mut grouped = BTreeMap::<usize, Vec<NativeRuntimeFilterConsumerSpec>>::new();
    for binding in bindings {
        let DecodedBindingRole::Consumer { target, .. } = &binding.role else {
            return Err(super::NativeFragmentDecodeError::inconsistent(
                path.clone(),
                format!(
                    "native runtime-filter binding_id={} expected consumer role",
                    binding.binding_id
                ),
            ));
        };
        let DecodedConsumerBindingTarget::DirectInput {
            input_ordinal: index,
        } = *target
        else {
            return Err(super::NativeFragmentDecodeError::inconsistent(
                path.clone(),
                format!(
                    "native runtime-filter consumer binding_id={} on node_id={owner_node_id} must target a direct input",
                    binding.binding_id
                ),
            ));
        };
        let child = children.get(index).ok_or_else(|| {
            super::NativeFragmentDecodeError::inconsistent(
                path.clone(),
                format!(
                    "native runtime-filter consumer binding_id={} on node_id={owner_node_id} targets missing direct input ordinal={index}, input_count={}",
                    binding.binding_id,
                    children.len()
                ),
            )
        })?;
        let expr_id =
            lower_binding_expression(binding, &child.layout, &child.output_schema, arena)?;
        grouped
            .entry(index)
            .or_default()
            .push(super::NativeFragmentDecodeError::map_invalid(
                path.clone(),
                consumer_spec(binding, expr_id),
            )?);
    }
    for (index, specs) in grouped {
        let child = &mut children[index];
        let input = child.node.clone();
        child.node = ExecNode {
            kind: ExecNodeKind::NativeRuntimeFilterConsumer(NativeRuntimeFilterConsumerNode {
                input: Box::new(input),
                owner_node_id,
                bindings: specs,
            }),
        };
    }
    Ok(())
}

fn attach_leaf_consumers(
    wire_node: &plan::DistributedNode,
    bindings: &[DecodedRuntimeFilterBinding],
    lowered: &mut DecodedNode,
    arena: &mut ExprArena,
    path: FieldPath,
) -> Result<(), super::NativeFragmentDecodeError> {
    for binding in bindings {
        let DecodedBindingRole::Consumer { target, .. } = &binding.role else {
            return Err(super::NativeFragmentDecodeError::inconsistent(
                path.clone().field("runtime_filter_binding_ids"),
                format!(
                    "native runtime-filter binding_id={} expected consumer role",
                    binding.binding_id
                ),
            ));
        };
        if *target != DecodedConsumerBindingTarget::SourceBoundary {
            return Err(super::NativeFragmentDecodeError::inconsistent(
                path.clone().field("runtime_filter_binding_ids"),
                format!(
                    "native runtime-filter consumer binding_id={} on leaf node_id={} must target source boundary",
                    binding.binding_id, wire_node.node_id
                ),
            ));
        }
    }
    let specs = bindings
        .iter()
        .map(|binding| {
            let expr_id =
                lower_binding_expression(binding, &lowered.layout, &lowered.output_schema, arena)?;
            consumer_spec(binding, expr_id).map_err(|error| {
                super::NativeFragmentDecodeError::inconsistent(
                    path.clone().field("runtime_filter_binding_ids"),
                    error,
                )
            })
        })
        .collect::<Result<Vec<_>, super::NativeFragmentDecodeError>>()?;
    let payload = wire_node.payload.as_ref().ok_or_else(|| {
        super::NativeFragmentDecodeError::missing(
            path.clone().field("payload"),
            format!("native node_id={} payload missing", wire_node.node_id),
        )
    })?;
    match payload {
        plan::distributed_node::Payload::Exchange(_) => {
            let exchange = find_exchange_source_mut(&mut lowered.node).ok_or_else(|| {
                super::NativeFragmentDecodeError::inconsistent(
                    path.clone().field("payload").field("exchange"),
                    format!(
                        "native node_id={} exchange lowering lost ExchangeSource boundary",
                        wire_node.node_id
                    ),
                )
            })?;
            exchange.set_native_runtime_filter_specs(specs);
        }
        plan::distributed_node::Payload::Physical(physical) => match physical.kind.as_ref() {
            Some(plan::plan_node::Kind::Scan(_)) => {
                set_native_scan_specs(&mut lowered.node, specs).map_err(|_| {
                    super::NativeFragmentDecodeError::inconsistent(
                        path.clone()
                            .field("payload")
                            .field("physical")
                            .field("scan"),
                        format!(
                            "native node_id={} scan lowering lost Scan boundary",
                            wire_node.node_id
                        ),
                    )
                })?;
            }
            Some(plan::plan_node::Kind::Values(_))
            | Some(plan::plan_node::Kind::GenerateSeries(_)) => {
                wrap_source_boundary(&mut lowered.node, wire_node.node_id, specs);
            }
            kind => {
                return Err(super::NativeFragmentDecodeError::unsupported(
                    path.field("runtime_filter_binding_ids"),
                    format!(
                        "native runtime-filter consumer binding on leaf node_id={} has unsupported source capability: {kind:?}",
                        wire_node.node_id
                    ),
                ));
            }
        },
    }
    Ok(())
}

fn wrap_source_boundary(
    node: &mut ExecNode,
    owner_node_id: i32,
    bindings: Vec<NativeRuntimeFilterConsumerSpec>,
) {
    let input = node.clone();
    *node = ExecNode {
        kind: ExecNodeKind::NativeRuntimeFilterConsumer(NativeRuntimeFilterConsumerNode {
            input: Box::new(input),
            owner_node_id,
            bindings,
        }),
    };
}

fn set_native_scan_specs(
    node: &mut ExecNode,
    specs: Vec<NativeRuntimeFilterConsumerSpec>,
) -> Result<(), Vec<NativeRuntimeFilterConsumerSpec>> {
    match &mut node.kind {
        ExecNodeKind::Scan(scan) => {
            scan.set_native_runtime_filter_specs(specs);
            Ok(())
        }
        ExecNodeKind::IcebergDeltaScan(scan) => {
            scan.set_native_runtime_filter_specs(specs);
            Ok(())
        }
        ExecNodeKind::Project(project) if project.is_subordinate => {
            set_native_scan_specs(&mut project.input, specs)
        }
        ExecNodeKind::Filter(filter) => set_native_scan_specs(&mut filter.input, specs),
        _ => Err(specs),
    }
}

fn find_exchange_source_mut(
    node: &mut ExecNode,
) -> Option<&mut crate::exec::node::exchange_source::ExchangeSourceNode> {
    match &mut node.kind {
        ExecNodeKind::ExchangeSource(exchange) => Some(exchange),
        ExecNodeKind::Limit(limit) => find_exchange_source_mut(&mut limit.input),
        ExecNodeKind::Sort(sort) => find_exchange_source_mut(&mut sort.input),
        _ => None,
    }
}

fn attach_hash_join_producers(
    wire_node: &plan::DistributedNode,
    bindings: &[DecodedRuntimeFilterBinding],
    direct_inputs: &[(Layout, ChunkSchemaRef)],
    lowered: &mut DecodedNode,
    arena: &mut ExprArena,
    path: FieldPath,
) -> Result<(), super::NativeFragmentDecodeError> {
    let binding_path = path.clone().field("runtime_filter_binding_ids");
    let ExecNodeKind::Join(join) = &mut lowered.node.kind else {
        return Err(super::NativeFragmentDecodeError::inconsistent(
            binding_path,
            format!(
                "native runtime-filter producer binding is only supported on HashJoin, node_id={}",
                wire_node.node_id
            ),
        ));
    };
    if direct_inputs.len() != 2 {
        return Err(super::NativeFragmentDecodeError::inconsistent(
            binding_path,
            format!(
                "native HashJoin node_id={} missing two direct inputs",
                wire_node.node_id
            ),
        ));
    }
    let build_input_index = if join.join_type == crate::exec::node::join::JoinType::RightSemi {
        0
    } else {
        1
    };
    let (build_layout, build_schema) = &direct_inputs[build_input_index];
    let payload = wire_node.payload.as_ref().ok_or_else(|| {
        super::NativeFragmentDecodeError::missing(
            path.clone().field("payload"),
            format!(
                "native HashJoin node_id={} payload missing",
                wire_node.node_id
            ),
        )
    })?;
    let plan::distributed_node::Payload::Physical(physical) = payload else {
        return Err(super::NativeFragmentDecodeError::inconsistent(
            path.clone().field("payload"),
            format!(
                "native runtime-filter producer node_id={} is not physical HashJoin",
                wire_node.node_id
            ),
        ));
    };
    let Some(plan::plan_node::Kind::HashJoin(wire_join)) = physical.kind.as_ref() else {
        return Err(super::NativeFragmentDecodeError::inconsistent(
            path.clone()
                .field("payload")
                .field("physical")
                .field("kind"),
            format!(
                "native runtime-filter producer node_id={} is not HashJoin",
                wire_node.node_id
            ),
        ));
    };
    let mut producers = Vec::with_capacity(bindings.len());
    for binding in bindings {
        let DecodedBindingRole::Producer {
            contribution_kinds,
            completion_requirement,
            join_key_ordinal,
        } = &binding.role
        else {
            return Err(super::NativeFragmentDecodeError::inconsistent(
                path.clone().field("runtime_filter_binding_ids"),
                format!(
                    "native runtime-filter binding_id={} expected producer role",
                    binding.binding_id
                ),
            ));
        };
        let build_key_index = *join_key_ordinal;
        let join_key_path = path
            .clone()
            .field("payload")
            .field("physical")
            .field("hash_join")
            .field("eq_conditions")
            .index(build_key_index);
        let condition = wire_join.eq_conditions.get(build_key_index).ok_or_else(|| {
            super::NativeFragmentDecodeError::inconsistent(
                join_key_path.clone(),
                format!(
                    "native runtime-filter producer binding_id={} targets missing join key ordinal={build_key_index}, key_count={}",
                    binding.binding_id,
                    wire_join.eq_conditions.len()
                ),
            )
        })?;
        if condition.null_safe {
            return Err(super::NativeFragmentDecodeError::inconsistent(
                join_key_path.clone().field("null_safe"),
                format!(
                    "native runtime-filter producer binding_id={} targets null-safe join key ordinal={build_key_index}",
                    binding.binding_id
                ),
            ));
        }
        let (raw_build, raw_build_path) =
            if join.join_type == crate::exec::node::join::JoinType::RightSemi {
                (condition.left.as_ref(), join_key_path.clone().field("left"))
            } else {
                (
                    condition.right.as_ref(),
                    join_key_path.clone().field("right"),
                )
            };
        let raw_build = raw_build.ok_or_else(|| {
            super::NativeFragmentDecodeError::missing(
                raw_build_path.clone(),
                format!(
                    "native runtime-filter producer binding_id={} join key ordinal={build_key_index} missing build expression",
                    binding.binding_id
                ),
            )
        })?;
        if raw_build != &binding.expression {
            return Err(super::NativeFragmentDecodeError::inconsistent(
                raw_build_path.clone(),
                format!(
                    "native runtime-filter producer binding_id={} expression does not match join key ordinal={build_key_index}",
                    binding.binding_id
                ),
            ));
        }
        validate_column_refs_exact(
            binding.binding_id,
            raw_build,
            build_layout,
            build_schema,
            raw_build_path,
        )?;
        let build_expr_id = lower_binding_expression(binding, build_layout, build_schema, arena)?;
        producers.push(NativeJoinRuntimeFilterProducerSpec {
            binding_id: binding.binding_id,
            channel_id: binding.channel_id,
            build_expr_id,
            build_key_index,
            contribution_kinds: contribution_kinds.clone(),
            completion_requirement: *completion_requirement,
            contract: native_contract(&binding.contract),
            reduction: native_reduction(&binding.reduction),
        });
    }
    join.runtime_filter_execution = JoinRuntimeFilterExecution::Native { producers };
    Ok(())
}

fn consumer_spec(
    binding: &DecodedRuntimeFilterBinding,
    expr_id: crate::exec::expr::ExprId,
) -> Result<NativeRuntimeFilterConsumerSpec, String> {
    let DecodedBindingRole::Consumer {
        capabilities,
        activation,
        ..
    } = &binding.role
    else {
        return Err(format!(
            "native runtime-filter binding_id={} expected consumer role",
            binding.binding_id
        ));
    };
    Ok(NativeRuntimeFilterConsumerSpec {
        binding_id: binding.binding_id,
        channel_id: binding.channel_id,
        expr_id,
        activation: *activation,
        capabilities: capabilities.clone(),
        contract: native_contract(&binding.contract),
        reduction: native_reduction(&binding.reduction),
    })
}

fn native_contract(contract: &DecodedRuntimeFilterContract) -> NativeRuntimeFilterContract {
    match contract {
        DecodedRuntimeFilterContract::Membership {
            canonical_schema,
            schema_digest,
        } => NativeRuntimeFilterContract::Membership {
            canonical_schema: Arc::clone(canonical_schema),
            schema_digest: *schema_digest,
        },
        DecodedRuntimeFilterContract::Ordered {
            keys,
            comparator_digest,
            order_contract_digest,
        } => NativeRuntimeFilterContract::Ordered {
            keys: Arc::clone(keys),
            comparator_digest: *comparator_digest,
            order_contract_digest: *order_contract_digest,
        },
    }
}

fn native_reduction(reduction: &DecodedRuntimeFilterReduction) -> NativeRuntimeFilterReduction {
    match reduction {
        DecodedRuntimeFilterReduction::SetUnion => NativeRuntimeFilterReduction::SetUnion,
        DecodedRuntimeFilterReduction::TightenOrderedBound => {
            NativeRuntimeFilterReduction::TightenOrderedBound
        }
        DecodedRuntimeFilterReduction::MergeTopKSummary { k, contract_digest } => {
            NativeRuntimeFilterReduction::MergeTopKSummary {
                k: *k,
                contract_digest: *contract_digest,
            }
        }
    }
}

fn lower_binding_expression(
    binding: &DecodedRuntimeFilterBinding,
    layout: &Layout,
    schema: &ChunkSchemaRef,
    arena: &mut ExprArena,
) -> Result<crate::exec::expr::ExprId, super::NativeFragmentDecodeError> {
    let expression_path = binding.expression_path.clone();
    validate_column_refs_exact(
        binding.binding_id,
        &binding.expression,
        layout,
        schema,
        expression_path.clone(),
    )?;
    super::expr::decode_expr_at(&binding.expression, expression_path, arena, layout)
}

fn validate_column_refs_exact(
    binding_id: u32,
    expression: &crate::proto::expr::Expr,
    layout: &Layout,
    schema: &ChunkSchemaRef,
    path: FieldPath,
) -> Result<(), super::NativeFragmentDecodeError> {
    use crate::proto::expr::expr::Kind;

    let kind = expression.kind.as_ref().ok_or_else(|| {
        super::NativeFragmentDecodeError::missing(
            path.clone().field("kind"),
            format!("native runtime-filter binding_id={binding_id} expression kind missing"),
        )
    })?;
    if let Kind::ColumnRef(column) = kind {
        let column_path = path.clone().field("column_ref");
        let slot_id = layout
            .resolve_column_id(column.column_id)
            .map_err(|error| {
                super::NativeFragmentDecodeError::invalid_value(
                    column_path.clone().field("column_id"),
                    format!("native runtime-filter binding_id={binding_id}: {error}"),
                )
            })?;
        let expected = schema.field_by_slot(slot_id).ok_or_else(|| {
            super::NativeFragmentDecodeError::inconsistent(
                column_path.clone().field("column_id"),
                format!(
                    "native runtime-filter binding_id={binding_id} ColumnRef column_id={} has no ChunkSchema field",
                    column.column_id
                ),
            )
        })?;
        let type_desc = expression.r#type.as_ref().ok_or_else(|| {
            super::NativeFragmentDecodeError::missing(
                path.clone().field("type"),
                format!(
                    "native runtime-filter binding_id={binding_id} ColumnRef column_id={} type missing",
                    column.column_id
                ),
            )
        })?;
        let actual = super::decode_field_type(
            "_runtime_filter_column",
            expression.nullable,
            type_desc,
        )
        .map_err(|error| {
            super::NativeFragmentDecodeError::invalid_value(
                path.clone().field("type"),
                format!(
                    "native runtime-filter binding_id={binding_id} ColumnRef column_id={} type: {error}",
                    column.column_id
                ),
            )
        })?;
        let expected_schema =
            crate::exec::chunk::ChunkFieldSchema::from_field(expected).map_err(|error| {
                super::NativeFragmentDecodeError::inconsistent(column_path.clone(), error)
            })?;
        let actual_schema =
            crate::exec::chunk::ChunkFieldSchema::from_field(&actual).map_err(|error| {
                super::NativeFragmentDecodeError::invalid_value(path.clone().field("type"), error)
            })?;
        if expected.data_type() != actual.data_type()
            || expected.is_nullable() != actual.is_nullable()
            || expected_schema != actual_schema
        {
            return Err(super::NativeFragmentDecodeError::inconsistent(
                column_path,
                format!(
                    "native runtime-filter binding_id={binding_id} ColumnRef column_id={} type/nullability does not exactly match direct input",
                    column.column_id
                ),
            ));
        }
    }

    let visit = |child: &crate::proto::expr::Expr, child_path: FieldPath| {
        validate_column_refs_exact(binding_id, child, layout, schema, child_path)
    };
    let missing = |child_path: FieldPath, detail: &'static str| {
        super::NativeFragmentDecodeError::missing(child_path, detail)
    };
    match kind {
        Kind::ColumnRef(_) | Kind::Literal(_) | Kind::LambdaParamRef(_) => Ok(()),
        Kind::BinaryOp(binary) => {
            let binary_path = path.field("binary_op");
            let left_path = binary_path.clone().field("left");
            visit(
                binary
                    .left
                    .as_ref()
                    .ok_or_else(|| missing(left_path.clone(), "BinaryOp.left missing"))?,
                left_path,
            )?;
            let right_path = binary_path.field("right");
            visit(
                binary
                    .right
                    .as_ref()
                    .ok_or_else(|| missing(right_path.clone(), "BinaryOp.right missing"))?,
                right_path,
            )
        }
        Kind::UnaryOp(unary) => {
            let operand_path = path.field("unary_op").field("operand");
            visit(
                unary
                    .operand
                    .as_ref()
                    .ok_or_else(|| missing(operand_path.clone(), "UnaryOp.operand missing"))?,
                operand_path,
            )
        }
        Kind::FunctionCall(call) => call.args.iter().enumerate().try_for_each(|(index, child)| {
            visit(
                child,
                path.clone()
                    .field("function_call")
                    .field("args")
                    .index(index),
            )
        }),
        Kind::AggregateCall(call) => {
            let call_path = path.clone().field("aggregate_call");
            call.args
                .iter()
                .enumerate()
                .try_for_each(|(index, child)| {
                    visit(child, call_path.clone().field("args").index(index))
                })?;
            call.order_by
                .iter()
                .enumerate()
                .try_for_each(|(index, item)| {
                    let expr_path = call_path
                        .clone()
                        .field("order_by")
                        .index(index)
                        .field("expr");
                    visit(
                        item.expr
                            .as_ref()
                            .ok_or_else(|| missing(expr_path.clone(), "SortItem.expr missing"))?,
                        expr_path,
                    )
                })
        }
        Kind::WindowCall(call) => {
            let call_path = path.clone().field("window_call");
            call.args
                .iter()
                .enumerate()
                .try_for_each(|(index, child)| {
                    visit(child, call_path.clone().field("args").index(index))
                })?;
            call.partition_by
                .iter()
                .enumerate()
                .try_for_each(|(index, child)| {
                    visit(child, call_path.clone().field("partition_by").index(index))
                })?;
            call.order_by
                .iter()
                .enumerate()
                .try_for_each(|(index, item)| {
                    let expr_path = call_path
                        .clone()
                        .field("order_by")
                        .index(index)
                        .field("expr");
                    visit(
                        item.expr
                            .as_ref()
                            .ok_or_else(|| missing(expr_path.clone(), "SortItem.expr missing"))?,
                        expr_path,
                    )
                })
        }
        Kind::Cast(cast) => {
            let operand_path = path.field("cast").field("operand");
            visit(
                cast.operand
                    .as_ref()
                    .ok_or_else(|| missing(operand_path.clone(), "Cast.operand missing"))?,
                operand_path,
            )
        }
        Kind::IsNull(is_null) => {
            let operand_path = path.field("is_null").field("operand");
            visit(
                is_null
                    .operand
                    .as_ref()
                    .ok_or_else(|| missing(operand_path.clone(), "IsNull.operand missing"))?,
                operand_path,
            )
        }
        Kind::InList(in_list) => {
            let list_path = path.clone().field("in_list");
            let operand_path = list_path.clone().field("operand");
            visit(
                in_list
                    .operand
                    .as_ref()
                    .ok_or_else(|| missing(operand_path.clone(), "InList.operand missing"))?,
                operand_path,
            )?;
            in_list
                .list
                .iter()
                .enumerate()
                .try_for_each(|(index, child)| {
                    visit(child, list_path.clone().field("list").index(index))
                })
        }
        Kind::Between(between) => {
            let between_path = path.clone().field("between");
            for (field, child, detail) in [
                (
                    "operand",
                    between.operand.as_ref(),
                    "Between.operand missing",
                ),
                ("low", between.low.as_ref(), "Between.low missing"),
                ("high", between.high.as_ref(), "Between.high missing"),
            ] {
                let child_path = between_path.clone().field(field);
                visit(
                    child.ok_or_else(|| missing(child_path.clone(), detail))?,
                    child_path,
                )?;
            }
            Ok(())
        }
        Kind::Like(like) => {
            let like_path = path.clone().field("like");
            for (field, child, detail) in [
                ("operand", like.operand.as_ref(), "Like.operand missing"),
                ("pattern", like.pattern.as_ref(), "Like.pattern missing"),
            ] {
                let child_path = like_path.clone().field(field);
                visit(
                    child.ok_or_else(|| missing(child_path.clone(), detail))?,
                    child_path,
                )?;
            }
            Ok(())
        }
        Kind::CaseExpr(case_expr) => {
            let case_path = path.clone().field("case_expr");
            if let Some(operand) = &case_expr.operand {
                visit(operand, case_path.clone().field("operand"))?;
            }
            for (index, branch) in case_expr.when_then.iter().enumerate() {
                let branch_path = case_path.clone().field("when_then").index(index);
                let when_path = branch_path.clone().field("when");
                visit(
                    branch
                        .when
                        .as_ref()
                        .ok_or_else(|| missing(when_path.clone(), "Case.when missing"))?,
                    when_path,
                )?;
                let then_path = branch_path.field("then");
                visit(
                    branch
                        .then
                        .as_ref()
                        .ok_or_else(|| missing(then_path.clone(), "Case.then missing"))?,
                    then_path,
                )?;
            }
            if let Some(else_expr) = &case_expr.else_expr {
                visit(else_expr, case_path.field("else_expr"))?;
            }
            Ok(())
        }
        Kind::IsTruth(is_truth) => {
            let operand_path = path.field("is_truth").field("operand");
            visit(
                is_truth
                    .operand
                    .as_ref()
                    .ok_or_else(|| missing(operand_path.clone(), "IsTruth.operand missing"))?,
                operand_path,
            )
        }
        Kind::Lambda(lambda) => {
            let body_path = path.field("lambda").field("body");
            visit(
                lambda
                    .body
                    .as_ref()
                    .ok_or_else(|| missing(body_path.clone(), "Lambda.body missing"))?,
                body_path,
            )
        }
        Kind::Nested(nested) => {
            let inner_path = path.field("nested").field("inner");
            visit(
                nested
                    .inner
                    .as_ref()
                    .ok_or_else(|| missing(inner_path.clone(), "Nested.inner missing"))?,
                inner_path,
            )
        }
    }
}

fn apply_distributed_limit_if_needed(
    node: &plan::DistributedNode,
    mut lowered: DecodedNode,
    path: FieldPath,
) -> Result<DecodedNode, super::NativeFragmentDecodeError> {
    let Some(limit) = super::NativeFragmentDecodeError::map_invalid(
        path.field("limit"),
        parse_distributed_limit(node.limit, "DistributedNode.limit"),
    )?
    else {
        return Ok(lowered);
    };
    if matches!(
        lowered.node.kind,
        ExecNodeKind::Limit(_) | ExecNodeKind::Sort(_)
    ) {
        return Ok(lowered);
    }
    lowered.node = ExecNode {
        kind: ExecNodeKind::Limit(LimitNode {
            input: Box::new(lowered.node),
            node_id: node.node_id,
            limit: Some(limit),
            offset: 0,
        }),
    };
    Ok(lowered)
}

fn lower_physical_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    path: FieldPath,
    node_path: FieldPath,
    children: Vec<DecodedNode>,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, super::NativeFragmentDecodeError> {
    let physical_output_path = path.clone().field("output_columns");
    let kind = physical.kind.as_ref().ok_or_else(|| {
        super::NativeFragmentDecodeError::missing(
            path.clone().field("kind"),
            format!("PlanNode node_id={} requires kind", node.node_id),
        )
    })?;
    match kind {
        plan::plan_node::Kind::Values(values) => values::lower_values_node(
            node,
            physical,
            values,
            path.clone().field("values"),
            physical_output_path.clone(),
            children,
            arena,
        ),
        plan::plan_node::Kind::Project(project) => project::lower_project_node(
            node,
            project,
            path.clone().field("project"),
            children,
            arena,
        ),
        plan::plan_node::Kind::Filter(filter) => {
            filter::lower_filter_node(node, filter, path.clone().field("filter"), children, arena)
        }
        plan::plan_node::Kind::Limit(limit) => limit::lower_limit_node(
            node,
            limit,
            path.clone().field("limit"),
            node_path,
            children,
        ),
        plan::plan_node::Kind::Sort(sort) => sort::lower_sort_node(
            node,
            physical,
            sort,
            path.clone().field("sort"),
            physical_output_path.clone(),
            children,
            arena,
        ),
        plan::plan_node::Kind::Topn(topn) => {
            topn::lower_topn_node(node, topn, path.clone().field("topn"), children, arena)
        }
        plan::plan_node::Kind::SetOp(set_op) => set_op::lower_set_op_node(
            node,
            physical,
            set_op,
            path.clone().field("set_op"),
            physical_output_path.clone(),
            children,
            arena,
        ),
        plan::plan_node::Kind::AssertOneRow(assert) => assert::lower_assert_one_row_node(
            node,
            assert,
            path.clone().field("assert_one_row"),
            children,
        ),
        plan::plan_node::Kind::Scan(scan) => super::scan::lower_scan_node(
            node,
            physical,
            scan,
            path.clone().field("scan"),
            ctx,
            arena,
        ),
        plan::plan_node::Kind::HashAggregate(aggregate) => aggregate::lower_hash_aggregate_node(
            node,
            physical,
            aggregate,
            path.clone().field("hash_aggregate"),
            physical_output_path.clone(),
            children,
            arena,
        ),
        plan::plan_node::Kind::HashJoin(join) => hash_join::lower_hash_join_node(
            node,
            physical,
            join,
            path.clone().field("hash_join"),
            node_path.clone(),
            physical_output_path.clone(),
            children,
            arena,
        ),
        plan::plan_node::Kind::NestLoopJoin(join) => nestloop_join::lower_nest_loop_join_node(
            node,
            physical,
            join,
            path.clone().field("nest_loop_join"),
            node_path.clone(),
            physical_output_path.clone(),
            children,
            arena,
        ),
        plan::plan_node::Kind::Window(window) => window::lower_window_node(
            node,
            physical,
            window,
            path.clone().field("window"),
            physical_output_path.clone(),
            children,
            arena,
        ),
        plan::plan_node::Kind::Repeat(repeat) => {
            repeat::lower_repeat_node(node, repeat, path.clone().field("repeat"), children)
        }
        plan::plan_node::Kind::GenerateSeries(generate_series) => {
            generate_series::lower_generate_series_node(
                node,
                generate_series,
                path.clone().field("generate_series"),
                children,
                arena,
            )
        }
        plan::plan_node::Kind::TableFunction(table_function) => {
            table_function::lower_table_function_node(
                node,
                table_function,
                path.clone().field("table_function"),
                children,
                arena,
            )
        }
        plan::plan_node::Kind::Decode(_) => Err(super::NativeFragmentDecodeError::unsupported(
            path.clone().field("decode"),
            "native physical node kind Decode is unsupported",
        )),
        plan::plan_node::Kind::ChangeEventExpand(expand) => {
            change_event_expand::lower_change_event_expand_node(
                node,
                physical,
                expand,
                path.clone().field("change_event_expand"),
                physical_output_path.clone(),
                children,
                arena,
            )
        }
        plan::plan_node::Kind::CteAnchor(_) => Err(super::NativeFragmentDecodeError::unsupported(
            path.clone().field("cte_anchor"),
            "native physical node kind CTEAnchor is unsupported",
        )),
        plan::plan_node::Kind::CteProduce(_) => Err(super::NativeFragmentDecodeError::unsupported(
            path.clone().field("cte_produce"),
            "native physical node kind CTEProduce is unsupported",
        )),
        plan::plan_node::Kind::CteConsume(_) => Err(super::NativeFragmentDecodeError::unsupported(
            path.clone().field("cte_consume"),
            "native physical node kind CTEConsume is unsupported",
        )),
        plan::plan_node::Kind::Redistribute(redistribute) => redistribute::lower_redistribute_node(
            physical,
            redistribute,
            path.clone().field("redistribute"),
            physical_output_path,
            children,
            arena,
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::expr::ExprArena;
    use crate::exec::node::ExecNodeKind;
    use crate::exec::node::assert::{AssertNumRowsMode, Assertion};
    use crate::exec::node::set_op::SetOpKind;
    use crate::proto::{common, expr, plan};
    use crate::protocol::native::type_mapping::encode_type;

    struct DummyScanOp;

    impl crate::exec::node::scan::ScanOp for DummyScanOp {
        fn execute_iter(
            &self,
            _morsel: crate::exec::node::scan::ScanMorsel,
            _profile: Option<crate::runtime::profile::RuntimeProfile>,
            _runtime_filters: Option<&crate::exec::node::scan::RuntimeFilterContext>,
        ) -> Result<crate::exec::node::BoxedExecIter, String> {
            Ok(Box::new(std::iter::empty()))
        }

        fn build_morsels(&self) -> Result<crate::exec::node::scan::ScanMorsels, String> {
            Ok(crate::exec::node::scan::ScanMorsels::default())
        }
    }

    pub(super) fn type_desc(data_type: &DataType) -> common::TypeDesc {
        encode_type(data_type).expect("encode type")
    }

    pub(super) fn output_column_with_nullable(
        column_id: u32,
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> common::OutputColumn {
        common::OutputColumn {
            column_id,
            name: name.to_string(),
            r#type: Some(type_desc(&data_type)),
            nullable,
            is_internal: false,
        }
    }

    pub(super) fn output_column(
        column_id: u32,
        name: &str,
        data_type: DataType,
    ) -> common::OutputColumn {
        output_column_with_nullable(column_id, name, data_type, true)
    }

    fn int_literal(value: i64) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Int64)),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::IntValue(value)),
                }),
            })),
        }
    }

    pub(super) fn string_literal(value: &str) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Utf8)),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::StringValue(value.to_string())),
                }),
            })),
        }
    }

    pub(super) fn bool_literal(value: bool) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Boolean)),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::BoolValue(value)),
                }),
            })),
        }
    }

    fn null_literal(data_type: DataType) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::NullValue(true)),
                }),
            })),
        }
    }

    pub(super) fn column_ref(column_id: u32, data_type: DataType) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
                column_id,
                qualifier: None,
                column: None,
            })),
        }
    }

    pub(super) fn sort_item(column_id: u32) -> expr::SortItem {
        expr::SortItem {
            expr: Some(column_ref(column_id, DataType::Int64)),
            asc: true,
            nulls_first: false,
        }
    }

    pub(super) fn physical_node(
        node_id: i32,
        kind: plan::plan_node::Kind,
        output_columns: Vec<common::OutputColumn>,
        children: Vec<plan::DistributedNode>,
    ) -> plan::DistributedNode {
        plan::DistributedNode {
            node_id,
            fragment_id: 1,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children,
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns,
                kind: Some(kind),
            })),
        }
    }

    pub(super) fn values_node(node_id: i32) -> plan::DistributedNode {
        let columns = vec![
            output_column(1, "id", DataType::Int64),
            output_column(2, "name", DataType::Utf8),
        ];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![
                    plan::ExprList {
                        values: vec![int_literal(10), string_literal("alice")],
                    },
                    plan::ExprList {
                        values: vec![int_literal(20), string_literal("bob")],
                    },
                ],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    pub(super) fn one_col_values_node(node_id: i32) -> plan::DistributedNode {
        one_col_values_node_with(node_id, 1, "id", 10)
    }

    pub(super) fn one_col_values_node_with(
        node_id: i32,
        column_id: u32,
        name: &str,
        value: i64,
    ) -> plan::DistributedNode {
        one_col_values_node_with_nullable(node_id, column_id, name, value, true)
    }

    pub(super) fn one_col_values_node_with_nullable(
        node_id: i32,
        column_id: u32,
        name: &str,
        value: i64,
        nullable: bool,
    ) -> plan::DistributedNode {
        let columns = vec![output_column_with_nullable(
            column_id,
            name,
            DataType::Int64,
            nullable,
        )];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![int_literal(value)],
                }],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    fn one_col_values_node_typed(
        node_id: i32,
        column_id: u32,
        name: &str,
        value: i64,
        data_type: DataType,
    ) -> plan::DistributedNode {
        let columns = vec![output_column(column_id, name, data_type)];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![int_literal(value)],
                }],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    pub(super) fn two_col_values_node(node_id: i32) -> plan::DistributedNode {
        let columns = vec![
            output_column(1, "a", DataType::Int64),
            output_column(2, "b", DataType::Int64),
        ];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![int_literal(10), int_literal(20)],
                }],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    pub(super) fn three_col_values_node(node_id: i32) -> plan::DistributedNode {
        let columns = vec![
            output_column(1, "a", DataType::Int64),
            output_column(2, "b", DataType::Int64),
            output_column(3, "c", DataType::Int64),
        ];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![int_literal(10), int_literal(20), int_literal(30)],
                }],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    pub(super) fn lower(node: &plan::DistributedNode) -> super::DecodedNode {
        let mut arena = ExprArena::default();
        decode_node(node, &mut arena, &NativePlanDecodeContext::default()).expect("lower node")
    }

    fn decode_error(node: &plan::DistributedNode) -> super::super::NativeFragmentDecodeError {
        decode_node(
            node,
            &mut ExprArena::default(),
            &NativePlanDecodeContext::default(),
        )
        .expect_err("invalid node must fail")
    }

    fn assert_children_error(node: &plan::DistributedNode) {
        let error = decode_error(node);
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(protocol.path().to_string(), "plan_fragment.root.children");
        assert_eq!(
            protocol.kind(),
            crate::protocol::common::error::ProtocolErrorKind::InconsistentFields
        );
    }

    fn dormant_consumer(
        binding_id: u32,
        node_id: i32,
        column_id: u32,
    ) -> DecodedRuntimeFilterBinding {
        DecodedRuntimeFilterBinding {
            binding_id,
            channel_id: binding_id + 10,
            node_id,
            apply_point: super::super::runtime_filter::DecodedApplyPoint::NodeInput,
            expression: column_ref(column_id, DataType::Int64),
            expression_path: FieldPath::root("test_runtime_filter_binding").field("expression"),
            role: DecodedBindingRole::Consumer {
                capabilities: BTreeSet::from([
                    crate::runtime_filter::model::contract::ArtifactCapability::Membership,
                    crate::runtime_filter::model::contract::ArtifactCapability::EmptyDomain,
                ]),
                activation:
                    crate::runtime_filter::model::contract::ConsumerActivation::BlockingSnapshot,
                target: DecodedConsumerBindingTarget::DirectInput { input_ordinal: 0 },
            },
            contract: DecodedRuntimeFilterContract::Membership {
                canonical_schema: Arc::from([]),
                schema_digest: [0; 32],
            },
            reduction: DecodedRuntimeFilterReduction::SetUnion,
        }
    }

    fn dormant_source_consumer(
        binding_id: u32,
        node_id: i32,
        column_id: u32,
    ) -> DecodedRuntimeFilterBinding {
        let mut binding = dormant_consumer(binding_id, node_id, column_id);
        let DecodedBindingRole::Consumer { target, .. } = &mut binding.role else {
            unreachable!("dormant_consumer always returns a consumer")
        };
        *target = DecodedConsumerBindingTarget::SourceBoundary;
        binding
    }

    fn membership_producer_wire(
        binding_id: u32,
        node_id: i32,
        expression: expr::Expr,
        data_type: &DataType,
    ) -> plan::RuntimeFilterBinding {
        membership_producer_wire_at(binding_id, node_id, expression, data_type, 0)
    }

    fn membership_producer_wire_at(
        binding_id: u32,
        node_id: i32,
        expression: expr::Expr,
        data_type: &DataType,
        join_key_ordinal: u32,
    ) -> plan::RuntimeFilterBinding {
        let schema = crate::runtime_filter::port::artifact::ArtifactMembershipSchema::new(
            data_type,
            crate::runtime_filter::model::contract::NullSemantics::NeverMatches,
        )
        .expect("membership schema");
        plan::RuntimeFilterBinding {
            binding_id,
            channel_id: binding_id + 10,
            node_id,
            apply_point: i32::from(plan::RuntimeFilterApplyPoint::NodeOutput),
            expression: Some(expression),
            contract: Some(plan::RuntimeFilterContract {
                kind: Some(plan::runtime_filter_contract::Kind::Membership(
                    plan::RuntimeFilterMembershipContract {
                        canonical_schema: schema.canonical_bytes().to_vec(),
                        schema_digest: schema.digest().bytes().to_vec(),
                    },
                )),
            }),
            reduction: Some(plan::RuntimeFilterReductionContract {
                kind: Some(plan::runtime_filter_reduction_contract::Kind::SetUnion(
                    true,
                )),
            }),
            role: Some(plan::runtime_filter_binding::Role::Producer(
                plan::RuntimeFilterProducerRole {
                    contribution_kinds: vec![
                        i32::from(plan::RuntimeFilterContributionKind::ValueDomainDelta),
                        i32::from(plan::RuntimeFilterContributionKind::ProducerClosed),
                    ],
                    completion_requirement: i32::from(
                        plan::RuntimeFilterCompletionRequirement::ProducerClosed,
                    ),
                    join_key_ordinal: Some(join_key_ordinal),
                },
            )),
        }
    }

    fn membership_consumer_wire(
        binding_id: u32,
        node_id: i32,
        expression: expr::Expr,
        data_type: &DataType,
    ) -> plan::RuntimeFilterBinding {
        let schema = crate::runtime_filter::port::artifact::ArtifactMembershipSchema::new(
            data_type,
            crate::runtime_filter::model::contract::NullSemantics::NeverMatches,
        )
        .expect("membership schema");
        plan::RuntimeFilterBinding {
            binding_id,
            channel_id: binding_id + 10,
            node_id,
            apply_point: i32::from(plan::RuntimeFilterApplyPoint::NodeInput),
            expression: Some(expression),
            contract: Some(plan::RuntimeFilterContract {
                kind: Some(plan::runtime_filter_contract::Kind::Membership(
                    plan::RuntimeFilterMembershipContract {
                        canonical_schema: schema.canonical_bytes().to_vec(),
                        schema_digest: schema.digest().bytes().to_vec(),
                    },
                )),
            }),
            reduction: Some(plan::RuntimeFilterReductionContract {
                kind: Some(plan::runtime_filter_reduction_contract::Kind::SetUnion(
                    true,
                )),
            }),
            role: Some(plan::runtime_filter_binding::Role::Consumer(
                plan::RuntimeFilterConsumerRole {
                    capabilities: vec![
                        i32::from(plan::RuntimeFilterArtifactCapability::Membership),
                        i32::from(plan::RuntimeFilterArtifactCapability::EmptyDomain),
                    ],
                    activation: Some(plan::RuntimeFilterConsumerActivation {
                        kind: Some(
                            plan::runtime_filter_consumer_activation::Kind::BlockingSnapshot(true),
                        ),
                    }),
                    target: Some(plan::runtime_filter_consumer_role::Target::SourceBoundary(
                        true,
                    )),
                },
            )),
        }
    }

    fn cast_expr(operand: expr::Expr, data_type: DataType, nullable: bool) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&data_type)),
            nullable,
            kind: Some(expr::expr::Kind::Cast(Box::new(expr::CastExpr {
                operand: Some(Box::new(operand)),
                target: Some(type_desc(&data_type)),
            }))),
        }
    }

    #[test]
    fn runtime_filter_expression_missing_binary_left_uses_exact_path_and_kind() {
        let expression = expr::Expr {
            r#type: Some(type_desc(&DataType::Boolean)),
            nullable: false,
            kind: Some(expr::expr::Kind::BinaryOp(Box::new(expr::BinaryOpExpr {
                op: expr::BinaryOp::Eq as i32,
                left: None,
                right: Some(Box::new(column_ref(1, DataType::Int64))),
            }))),
        };
        let lowered = lower(&one_col_values_node(10));

        let error = validate_column_refs_exact(
            1,
            &expression,
            &lowered.layout,
            &lowered.output_schema,
            FieldPath::root("runtime_filter_expression"),
        )
        .expect_err("missing binary left must fail");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "runtime_filter_expression.binary_op.left"
        );
        assert_eq!(
            protocol.kind(),
            crate::protocol::common::error::ProtocolErrorKind::MissingField
        );
    }

    #[test]
    fn consumer_expression_failure_uses_binding_table_wire_path() {
        let mut wire = one_col_values_node(10);
        wire.runtime_filter_binding_ids = vec![7];
        let table = plan::RuntimeFilterBindingTable {
            fragment_id: 1,
            bindings: vec![membership_consumer_wire(
                7,
                10,
                column_ref(999, DataType::Int64),
                &DataType::Int64,
            )],
        };
        let mut ledger = NativeRuntimeFilterDecodeLedger::decode(1, Some(&table))
            .expect("decode consumer table");

        let error = decode_node_with_runtime_filters(
            &wire,
            &mut ExprArena::default(),
            &NativePlanDecodeContext::default(),
            &mut ledger,
        )
        .expect_err("unknown consumer column must fail");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.runtime_filter_bindings.bindings[0].expression.column_ref.column_id"
        );
        assert_eq!(
            protocol.kind(),
            crate::protocol::common::error::ProtocolErrorKind::InvalidValue
        );
    }

    #[test]
    fn representative_node_arities_use_distributed_children_path_and_kind() {
        let values_with_child = physical_node(
            20,
            plan::plan_node::Kind::Values(plan::ValuesNode::default()),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        assert_children_error(&values_with_child);

        let sort_without_child = physical_node(
            20,
            plan::plan_node::Kind::Sort(plan::SortNode::default()),
            Vec::new(),
            Vec::new(),
        );
        assert_children_error(&sort_without_child);

        let join_with_one_child = physical_node(
            20,
            plan::plan_node::Kind::HashJoin(plan::HashJoinNode::default()),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        assert_children_error(&join_with_one_child);

        let set_op_with_one_child = physical_node(
            20,
            plan::plan_node::Kind::SetOp(plan::SetOpNode::default()),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        assert_children_error(&set_op_with_one_child);

        let exchange_with_child = plan::DistributedNode {
            node_id: 20,
            fragment_id: 1,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: vec![one_col_values_node(10)],
            payload: Some(plan::distributed_node::Payload::Exchange(
                plan::ExchangeReceiver::default(),
            )),
        };
        assert_children_error(&exchange_with_child);
    }

    #[test]
    fn producer_raw_build_mismatch_uses_join_expression_wire_path() {
        for (join_kind, build_on_left, expected_side) in [
            (plan::JoinKind::Inner, false, "right"),
            (plan::JoinKind::RightSemi, true, "left"),
        ] {
            let mut raw_build = if build_on_left {
                column_ref(1, DataType::Int64)
            } else {
                column_ref(2, DataType::Int64)
            };
            raw_build.nullable = false;
            let left_expr = if build_on_left {
                raw_build.clone()
            } else {
                column_ref(1, DataType::Int64)
            };
            let right_expr = if build_on_left {
                column_ref(2, DataType::Int64)
            } else {
                raw_build.clone()
            };
            let mut wire = physical_node(
                30,
                plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                    join_type: i32::from(join_kind),
                    eq_conditions: vec![plan::HashJoinEqCondition {
                        left: Some(left_expr),
                        right: Some(right_expr),
                        null_safe: false,
                    }],
                    other_condition: None,
                    distribution: i32::from(plan::JoinDistribution::Broadcast),
                    execution_mode: None,
                }),
                Vec::new(),
                vec![
                    one_col_values_node_with(10, 1, "lhs", 10),
                    one_col_values_node_with(11, 2, "rhs", 20),
                ],
            );
            wire.runtime_filter_binding_ids = vec![1];
            let table = plan::RuntimeFilterBindingTable {
                fragment_id: 1,
                bindings: vec![membership_producer_wire(1, 30, raw_build, &DataType::Int64)],
            };
            let mut ledger = NativeRuntimeFilterDecodeLedger::decode(1, Some(&table))
                .expect("decode producer table");
            let error = decode_node_with_runtime_filters(
                &wire,
                &mut ExprArena::default(),
                &NativePlanDecodeContext::default(),
                &mut ledger,
            )
            .expect_err("raw build nullability mismatch must fail");
            let protocol = error.protocol().expect("protocol error");
            assert_eq!(
                protocol.path().to_string(),
                format!(
                    "plan_fragment.root.payload.physical.hash_join.eq_conditions[0].{expected_side}.column_ref"
                )
            );
            assert_eq!(
                protocol.kind(),
                crate::protocol::common::error::ProtocolErrorKind::InconsistentFields
            );
        }
    }

    #[test]
    fn producer_lookup_is_validated_and_consumed_by_interim_dormant_seam() {
        let left_wire = one_col_values_node_with(10, 1, "lhs", 10);
        let right_wire = one_col_values_node_with(11, 2, "rhs", 20);
        let mut wire = physical_node(
            30,
            plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                join_type: i32::from(plan::JoinKind::Inner),
                eq_conditions: vec![plan::HashJoinEqCondition {
                    left: Some(column_ref(1, DataType::Int64)),
                    right: Some(column_ref(2, DataType::Int64)),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: i32::from(plan::JoinDistribution::Broadcast),
                execution_mode: None,
            }),
            Vec::new(),
            vec![left_wire.clone(), right_wire.clone()],
        );
        wire.runtime_filter_binding_ids = vec![1];
        let table = plan::RuntimeFilterBindingTable {
            fragment_id: 1,
            bindings: vec![membership_producer_wire(
                1,
                30,
                column_ref(2, DataType::Int64),
                &DataType::Int64,
            )],
        };
        let mut ledger = NativeRuntimeFilterDecodeLedger::decode(1, Some(&table))
            .expect("decode producer table");
        let mut arena = ExprArena::default();
        let lowered = decode_node_with_runtime_filters(
            &wire,
            &mut arena,
            &NativePlanDecodeContext::default(),
            &mut ledger,
        )
        .expect("producer seam");
        ledger.finish().expect("producer binding consumed");
        let ExecNodeKind::Join(join) = lowered.node.kind else {
            panic!("producer seam")
        };
        let JoinRuntimeFilterExecution::Native { producers } = join.runtime_filter_execution else {
            panic!("native producer execution")
        };
        assert_eq!(producers.len(), 1);

        let mut nullable_mismatch = column_ref(2, DataType::Int64);
        nullable_mismatch.nullable = false;
        let invalid_table = plan::RuntimeFilterBindingTable {
            fragment_id: 1,
            bindings: vec![membership_producer_wire(
                1,
                30,
                nullable_mismatch,
                &DataType::Int64,
            )],
        };
        let mut ledger = NativeRuntimeFilterDecodeLedger::decode(1, Some(&invalid_table))
            .expect("decode invalid lowering table");
        assert!(
            decode_node_with_runtime_filters(
                &wire,
                &mut ExprArena::default(),
                &NativePlanDecodeContext::default(),
                &mut ledger,
            )
            .is_err()
        );
        assert!(
            ledger.finish().is_err(),
            "failed lowering must not consume binding"
        );
    }

    #[test]
    fn producer_matches_and_references_once_lowered_raw_build_expression() {
        let left = one_col_values_node_typed(10, 1, "lhs", 10, DataType::Int64);
        let right = one_col_values_node_typed(11, 2, "rhs", 20, DataType::Int32);
        let mut wire = physical_node(
            30,
            plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                join_type: i32::from(plan::JoinKind::Inner),
                eq_conditions: vec![plan::HashJoinEqCondition {
                    left: Some(column_ref(1, DataType::Int64)),
                    right: Some(column_ref(2, DataType::Int32)),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: i32::from(plan::JoinDistribution::Broadcast),
                execution_mode: None,
            }),
            Vec::new(),
            vec![left, right],
        );
        wire.runtime_filter_binding_ids = vec![1];
        let table = plan::RuntimeFilterBindingTable {
            fragment_id: 1,
            bindings: vec![membership_producer_wire(
                1,
                30,
                column_ref(2, DataType::Int32),
                &DataType::Int32,
            )],
        };
        let mut ledger = NativeRuntimeFilterDecodeLedger::decode(1, Some(&table))
            .expect("decode producer table");
        let mut arena = ExprArena::default();
        let lowered = decode_node_with_runtime_filters(
            &wire,
            &mut arena,
            &NativePlanDecodeContext::default(),
            &mut ledger,
        )
        .expect("coerced producer seam");
        ledger.finish().expect("producer binding consumed");
        let ExecNodeKind::Join(join) = lowered.node.kind else {
            panic!("producer seam")
        };
        let JoinRuntimeFilterExecution::Native { producers } = join.runtime_filter_execution else {
            panic!("native producer execution")
        };
        let build_expr_id = producers[0].build_expr_id;
        assert_eq!(arena.data_type(build_expr_id), Some(&DataType::Int32));
        assert!(matches!(
            arena.node(build_expr_id),
            Some(crate::exec::expr::ExprNode::SlotId(_))
        ));
    }

    #[test]
    fn distinct_producer_bindings_may_share_one_unique_raw_build_key() {
        let left = one_col_values_node_with(10, 1, "lhs", 10);
        let right = one_col_values_node_with(11, 2, "rhs", 20);
        let mut wire = physical_node(
            30,
            plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                join_type: i32::from(plan::JoinKind::Inner),
                eq_conditions: vec![plan::HashJoinEqCondition {
                    left: Some(column_ref(1, DataType::Int64)),
                    right: Some(column_ref(2, DataType::Int64)),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: i32::from(plan::JoinDistribution::Broadcast),
                execution_mode: None,
            }),
            Vec::new(),
            vec![left, right],
        );
        wire.runtime_filter_binding_ids = vec![1, 2];
        let table = plan::RuntimeFilterBindingTable {
            fragment_id: 1,
            bindings: vec![
                membership_producer_wire(1, 30, column_ref(2, DataType::Int64), &DataType::Int64),
                membership_producer_wire(2, 30, column_ref(2, DataType::Int64), &DataType::Int64),
            ],
        };
        let mut ledger = NativeRuntimeFilterDecodeLedger::decode(1, Some(&table))
            .expect("decode shared-key producer table");
        let lowered = decode_node_with_runtime_filters(
            &wire,
            &mut ExprArena::default(),
            &NativePlanDecodeContext::default(),
            &mut ledger,
        )
        .expect("two channels may bind the same unique raw key");
        ledger.finish().expect("both producer bindings consumed");
        let ExecNodeKind::Join(join) = lowered.node.kind else {
            panic!("producer seam")
        };
        let JoinRuntimeFilterExecution::Native { producers } = join.runtime_filter_execution else {
            panic!("native producer execution")
        };
        assert_eq!(
            producers
                .iter()
                .map(|binding| binding.binding_id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn duplicate_raw_build_keys_are_disambiguated_by_join_key_ordinal() {
        let left = one_col_values_node_with(10, 1, "lhs", 10);
        let right = one_col_values_node_with(11, 2, "rhs", 20);
        let condition = plan::HashJoinEqCondition {
            left: Some(column_ref(1, DataType::Int64)),
            right: Some(column_ref(2, DataType::Int64)),
            null_safe: false,
        };
        let mut wire = physical_node(
            30,
            plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                join_type: i32::from(plan::JoinKind::Inner),
                eq_conditions: vec![condition.clone(), condition],
                other_condition: None,
                distribution: i32::from(plan::JoinDistribution::Broadcast),
                execution_mode: None,
            }),
            Vec::new(),
            vec![left, right],
        );
        wire.runtime_filter_binding_ids = vec![1, 2];
        let table = plan::RuntimeFilterBindingTable {
            fragment_id: 1,
            bindings: vec![
                membership_producer_wire_at(
                    1,
                    30,
                    column_ref(2, DataType::Int64),
                    &DataType::Int64,
                    0,
                ),
                membership_producer_wire_at(
                    2,
                    30,
                    column_ref(2, DataType::Int64),
                    &DataType::Int64,
                    1,
                ),
            ],
        };
        let mut ledger = NativeRuntimeFilterDecodeLedger::decode(1, Some(&table))
            .expect("decode duplicate-key producer table");
        let lowered = decode_node_with_runtime_filters(
            &wire,
            &mut ExprArena::default(),
            &NativePlanDecodeContext::default(),
            &mut ledger,
        )
        .expect("exact ordinals disambiguate duplicate raw build keys");
        ledger.finish().expect("both producer bindings consumed");
        let ExecNodeKind::Join(join) = lowered.node.kind else {
            panic!("producer seam")
        };
        let JoinRuntimeFilterExecution::Native { producers } = join.runtime_filter_execution else {
            panic!("native producer execution")
        };
        assert_eq!(
            producers
                .iter()
                .map(|binding| binding.build_key_index)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
    }

    #[test]
    fn producer_rejects_nested_intermediate_nullability_mismatch() {
        let raw_build = cast_expr(
            cast_expr(column_ref(2, DataType::Int64), DataType::Int64, true),
            DataType::Int64,
            true,
        );
        let left = one_col_values_node_with(10, 1, "lhs", 10);
        let right = one_col_values_node_with(11, 2, "rhs", 20);
        let mut wire = physical_node(
            30,
            plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                join_type: i32::from(plan::JoinKind::Inner),
                eq_conditions: vec![plan::HashJoinEqCondition {
                    left: Some(column_ref(1, DataType::Int64)),
                    right: Some(raw_build.clone()),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: i32::from(plan::JoinDistribution::Broadcast),
                execution_mode: None,
            }),
            Vec::new(),
            vec![left, right],
        );
        wire.runtime_filter_binding_ids = vec![1];
        let mut mismatched = raw_build;
        let Some(expr::expr::Kind::Cast(outer)) = mismatched.kind.as_mut() else {
            panic!("outer cast")
        };
        let inner = outer.operand.as_mut().expect("inner cast");
        inner.nullable = false;
        let table = plan::RuntimeFilterBindingTable {
            fragment_id: 1,
            bindings: vec![membership_producer_wire(
                1,
                30,
                mismatched,
                &DataType::Int64,
            )],
        };
        let mut ledger = NativeRuntimeFilterDecodeLedger::decode(1, Some(&table))
            .expect("decode nested mismatch table");
        assert!(
            decode_node_with_runtime_filters(
                &wire,
                &mut ExprArena::default(),
                &NativePlanDecodeContext::default(),
                &mut ledger,
            )
            .is_err(),
            "nested intermediate nullability drift must not match"
        );
        assert!(
            ledger.finish().is_err(),
            "failed match must remain unconsumed"
        );
    }

    #[test]
    fn scan_binding_uses_leaf_local_native_consumer_spec() {
        let wire = physical_node(
            10,
            plan::plan_node::Kind::Scan(plan::ScanNode::default()),
            vec![output_column(1, "v", DataType::Int64)],
            Vec::new(),
        );
        let baseline = lower(&one_col_values_node(10));
        let mut lowered = DecodedNode {
            node: ExecNode {
                kind: ExecNodeKind::Scan(crate::exec::node::scan::ScanNode::new(Arc::new(
                    DummyScanOp,
                ))),
            },
            layout: baseline.layout,
            output_schema: baseline.output_schema,
        };
        attach_leaf_consumers(
            &wire,
            &[dormant_source_consumer(1, 10, 1)],
            &mut lowered,
            &mut ExprArena::default(),
            FieldPath::root("test_node"),
        )
        .expect("scan leaf binding");
        let ExecNodeKind::Scan(scan) = &lowered.node.kind else {
            panic!("scan")
        };
        assert_eq!(scan.native_runtime_filter_specs().len(), 1);
        assert!(scan.runtime_filter_specs().is_empty());
    }

    #[test]
    fn exchange_binding_uses_leaf_local_native_consumer_spec() {
        let baseline = lower(&one_col_values_node(10));
        let mut wire = physical_node(
            10,
            plan::plan_node::Kind::Values(plan::ValuesNode::default()),
            Vec::new(),
            Vec::new(),
        );
        wire.payload = Some(plan::distributed_node::Payload::Exchange(
            plan::ExchangeReceiver::default(),
        ));
        let mut lowered = DecodedNode {
            node: ExecNode {
                kind: ExecNodeKind::ExchangeSource(
                    crate::exec::node::exchange_source::ExchangeSourceNode::new(
                        crate::runtime::exchange::ExchangeKey {
                            finst_id_hi: 1,
                            finst_id_lo: 2,
                            node_id: 3,
                        },
                        1,
                        std::time::Duration::from_secs(1),
                        Arc::clone(&baseline.output_schema),
                    ),
                ),
            },
            layout: baseline.layout,
            output_schema: baseline.output_schema,
        };
        attach_leaf_consumers(
            &wire,
            &[dormant_source_consumer(1, 10, 1)],
            &mut lowered,
            &mut ExprArena::default(),
            FieldPath::root("test_node"),
        )
        .expect("exchange leaf binding");
        let ExecNodeKind::ExchangeSource(exchange) = &lowered.node.kind else {
            panic!("exchange")
        };
        assert_eq!(exchange.native_runtime_filter_specs().len(), 1);
        assert!(exchange.runtime_filter_specs().is_empty());
    }

    #[test]
    fn unary_node_wraps_only_its_direct_input() {
        let mut children = vec![lower(&one_col_values_node(10))];
        let mut arena = ExprArena::default();
        attach_direct_input_consumers(
            20,
            &[dormant_consumer(1, 20, 1)],
            &mut children,
            &mut arena,
            FieldPath::root("test_node"),
        )
        .expect("attach");
        let ExecNodeKind::NativeRuntimeFilterConsumer(consumer) = &children[0].node.kind else {
            panic!("consumer wrapper")
        };
        assert_eq!(consumer.owner_node_id, 20);
        assert!(matches!(consumer.input.kind, ExecNodeKind::Values(_)));
    }

    #[test]
    fn multi_input_requires_exactly_one_matching_direct_input() {
        let mut children = vec![
            lower(&one_col_values_node_with(10, 1, "left", 1)),
            lower(&one_col_values_node_with(11, 2, "right", 2)),
        ];
        let mut arena = ExprArena::default();
        attach_direct_input_consumers(
            20,
            &[dormant_consumer(1, 20, 1)],
            &mut children,
            &mut arena,
            FieldPath::root("test_node"),
        )
        .expect("unique left input");
        assert!(matches!(
            children[0].node.kind,
            ExecNodeKind::NativeRuntimeFilterConsumer(_)
        ));
        assert!(matches!(children[1].node.kind, ExecNodeKind::Values(_)));

        let mut missing_input = dormant_consumer(2, 20, 1);
        let DecodedBindingRole::Consumer { target, .. } = &mut missing_input.role else {
            unreachable!("consumer")
        };
        *target = DecodedConsumerBindingTarget::DirectInput { input_ordinal: 2 };
        let mut children = vec![
            lower(&one_col_values_node(10)),
            lower(&one_col_values_node(11)),
        ];
        assert!(
            attach_direct_input_consumers(
                20,
                &[missing_input],
                &mut children,
                &mut ExprArena::default(),
                FieldPath::root("test_node"),
            )
            .is_err()
        );

        let mut children = vec![
            lower(&one_col_values_node_with(10, 1, "left", 1)),
            lower(&one_col_values_node_with(11, 2, "right", 2)),
        ];
        assert!(
            attach_direct_input_consumers(
                20,
                &[dormant_consumer(3, 20, 3)],
                &mut children,
                &mut ExprArena::default(),
                FieldPath::root("test_node"),
            )
            .is_err()
        );
    }

    #[test]
    fn filter_binding_does_not_move_to_scan_without_scan_binding() {
        let baseline = lower(&one_col_values_node(10));
        let mut children = vec![DecodedNode {
            node: ExecNode {
                kind: ExecNodeKind::Scan(crate::exec::node::scan::ScanNode::new(Arc::new(
                    DummyScanOp,
                ))),
            },
            layout: baseline.layout,
            output_schema: baseline.output_schema,
        }];
        attach_direct_input_consumers(
            20,
            &[dormant_consumer(1, 20, 1)],
            &mut children,
            &mut ExprArena::default(),
            FieldPath::root("test_node"),
        )
        .expect("filter input boundary");
        let ExecNodeKind::NativeRuntimeFilterConsumer(consumer) = &children[0].node.kind else {
            panic!("exact filter input wrapper")
        };
        let ExecNodeKind::Scan(scan) = &consumer.input.kind else {
            panic!("scan remains the direct input")
        };
        assert!(scan.native_runtime_filter_specs().is_empty());
        assert!(scan.runtime_filter_specs().is_empty());
    }

    #[test]
    fn values_binding_wraps_the_source_boundary() {
        let wire = one_col_values_node(10);
        let mut lowered = lower(&wire);
        attach_leaf_consumers(
            &wire,
            &[dormant_source_consumer(1, 10, 1)],
            &mut lowered,
            &mut ExprArena::default(),
            FieldPath::root("test_node"),
        )
        .expect("values source boundary");
        let ExecNodeKind::NativeRuntimeFilterConsumer(consumer) = lowered.node.kind else {
            panic!("consumer")
        };
        assert!(matches!(consumer.input.kind, ExecNodeKind::Values(_)));
    }

    #[test]
    fn generate_series_binding_wraps_the_source_boundary() {
        let wire = physical_node(
            10,
            plan::plan_node::Kind::GenerateSeries(plan::GenerateSeriesNode {
                start: 1,
                end: 3,
                step: 1,
                output_column_id: 1,
                column_name: "v".to_string(),
                alias: None,
            }),
            vec![output_column_with_nullable(1, "v", DataType::Int64, false)],
            Vec::new(),
        );
        let mut lowered = lower(&wire);
        let mut binding = dormant_source_consumer(1, 10, 1);
        binding.expression.nullable = false;
        attach_leaf_consumers(
            &wire,
            &[binding],
            &mut lowered,
            &mut ExprArena::default(),
            FieldPath::root("test_node"),
        )
        .expect("generate series source boundary");
        let ExecNodeKind::NativeRuntimeFilterConsumer(consumer) = lowered.node.kind else {
            panic!("consumer")
        };
        assert!(matches!(
            consumer.input.kind,
            ExecNodeKind::TableFunction(_)
        ));
    }

    #[test]
    fn unsupported_leaf_capability_fails_before_execution() {
        let wire = physical_node(
            10,
            plan::plan_node::Kind::Decode(plan::DecodeNode::default()),
            Vec::new(),
            Vec::new(),
        );
        let mut lowered = lower(&one_col_values_node(10));
        assert!(
            attach_leaf_consumers(
                &wire,
                &[dormant_source_consumer(1, 10, 1)],
                &mut lowered,
                &mut ExprArena::default(),
                FieldPath::root("test_node"),
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_scan_without_context_and_union_distinct() {
        let scan = physical_node(
            50,
            plan::plan_node::Kind::Scan(plan::ScanNode::default()),
            Vec::new(),
            Vec::new(),
        );
        let mut arena = ExprArena::default();
        let err = decode_node(&scan, &mut arena, &NativePlanDecodeContext::default()).unwrap_err();
        assert!(err.contains("Scan"));
        assert!(err.contains("table missing"));

        let union_distinct = physical_node(
            60,
            plan::plan_node::Kind::SetOp(plan::SetOpNode {
                kind: plan::PlanSetOpKind::UnionDistinct as i32,
                output_columns: vec![output_column(1, "id", DataType::Int64)],
                child_output_columns: Vec::new(),
            }),
            Vec::new(),
            vec![one_col_values_node(10), one_col_values_node(11)],
        );
        let err = decode_node(
            &union_distinct,
            &mut arena,
            &NativePlanDecodeContext::default(),
        )
        .unwrap_err();
        assert!(err.contains("UnionDistinct"));
        assert!(err.contains("not implemented"));
    }

    #[test]
    fn lowers_union_all_intersect_except_and_assert_one_row() {
        let output_columns = vec![output_column(1, "id", DataType::Int64)];
        let union_all = physical_node(
            60,
            plan::plan_node::Kind::SetOp(plan::SetOpNode {
                kind: plan::PlanSetOpKind::UnionAll as i32,
                output_columns: output_columns.clone(),
                child_output_columns: Vec::new(),
            }),
            output_columns.clone(),
            vec![one_col_values_node(10), one_col_values_node(11)],
        );
        let lowered = lower(&union_all);
        assert!(matches!(lowered.node.kind, ExecNodeKind::UnionAll(_)));

        for (kind, expected) in [
            (plan::PlanSetOpKind::Intersect, SetOpKind::Intersect),
            (plan::PlanSetOpKind::Except, SetOpKind::Except),
        ] {
            let set_op = physical_node(
                61,
                plan::plan_node::Kind::SetOp(plan::SetOpNode {
                    kind: kind as i32,
                    output_columns: output_columns.clone(),
                    child_output_columns: Vec::new(),
                }),
                output_columns.clone(),
                vec![one_col_values_node(10), one_col_values_node(11)],
            );
            let lowered = lower(&set_op);
            let ExecNodeKind::SetOp(set_op) = lowered.node.kind else {
                panic!("expected SetOp");
            };
            assert_eq!(
                std::mem::discriminant(&set_op.kind),
                std::mem::discriminant(&expected)
            );
            assert_eq!(set_op.output_chunk_schema.slot_ids(), &[SlotId::new(1)]);
        }

        let assert_one_row = physical_node(
            70,
            plan::plan_node::Kind::AssertOneRow(plan::AssertOneRowNode {
                subquery_text: "select id from t".to_string(),
                desired_num_rows: Some(1),
                assertion: plan::RowCountAssertion::Le as i32,
                group_key_column_ids: Vec::new(),
                group_key_labels: Vec::new(),
                keyed_message_prefix: None,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&assert_one_row);
        let ExecNodeKind::AssertNumRows(assert) = lowered.node.kind else {
            panic!("expected AssertNumRows");
        };
        match assert.mode {
            AssertNumRowsMode::Global {
                desired_num_rows,
                assertion,
                subquery_string,
            } => {
                assert_eq!(desired_num_rows, Some(1));
                assert!(matches!(assertion, Assertion::Le));
                assert_eq!(subquery_string.as_deref(), Some("select id from t"));
            }
            AssertNumRowsMode::PerKeyAtMostOne { .. } => panic!("expected global assert"),
        }
    }

    #[test]
    fn lowers_hash_aggregate_and_join_shapes() {
        let output_columns = vec![
            output_column(1, "id", DataType::Int64),
            output_column(2, "cnt", DataType::Int64),
        ];
        let aggregate = physical_node(
            20,
            plan::plan_node::Kind::HashAggregate(plan::HashAggregateNode {
                mode: plan::AggMode::Single as i32,
                group_by: vec![column_ref(1, DataType::Int64)],
                aggregates: vec![plan::PlanAggregateCall {
                    name: "count".to_string(),
                    args: Vec::new(),
                    distinct: false,
                    result_type: Some(type_desc(&DataType::Int64)),
                    order_by: Vec::new(),
                    output_column_id: 2,
                }],
                is_merge: vec![false],
                output_layout: Some(plan::AggregateOutputLayout {
                    group_key_columns: vec![output_columns[0].clone()],
                    aggregate_columns: vec![output_columns[1].clone()],
                }),
                output_columns: output_columns.clone(),
            }),
            output_columns,
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&aggregate);
        let ExecNodeKind::Aggregate(aggregate) = lowered.node.kind else {
            panic!("expected Aggregate");
        };
        assert_eq!(aggregate.node_id, 20);
        assert_eq!(aggregate.group_by.len(), 1);
        assert_eq!(aggregate.functions.len(), 1);
        assert!(aggregate.need_finalize);
        assert_eq!(
            aggregate.output_chunk_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );

        let join = physical_node(
            30,
            plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                join_type: plan::JoinKind::Inner as i32,
                eq_conditions: vec![plan::HashJoinEqCondition {
                    left: Some(column_ref(1, DataType::Int64)),
                    right: Some(column_ref(2, DataType::Int64)),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: plan::JoinDistribution::Broadcast as i32,
                execution_mode: None,
            }),
            Vec::new(),
            vec![
                one_col_values_node_with(10, 1, "lhs", 10),
                one_col_values_node_with(11, 2, "rhs", 10),
            ],
        );
        let lowered = lower(&join);
        let ExecNodeKind::Join(join) = lowered.node.kind else {
            panic!("expected Join");
        };
        assert_eq!(join.probe_keys.len(), 1);
        assert_eq!(join.build_keys.len(), 1);
        assert_eq!(join.eq_null_safe, vec![false]);
        assert_eq!(
            join.join_scope_chunk_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );
        assert!(matches!(
            join.join_type,
            crate::exec::node::join::JoinType::Inner
        ));
    }

    #[test]
    fn lowers_repeat_change_event_and_redistribute_shapes() {
        let repeat = physical_node(
            20,
            plan::plan_node::Kind::Repeat(plan::RepeatNode {
                repeat_column_ref_list: Vec::new(),
                repeat_column_ref_ids: vec![
                    plan::UInt32List { values: vec![1] },
                    plan::UInt32List { values: Vec::new() },
                ],
                grouping_ids: vec![0, 1],
                all_rollup_columns: vec!["id".to_string()],
                all_rollup_column_ids: vec![1],
                grouping_key_aliases: Vec::new(),
                grouping_fn_args: Vec::new(),
                grouping_fn_arg_ids: vec![plan::UInt32List { values: vec![1] }],
                grouping_fn_ids: vec![plan::NamedUInt32 {
                    name: "__grouping_fn_0".to_string(),
                    value: 9,
                }],
                virtual_tuple_id: Some(7),
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&repeat);
        let ExecNodeKind::Repeat(repeat) = lowered.node.kind else {
            panic!("expected Repeat");
        };
        assert_eq!(repeat.repeat_times, 2);
        assert_eq!(repeat.null_slot_ids, vec![vec![], vec![SlotId::new(1)]]);
        assert_eq!(repeat.grouping_slot_ids, vec![SlotId::new(9)]);
        assert_eq!(repeat.grouping_list, vec![vec![0, 1]]);
        assert_eq!(lowered.layout.order(), &[SlotId::new(1), SlotId::new(9)]);
        assert_eq!(
            lowered.output_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(9)]
        );

        let change_event = physical_node(
            30,
            plan::plan_node::Kind::ChangeEventExpand(plan::ChangeEventExpandNode {
                events: vec![plan::DistributedChangeEventSpec {
                    predicate: None,
                    branch_kind: plan::ChangeStreamBranchKind::DeleteDv as i32,
                    assignments: vec![plan::DistributedChangeEventOutputExpr {
                        output_column_id: 2,
                        expr: None,
                    }],
                }],
                output_columns: vec![
                    output_column(1, "id", DataType::Int64),
                    output_column(2, "op", DataType::Int8),
                ],
                change_op_column_id: 2,
                data_route_column_id: None,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&change_event);
        let ExecNodeKind::ChangeEventExpand(change_event) = lowered.node.kind else {
            panic!("expected ChangeEventExpand");
        };
        assert_eq!(
            change_event.output_slot_ids,
            vec![SlotId::new(1), SlotId::new(2)]
        );
        assert_eq!(change_event.change_op_slot_id, SlotId::new(2));
        assert_eq!(change_event.events.len(), 1);

        let redistribute = physical_node(
            40,
            plan::plan_node::Kind::Redistribute(plan::RedistributeNode {
                mode: Some(plan::RedistributeMode {
                    mode: Some(plan::redistribute_mode::Mode::Gather(true)),
                }),
                partition_exprs: Vec::new(),
                output_columns: vec![output_column(1, "id", DataType::Int64)],
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&redistribute);
        assert!(matches!(lowered.node.kind, ExecNodeKind::Values(_)));
        assert_eq!(lowered.layout.order(), &[SlotId::new(1)]);
    }
}
