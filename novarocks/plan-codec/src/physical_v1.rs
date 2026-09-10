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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Lossless layout and expressibility checks for the legacy native wire.
//!
//! The physical contract owns semantic values while wire v1 addresses batch
//! columns by positive integer slots. A layout is therefore derived once from
//! the complete fragment and is only an encoding table. It never becomes a
//! second value identity or a source of plan semantics.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;

use novarocks_physical_plan::{
    BinaryOperator, ExprKind, Fragment, FragmentId, FragmentSink, FunctionId, FunctionKind,
    FunctionOverloadId, LiteralValue, NodeId, NodeKind, PhysicalPlan, Relation, UnaryOperator,
    ValueId, ValueOrigin, WindowBound, WindowFrameExclusion, WindowFrameUnits,
};

use crate::physical_type::validate_physical_type;

/// Maximum number of `DistributedNode` messages on one native-v1 root-to-leaf path.
///
/// Prost 0.13 enforces a recursion budget of 100 nested messages while decoding.
/// The plan-tree budget leaves 36 levels for the enclosing distributed-plan
/// messages and the selected node payload, expressions, and physical schema.
/// Keep the near-boundary decode test in `physical_encode` coupled to the
/// production `prost::Message::decode` path when this value changes.
pub const NATIVE_V1_MAX_TREE_DEPTH: usize = 64;

/// One fragment-local slot in the v1 protobuf carrier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WireSlotId(i32);

impl WireSlotId {
    pub const fn get(self) -> i32 {
        self.0
    }

    pub const fn get_u32(self) -> u32 {
        self.0 as u32
    }
}

/// Deterministic occurrence-to-slot translation for one fragment.
///
/// Every node output occurrence maps to the exact slot produced by the v1
/// backend contract. New repeated outputs receive distinct slots, while
/// transparent and pass-through outputs reuse the corresponding child slot.
/// Expression value references resolve only through the declared input ports
/// of their owner node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WireLayout {
    fragment: FragmentId,
    output_slots: BTreeMap<(NodeId, u32), WireSlotId>,
    input_slots: BTreeMap<(NodeId, ValueId), Vec<WireSlotId>>,
    input_edge_slots: BTreeMap<(NodeId, u32, ValueId), Vec<WireSlotId>>,
}

impl WireLayout {
    pub fn try_new(fragment: &Fragment) -> Result<Self, WireLayoutError> {
        validate_native_v1_tree_depth(fragment)?;
        let mut output_slots = BTreeMap::new();
        let mut next_slot = 1_i32;
        let mut visiting = BTreeMap::new();
        for node in fragment.nodes().keys().copied() {
            assign_wire_output(
                fragment,
                node,
                &mut output_slots,
                &mut next_slot,
                &mut visiting,
            )?;
        }

        let mut input_slots = BTreeMap::new();
        let mut input_edge_slots = BTreeMap::new();
        for node in fragment.nodes().values() {
            for (input_ordinal, input) in node.inputs.iter().enumerate() {
                let input_ordinal = u32::try_from(input_ordinal).map_err(|_| {
                    WireLayoutError::InputOrdinalSpaceExhausted {
                        fragment: fragment.id(),
                        node: node.id,
                    }
                })?;
                let input_node =
                    fragment
                        .nodes()
                        .get(input)
                        .ok_or(WireLayoutError::MissingInputNode {
                            fragment: fragment.id(),
                            node: node.id,
                            input: *input,
                        })?;
                for (ordinal, value) in input_node.output.columns.iter().copied().enumerate() {
                    let ordinal = u32::try_from(ordinal).map_err(|_| {
                        WireLayoutError::OutputOccurrenceSpaceExhausted {
                            fragment: fragment.id(),
                            node: *input,
                        }
                    })?;
                    let slot = output_slots[&(*input, ordinal)];
                    input_slots
                        .entry((node.id, value))
                        .or_insert_with(Vec::new)
                        .push(slot);
                    input_edge_slots
                        .entry((node.id, input_ordinal, value))
                        .or_insert_with(Vec::new)
                        .push(slot);
                }
            }
        }

        Ok(Self {
            fragment: fragment.id(),
            output_slots,
            input_slots,
            input_edge_slots,
        })
    }

    pub const fn fragment(&self) -> FragmentId {
        self.fragment
    }

    pub fn output_slot(
        &self,
        node: NodeId,
        occurrence: u32,
    ) -> Result<WireSlotId, WireLayoutError> {
        self.output_slots.get(&(node, occurrence)).copied().ok_or(
            WireLayoutError::MissingOutputOccurrence {
                fragment: self.fragment,
                node,
                occurrence,
            },
        )
    }

    pub fn input_value_slot(
        &self,
        node: NodeId,
        value: ValueId,
    ) -> Result<WireSlotId, WireLayoutError> {
        self.input_slots
            .get(&(node, value))
            .and_then(|slots| slots.first())
            .copied()
            .ok_or(WireLayoutError::ValueOutsideInputPort {
                fragment: self.fragment,
                node,
                value,
            })
    }

    /// Every direct-input occurrence for one semantic value, in child/column order.
    pub fn input_value_slots(
        &self,
        node: NodeId,
        value: ValueId,
    ) -> Result<&[WireSlotId], WireLayoutError> {
        self.input_slots
            .get(&(node, value))
            .map(Vec::as_slice)
            .ok_or(WireLayoutError::ValueOutsideInputPort {
                fragment: self.fragment,
                node,
                value,
            })
    }

    /// Resolve a semantic value within one exact direct-child input port.
    pub fn input_value_slot_at(
        &self,
        node: NodeId,
        input_ordinal: u32,
        value: ValueId,
    ) -> Result<WireSlotId, WireLayoutError> {
        self.input_edge_slots
            .get(&(node, input_ordinal, value))
            .and_then(|slots| slots.first())
            .copied()
            .ok_or(WireLayoutError::ValueOutsideInputOccurrence {
                fragment: self.fragment,
                node,
                input_ordinal,
                value,
            })
    }

    /// Translate an ordered value projection through one exact output port.
    ///
    /// Repeated values consume repeated occurrences from left to right. A
    /// projection cannot manufacture another occurrence of a value that the
    /// source port did not publish.
    pub fn project_output(
        &self,
        fragment: &Fragment,
        node: NodeId,
        projection: &[ValueId],
    ) -> Result<Vec<WireSlotId>, WireLayoutError> {
        if fragment.id() != self.fragment {
            return Err(WireLayoutError::FragmentMismatch {
                expected: self.fragment,
                actual: fragment.id(),
            });
        }
        let source = fragment
            .nodes()
            .get(&node)
            .ok_or(WireLayoutError::MissingProjectionNode {
                fragment: self.fragment,
                node,
            })?;
        let mut available = BTreeMap::<ValueId, VecDeque<WireSlotId>>::new();
        for (ordinal, value) in source.output.columns.iter().copied().enumerate() {
            let ordinal = u32::try_from(ordinal).map_err(|_| {
                WireLayoutError::OutputOccurrenceSpaceExhausted {
                    fragment: self.fragment,
                    node,
                }
            })?;
            available
                .entry(value)
                .or_default()
                .push_back(self.output_slot(node, ordinal)?);
        }
        projection
            .iter()
            .copied()
            .map(|value| {
                available
                    .get_mut(&value)
                    .and_then(VecDeque::pop_front)
                    .ok_or(WireLayoutError::ProjectionOccurrenceMissing {
                        fragment: self.fragment,
                        node,
                        value,
                    })
            })
            .collect()
    }
}

/// Validate native-v1 wire nesting without recursive traversal.
///
/// This is deliberately the first operation in both physical-plan preflight
/// and standalone layout construction, before either recursive encoder pass
/// can consume the frontend thread stack.
fn validate_native_v1_tree_depth(fragment: &Fragment) -> Result<(), WireLayoutError> {
    native_v1_node_wire_depths(fragment).map(|_| ())
}

pub(crate) fn native_v1_node_wire_depths(
    fragment: &Fragment,
) -> Result<BTreeMap<NodeId, usize>, WireLayoutError> {
    let mut remaining_inputs = BTreeMap::new();
    let mut dependents = BTreeMap::<NodeId, Vec<NodeId>>::new();
    let mut ready = Vec::new();
    for (node_id, node) in fragment.nodes() {
        let unique_inputs = node.inputs.iter().copied().collect::<BTreeSet<_>>();
        for input in &unique_inputs {
            if !fragment.nodes().contains_key(input) {
                return Err(WireLayoutError::MissingInputNode {
                    fragment: fragment.id(),
                    node: *node_id,
                    input: *input,
                });
            }
            dependents.entry(*input).or_default().push(*node_id);
        }
        remaining_inputs.insert(*node_id, unique_inputs.len());
        if unique_inputs.is_empty() {
            ready.push(*node_id);
        }
    }

    let mut depths = BTreeMap::<NodeId, usize>::new();
    let mut topological = Vec::with_capacity(fragment.nodes().len());
    while let Some(node_id) = ready.pop() {
        let node =
            fragment
                .nodes()
                .get(&node_id)
                .ok_or(WireLayoutError::MissingProjectionNode {
                    fragment: fragment.id(),
                    node: node_id,
                })?;
        let depth = node
            .inputs
            .iter()
            .filter_map(|input| depths.get(input).copied())
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        if depth > NATIVE_V1_MAX_TREE_DEPTH {
            return Err(WireLayoutError::TreeDepthExceeded {
                fragment: fragment.id(),
                depth,
                max: NATIVE_V1_MAX_TREE_DEPTH,
            });
        }
        depths.insert(node_id, depth);
        topological.push(node_id);
        if let Some(users) = dependents.get(&node_id) {
            for user in users {
                let remaining = remaining_inputs.get_mut(user).ok_or(
                    WireLayoutError::MissingProjectionNode {
                        fragment: fragment.id(),
                        node: *user,
                    },
                )?;
                *remaining -= 1;
                if *remaining == 0 {
                    ready.push(*user);
                }
            }
        }
    }
    if depths.len() != fragment.nodes().len() {
        return Err(WireLayoutError::MechanicalOutputMismatch {
            fragment: fragment.id(),
            node: fragment.root(),
            reason: "node graph contains a cycle".into(),
        });
    }
    let mut wire_depths = BTreeMap::from([(fragment.root(), 1_usize)]);
    for node_id in topological.into_iter().rev() {
        let Some(depth) = wire_depths.get(&node_id).copied() else {
            continue;
        };
        let node = &fragment.nodes()[&node_id];
        for input in &node.inputs {
            let candidate = depth.saturating_add(1);
            wire_depths
                .entry(*input)
                .and_modify(|current| *current = (*current).max(candidate))
                .or_insert(candidate);
        }
    }
    Ok(wire_depths)
}

fn assign_wire_output(
    fragment: &Fragment,
    node_id: NodeId,
    output_slots: &mut BTreeMap<(NodeId, u32), WireSlotId>,
    next_slot: &mut i32,
    visiting: &mut BTreeMap<NodeId, bool>,
) -> Result<(), WireLayoutError> {
    if visiting.get(&node_id) == Some(&false) {
        return Ok(());
    }
    if visiting.insert(node_id, true) == Some(true) {
        return Err(WireLayoutError::MechanicalOutputMismatch {
            fragment: fragment.id(),
            node: node_id,
            reason: "node graph contains a cycle".into(),
        });
    }
    let node = fragment
        .nodes()
        .get(&node_id)
        .ok_or(WireLayoutError::MissingProjectionNode {
            fragment: fragment.id(),
            node: node_id,
        })?;
    for input in &node.inputs {
        if !fragment.nodes().contains_key(input) {
            return Err(WireLayoutError::MissingInputNode {
                fragment: fragment.id(),
                node: node.id,
                input: *input,
            });
        }
        assign_wire_output(fragment, *input, output_slots, next_slot, visiting)?;
    }

    let slots = mechanical_output_slots(fragment, node, output_slots, next_slot)?;
    if slots.len() != node.output.columns.len() {
        return Err(WireLayoutError::MechanicalOutputMismatch {
            fragment: fragment.id(),
            node: node.id,
            reason: format!(
                "wire output width {} differs from final output width {}",
                slots.len(),
                node.output.columns.len()
            ),
        });
    }
    for (ordinal, slot) in slots.into_iter().enumerate() {
        let ordinal = u32::try_from(ordinal).map_err(|_| {
            WireLayoutError::OutputOccurrenceSpaceExhausted {
                fragment: fragment.id(),
                node: node.id,
            }
        })?;
        output_slots.insert((node.id, ordinal), slot);
    }
    visiting.insert(node_id, false);
    Ok(())
}

fn mechanical_output_slots(
    fragment: &Fragment,
    node: &novarocks_physical_plan::PhysicalNode,
    output_slots: &BTreeMap<(NodeId, u32), WireSlotId>,
    next_slot: &mut i32,
) -> Result<Vec<WireSlotId>, WireLayoutError> {
    use novarocks_physical_plan::{JoinKind, TableFunctionOutput, ValueOrigin};

    let child = |ordinal: usize| -> Result<(&[ValueId], Vec<WireSlotId>), WireLayoutError> {
        let input =
            node.inputs
                .get(ordinal)
                .ok_or_else(|| WireLayoutError::MechanicalOutputMismatch {
                    fragment: fragment.id(),
                    node: node.id,
                    reason: format!("wire output contract requires child {ordinal}"),
                })?;
        let child = &fragment.nodes()[input];
        let slots = child
            .output
            .columns
            .iter()
            .enumerate()
            .map(|(column, _)| {
                let column = u32::try_from(column).map_err(|_| {
                    WireLayoutError::OutputOccurrenceSpaceExhausted {
                        fragment: fragment.id(),
                        node: *input,
                    }
                })?;
                Ok(output_slots[&(*input, column)])
            })
            .collect::<Result<Vec<_>, WireLayoutError>>()?;
        Ok((&child.output.columns, slots))
    };
    let allocate = |count: usize, next_slot: &mut i32| {
        (0..count)
            .map(|_| allocate_wire_slot(fragment.id(), next_slot))
            .collect::<Result<Vec<_>, _>>()
    };
    let exact = |expected_values: &[ValueId], slots: Vec<WireSlotId>| {
        if expected_values != node.output.columns.as_ref() {
            Err(WireLayoutError::MechanicalOutputMismatch {
                fragment: fragment.id(),
                node: node.id,
                reason: "final output order differs from the backend v1 mechanical output".into(),
            })
        } else {
            Ok(slots)
        }
    };

    match &node.kind {
        NodeKind::Scan { .. }
        | NodeKind::Project { .. }
        | NodeKind::Aggregate { .. }
        | NodeKind::SetOp { .. }
        | NodeKind::Values { .. }
        | NodeKind::Unpivot { .. }
        | NodeKind::GenerateSeries { .. }
        | NodeKind::ChangeEventExpand { .. }
        | NodeKind::TableWriter { .. }
        | NodeKind::TableFinish(_)
        | NodeKind::ExchangeSource { .. } => allocate(node.output.columns.len(), next_slot),
        NodeKind::Filter { .. }
        | NodeKind::Sort { .. }
        | NodeKind::TopN { .. }
        | NodeKind::Limit { .. }
        | NodeKind::AssertOneRow(_) => {
            let (values, slots) = child(0)?;
            exact(values, slots)
        }
        NodeKind::Window(spec) => {
            let (values, mut slots) = child(0)?;
            let expected = values
                .iter()
                .copied()
                .chain(spec.expressions.iter().map(|expression| expression.output))
                .collect::<Vec<_>>();
            slots.extend(allocate(spec.expressions.len(), next_slot)?);
            exact(&expected, slots)
        }
        NodeKind::Repeat {
            grouping_values,
            grouping_outputs,
            ..
        } => {
            let (values, mut slots) = child(0)?;
            let replacements = grouping_values.iter().copied().collect::<BTreeMap<_, _>>();
            let expected = values
                .iter()
                .map(|value| replacements.get(value).copied().unwrap_or(*value))
                .chain(grouping_outputs.iter().map(|output| output.output))
                .collect::<Vec<_>>();
            slots.extend(allocate(grouping_outputs.len(), next_slot)?);
            exact(&expected, slots)
        }
        NodeKind::TableFunction { outputs, .. } => {
            let (values, mut slots) = child(0)?;
            let mut results = outputs
                .iter()
                .filter_map(|output| match output {
                    TableFunctionOutput::PassThrough(_) => None,
                    TableFunctionOutput::FunctionResult {
                        result_ordinal,
                        value,
                    } => Some((*result_ordinal, *value)),
                })
                .collect::<Vec<_>>();
            results.sort_by_key(|(ordinal, _)| *ordinal);
            let expected = values
                .iter()
                .copied()
                .chain(results.iter().map(|(_, value)| *value))
                .collect::<Vec<_>>();
            slots.extend(allocate(results.len(), next_slot)?);
            exact(&expected, slots)
        }
        NodeKind::HashJoin {
            kind,
            null_extended,
            ..
        }
        | NodeKind::NestLoopJoin {
            kind,
            null_extended,
            ..
        } => {
            let (left_values, left_slots) = child(0)?;
            let (right_values, right_slots) = child(1)?;
            let (base_values, base_slots) = match kind {
                JoinKind::LeftSemi | JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti => {
                    (left_values.to_vec(), left_slots)
                }
                JoinKind::RightSemi | JoinKind::RightAnti => (right_values.to_vec(), right_slots),
                _ => (
                    left_values
                        .iter()
                        .chain(right_values.iter())
                        .copied()
                        .collect(),
                    left_slots.into_iter().chain(right_slots).collect(),
                ),
            };
            let nulls = null_extended
                .iter()
                .filter_map(|value| {
                    let definition = fragment.values().get(value)?;
                    match definition.origin {
                        ValueOrigin::NullExtended { of, .. } => Some((of, *value)),
                        _ => None,
                    }
                })
                .collect::<BTreeMap<_, _>>();
            let mut available = BTreeMap::<ValueId, VecDeque<WireSlotId>>::new();
            for (value, slot) in base_values.into_iter().zip(base_slots) {
                available.entry(value).or_default().push_back(slot);
            }
            node.output
                .columns
                .iter()
                .copied()
                .map(|value| {
                    let source = nulls
                        .iter()
                        .find_map(|(source, replacement)| {
                            (*replacement == value).then_some(*source)
                        })
                        .unwrap_or(value);
                    available
                        .get_mut(&source)
                        .and_then(VecDeque::pop_front)
                        .ok_or_else(|| WireLayoutError::MechanicalOutputMismatch {
                            fragment: fragment.id(),
                            node: node.id,
                            reason: format!(
                                "join output value {} has no remaining v1 source occurrence",
                                value.get()
                            ),
                        })
                })
                .collect()
        }
    }
}

fn allocate_wire_slot(
    fragment: FragmentId,
    next_slot: &mut i32,
) -> Result<WireSlotId, WireLayoutError> {
    if *next_slot <= 0 {
        return Err(WireLayoutError::SlotSpaceExhausted { fragment });
    }
    let slot = WireSlotId(*next_slot);
    *next_slot = next_slot
        .checked_add(1)
        .ok_or(WireLayoutError::SlotSpaceExhausted { fragment })?;
    Ok(slot)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WireLayoutError {
    TreeDepthExceeded {
        fragment: FragmentId,
        depth: usize,
        max: usize,
    },
    SlotSpaceExhausted {
        fragment: FragmentId,
    },
    OutputOccurrenceSpaceExhausted {
        fragment: FragmentId,
        node: NodeId,
    },
    InputOrdinalSpaceExhausted {
        fragment: FragmentId,
        node: NodeId,
    },
    MissingInputNode {
        fragment: FragmentId,
        node: NodeId,
        input: NodeId,
    },
    MissingOutputOccurrence {
        fragment: FragmentId,
        node: NodeId,
        occurrence: u32,
    },
    ValueOutsideInputPort {
        fragment: FragmentId,
        node: NodeId,
        value: ValueId,
    },
    ValueOutsideInputOccurrence {
        fragment: FragmentId,
        node: NodeId,
        input_ordinal: u32,
        value: ValueId,
    },
    FragmentMismatch {
        expected: FragmentId,
        actual: FragmentId,
    },
    MissingProjectionNode {
        fragment: FragmentId,
        node: NodeId,
    },
    ProjectionOccurrenceMissing {
        fragment: FragmentId,
        node: NodeId,
        value: ValueId,
    },
    MechanicalOutputMismatch {
        fragment: FragmentId,
        node: NodeId,
        reason: String,
    },
}

impl fmt::Display for WireLayoutError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TreeDepthExceeded {
                fragment,
                depth,
                max,
            } => write!(
                formatter,
                "fragment {} native wire v1 tree depth {depth} exceeds decoder-safe maximum {max}",
                fragment.get()
            ),
            Self::SlotSpaceExhausted { fragment } => write!(
                formatter,
                "fragment {} has more output occurrences than native wire v1 can address",
                fragment.get()
            ),
            Self::OutputOccurrenceSpaceExhausted { fragment, node } => write!(
                formatter,
                "fragment {} node {} output occurrence ordinal exceeds u32",
                fragment.get(),
                node.get()
            ),
            Self::InputOrdinalSpaceExhausted { fragment, node } => write!(
                formatter,
                "fragment {} node {} input ordinal exceeds u32",
                fragment.get(),
                node.get()
            ),
            Self::MissingInputNode {
                fragment,
                node,
                input,
            } => write!(
                formatter,
                "fragment {} node {} names missing input node {}",
                fragment.get(),
                node.get(),
                input.get()
            ),
            Self::MissingOutputOccurrence {
                fragment,
                node,
                occurrence,
            } => write!(
                formatter,
                "fragment {} node {} has no output occurrence {}",
                fragment.get(),
                node.get(),
                occurrence
            ),
            Self::ValueOutsideInputPort {
                fragment,
                node,
                value,
            } => write!(
                formatter,
                "fragment {} node {} input port does not contain value {}",
                fragment.get(),
                node.get(),
                value.get()
            ),
            Self::ValueOutsideInputOccurrence {
                fragment,
                node,
                input_ordinal,
                value,
            } => write!(
                formatter,
                "fragment {} node {} input {} does not contain value {}",
                fragment.get(),
                node.get(),
                input_ordinal,
                value.get()
            ),
            Self::FragmentMismatch { expected, actual } => write!(
                formatter,
                "wire layout belongs to fragment {}, not fragment {}",
                expected.get(),
                actual.get()
            ),
            Self::MissingProjectionNode { fragment, node } => write!(
                formatter,
                "fragment {} has no projection source node {}",
                fragment.get(),
                node.get()
            ),
            Self::ProjectionOccurrenceMissing {
                fragment,
                node,
                value,
            } => write!(
                formatter,
                "fragment {} node {} cannot project another occurrence of value {}",
                fragment.get(),
                node.get(),
                value.get()
            ),
            Self::MechanicalOutputMismatch {
                fragment,
                node,
                reason,
            } => write!(
                formatter,
                "fragment {} node {} is not lossless on native wire v1: {reason}",
                fragment.get(),
                node.get()
            ),
        }
    }
}

impl std::error::Error for WireLayoutError {}

/// Reject final-plan vocabulary that the v1 protobuf cannot carry losslessly.
///
/// This runs before a wire layout or protobuf tree is allocated. It never
/// patches unsupported semantics into a nearby v1 shape.
pub fn preflight_physical_plan_v1(plan: &PhysicalPlan) -> Result<(), PhysicalV1PreflightError> {
    if !plan.artifact_refs().is_empty() {
        return Err(PhysicalV1PreflightError::ArtifactReferences);
    }
    for fragment in plan.fragments().values() {
        validate_native_v1_tree_depth(fragment).map_err(|error| {
            PhysicalV1PreflightError::TreeDepth {
                fragment: fragment.id(),
                reason: error.to_string().into(),
            }
        })?;
        for value in fragment.values().values() {
            if matches!(value.origin, ValueOrigin::WriterDerived { .. }) {
                // Writer relation values are carried by the exact
                // ArrowPhysicalColumn schema on TableWriter/TableFinish. They
                // never pass through the lossy v1 TypeDesc path unless another
                // node explicitly reads them; that node's expression/type
                // preflight remains authoritative for such a use.
                continue;
            }
            validate_physical_type(&value.ty.data_type).map_err(|reason| {
                PhysicalV1PreflightError::TypeShape {
                    fragment: fragment.id(),
                    subject: format!("value {}", value.id.get()).into(),
                    reason: reason.into(),
                }
            })?;
        }
        for (expression_id, expression) in fragment.expressions().iter() {
            validate_physical_type(&expression.ty.data_type).map_err(|reason| {
                PhysicalV1PreflightError::TypeShape {
                    fragment: fragment.id(),
                    subject: format!("expression {}", expression_id.get()).into(),
                    reason: reason.into(),
                }
            })?;
            if let ExprKind::Cast { target, .. } = &expression.kind {
                validate_physical_type(target).map_err(|reason| {
                    PhysicalV1PreflightError::TypeShape {
                        fragment: fragment.id(),
                        subject: format!("expression {} cast target", expression_id.get()).into(),
                        reason: reason.into(),
                    }
                })?;
            }
        }
        for node in fragment.nodes().values() {
            if !matches!(
                node.kind,
                NodeKind::TableWriter { .. } | NodeKind::TableFinish(_) | NodeKind::Unpivot { .. }
            ) {
                for value in &node.output.columns {
                    let definition = &fragment.values()[value];
                    validate_physical_type(&definition.ty.data_type).map_err(|reason| {
                        PhysicalV1PreflightError::TypeShape {
                            fragment: fragment.id(),
                            subject: format!(
                                "node {} output value {}",
                                node.id.get(),
                                definition.id.get()
                            )
                            .into(),
                            reason: reason.into(),
                        }
                    })?;
                }
            }
            if let NodeKind::Scan { relation, .. } = &node.kind {
                for (field_ordinal, field) in relation.schema().iter().enumerate() {
                    validate_physical_type(&field.ty.data_type).map_err(|reason| {
                        PhysicalV1PreflightError::TypeShape {
                            fragment: fragment.id(),
                            subject: format!(
                                "node {} relation field {}",
                                node.id.get(),
                                field_ordinal
                            )
                            .into(),
                            reason: reason.into(),
                        }
                    })?;
                }
            }
        }
        let mut input_uses = BTreeMap::<NodeId, u32>::new();
        for node in fragment.nodes().values() {
            if node.id.get() > i32::MAX as u32 {
                return Err(PhysicalV1PreflightError::NodeIdentity {
                    fragment: fragment.id(),
                    node: node.id,
                });
            }
            for input in &node.inputs {
                let uses = input_uses.entry(*input).or_default();
                *uses = uses.saturating_add(1);
                if *uses > 1 {
                    return Err(PhysicalV1PreflightError::SharedNode {
                        fragment: fragment.id(),
                        node: *input,
                    });
                }
            }
        }
        match fragment.sink() {
            FragmentSink::SealedArtifact(_) => {
                return Err(PhysicalV1PreflightError::SealedArtifactSink {
                    fragment: fragment.id(),
                });
            }
            FragmentSink::Noop => {
                return Err(PhysicalV1PreflightError::NoopSink {
                    fragment: fragment.id(),
                });
            }
            FragmentSink::Result
            | FragmentSink::Stream { .. }
            | FragmentSink::Multicast { .. }
            | FragmentSink::Router { .. } => {}
        }
        for node in fragment.nodes().values() {
            match &node.kind {
                NodeKind::Scan { relation, .. }
                    if matches!(relation.as_ref(), Relation::Metadata(_)) =>
                {
                    return Err(PhysicalV1PreflightError::MetadataRelation {
                        fragment: fragment.id(),
                        node: node.id,
                    });
                }
                NodeKind::TableFunction { function, .. } => {
                    validate_v1_function_identity(
                        &function.function_id,
                        &function.overload,
                        FunctionKind::Table,
                        fragment.id(),
                        node.id,
                    )?;
                }
                NodeKind::Aggregate { calls, .. } => {
                    for call in calls {
                        validate_v1_function_identity(
                            &call.binding.function.function_id,
                            &call.binding.function.overload,
                            FunctionKind::Aggregate,
                            fragment.id(),
                            node.id,
                        )?;
                    }
                }
                NodeKind::TableWriter { target } => {
                    for call in &target.partial_aggregates {
                        validate_v1_function_identity(
                            &call.binding.function.function_id,
                            &call.binding.function.overload,
                            FunctionKind::Aggregate,
                            fragment.id(),
                            node.id,
                        )?;
                    }
                }
                NodeKind::TableFinish(spec) => {
                    for call in &spec.final_aggregates {
                        validate_v1_function_identity(
                            &call.binding.function.function_id,
                            &call.binding.function.overload,
                            FunctionKind::Aggregate,
                            fragment.id(),
                            node.id,
                        )?;
                    }
                }
                _ => {}
            }
        }
        for expression in fragment
            .expressions()
            .iter()
            .map(|(_, expression)| expression)
        {
            match &expression.kind {
                ExprKind::FunctionCall { function, .. } => {
                    validate_v1_function_identity(
                        &function.function_id,
                        &function.overload,
                        function.kind,
                        fragment.id(),
                        expression.owner,
                    )?;
                }
                ExprKind::WindowCall {
                    function, frame, ..
                } => {
                    validate_v1_function_identity(
                        &function.function_id,
                        &function.overload,
                        function.kind,
                        fragment.id(),
                        expression.owner,
                    )?;
                    if let Some(frame) = frame
                        && (frame.units == WindowFrameUnits::Groups
                            || frame.exclusion != WindowFrameExclusion::NoOthers
                            || !v1_window_bound_is_literal(fragment, &frame.start)
                            || !v1_window_bound_is_literal(fragment, &frame.end))
                    {
                        return Err(PhysicalV1PreflightError::ExpressionShape {
                            fragment: fragment.id(),
                            node: expression.owner,
                            reason: "window frame has no lossless native wire v1 representation",
                        });
                    }
                }
                ExprKind::Unary {
                    op: UnaryOperator::Plus,
                    ..
                } => {
                    return Err(PhysicalV1PreflightError::ExpressionShape {
                        fragment: fragment.id(),
                        node: expression.owner,
                        reason: "unary plus has no native wire v1 representation",
                    });
                }
                ExprKind::Binary {
                    op: BinaryOperator::BitAnd | BinaryOperator::BitOr | BinaryOperator::BitXor,
                    ..
                } => {
                    return Err(PhysicalV1PreflightError::ExpressionShape {
                        fragment: fragment.id(),
                        node: expression.owner,
                        reason: "bitwise binary operators have no native wire v1 representation",
                    });
                }
                ExprKind::Literal(
                    LiteralValue::UInt64(_)
                    | LiteralValue::Time64(_)
                    | LiteralValue::Timestamp(_)
                    | LiteralValue::IntervalMonthDayNano(_),
                ) => {
                    return Err(PhysicalV1PreflightError::ExpressionShape {
                        fragment: fragment.id(),
                        node: expression.owner,
                        reason: "literal has no lossless native wire v1 representation",
                    });
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn v1_window_bound_is_literal(fragment: &Fragment, bound: &WindowBound) -> bool {
    let (WindowBound::Preceding(expression) | WindowBound::Following(expression)) = bound else {
        return true;
    };
    matches!(
        fragment.expressions().get(*expression).map(|node| &node.kind),
        Some(ExprKind::Literal(LiteralValue::Int64(value))) if *value >= 0
    )
}

fn validate_v1_function_identity(
    function_id: &FunctionId,
    overload: &FunctionOverloadId,
    kind: FunctionKind,
    fragment: FragmentId,
    node: NodeId,
) -> Result<(), PhysicalV1PreflightError> {
    let family = match kind {
        FunctionKind::Scalar => "scalar",
        FunctionKind::Aggregate => "aggregate",
        FunctionKind::Window => "window",
        FunctionKind::Table => "table",
    };
    let prefix = format!("builtin.{family}/");
    let Some(name_and_version) = function_id.as_str().strip_prefix(&prefix) else {
        return Err(PhysicalV1PreflightError::FunctionIdentity {
            fragment,
            node,
            function: function_id.as_str().into(),
        });
    };
    let Some(name) = name_and_version.strip_suffix("/v1") else {
        return Err(PhysicalV1PreflightError::FunctionIdentity {
            fragment,
            node,
            function: function_id.as_str().into(),
        });
    };
    let overload_prefix = format!("builtin.{family}/{name}/");
    if name.is_empty() || !overload.as_str().starts_with(&overload_prefix) {
        return Err(PhysicalV1PreflightError::FunctionIdentity {
            fragment,
            node,
            function: function_id.as_str().into(),
        });
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhysicalV1PreflightError {
    ArtifactReferences,
    TreeDepth {
        fragment: FragmentId,
        reason: Box<str>,
    },
    SealedArtifactSink {
        fragment: FragmentId,
    },
    NoopSink {
        fragment: FragmentId,
    },
    MetadataRelation {
        fragment: FragmentId,
        node: NodeId,
    },
    FunctionIdentity {
        fragment: FragmentId,
        node: NodeId,
        function: Box<str>,
    },
    NodeIdentity {
        fragment: FragmentId,
        node: NodeId,
    },
    SharedNode {
        fragment: FragmentId,
        node: NodeId,
    },
    ExpressionShape {
        fragment: FragmentId,
        node: NodeId,
        reason: &'static str,
    },
    TypeShape {
        fragment: FragmentId,
        subject: Box<str>,
        reason: Box<str>,
    },
}

impl fmt::Display for PhysicalV1PreflightError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArtifactReferences => formatter
                .write_str("native wire v1 cannot encode sealed artifact references losslessly"),
            Self::TreeDepth { fragment, reason } => write!(
                formatter,
                "native wire v1 cannot encode fragment {} safely: {reason}",
                fragment.get()
            ),
            Self::SealedArtifactSink { fragment } => write!(
                formatter,
                "native wire v1 cannot encode sealed artifact sink in fragment {}",
                fragment.get()
            ),
            Self::NoopSink { fragment } => write!(
                formatter,
                "native wire v1 requires an explicit sink for fragment {}",
                fragment.get()
            ),
            Self::MetadataRelation { fragment, node } => write!(
                formatter,
                "native wire v1 cannot encode metadata relation at fragment {} node {}",
                fragment.get(),
                node.get()
            ),
            Self::FunctionIdentity {
                fragment,
                node,
                function,
            } => write!(
                formatter,
                "native wire v1 cannot prove exact function binding `{function}` at fragment {} node {}",
                fragment.get(),
                node.get()
            ),
            Self::NodeIdentity { fragment, node } => write!(
                formatter,
                "fragment {} node {} cannot fit native wire v1 signed node identity",
                fragment.get(),
                node.get()
            ),
            Self::SharedNode { fragment, node } => write!(
                formatter,
                "fragment {} node {} has multiple consumers and cannot be encoded as a native wire v1 tree without duplicating execution",
                fragment.get(),
                node.get()
            ),
            Self::ExpressionShape {
                fragment,
                node,
                reason,
            } => write!(
                formatter,
                "native wire v1 cannot encode expression at fragment {} node {} losslessly: {reason}",
                fragment.get(),
                node.get()
            ),
            Self::TypeShape {
                fragment,
                subject,
                reason,
            } => write!(
                formatter,
                "native wire v1 cannot encode {subject} in fragment {} losslessly: {reason}",
                fragment.get()
            ),
        }
    }
}

impl std::error::Error for PhysicalV1PreflightError {}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use novarocks_physical_plan::{
        Distribution, ExprKind, FragmentBuilder, FragmentId, FragmentSink, LiteralValue, NodeKind,
        OutputPort, PhysicalNode, PhysicalProperties, PipelineDopDomain, RowMultiplicity,
        ValueOrigin, ValueType,
    };

    use super::{WireLayout, WireLayoutError};

    fn properties() -> PhysicalProperties {
        PhysicalProperties {
            distribution: Distribution::Singleton,
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering: Box::default(),
        }
    }

    fn repeated_projection_fragment() -> (
        novarocks_physical_plan::Fragment,
        novarocks_physical_plan::NodeId,
        novarocks_physical_plan::NodeId,
        novarocks_physical_plan::NodeId,
        novarocks_physical_plan::ValueId,
    ) {
        let mut builder = FragmentBuilder::new(FragmentId::new(7));
        let values = builder.reserve_node_id().unwrap();
        let ty = ValueType::new(DataType::Int64, false);
        let literal = builder
            .add_expression(
                values,
                ty.clone(),
                ExprKind::Literal(LiteralValue::Int64(5)),
            )
            .unwrap();
        let value = builder
            .add_value(
                ty.clone(),
                ValueOrigin::NodeOutput {
                    node: values,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        builder
            .insert_node(PhysicalNode {
                id: values,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: properties(),
                output: OutputPort {
                    node: values,
                    columns: Box::from([value]),
                },
                kind: NodeKind::Values {
                    rows: Box::from([Box::from([literal])]),
                },
            })
            .unwrap();

        let project = builder.reserve_node_id().unwrap();
        let reference = builder
            .add_expression(project, ty, ExprKind::Value(value))
            .unwrap();
        builder
            .insert_node(PhysicalNode {
                id: project,
                inputs: Box::from([values]),
                required_inputs: Box::from([properties()]),
                output_properties: properties(),
                output: OutputPort {
                    node: project,
                    columns: Box::from([value, value]),
                },
                kind: NodeKind::Project {
                    expressions: Box::from([(reference, value), (reference, value)]),
                },
            })
            .unwrap();

        let limit = builder.reserve_node_id().unwrap();
        builder
            .insert_node(PhysicalNode {
                id: limit,
                inputs: Box::from([project]),
                required_inputs: Box::from([properties()]),
                output_properties: properties(),
                output: OutputPort {
                    node: limit,
                    columns: Box::from([value, value]),
                },
                kind: NodeKind::Limit {
                    limit: Some(1),
                    offset: 0,
                },
            })
            .unwrap();
        let fragment = builder
            .finish_definition(
                limit,
                FragmentSink::Noop,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .unwrap();
        (fragment, values, project, limit, value)
    }

    #[test]
    fn repeated_value_occurrences_receive_distinct_wire_slots() {
        let (fragment, values, project, limit, value) = repeated_projection_fragment();
        let layout = WireLayout::try_new(&fragment).unwrap();

        assert_eq!(layout.output_slot(values, 0).unwrap().get(), 1);
        assert_eq!(layout.output_slot(project, 0).unwrap().get(), 2);
        assert_eq!(layout.output_slot(project, 1).unwrap().get(), 3);
        assert_eq!(layout.output_slot(limit, 0).unwrap().get(), 2);
        assert_eq!(layout.output_slot(limit, 1).unwrap().get(), 3);
        assert_eq!(layout.input_value_slot(project, value).unwrap().get(), 1);
        assert_eq!(
            layout
                .input_value_slots(limit, value)
                .unwrap()
                .iter()
                .map(|slot| slot.get())
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(
            layout.input_value_slot_at(limit, 0, value).unwrap().get(),
            2
        );
        assert!(matches!(
            layout.input_value_slot_at(limit, 1, value),
            Err(WireLayoutError::ValueOutsideInputOccurrence { .. })
        ));
        assert_eq!(
            layout
                .project_output(&fragment, project, &[value, value])
                .unwrap()
                .into_iter()
                .map(|slot| slot.get())
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[test]
    fn projection_cannot_manufacture_a_repeated_occurrence() {
        let (fragment, _, project, _, value) = repeated_projection_fragment();
        let layout = WireLayout::try_new(&fragment).unwrap();
        assert!(matches!(
            layout.project_output(&fragment, project, &[value, value, value]),
            Err(WireLayoutError::ProjectionOccurrenceMissing { .. })
        ));
    }
}
