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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use novarocks_physical_plan::{
    BinaryOperator, ExprKind, Fragment, FragmentId, FragmentSink, FunctionId, FunctionKind,
    FunctionOverloadId, LiteralValue, NodeId, NodeKind, PhysicalPlan, UnaryOperator, ValueId,
    ValueOrigin, WindowBound, WindowFrameExclusion, WindowFrameUnits,
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
#[path = "physical_v1/layout.rs"]
mod layout;
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WireLayout {
    fragment: FragmentId,
    output_slots: Vec<((NodeId, u32), WireSlotId)>,
    internal_slots: Vec<((NodeId, ValueId), WireSlotId)>,
    input_slots: Vec<((NodeId, ValueId), (usize, usize))>,
    input_backing: Vec<WireSlotId>,
    input_edge_slots: Vec<((NodeId, u32, ValueId), (usize, usize))>,
    input_edge_backing: Vec<WireSlotId>,
}
struct PureLayoutControl;
impl novarocks_type_contract::PureCompileControl for PureLayoutControl {
    fn checkpoint(
        &self,
        _: novarocks_type_contract::CompilePhase,
        _: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        Ok(())
    }
}
fn pure_layout<T>(
    body: impl FnOnce(&mut layout::Context<'_, '_, WireLayoutError>) -> Result<T, WireLayoutError>,
) -> Result<T, WireLayoutError> {
    let mut request = |_: std::alloc::Layout| Ok(());
    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
        &PureLayoutControl,
        novarocks_type_contract::CompilePhase::Validate,
    )?;
    let result = body(&mut layout::Context {
        request: &mut request,
        work: &mut work,
    });
    if matches!(&result, Err(WireLayoutError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn find_layout_key<K: Ord, T>(
    values: &[(K, T)],
    key: &K,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<Option<usize>, novarocks_type_contract::CompileControlError> {
    let (mut low, mut high) = (0, values.len());
    while low < high {
        work.step()?;
        let middle = low + (high - low) / 2;
        match values[middle].0.cmp(key) {
            std::cmp::Ordering::Equal => return Ok(Some(middle)),
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
        }
    }
    Ok(None)
}
impl WireLayout {
    /// Pure expansion adapter. Production encoding supplies its actual creator.
    pub fn try_new(fragment: &Fragment) -> Result<Self, WireLayoutError> {
        pure_layout(|context| layout::construct(fragment, context))
    }
    pub fn try_new_observed<
        E: From<WireLayoutError> + From<novarocks_type_contract::CompileControlError>,
    >(
        fragment: &Fragment,
        request: &mut impl FnMut(std::alloc::Layout) -> Result<(), E>,
        work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    ) -> Result<Self, E> {
        layout::construct(fragment, &mut layout::Context { request, work })
    }
    pub const fn fragment(&self) -> FragmentId {
        self.fragment
    }
    pub fn internal_slot(&self, node: NodeId, value: ValueId) -> Option<WireSlotId> {
        self.internal_slots
            .binary_search_by_key(&(node, value), |entry| entry.0)
            .ok()
            .map(|index| self.internal_slots[index].1)
    }
    pub fn internal_slot_observed<E: From<novarocks_type_contract::CompileControlError>>(
        &self,
        node: NodeId,
        value: ValueId,
        work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    ) -> Result<Option<WireSlotId>, E> {
        Ok(find_layout_key(&self.internal_slots, &(node, value), work)?
            .map(|index| self.internal_slots[index].1))
    }
    pub fn output_slot(
        &self,
        node: NodeId,
        occurrence: u32,
    ) -> Result<WireSlotId, WireLayoutError> {
        self.output_slots
            .binary_search_by_key(&(node, occurrence), |entry| entry.0)
            .ok()
            .map(|index| self.output_slots[index].1)
            .ok_or(WireLayoutError::MissingOutputOccurrence {
                fragment: self.fragment,
                node,
                occurrence,
            })
    }
    pub fn output_slot_observed<
        E: From<WireLayoutError> + From<novarocks_type_contract::CompileControlError>,
    >(
        &self,
        node: NodeId,
        occurrence: u32,
        work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    ) -> Result<WireSlotId, E> {
        let index = find_layout_key(&self.output_slots, &(node, occurrence), work)?.ok_or(
            WireLayoutError::MissingOutputOccurrence {
                fragment: self.fragment,
                node,
                occurrence,
            },
        )?;
        Ok(self.output_slots[index].1)
    }
    pub fn input_value_slots(
        &self,
        node: NodeId,
        value: ValueId,
    ) -> Result<&[WireSlotId], WireLayoutError> {
        let index = self
            .input_slots
            .binary_search_by_key(&(node, value), |entry| entry.0)
            .map_err(|_| WireLayoutError::ValueOutsideInputPort {
                fragment: self.fragment,
                node,
                value,
            })?;
        let (start, len) = self.input_slots[index].1;
        Ok(&self.input_backing[start..start + len])
    }
    pub fn input_value_slot(
        &self,
        node: NodeId,
        value: ValueId,
    ) -> Result<WireSlotId, WireLayoutError> {
        self.input_value_slots(node, value)?.first().copied().ok_or(
            WireLayoutError::ValueOutsideInputPort {
                fragment: self.fragment,
                node,
                value,
            },
        )
    }
    pub fn input_value_slots_observed<
        E: From<WireLayoutError> + From<novarocks_type_contract::CompileControlError>,
    >(
        &self,
        node: NodeId,
        value: ValueId,
        work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    ) -> Result<&[WireSlotId], E> {
        let index = find_layout_key(&self.input_slots, &(node, value), work)?.ok_or(
            WireLayoutError::ValueOutsideInputPort {
                fragment: self.fragment,
                node,
                value,
            },
        )?;
        let (start, len) = self.input_slots[index].1;
        Ok(&self.input_backing[start..start + len])
    }
    pub fn input_value_slot_observed<
        E: From<WireLayoutError> + From<novarocks_type_contract::CompileControlError>,
    >(
        &self,
        node: NodeId,
        value: ValueId,
        work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    ) -> Result<WireSlotId, E> {
        self.input_value_slots_observed::<E>(node, value, work)?
            .first()
            .copied()
            .ok_or_else(|| {
                WireLayoutError::ValueOutsideInputPort {
                    fragment: self.fragment,
                    node,
                    value,
                }
                .into()
            })
    }
    pub fn input_value_slot_at(
        &self,
        node: NodeId,
        input_ordinal: u32,
        value: ValueId,
    ) -> Result<WireSlotId, WireLayoutError> {
        let index = self
            .input_edge_slots
            .binary_search_by_key(&(node, input_ordinal, value), |entry| entry.0)
            .map_err(|_| WireLayoutError::ValueOutsideInputOccurrence {
                fragment: self.fragment,
                node,
                input_ordinal,
                value,
            })?;
        let (start, _) = self.input_edge_slots[index].1;
        Ok(self.input_edge_backing[start])
    }
    pub fn input_value_slot_at_observed<
        E: From<WireLayoutError> + From<novarocks_type_contract::CompileControlError>,
    >(
        &self,
        node: NodeId,
        input_ordinal: u32,
        value: ValueId,
        work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    ) -> Result<WireSlotId, E> {
        let index = find_layout_key(&self.input_edge_slots, &(node, input_ordinal, value), work)?
            .ok_or(WireLayoutError::ValueOutsideInputOccurrence {
            fragment: self.fragment,
            node,
            input_ordinal,
            value,
        })?;
        let (start, _) = self.input_edge_slots[index].1;
        Ok(self.input_edge_backing[start])
    }
    /// Pure expansion projection; observed production uses the same body.
    pub fn project_output(
        &self,
        fragment: &Fragment,
        node: NodeId,
        projection: &[ValueId],
    ) -> Result<Vec<WireSlotId>, WireLayoutError> {
        pure_layout(|context| layout::project(self, fragment, node, projection, context))
    }
    pub fn project_output_observed<
        E: From<WireLayoutError> + From<novarocks_type_contract::CompileControlError>,
    >(
        &self,
        fragment: &Fragment,
        node: NodeId,
        projection: &[ValueId],
        request: &mut impl FnMut(std::alloc::Layout) -> Result<(), E>,
        work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    ) -> Result<Vec<WireSlotId>, E> {
        layout::project(
            self,
            fragment,
            node,
            projection,
            &mut layout::Context { request, work },
        )
    }
}
pub(crate) fn native_v1_node_wire_depths_observed<
    E: From<WireLayoutError> + From<novarocks_type_contract::CompileControlError>,
>(
    fragment: &Fragment,
    request: &mut impl FnMut(std::alloc::Layout) -> Result<(), E>,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<Vec<(NodeId, usize)>, E> {
    layout::depths(fragment, &mut layout::Context { request, work })
}
fn validate_native_v1_tree_depth(fragment: &Fragment) -> Result<(), WireLayoutError> {
    pure_layout(|context| layout::depths(fragment, context).map(|_| ()))
}
/// Pure expansion collector; actual creators use the sorted-vector body.
pub(crate) fn native_v1_node_wire_depths(
    fragment: &Fragment,
) -> Result<BTreeMap<NodeId, usize>, WireLayoutError> {
    pure_layout(|context| layout::depths(fragment, context))
        .map(|values| values.into_iter().collect())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WireLayoutError {
    Control(novarocks_type_contract::CompileControlError),
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
            Self::Control(error) => error.fmt(formatter),
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
impl From<novarocks_type_contract::CompileControlError> for WireLayoutError {
    fn from(error: novarocks_type_contract::CompileControlError) -> Self {
        Self::Control(error)
    }
}

/// Reject final-plan vocabulary that the v1 protobuf cannot carry losslessly.
///
/// This runs before a wire layout or protobuf tree is allocated. It never
/// patches unsupported semantics into a nearby v1 shape.
pub fn preflight_physical_plan_v1(plan: &PhysicalPlan) -> Result<(), PhysicalV1PreflightError> {
    preflight_physical_plan_inner(plan, false, &mut PurePreflight)
}
/// The full execution table preserves recursive semantics and exact layouts.
/// All original node, expression, function and tree laws remain shared; its
/// bookkeeping is admitted by the original caller before construction.
pub(crate) fn preflight_physical_plan_typed<H>(
    plan: &PhysicalPlan,
    resources: &mut crate::physical_encode::fragment_type_owner::TypeEncodingResources<'_, H>,
) -> crate::physical_encode::fragment_type_owner::TypeEncodingResult<(), H> {
    preflight_physical_plan_inner(plan, true, &mut ObservedPreflight(resources))
}

trait PreflightPreparation {
    type Error: From<PhysicalV1PreflightError>;
    fn step(&mut self) -> Result<(), Self::Error>;
    fn depth(&mut self, fragment: &Fragment) -> Result<(), Self::Error>;
    fn repeated_input(&mut self, fragment: &Fragment) -> Result<Option<NodeId>, Self::Error>;
    fn cast_layout(
        &mut self,
        left: &arrow::datatypes::DataType,
        right: &arrow::datatypes::DataType,
    ) -> Result<bool, Self::Error>;
}
fn preflight_node_identity(
    fragment: &Fragment,
    node: &novarocks_physical_plan::PhysicalNode,
) -> Result<(), PhysicalV1PreflightError> {
    if node.id.get() > i32::MAX as u32 {
        return Err(PhysicalV1PreflightError::NodeIdentity {
            fragment: fragment.id(),
            node: node.id,
        });
    }
    Ok(())
}
struct PurePreflight;
impl PreflightPreparation for PurePreflight {
    type Error = PhysicalV1PreflightError;
    fn step(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn depth(&mut self, fragment: &Fragment) -> Result<(), Self::Error> {
        validate_native_v1_tree_depth(fragment).map_err(|error| {
            PhysicalV1PreflightError::TreeDepth {
                fragment: fragment.id(),
                reason: error.to_string().into(),
            }
        })
    }
    fn repeated_input(&mut self, fragment: &Fragment) -> Result<Option<NodeId>, Self::Error> {
        let mut uses = BTreeSet::new();
        for node in fragment.nodes().values() {
            preflight_node_identity(fragment, node)?;
            for input in &node.inputs {
                if !uses.insert(*input) {
                    return Ok(Some(*input));
                }
            }
        }
        Ok(None)
    }
    fn cast_layout(
        &mut self,
        left: &arrow::datatypes::DataType,
        right: &arrow::datatypes::DataType,
    ) -> Result<bool, Self::Error> {
        // Legacy expansion never selects this complete-binding branch.
        Ok(left == right)
    }
}
struct ObservedPreflight<'a, 'c, H>(
    &'a mut crate::physical_encode::fragment_type_owner::TypeEncodingResources<'c, H>,
);
impl<H> PreflightPreparation for ObservedPreflight<'_, '_, H> {
    type Error = crate::host_projection_v2::ProjectionFailure<crate::PhysicalEncodeError, H>;
    fn step(&mut self) -> Result<(), Self::Error> {
        self.0.work.step().map_err(Into::into)
    }
    fn depth(&mut self, fragment: &Fragment) -> Result<(), Self::Error> {
        native_v1_node_wire_depths_observed(
            fragment,
            &mut |layout| crate::physical_encode::request_layout(layout, self.0.request),
            &mut self.0.work,
        )
        .map(|_| ())
    }
    fn repeated_input(&mut self, fragment: &Fragment) -> Result<Option<NodeId>, Self::Error> {
        let mut uses = self.0.vector(fragment.nodes().len())?;
        for node in fragment.nodes().values() {
            self.step()?;
            uses.push((node.id, false));
        }
        for node in fragment.nodes().values() {
            self.step()?;
            preflight_node_identity(fragment, node)?;
            for input in &node.inputs {
                let index = find_layout_key(&uses, input, &mut self.0.work)?
                    .ok_or("native preflight input node is absent")?;
                if uses[index].1 {
                    return Ok(Some(*input));
                }
                uses[index].1 = true;
            }
        }
        Ok(None)
    }
    fn cast_layout(
        &mut self,
        left: &arrow::datatypes::DataType,
        right: &arrow::datatypes::DataType,
    ) -> Result<bool, Self::Error> {
        novarocks_type_contract::arrow_data_types_exact_observed(left, right, || {
            self.0.work.step().map_err(Into::into)
        })
    }
}
fn preflight_physical_plan_inner<P: PreflightPreparation>(
    plan: &PhysicalPlan,
    complete_types: bool,
    preparation: &mut P,
) -> Result<(), P::Error> {
    let validate_type = |ty: &novarocks_physical_plan::ValueType| -> Result<(), String> {
        if complete_types {
            ty.validate().map_err(|error| error.to_string())
        } else {
            validate_physical_type(ty.data_type())
        }
    };

    for fragment in plan.fragments().values() {
        preparation.step()?;
        preparation.depth(fragment)?;
        for value in fragment.values().values() {
            preparation.step()?;
            if matches!(value.origin, ValueOrigin::WriterDerived { .. }) {
                // Writer relation values are carried by the exact
                // ArrowPhysicalColumn schema on TableWriter/TableFinish. They
                // never pass through the lossy v1 TypeDesc path unless another
                // node explicitly reads them; that node's expression/type
                // preflight remains authoritative for such a use.
                continue;
            }
            validate_type(&value.ty).map_err(|reason| PhysicalV1PreflightError::TypeShape {
                fragment: fragment.id(),
                subject: format!("value {}", value.id.get()).into(),
                reason: reason.into(),
            })?;
        }
        for (expression_id, expression) in fragment.expressions().iter() {
            preparation.step()?;
            validate_type(&expression.ty).map_err(|reason| {
                PhysicalV1PreflightError::TypeShape {
                    fragment: fragment.id(),
                    subject: format!("expression {}", expression_id.get()).into(),
                    reason: reason.into(),
                }
            })?;
            if let ExprKind::Cast { target, .. } = &expression.kind {
                let valid = if complete_types {
                    if preparation.cast_layout(expression.ty.data_type(), target)? {
                        Ok(())
                    } else {
                        Err("cast target differs from its exact declared binding layout".into())
                    }
                } else {
                    validate_physical_type(target)
                };
                valid.map_err(|reason| PhysicalV1PreflightError::TypeShape {
                    fragment: fragment.id(),
                    subject: format!("expression {} cast target", expression_id.get()).into(),
                    reason: reason.into(),
                })?;
            }
        }
        for node in fragment.nodes().values() {
            preparation.step()?;
            if !matches!(
                node.kind,
                NodeKind::TableWriter { .. } | NodeKind::TableFinish(_) | NodeKind::Unpivot { .. }
            ) {
                for value in &node.output.columns {
                    preparation.step()?;
                    let definition = &fragment.values()[value];
                    validate_type(&definition.ty).map_err(|reason| {
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
                    preparation.step()?;
                    validate_type(&field.ty).map_err(|reason| {
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
        if let Some(node) = preparation.repeated_input(fragment)? {
            return Err(PhysicalV1PreflightError::SharedNode {
                fragment: fragment.id(),
                node,
            }
            .into());
        }
        match fragment.sink() {
            FragmentSink::Noop => {
                return Err(PhysicalV1PreflightError::NoopSink {
                    fragment: fragment.id(),
                }
                .into());
            }
            FragmentSink::Result
            | FragmentSink::RootResult(_)
            | FragmentSink::Stream { .. }
            | FragmentSink::Multicast { .. }
            | FragmentSink::Router { .. } => {}
        }
        for node in fragment.nodes().values() {
            preparation.step()?;
            match &node.kind {
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
            preparation.step()?;
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
                        }
                        .into());
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
                    }
                    .into());
                }
                ExprKind::Binary {
                    op: BinaryOperator::BitAnd | BinaryOperator::BitOr | BinaryOperator::BitXor,
                    ..
                } => {
                    return Err(PhysicalV1PreflightError::ExpressionShape {
                        fragment: fragment.id(),
                        node: expression.owner,
                        reason: "bitwise binary operators have no native wire v1 representation",
                    }
                    .into());
                }
                ExprKind::Literal(
                    LiteralValue::UInt64(_)
                    | LiteralValue::Time64(_)
                    | LiteralValue::Timestamp(_)
                    | LiteralValue::IntervalMonthDayNano { .. },
                ) => {
                    return Err(PhysicalV1PreflightError::ExpressionShape {
                        fragment: fragment.id(),
                        node: expression.owner,
                        reason: "literal has no lossless native wire v1 representation",
                    }
                    .into());
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
    let Some(node) = fragment.expressions().get(*expression) else {
        return false;
    };
    match &node.kind {
        // This unobserved shape gate cannot consume selected backing values.
        // The complete encoder validates the source table first, then its
        // original-control window projection checks NULL and nonnegative value.
        ExprKind::Constant(_) => {
            node.ty.logical_type() == novarocks_type_contract::ValueLogicalType::Physical
                && node.ty.data_type() == &arrow::datatypes::DataType::Int64
        }
        ExprKind::Literal(LiteralValue::Int64(value)) => *value >= 0,
        _ => false,
    }
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
    let reject = || PhysicalV1PreflightError::FunctionIdentity {
        fragment,
        node,
        function: function_id.as_str().into(),
    };
    // Which namespace an identity names decides who proves its overload.
    // A `builtin.` function is the engine's own, so its overloads are named
    // inside its own namespace and the pairing is a string fact checkable
    // here. A `parametric.` function belongs to whoever registered it -- a
    // connector's statistics aggregate, say -- and its overload carries that
    // owner's identity, which was proven when the owner's resolver answered
    // with it. Requiring the engine's spelling of a provider's overload would
    // reject a binding that is already exact.
    let identity = function_id.as_str();
    let builtin_prefix = format!("builtin.{family}/");
    let parametric_prefix = format!("parametric.{family}/");
    let (name, owned_overload) = if let Some(rest) = identity.strip_prefix(&builtin_prefix) {
        (rest, true)
    } else if let Some(rest) = identity.strip_prefix(&parametric_prefix) {
        (rest, false)
    } else {
        return Err(reject());
    };
    let Some(name) = name.strip_suffix("/v1").filter(|name| !name.is_empty()) else {
        return Err(reject());
    };
    let overload_is_exact = if owned_overload {
        overload
            .as_str()
            .starts_with(&format!("{builtin_prefix}{name}/"))
    } else {
        !overload.as_str().is_empty()
    };
    if !overload_is_exact {
        return Err(reject());
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhysicalV1PreflightError {
    TreeDepth {
        fragment: FragmentId,
        reason: Box<str>,
    },
    NoopSink {
        fragment: FragmentId,
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
            Self::TreeDepth { fragment, reason } => write!(
                formatter,
                "native wire v1 cannot encode fragment {} safely: {reason}",
                fragment.get()
            ),
            Self::NoopSink { fragment } => write!(
                formatter,
                "native wire v1 requires an explicit sink for fragment {}",
                fragment.get()
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
    mod observed_layout_tests {
        include!("physical_v1/layout_tests.rs");
    }

    fn pure_resources<T>(
        context: crate::physical_expr::ExpressionEncodingContext<'_>,
        body: impl FnOnce(
            &mut crate::physical_encode::fragment_type_owner::TypeEncodingResources<
                '_,
                std::convert::Infallible,
            >,
        ) -> crate::physical_encode::fragment_type_owner::TypeEncodingResult<
            T,
            std::convert::Infallible,
        >,
    ) -> Result<T, crate::PhysicalEncodeError> {
        use crate::host_projection_v2::{AdmissionRefusal, ProjectionFailure};
        use crate::physical_encode::fragment_type_owner::TypeEncodingResources;
        let mut request =
            |_: std::alloc::Layout| Ok::<_, AdmissionRefusal<std::convert::Infallible>>(());
        let mut resources = TypeEncodingResources {
            request: &mut request,
            work: novarocks_type_contract::CompileCheckpoints::try_new(
                context.control,
                novarocks_type_contract::CompilePhase::Encode,
            )?,
            type_policy: crate::physical_encode::fragment_type_owner::ExecutionTypeEncodingPolicy::LegacyInline,
            storage: novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        };
        let result = body(&mut resources).map_err(ProjectionFailure::without_host);
        if matches!(&result, Err(crate::PhysicalEncodeError::Control(_))) {
            return result;
        }
        resources.work.finish()?;
        result
    }
    fn encode_predicate_conjunction_expansion(
        fragment: &novarocks_physical_plan::Fragment,
        layout: &WireLayout,
        owner: novarocks_physical_plan::NodeId,
        predicates: &[novarocks_physical_plan::ExprId],
        resolution: crate::physical_expr::ValueResolution<'_>,
        context: crate::physical_expr::ExpressionEncodingContext<'_>,
    ) -> Result<novarocks_proto_models::expr::Expr, crate::PhysicalEncodeError> {
        pure_resources(context, |resources| {
            crate::physical_expr::encode_predicate_conjunction(
                fragment, layout, owner, predicates, resolution, context, resources,
            )
        })
    }
    fn encode_window_frame_expansion(
        fragment: &novarocks_physical_plan::Fragment,
        frame: &novarocks_physical_plan::WindowFrame,
        context: crate::physical_expr::ExpressionEncodingContext<'_>,
    ) -> Result<novarocks_proto_models::expr::WindowFrame, crate::PhysicalEncodeError> {
        pure_resources(context, |resources| {
            crate::physical_expr::encode_window_frame(fragment, frame, context, resources)
        })
    }

    fn properties() -> PhysicalProperties {
        PhysicalProperties {
            distribution: Distribution::Singleton,
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering: Box::default(),
        }
    }

    /// Nesting depth of an encoded wire expression tree.
    fn wire_expression_depth(expression: &novarocks_proto_models::expr::Expr) -> usize {
        use novarocks_proto_models::expr::expr::Kind;
        let children: Vec<&novarocks_proto_models::expr::Expr> = match expression.kind.as_ref() {
            Some(Kind::BinaryOp(binary)) => binary
                .left
                .as_deref()
                .into_iter()
                .chain(binary.right.as_deref())
                .collect(),
            Some(Kind::UnaryOp(unary)) => unary.operand.as_deref().into_iter().collect(),
            _ => Vec::new(),
        };
        1 + children
            .into_iter()
            .map(wire_expression_depth)
            .max()
            .unwrap_or(0)
    }

    /// A wide conjunction must not become a deep wire message.
    ///
    /// Native wire v1 only spells `AND` as a binary operator, so the encoder
    /// has to turn the argument list into nesting. A left-deep chain would
    /// make an ordinary 512-condition filter 512 messages deep and the
    /// decoder's own recursion limit would reject it at the worker, after the
    /// plan had already validated. Balancing keeps nesting logarithmic.
    #[test]
    fn a_wide_conjunction_encodes_to_a_shallow_wire_tree() {
        const CONJUNCTS: usize = 512;
        let mut builder = FragmentBuilder::new(FragmentId::new(11));
        let values = builder.reserve_node_id().unwrap();
        let filter = builder.reserve_node_id().unwrap();
        let boolean = ValueType::new(DataType::Boolean, false);
        struct Control;
        impl novarocks_type_contract::PureCompileControl for Control {
            fn checkpoint(
                &self,
                _: novarocks_type_contract::CompilePhase,
                _: u32,
            ) -> Result<(), novarocks_type_contract::CompileControlError> {
                Ok(())
            }
        }
        let constant = novarocks_constant_contract::ConstantValue::from_boolean(
            std::sync::Arc::new(arrow::datatypes::Field::new(
                "predicate",
                DataType::Boolean,
                false,
            )),
            boolean.clone(),
            true,
            novarocks_constant_contract::ConstantPolicy {
                max_rows: 8,
                max_array_nodes: 8,
                max_logical_elements: 64,
                max_retained_buffer_bytes: 65536,
                max_type_depth: 64,
                max_type_nodes: 4096,
                max_dictionary_depth: 16,
                max_metadata_bytes: 65536,
                max_library_validation_work: 65536,
                max_library_validation_bytes: 65536,
            },
            novarocks_type_contract::CompilePhase::Encode,
            &Control,
        )
        .unwrap();
        let pool = novarocks_physical_plan::ConstantPoolId::new(0);
        let mut constants = novarocks_physical_plan::ConstantPools::empty();
        constants.insert(pool, constant.pool().clone()).unwrap();
        let mut predicates = Vec::with_capacity(CONJUNCTS);
        let mut columns = Vec::with_capacity(CONJUNCTS);
        for ordinal in 0..CONJUNCTS {
            predicates.push(
                builder
                    .add_expression(
                        values,
                        boolean.clone(),
                        ExprKind::Constant(novarocks_physical_plan::ConstantReference {
                            pool,
                            ordinal: 0,
                        }),
                    )
                    .unwrap(),
            );
            columns.push(
                builder
                    .add_value(
                        boolean.clone(),
                        ValueOrigin::NodeOutput {
                            node: values,
                            output_ordinal: u32::try_from(ordinal).unwrap(),
                        },
                    )
                    .unwrap(),
            );
        }
        builder
            .insert_node_unchecked(PhysicalNode {
                id: values,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: properties(),
                output: OutputPort {
                    node: values,
                    columns: columns.into_boxed_slice(),
                },
                kind: NodeKind::Values {
                    rows: Box::from([predicates.clone().into_boxed_slice()]),
                },
            })
            .unwrap();
        // Multi-predicate filtering is the real source of the wire AND's
        // synthetic Boolean. Freeze that actual node before type preparation.
        let predicates = (0..CONJUNCTS)
            .map(|_| {
                builder
                    .add_expression(
                        filter,
                        boolean.clone(),
                        ExprKind::Constant(novarocks_physical_plan::ConstantReference {
                            pool,
                            ordinal: 0,
                        }),
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        builder
            .add_filter(filter, values, predicates.clone().into_boxed_slice())
            .unwrap();
        let fragment = builder
            .finish_definition(
                filter,
                FragmentSink::Noop,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .expect("a wide conjunction is an ordinary fragment");
        let layout = WireLayout::try_new(&fragment).expect("layout");

        // The fixture's actual frozen declaration owns every predicate type;
        // no retired inline emitter is available even for this shape test.
        use crate::host_projection_v2::AdmissionRefusal;
        use crate::physical_encode::fragment_type_owner::{
            ExecutionTypeEncodingPolicy, PreparedFragmentTypes, PreparedSyntheticTypes,
            TypeEncodingResources,
        };
        use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};
        let control = Control;
        let mut request = |_: std::alloc::Layout| {
            control
                .checkpoint(CompilePhase::Encode, 0)
                .map_err(AdmissionRefusal::<std::convert::Infallible>::Control)
        };
        let mut resources = TypeEncodingResources {
            request: &mut request,
            work: CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap(),
            type_policy: ExecutionTypeEncodingPolicy::PureGrammar,
            storage: novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        };
        let synthetic = PreparedSyntheticTypes::prepare(&fragment, &mut resources).unwrap();
        let original =
            crate::physical_encode::fragment_type_owner::original_requests::PreparedOriginalRequests::prepare(
                &fragment,
                &constants,
                &mut resources,
            )
            .unwrap();
        let types = PreparedFragmentTypes::prepare(
            &fragment,
            &synthetic,
            &original,
            None,
            &crate::NoPhysicalV1PrivateFacts,
            resources.type_policy,
            &mut |layout| (resources.request)(layout),
            &mut resources.work,
        )
        .unwrap();
        let encoded = encode_predicate_conjunction_expansion(
            &fragment,
            &layout,
            filter,
            &predicates,
            crate::physical_expr::ValueResolution::NodeInput,
            crate::physical_expr::ExpressionEncodingContext {
                writer_calls: None,
                constants: &constants,
                control: &control,
                types: Some(&types),
            },
        )
        .expect("encode a wide conjunction");
        resources.work.finish().unwrap();

        let depth = wire_expression_depth(&encoded);
        // ceil(log2(512)) + one level for the literal leaves.
        assert!(depth <= 11, "wire nesting depth {depth} is not logarithmic");
        assert!(depth >= 10, "unexpected shape: depth {depth}");
    }

    #[test]
    fn checked_window_shape_projects_selected_ordinal_and_refuses_negative_or_null() {
        use arrow::array::{Array, Int64Array};
        use novarocks_constant_contract::{ConstantPolicy, ConstantPool};
        use novarocks_physical_plan::{
            ConstantPoolId, ConstantPools, ConstantReference, WindowFrame,
        };
        use novarocks_type_contract::{
            CompileControlError, CompilePhase, PureCompileControl, WindowFrameExclusion,
            WindowFrameUnits,
        };
        use std::sync::{Arc, Mutex};
        #[derive(Default)]
        struct Control {
            trace: Mutex<Vec<(CompilePhase, u32)>>,
            refusal: Option<(usize, CompileControlError)>,
        }
        impl PureCompileControl for Control {
            fn checkpoint(
                &self,
                phase: CompilePhase,
                units: u32,
            ) -> Result<(), CompileControlError> {
                let mut trace = self.trace.lock().unwrap();
                trace.push((phase, units));
                if let Some((position, cause)) = self.refusal {
                    if position == trace.len() - 1 {
                        return Err(cause);
                    }
                }
                Ok(())
            }
        }
        let ty = ValueType::new(DataType::Int64, true);
        let pool = ConstantPool::try_new(
            Arc::new(arrow::datatypes::Field::new(
                "offset",
                DataType::Int64,
                true,
            )),
            ty.clone(),
            Int64Array::from(vec![Some(-1), Some(7), None]).to_data(),
            ConstantPolicy {
                max_rows: 8,
                max_array_nodes: 8,
                max_logical_elements: 64,
                max_retained_buffer_bytes: 65536,
                max_type_depth: 64,
                max_type_nodes: 4096,
                max_dictionary_depth: 16,
                max_metadata_bytes: 65536,
                max_library_validation_work: 65536,
                max_library_validation_bytes: 65536,
            },
            CompilePhase::Encode,
            &Control::default(),
        )
        .unwrap();
        let pool_id = ConstantPoolId::new(u32::MAX);
        let mut constants = ConstantPools::empty();
        constants.insert(pool_id, pool).unwrap();
        for ordinal in [1, 0, 2] {
            let mut builder = FragmentBuilder::new(FragmentId::new(11));
            let node = builder.reserve_node_id().unwrap();
            let expression = builder
                .add_expression(
                    node,
                    ty.clone(),
                    ExprKind::Constant(ConstantReference {
                        pool: pool_id,
                        ordinal,
                    }),
                )
                .unwrap();
            let value = builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::NodeOutput {
                        node,
                        output_ordinal: 0,
                    },
                )
                .unwrap();
            builder
                .insert_node_unchecked(PhysicalNode {
                    id: node,
                    inputs: Box::default(),
                    required_inputs: Box::default(),
                    output_properties: properties(),
                    output: OutputPort {
                        node,
                        columns: Box::from([value]),
                    },
                    kind: NodeKind::Values {
                        rows: Box::from([Box::from([expression])]),
                    },
                })
                .unwrap();
            let fragment = builder
                .finish_definition(
                    node,
                    FragmentSink::Noop,
                    PipelineDopDomain {
                        min: 1,
                        max: 1,
                        requires_power_of_two: false,
                    },
                )
                .unwrap();
            let frame = WindowFrame {
                units: WindowFrameUnits::Rows,
                start: super::WindowBound::Preceding(expression),
                end: super::WindowBound::CurrentRow,
                exclusion: WindowFrameExclusion::NoOthers,
            };
            // Shape alone does not authorize the selected payload. All three
            // precise Int64 sources reach the original observed value owner.
            assert!(super::v1_window_bound_is_literal(&fragment, &frame.start));
            let control = Control::default();
            let result = encode_window_frame_expansion(
                &fragment,
                &frame,
                crate::physical_expr::ExpressionEncodingContext {
                    writer_calls: None,
                    constants: &constants,
                    control: &control,
                    types: None,
                },
            );
            if ordinal == 1 {
                assert_eq!(
                    result.unwrap().start.unwrap().bound,
                    Some(novarocks_proto_models::expr::window_bound::Bound::Preceding(7))
                );
            } else {
                assert!(matches!(
                    result,
                    Err(crate::PhysicalEncodeError::Invalid(_))
                ));
            }
            let trace = control.trace.into_inner().unwrap();
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                for position in 0..trace.len() {
                    let control = Control {
                        trace: Mutex::new(Vec::new()),
                        refusal: Some((position, cause)),
                    };
                    assert_eq!(
                        encode_window_frame_expansion(
                            &fragment,
                            &frame,
                            crate::physical_expr::ExpressionEncodingContext {
                                writer_calls: None,
                                constants: &constants,
                                control: &control,
                                types: None,
                            }
                        ),
                        Err(crate::PhysicalEncodeError::Control(cause))
                    );
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=position]);
                }
            }
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
            .insert_node_unchecked(PhysicalNode {
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
            .insert_node_unchecked(PhysicalNode {
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
            .insert_node_unchecked(PhysicalNode {
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
