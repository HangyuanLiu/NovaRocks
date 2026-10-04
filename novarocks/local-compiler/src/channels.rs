// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Ordered channels and exact input sources for the supported linear graph.
//! Repeated publication is proved from transparent reads, never from sharing
//! an expression definition or from a function's apparent determinism.

use std::{collections::BTreeMap, error::Error, fmt, sync::Arc};

use novarocks_local_program::{ProgramChannelLayoutRole, ProgramChannelSite, ProgramNodeId};
use novarocks_physical_plan::{ExprId, ExprKind, FragmentPackage, NodeId, NodeKind, ValueId};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
    ValueTypeError,
};
use novarocks_types::SlotId;

use crate::unpivot::{UnpivotInputPort, UnpivotLoweringError, plan_unpivot_channels};

use crate::repeat::{RepeatInputPort, RepeatLoweringError, plan_repeat_channels};

pub(crate) struct LinearChannels {
    pub nodes: BTreeMap<NodeId, NodeChannels>,
    pub inputs: BTreeMap<ExprId, ResolvedInput>,
    pub unpivot_sources: BTreeMap<NodeId, BTreeMap<ValueId, SlotId>>,
}

pub(crate) struct NodeChannels {
    pub local: ProgramNodeId,
    pub slots: Arc<[SlotId]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedInput {
    pub slot: SlotId,
    pub source: ProgramChannelSite,
}

#[derive(Debug)]
pub(crate) enum ChannelLoweringError {
    Control(CompileControlError),
    ValueType(ValueTypeError),
    Repeat(RepeatLoweringError),
    Unpivot(UnpivotLoweringError),
    Invalid(&'static str),
}

impl From<CompileControlError> for ChannelLoweringError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<ValueTypeError> for ChannelLoweringError {
    fn from(error: ValueTypeError) -> Self {
        Self::ValueType(error)
    }
}
impl From<RepeatLoweringError> for ChannelLoweringError {
    fn from(error: RepeatLoweringError) -> Self {
        match error {
            RepeatLoweringError::Control(cause) => Self::Control(cause),
            RepeatLoweringError::Invalid(message) => Self::Invalid(message),
            error => Self::Repeat(error),
        }
    }
}
impl From<UnpivotLoweringError> for ChannelLoweringError {
    fn from(error: UnpivotLoweringError) -> Self {
        match error {
            UnpivotLoweringError::Control(c) => Self::Control(c),
            UnpivotLoweringError::Invalid(m) => Self::Invalid(m),
            e => Self::Unpivot(e),
        }
    }
}
impl fmt::Display for ChannelLoweringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::ValueType(error) => error.fmt(f),
            Self::Repeat(error) => error.fmt(f),
            Self::Unpivot(error) => error.fmt(f),
            Self::Invalid(message) => write!(f, "invalid linear channels: {message}"),
        }
    }
}
impl Error for ChannelLoweringError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Control(error) => Some(error),
            Self::ValueType(error) => Some(error),
            Self::Repeat(error) => Some(error),
            Self::Unpivot(error) => Some(error),
            Self::Invalid(_) => None,
        }
    }
}

// A representative is inserted only after every occurrence of that value in
// this port has been proved equal by the supported operator's construction.
// It is an actual column ordinal, not an ordinal inferred from a ValueId.
type Port = BTreeMap<ValueId, usize>;

pub(crate) fn resolve_linear_channels(
    package: &FragmentPackage,
    root_first: &[NodeId],
    control: &dyn PureCompileControl,
) -> Result<LinearChannels, ChannelLoweringError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = resolve_core(package, root_first, &mut work);
    if matches!(&result, Err(ChannelLoweringError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

fn resolve_core(
    package: &FragmentPackage,
    root_first: &[NodeId],
    work: &mut CompileCheckpoints<'_>,
) -> Result<LinearChannels, ChannelLoweringError> {
    let fragment = package.fragment();
    if root_first.first() != Some(&fragment.root()) || root_first.len() != fragment.nodes().len() {
        return Err(ChannelLoweringError::Invalid(
            "linear node coverage differs",
        ));
    }
    let mut nodes = BTreeMap::<NodeId, NodeChannels>::new();
    let mut ports = BTreeMap::<NodeId, Port>::new();
    let mut next_slot = 0_u64;
    let mut unpivot_sources = BTreeMap::new();
    let mut previous = None;
    for &source in root_first.iter().rev() {
        let node = fragment
            .nodes()
            .get(&source)
            .ok_or(ChannelLoweringError::Invalid("missing physical node"))?;
        if nodes.contains_key(&source) {
            return Err(ChannelLoweringError::Invalid("duplicate linear node"));
        }
        let local = ProgramNodeId::new(nodes.len());
        work.step()?;
        let (slots, port) = match &node.kind {
            NodeKind::Values { rows }
                if previous.is_none()
                    && node.inputs.is_empty()
                    && rows.len() == 1
                    && rows[0].is_empty()
                    && node.output.columns.is_empty() =>
            {
                (Arc::<[SlotId]>::from([]), Port::new())
            }
            NodeKind::Project { expressions } => {
                let child = linear_child(node.inputs.as_ref(), previous)?;
                let child_node = &fragment.nodes()[&child];
                let child_channels = nodes
                    .get(&child)
                    .ok_or(ChannelLoweringError::Invalid("missing lowered child"))?;
                let child_port = &ports[&child];
                if expressions.len() != node.output.columns.len() {
                    return Err(ChannelLoweringError::Invalid(
                        "project occurrence width differs",
                    ));
                }
                let mut slots = Vec::with_capacity(expressions.len());
                let mut port = Port::new();
                let mut producers = BTreeMap::<ValueId, Option<ResolvedInput>>::new();
                for (ordinal, &(expression, value)) in expressions.iter().enumerate() {
                    if node.output.columns[ordinal] != value {
                        return Err(ChannelLoweringError::Invalid(
                            "ordered project values differ",
                        ));
                    }
                    let definition = fragment
                        .expressions()
                        .get(expression)
                        .ok_or(ChannelLoweringError::Invalid("missing project definition"))?;
                    let output = fragment
                        .values()
                        .get(&value)
                        .ok_or(ChannelLoweringError::Invalid("missing project value"))?;
                    if definition.owner != source {
                        return Err(ChannelLoweringError::Invalid(
                            "project expression has a foreign owner",
                        ));
                    }
                    work.step()?;
                    exact_type(&definition.ty, &output.ty, work)?;
                    let input = match definition.kind {
                        ExprKind::Value(value) => {
                            let input = resolve_input(value, child_channels, child_port)?;
                            let source_type = &fragment
                                .values()
                                .get(&value)
                                .ok_or(ChannelLoweringError::Invalid("missing input value"))?
                                .ty;
                            work.step()?;
                            exact_type(&definition.ty, source_type, work)?;
                            // The child port is complete before this node is
                            // visited; a duplicate source was already proved.
                            if child_node.output.columns[input_ordinal(input)?] != value {
                                return Err(ChannelLoweringError::Invalid(
                                    "input representative differs",
                                ));
                            }
                            Some(input)
                        }
                        _ => None,
                    };
                    let publication = record_producer(&mut producers, value, input);
                    // The comparison/publication completed even when it found
                    // an ordinary conflict. Observe that work before returning
                    // it, so an originating control refusal remains primary.
                    work.step()?;
                    publication?;
                    let slot = u32::try_from(next_slot)
                        .map_err(|_| ChannelLoweringError::Invalid("slot identity exhausted"))?;
                    next_slot = next_slot
                        .checked_add(1)
                        .ok_or(ChannelLoweringError::Invalid("slot identity exhausted"))?;
                    slots.push(SlotId::new(slot));
                    port.entry(value).or_insert(ordinal);
                    work.step()?;
                }
                work.flush()?;
                // Vec -> Arc allocation is opaque; the surrounding observations
                // do not claim an allocation grant or internal cooperation.
                let slots: Arc<[SlotId]> = Arc::from(slots);
                work.flush()?;
                (slots, port)
            }
            NodeKind::Filter { predicates } if predicates.len() == 1 => {
                passthrough(fragment, node, previous, &nodes, &ports, work)?
            }
            NodeKind::Limit { .. } => passthrough(fragment, node, previous, &nodes, &ports, work)?,
            NodeKind::Unpivot { .. } => {
                let child = linear_child(&node.inputs, previous)?;
                let child_channels = nodes.get(&child).ok_or(ChannelLoweringError::Invalid(
                    "missing lowered Unpivot child",
                ))?;
                let planned = plan_unpivot_channels(
                    fragment,
                    node,
                    UnpivotInputPort {
                        node: &fragment.nodes()[&child],
                        slots: &child_channels.slots,
                        representatives: &ports[&child],
                    },
                    &mut next_slot,
                    work,
                )?;
                unpivot_sources.insert(source, planned.sources);
                work.step()?;
                (planned.slots, planned.port)
            }
            NodeKind::Repeat { .. } => {
                let child = linear_child(&node.inputs, previous)?;
                let child_channels = nodes.get(&child).ok_or(ChannelLoweringError::Invalid(
                    "missing lowered Repeat child",
                ))?;
                let planned = plan_repeat_channels(
                    fragment,
                    node,
                    RepeatInputPort {
                        node: &fragment.nodes()[&child],
                        slots: &child_channels.slots,
                        representatives: &ports[&child],
                    },
                    &mut next_slot,
                    work,
                )?;
                (planned.slots, planned.port)
            }
            _ => {
                return Err(ChannelLoweringError::Invalid(
                    "unsupported linear channel node",
                ));
            }
        };
        nodes.insert(source, NodeChannels { local, slots });
        ports.insert(source, port);
        previous = Some(source);
        work.step()?;
    }
    let mut inputs = BTreeMap::new();
    for (&id, definition) in fragment.expressions().iter() {
        if let ExprKind::Value(value) = definition.kind {
            let owner =
                fragment
                    .nodes()
                    .get(&definition.owner)
                    .ok_or(ChannelLoweringError::Invalid(
                        "missing value-reference owner",
                    ))?;
            if owner.inputs.len() != 1 {
                return Err(ChannelLoweringError::Invalid(
                    "value reference requires one exact input",
                ));
            }
            let child = owner.inputs[0];
            let child_channels = nodes.get(&child).ok_or(ChannelLoweringError::Invalid(
                "value input is outside the linear graph",
            ))?;
            let input = resolve_input(value, child_channels, &ports[&child])?;
            let source_type = &fragment
                .values()
                .get(&value)
                .ok_or(ChannelLoweringError::Invalid("missing referenced value"))?
                .ty;
            work.step()?;
            exact_type(&definition.ty, source_type, work)?;
            inputs.insert(id, input);
        }
        work.step()?;
    }
    Ok(LinearChannels {
        nodes,
        inputs,
        unpivot_sources,
    })
}

fn linear_child(
    inputs: &[NodeId],
    previous: Option<NodeId>,
) -> Result<NodeId, ChannelLoweringError> {
    if inputs.len() != 1 || previous != Some(inputs[0]) {
        return Err(ChannelLoweringError::Invalid("linear child order differs"));
    }
    Ok(inputs[0])
}

fn passthrough(
    fragment: &novarocks_physical_plan::Fragment,
    node: &novarocks_physical_plan::PhysicalNode,
    previous: Option<NodeId>,
    nodes: &BTreeMap<NodeId, NodeChannels>,
    ports: &BTreeMap<NodeId, Port>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(Arc<[SlotId]>, Port), ChannelLoweringError> {
    let child = linear_child(&node.inputs, previous)?;
    let child_node = &fragment.nodes()[&child];
    if node.output.columns.len() != child_node.output.columns.len() {
        return Err(ChannelLoweringError::Invalid(
            "passthrough occurrence width differs",
        ));
    }
    for (actual, expected) in node.output.columns.iter().zip(&child_node.output.columns) {
        let same = actual == expected;
        work.step()?;
        if !same {
            return Err(ChannelLoweringError::Invalid(
                "passthrough occurrence order differs",
            ));
        }
    }
    let mut port = Port::new();
    for (&value, &ordinal) in &ports[&child] {
        port.insert(value, ordinal);
        work.step()?;
    }
    Ok((Arc::clone(&nodes[&child].slots), port))
}

fn resolve_input(
    value: ValueId,
    channels: &NodeChannels,
    port: &Port,
) -> Result<ResolvedInput, ChannelLoweringError> {
    let ordinal = *port.get(&value).ok_or(ChannelLoweringError::Invalid(
        "value is outside its owner input",
    ))?;
    let slot = *channels
        .slots
        .get(ordinal)
        .ok_or(ChannelLoweringError::Invalid(
            "input occurrence has no slot",
        ))?;
    let ordinal = u32::try_from(ordinal)
        .map_err(|_| ChannelLoweringError::Invalid("input ordinal exhausted"))?;
    Ok(ResolvedInput {
        slot,
        source: ProgramChannelSite::Layout {
            node: channels.local,
            role: ProgramChannelLayoutRole::NodeOutput,
            ordinal,
        },
    })
}

fn input_ordinal(input: ResolvedInput) -> Result<usize, ChannelLoweringError> {
    match input.source {
        ProgramChannelSite::Layout { ordinal, .. } => usize::try_from(ordinal)
            .map_err(|_| ChannelLoweringError::Invalid("input ordinal exceeds host range")),
        _ => Err(ChannelLoweringError::Invalid(
            "input source is not an output occurrence",
        )),
    }
}

fn record_producer(
    producers: &mut BTreeMap<ValueId, Option<ResolvedInput>>,
    value: ValueId,
    input: Option<ResolvedInput>,
) -> Result<(), ChannelLoweringError> {
    if let Some(previous) = producers.get(&value) {
        if input.is_none() || previous != &input {
            return Err(ChannelLoweringError::Invalid(
                "independent project roots share a produced value",
            ));
        }
    } else {
        producers.insert(value, input);
    }
    Ok(())
}

fn exact_type(
    left: &FunctionValueType,
    right: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ChannelLoweringError> {
    work.flush()?;
    let equal = left.exactly_equals_observed::<ChannelLoweringError>(right, || {
        // The shared type walk observes before borrowed operations. Zero-unit
        // observations preserve control without claiming completed work.
        work.control()
            .checkpoint(CompilePhase::LowerProgram, 0)
            .map_err(ChannelLoweringError::Control)
    })?;
    work.flush()?;
    if equal {
        Ok(())
    } else {
        Err(ChannelLoweringError::Invalid("complete value types differ"))
    }
}
