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

//! Complete Repeat node projection, including the original node envelope.
//! Values and their full types remain in the same sealed Value namespace.
//! Node references, child visibility, nullable replacements, grouping semantics
//! and property proofs still require the original Fragment/Package authors.
//! Source invoices are trusted whole-source backing facts; these floors are
//! necessary lower bounds, not host grants or allocator retained measurements.

use crate::{
    allocation_exit_v2::reserve_exit,
    physical_properties_v2::{
        self as properties, PhysicalPropertyCodecError, PhysicalPropertyProjectionFacts,
        PhysicalPropertyProjectionLimits,
    },
    physical_value_v2::{DecodedValues, EncodedValues, ValueCodecError},
};
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{CompileCheckpoints, CompileControlError, CompilePhase};
use std::{alloc::Layout, fmt, mem::size_of};

#[derive(Clone, Copy, Debug)]
pub struct RepeatNodeProjectionLimits {
    pub max_input_nodes: usize,
    pub max_value_references: usize,
    pub max_list_items: usize,
    pub max_allocation_requests: usize,
    pub max_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_work: usize,
    pub properties: PhysicalPropertyProjectionLimits,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RepeatNodeProjectionFacts {
    pub input_node_count: usize,
    pub value_reference_count: usize,
    pub list_item_count: usize,
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}
#[derive(Debug)]
pub enum RepeatNodeCodecError {
    Control(CompileControlError),
    Properties(PhysicalPropertyCodecError),
    Value(ValueCodecError),
    InvalidShape(&'static str),
}
impl From<CompileControlError> for RepeatNodeCodecError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl From<PhysicalPropertyCodecError> for RepeatNodeCodecError {
    fn from(error: PhysicalPropertyCodecError) -> Self {
        match error {
            PhysicalPropertyCodecError::Control(cause) => Self::Control(cause),
            error => Self::Properties(error),
        }
    }
}
impl From<ValueCodecError> for RepeatNodeCodecError {
    fn from(error: ValueCodecError) -> Self {
        match error {
            ValueCodecError::Control(cause) => Self::Control(cause),
            error => Self::Value(error),
        }
    }
}
impl fmt::Display for RepeatNodeCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Properties(e) => e.fmt(f),
            Self::Value(e) => e.fmt(f),
            Self::InvalidShape(s) => f.write_str(s),
        }
    }
}
impl std::error::Error for RepeatNodeCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(e) => Some(e),
            Self::Properties(e) => Some(e),
            Self::Value(e) => Some(e),
            Self::InvalidShape(_) => None,
        }
    }
}
type Error = RepeatNodeCodecError;
fn invalid(text: &'static str) -> Error {
    Error::InvalidShape(text)
}
fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b)
        .ok_or_else(|| invalid("Repeat resource sum overflow"))
}
fn mul(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_mul(b)
        .ok_or_else(|| invalid("Repeat resource product overflow"))
}
fn bytes<T>(n: usize) -> Result<usize, Error> {
    Layout::array::<T>(n)
        .map(|layout| layout.size())
        .map_err(|_| invalid("Repeat allocation layout is unrepresentable"))
}
fn cap(n: usize, maximum: usize, w: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    let accepted = n <= maximum;
    w.step()?;
    if accepted {
        Ok(())
    } else {
        Err(invalid("Repeat projection envelope exceeded"))
    }
}
fn floor(invoice: usize, known: usize, w: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    let accepted = invoice >= known;
    w.step()?;
    if accepted {
        Ok(())
    } else {
        Err(invalid("Repeat source invoice omits original backing"))
    }
}
fn finish<T>(w: CompileCheckpoints<'_>, result: Result<T, Error>) -> Result<T, Error> {
    if matches!(&result, Err(Error::Control(_))) {
        return result;
    }
    w.finish()?;
    result
}
fn reserve<T>(n: usize, w: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, Error> {
    bytes::<T>(n)?;
    w.flush()?;
    let mut output = Vec::new();
    let result = output.try_reserve_exact(n);
    reserve_exit::<Error>(result, w)?;
    Ok(output)
}
fn boxed<T>(input: Vec<T>, w: &mut CompileCheckpoints<'_>) -> Result<Box<[T]>, Error> {
    w.flush()?;
    let output = input.into_boxed_slice();
    w.flush()?;
    Ok(output)
}

trait Values {
    fn count(&self) -> usize;
    fn retained_floor(&self, w: &mut CompileCheckpoints<'_>) -> Result<usize, Error>;
    fn contains(&self, id: u32, w: &mut CompileCheckpoints<'_>) -> Result<bool, Error>;
}
impl Values for EncodedValues<'_, '_, '_> {
    fn count(&self) -> usize {
        self.source_count()
    }
    fn retained_floor(&self, w: &mut CompileCheckpoints<'_>) -> Result<usize, Error> {
        Ok(self.retained_floor_observed(w)?)
    }
    fn contains(&self, id: u32, w: &mut CompileCheckpoints<'_>) -> Result<bool, Error> {
        Ok(self.value_observed(id, w)?.is_some())
    }
}
impl Values for DecodedValues<'_, '_, '_> {
    fn count(&self) -> usize {
        self.source_count()
    }
    fn retained_floor(&self, w: &mut CompileCheckpoints<'_>) -> Result<usize, Error> {
        Ok(self.retained_floor_observed(w)?)
    }
    fn contains(&self, id: u32, w: &mut CompileCheckpoints<'_>) -> Result<bool, Error> {
        Ok(self.value_observed(id, w)?.is_some())
    }
}
fn reference(id: u32, values: &impl Values, w: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    let found = values.contains(id, w)?;
    w.step()?;
    if found {
        Ok(())
    } else {
        Err(invalid(
            "Repeat value reference is not in the original namespace",
        ))
    }
}
fn physical_property_refs(
    input: &p::PhysicalProperties,
    values: &impl Values,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    if let p::Distribution::Hash { keys, .. } | p::Distribution::BucketShuffle { keys, .. } =
        &input.distribution
    {
        for id in keys {
            reference(id.get(), values, w)?;
        }
    }
    for key in &input.ordering {
        reference(key.value.get(), values, w)?;
    }
    Ok(())
}
fn wire_property_refs(
    input: &wire::PhysicalProperties,
    values: &impl Values,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    if let Some(kind) = input.distribution.as_ref().and_then(|v| v.kind.as_ref()) {
        let keys = match kind {
            wire::distribution::Kind::Hash(v) => &v.key_value_ids[..],
            wire::distribution::Kind::BucketShuffle(v) => &v.key_value_ids[..],
            _ => &[],
        };
        for id in keys {
            reference(*id, values, w)?;
        }
    }
    for key in &input.ordering {
        let id = key
            .value_id
            .ok_or_else(|| invalid("Repeat property ordering value is absent"))?;
        reference(id, values, w)?;
    }
    Ok(())
}

#[derive(Default)]
struct Model {
    inputs: usize,
    refs: usize,
    items: usize,
    requests: usize,
    requested: usize,
    delegated_work: usize,
}
impl Model {
    fn request<T>(&mut self, n: usize, copies: usize) -> Result<(), Error> {
        self.requested = add(self.requested, mul(bytes::<T>(n)?, copies)?)?;
        if n != 0 {
            self.requests = add(self.requests, copies)?;
        }
        Ok(())
    }
    fn property(&mut self, facts: PhysicalPropertyProjectionFacts) -> Result<(), Error> {
        self.refs = add(self.refs, facts.value_reference_count)?;
        self.requests = add(self.requests, facts.allocation_requests_upper_bound)?;
        self.requested = add(self.requested, facts.allocation_request_bytes_upper_bound)?;
        // The sole property author performs preflight here and again in emit.
        // Admit both passes; never duplicate its numerical/layout algorithm.
        self.delegated_work = add(
            self.delegated_work,
            mul(facts.cumulative_work_upper_bound, 2)?,
        )?;
        Ok(())
    }
    fn facts(
        &self,
        source: usize,
        values: usize,
        l: RepeatNodeProjectionLimits,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<RepeatNodeProjectionFacts, Error> {
        let search_height = (usize::BITS - values.leading_zeros()) as usize + 1;
        // Each original list/reference has a bounded count/shape pass, exact
        // namespace lookup and emission pass. The multiplier covers their
        // fixed-size arithmetic/header gates and the final footer. Requested
        // bytes cover writes and possible Vec-to-Box moves; library internals
        // remain opaque boundaries rather than synthetic work callbacks.
        let own_work = add(
            256,
            add(
                mul(add(self.items, self.inputs)?, 32)?,
                add(
                    mul(self.refs, add(search_height, 32)?)?,
                    mul(self.requested, 4)?,
                )?,
            )?,
        )?;
        let facts = RepeatNodeProjectionFacts {
            input_node_count: self.inputs,
            value_reference_count: self.refs,
            list_item_count: self.items,
            allocation_requests_upper_bound: self.requests,
            allocation_request_bytes_upper_bound: self.requested,
            coexisting_source_and_request_bytes_upper_bound: add(source, self.requested)?,
            cumulative_work_upper_bound: add(own_work, self.delegated_work)?,
        };
        cap(facts.input_node_count, l.max_input_nodes, w)?;
        cap(facts.value_reference_count, l.max_value_references, w)?;
        cap(facts.list_item_count, l.max_list_items, w)?;
        cap(
            facts.allocation_requests_upper_bound,
            l.max_allocation_requests,
            w,
        )?;
        cap(
            facts.allocation_request_bytes_upper_bound,
            l.max_allocation_request_bytes,
            w,
        )?;
        cap(
            facts.coexisting_source_and_request_bytes_upper_bound,
            l.max_coexisting_source_and_request_bytes,
            w,
        )?;
        cap(facts.cumulative_work_upper_bound, l.max_work, w)?;
        Ok(facts)
    }
}
fn count_prefix(
    inputs: usize,
    items: usize,
    source: usize,
    known: usize,
    l: RepeatNodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    cap(inputs, l.max_input_nodes, w)?;
    cap(items, l.max_list_items, w)?;
    // This lower bound precedes all variable-length numerical counting.
    cap(add(256, mul(add(items, inputs)?, 32)?)?, l.max_work, w)?;
    floor(source, known, w)
}
fn prepare_encode(
    input: &p::PhysicalNode,
    values: &impl Values,
    source: usize,
    l: RepeatNodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<RepeatNodeProjectionFacts, Error> {
    let p::NodeKind::Repeat {
        rollup_keys,
        grouping_sets,
        grouping_values,
        grouping_outputs,
    } = &input.kind
    else {
        return Err(invalid("physical node kind is not Repeat"));
    };
    let outer = add(
        add(input.required_inputs.len(), input.output.columns.len())?,
        add(
            add(rollup_keys.len(), grouping_sets.len())?,
            add(grouping_values.len(), grouping_outputs.len())?,
        )?,
    )?;
    let mut known = add(
        size_of::<p::PhysicalNode>(),
        add(
            bytes::<p::NodeId>(input.inputs.len())?,
            add(
                bytes::<p::PhysicalProperties>(input.required_inputs.len())?,
                bytes::<p::ValueId>(input.output.columns.len())?,
            )?,
        )?,
    )?;
    known = add(
        known,
        add(
            bytes::<p::ValueId>(rollup_keys.len())?,
            add(
                bytes::<Box<[p::ValueId]>>(grouping_sets.len())?,
                add(
                    bytes::<(p::ValueId, p::ValueId)>(grouping_values.len())?,
                    bytes::<p::GroupingOutput>(grouping_outputs.len())?,
                )?,
            )?,
        )?,
    )?;
    count_prefix(input.inputs.len(), outer, source, known, l, w)?;
    floor(source, values.retained_floor(w)?, w)?;
    let mut m = Model {
        inputs: input.inputs.len(),
        items: outer,
        refs: add(
            input.output.columns.len(),
            add(
                rollup_keys.len(),
                add(mul(grouping_values.len(), 2)?, grouping_outputs.len())?,
            )?,
        )?,
        ..Model::default()
    };
    m.request::<u32>(input.inputs.len(), 1)?;
    m.request::<wire::PhysicalProperties>(input.required_inputs.len(), 1)?;
    m.request::<u32>(input.output.columns.len(), 1)?;
    m.request::<u32>(rollup_keys.len(), 1)?;
    m.request::<wire::ValueIds>(grouping_sets.len(), 1)?;
    m.request::<wire::ValueMapping>(grouping_values.len(), 1)?;
    m.request::<wire::GroupingOutput>(grouping_outputs.len(), 1)?;
    for set in grouping_sets {
        m.items = add(m.items, set.len())?;
        m.refs = add(m.refs, set.len())?;
        known = add(known, bytes::<p::ValueId>(set.len())?)?;
        m.request::<u32>(set.len(), 1)?;
        w.step()?;
    }
    for output in grouping_outputs {
        m.items = add(m.items, output.arguments.len())?;
        m.refs = add(m.refs, output.arguments.len())?;
        known = add(known, bytes::<p::ValueId>(output.arguments.len())?)?;
        m.request::<u32>(output.arguments.len(), 1)?;
        w.step()?;
    }
    floor(source, known, w)?;
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(&input.output_properties))
    {
        let pf = properties::preflight_encode_observed(property, source, l.properties, w)?;
        m.property(pf)?;
        w.step()?;
    }
    let facts = m.facts(source, values.count(), l, w)?;
    for id in &input.output.columns {
        reference(id.get(), values, w)?;
    }
    for id in rollup_keys {
        reference(id.get(), values, w)?;
    }
    for set in grouping_sets {
        for id in set {
            reference(id.get(), values, w)?;
        }
    }
    for (from, to) in grouping_values {
        reference(from.get(), values, w)?;
        reference(to.get(), values, w)?;
    }
    for output in grouping_outputs {
        reference(output.output.get(), values, w)?;
        for id in &output.arguments {
            reference(id.get(), values, w)?;
        }
    }
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(&input.output_properties))
    {
        physical_property_refs(property, values, w)?;
    }
    Ok(facts)
}
fn prepare_decode(
    input: &wire::PhysicalNode,
    values: &impl Values,
    source: usize,
    l: RepeatNodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<RepeatNodeProjectionFacts, Error> {
    let Some(wire::physical_node::Kind::Repeat(repeat)) = input.kind.as_ref() else {
        return Err(invalid("wire physical node kind is not Repeat"));
    };
    let port = input
        .output
        .as_ref()
        .ok_or_else(|| invalid("Repeat output port is absent"))?;
    port.node_id
        .ok_or_else(|| invalid("Repeat output node ID is absent"))?;
    let output_property = input
        .output_properties
        .as_ref()
        .ok_or_else(|| invalid("Repeat output properties are absent"))?;
    let outer = add(
        add(input.required_inputs.len(), port.value_ids.len())?,
        add(
            add(
                repeat.rollup_key_value_ids.len(),
                repeat.grouping_sets.len(),
            )?,
            add(repeat.grouping_values.len(), repeat.grouping_outputs.len())?,
        )?,
    )?;
    let mut known = add(
        size_of::<wire::PhysicalNode>(),
        add(
            bytes::<u32>(input.input_node_ids.capacity())?,
            add(
                bytes::<wire::PhysicalProperties>(input.required_inputs.capacity())?,
                bytes::<u32>(port.value_ids.capacity())?,
            )?,
        )?,
    )?;
    known = add(
        known,
        add(
            bytes::<u32>(repeat.rollup_key_value_ids.capacity())?,
            add(
                bytes::<wire::ValueIds>(repeat.grouping_sets.capacity())?,
                add(
                    bytes::<wire::ValueMapping>(repeat.grouping_values.capacity())?,
                    bytes::<wire::GroupingOutput>(repeat.grouping_outputs.capacity())?,
                )?,
            )?,
        )?,
    )?;
    count_prefix(input.input_node_ids.len(), outer, source, known, l, w)?;
    floor(source, values.retained_floor(w)?, w)?;
    let mut m = Model {
        inputs: input.input_node_ids.len(),
        items: outer,
        refs: add(
            port.value_ids.len(),
            add(
                repeat.rollup_key_value_ids.len(),
                add(
                    mul(repeat.grouping_values.len(), 2)?,
                    repeat.grouping_outputs.len(),
                )?,
            )?,
        )?,
        ..Model::default()
    };
    m.request::<p::NodeId>(input.input_node_ids.len(), 2)?;
    m.request::<p::PhysicalProperties>(input.required_inputs.len(), 2)?;
    m.request::<p::ValueId>(port.value_ids.len(), 2)?;
    m.request::<p::ValueId>(repeat.rollup_key_value_ids.len(), 2)?;
    m.request::<Box<[p::ValueId]>>(repeat.grouping_sets.len(), 2)?;
    m.request::<(p::ValueId, p::ValueId)>(repeat.grouping_values.len(), 2)?;
    m.request::<p::GroupingOutput>(repeat.grouping_outputs.len(), 2)?;
    for set in &repeat.grouping_sets {
        m.items = add(m.items, set.value_ids.len())?;
        m.refs = add(m.refs, set.value_ids.len())?;
        known = add(known, bytes::<u32>(set.value_ids.capacity())?)?;
        m.request::<p::ValueId>(set.value_ids.len(), 2)?;
        w.step()?;
    }
    for output in &repeat.grouping_outputs {
        m.items = add(m.items, output.argument_value_ids.len())?;
        m.refs = add(m.refs, output.argument_value_ids.len())?;
        known = add(known, bytes::<u32>(output.argument_value_ids.capacity())?)?;
        m.request::<p::ValueId>(output.argument_value_ids.len(), 2)?;
        w.step()?;
    }
    floor(source, known, w)?;
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(output_property))
    {
        let pf = properties::preflight_decode_observed(property, source, l.properties, w)?;
        m.property(pf)?;
        w.step()?;
    }
    let facts = m.facts(source, values.count(), l, w)?;
    for id in &port.value_ids {
        reference(*id, values, w)?;
    }
    for id in &repeat.rollup_key_value_ids {
        reference(*id, values, w)?;
    }
    for set in &repeat.grouping_sets {
        for id in &set.value_ids {
            reference(*id, values, w)?;
        }
    }
    for pair in &repeat.grouping_values {
        let from = pair
            .source_value_id
            .ok_or_else(|| invalid("Repeat grouping source value is absent"))?;
        let to = pair
            .destination_value_id
            .ok_or_else(|| invalid("Repeat grouping destination value is absent"))?;
        reference(from, values, w)?;
        reference(to, values, w)?;
    }
    for output in &repeat.grouping_outputs {
        let id = output
            .output_value_id
            .ok_or_else(|| invalid("Repeat grouping output value is absent"))?;
        reference(id, values, w)?;
        for id in &output.argument_value_ids {
            reference(*id, values, w)?;
        }
    }
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(output_property))
    {
        wire_property_refs(property, values, w)?;
    }
    Ok(facts)
}

/// Sealed preparation borrowing the exact node and Value emission.
pub struct PreparedRepeatNodeEncode<'node, 'namespace, 'loan, 'source, 'control> {
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    source: usize,
    limits: RepeatNodeProjectionLimits,
    facts: RepeatNodeProjectionFacts,
}
impl PreparedRepeatNodeEncode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &RepeatNodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(wire::PhysicalNode, RepeatNodeProjectionFacts), Error> {
        let mut w =
            CompileCheckpoints::try_new(self.values.original_control(), CompilePhase::Encode)?;
        let result = emit_encode(self.input, self.source, self.limits, &mut w)
            .map(|node| (node, self.facts));
        finish(w, result)
    }
}
pub fn prepare_repeat_node_encode<'node, 'namespace, 'loan, 'source, 'control>(
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    source_retained_bytes: usize,
    limits: RepeatNodeProjectionLimits,
) -> Result<PreparedRepeatNodeEncode<'node, 'namespace, 'loan, 'source, 'control>, Error> {
    let mut w = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Encode)?;
    let result = prepare_encode(input, values, source_retained_bytes, limits, &mut w);
    let facts = finish(w, result)?;
    Ok(PreparedRepeatNodeEncode {
        input,
        values,
        source: source_retained_bytes,
        limits,
        facts,
    })
}
/// Sealed preparation borrowing the exact received node and Value owner.
pub struct PreparedRepeatNodeDecode<'node, 'namespace, 'loan, 'wire, 'control> {
    input: &'node wire::PhysicalNode,
    values: &'namespace DecodedValues<'loan, 'wire, 'control>,
    source: usize,
    limits: RepeatNodeProjectionLimits,
    facts: RepeatNodeProjectionFacts,
}
impl PreparedRepeatNodeDecode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &RepeatNodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(p::PhysicalNode, RepeatNodeProjectionFacts), Error> {
        let mut w =
            CompileCheckpoints::try_new(self.values.original_control(), CompilePhase::Decode)?;
        let result = emit_decode(self.input, self.source, self.limits, &mut w)
            .map(|node| (node, self.facts));
        finish(w, result)
    }
}
pub fn prepare_repeat_node_decode<'node, 'namespace, 'loan, 'wire, 'control>(
    input: &'node wire::PhysicalNode,
    values: &'namespace DecodedValues<'loan, 'wire, 'control>,
    source_retained_bytes: usize,
    limits: RepeatNodeProjectionLimits,
) -> Result<PreparedRepeatNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>, Error> {
    let mut w = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Decode)?;
    let result = prepare_decode(input, values, source_retained_bytes, limits, &mut w);
    let facts = finish(w, result)?;
    Ok(PreparedRepeatNodeDecode {
        input,
        values,
        source: source_retained_bytes,
        limits,
        facts,
    })
}
/// Single-phase complete projection. Preparation and emission share one meter.
pub fn encode_repeat_node(
    input: &p::PhysicalNode,
    values: &EncodedValues<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: RepeatNodeProjectionLimits,
) -> Result<(wire::PhysicalNode, RepeatNodeProjectionFacts), Error> {
    let mut w = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Encode)?;
    let result = (|| {
        let facts = prepare_encode(input, values, source_retained_bytes, limits, &mut w)?;
        let node = emit_encode(input, source_retained_bytes, limits, &mut w)?;
        Ok((node, facts))
    })();
    finish(w, result)
}
/// Single-phase receiving projection; Fragment/Package checks remain required.
pub fn decode_repeat_node(
    input: &wire::PhysicalNode,
    values: &DecodedValues<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: RepeatNodeProjectionLimits,
) -> Result<(p::PhysicalNode, RepeatNodeProjectionFacts), Error> {
    let mut w = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Decode)?;
    let result = (|| {
        let facts = prepare_decode(input, values, source_retained_bytes, limits, &mut w)?;
        let node = emit_decode(input, source_retained_bytes, limits, &mut w)?;
        Ok((node, facts))
    })();
    finish(w, result)
}
fn encode_ids(input: &[p::ValueId], w: &mut CompileCheckpoints<'_>) -> Result<Vec<u32>, Error> {
    let mut output = reserve(input.len(), w)?;
    for id in input {
        output.push(id.get());
        w.step()?;
    }
    Ok(output)
}
fn decode_ids(input: &[u32], w: &mut CompileCheckpoints<'_>) -> Result<Box<[p::ValueId]>, Error> {
    let mut output = reserve(input.len(), w)?;
    for id in input {
        output.push(p::ValueId::new(*id));
        w.step()?;
    }
    boxed(output, w)
}
fn emit_encode(
    input: &p::PhysicalNode,
    source: usize,
    l: RepeatNodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<wire::PhysicalNode, Error> {
    let p::NodeKind::Repeat {
        rollup_keys,
        grouping_sets,
        grouping_values,
        grouping_outputs,
    } = &input.kind
    else {
        return Err(invalid("prepared physical node kind is not Repeat"));
    };
    let mut inputs = reserve(input.inputs.len(), w)?;
    for id in &input.inputs {
        inputs.push(id.get());
        w.step()?;
    }
    let mut required_inputs = reserve(input.required_inputs.len(), w)?;
    for property in &input.required_inputs {
        required_inputs.push(properties::encode_observed(property, source, l.properties, w)?.0);
        w.step()?;
    }
    let output_properties =
        properties::encode_observed(&input.output_properties, source, l.properties, w)?.0;
    let output = wire::OutputPort {
        node_id: Some(input.output.node.get()),
        value_ids: encode_ids(&input.output.columns, w)?,
    };
    let rollup_key_value_ids = encode_ids(rollup_keys, w)?;
    let mut sets = reserve(grouping_sets.len(), w)?;
    for set in grouping_sets {
        sets.push(wire::ValueIds {
            value_ids: encode_ids(set, w)?,
        });
        w.step()?;
    }
    let mut mappings = reserve(grouping_values.len(), w)?;
    for (from, to) in grouping_values {
        mappings.push(wire::ValueMapping {
            source_value_id: Some(from.get()),
            destination_value_id: Some(to.get()),
        });
        w.step()?;
    }
    let mut outputs = reserve(grouping_outputs.len(), w)?;
    for output in grouping_outputs {
        outputs.push(wire::GroupingOutput {
            output_value_id: Some(output.output.get()),
            argument_value_ids: encode_ids(&output.arguments, w)?,
        });
        w.step()?;
    }
    let node = wire::PhysicalNode {
        id: input.id.get(),
        input_node_ids: inputs,
        required_inputs,
        output_properties: Some(output_properties),
        output: Some(output),
        kind: Some(wire::physical_node::Kind::Repeat(wire::RepeatNode {
            rollup_key_value_ids,
            grouping_sets: sets,
            grouping_values: mappings,
            grouping_outputs: outputs,
        })),
    };
    w.step()?;
    Ok(node)
}
fn emit_decode(
    input: &wire::PhysicalNode,
    source: usize,
    l: RepeatNodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::PhysicalNode, Error> {
    let Some(wire::physical_node::Kind::Repeat(repeat)) = input.kind.as_ref() else {
        return Err(invalid("prepared wire node kind is not Repeat"));
    };
    let port = input
        .output
        .as_ref()
        .ok_or_else(|| invalid("prepared Repeat output port is absent"))?;
    let mut inputs = reserve(input.input_node_ids.len(), w)?;
    for id in &input.input_node_ids {
        inputs.push(p::NodeId::new(*id));
        w.step()?;
    }
    let inputs = boxed(inputs, w)?;
    let mut required_inputs = reserve(input.required_inputs.len(), w)?;
    for property in &input.required_inputs {
        required_inputs.push(properties::decode_observed(property, source, l.properties, w)?.0);
        w.step()?;
    }
    let required_inputs = boxed(required_inputs, w)?;
    let output_properties = properties::decode_observed(
        input
            .output_properties
            .as_ref()
            .ok_or_else(|| invalid("prepared Repeat output properties are absent"))?,
        source,
        l.properties,
        w,
    )?
    .0;
    let output = p::OutputPort {
        node: p::NodeId::new(
            port.node_id
                .ok_or_else(|| invalid("prepared Repeat output node ID is absent"))?,
        ),
        columns: decode_ids(&port.value_ids, w)?,
    };
    let rollup_keys = decode_ids(&repeat.rollup_key_value_ids, w)?;
    let mut sets = reserve(repeat.grouping_sets.len(), w)?;
    for set in &repeat.grouping_sets {
        sets.push(decode_ids(&set.value_ids, w)?);
        w.step()?;
    }
    let grouping_sets = boxed(sets, w)?;
    let mut mappings = reserve(repeat.grouping_values.len(), w)?;
    for pair in &repeat.grouping_values {
        mappings.push((
            p::ValueId::new(
                pair.source_value_id
                    .ok_or_else(|| invalid("prepared Repeat grouping source is absent"))?,
            ),
            p::ValueId::new(
                pair.destination_value_id
                    .ok_or_else(|| invalid("prepared Repeat grouping destination is absent"))?,
            ),
        ));
        w.step()?;
    }
    let grouping_values = boxed(mappings, w)?;
    let mut outputs = reserve(repeat.grouping_outputs.len(), w)?;
    for output in &repeat.grouping_outputs {
        outputs.push(p::GroupingOutput {
            output: p::ValueId::new(
                output
                    .output_value_id
                    .ok_or_else(|| invalid("prepared Repeat grouping output is absent"))?,
            ),
            arguments: decode_ids(&output.argument_value_ids, w)?,
        });
        w.step()?;
    }
    let grouping_outputs = boxed(outputs, w)?;
    let node = p::PhysicalNode {
        id: p::NodeId::new(input.id),
        inputs,
        required_inputs,
        output_properties,
        output,
        kind: p::NodeKind::Repeat {
            rollup_keys,
            grouping_sets,
            grouping_values,
            grouping_outputs,
        },
    };
    w.step()?;
    Ok(node)
}

#[cfg(test)]
mod tests;
