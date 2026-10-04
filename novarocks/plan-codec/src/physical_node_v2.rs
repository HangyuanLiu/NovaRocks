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

//! Shared complete node envelope resource and observed namespace author.
//! Kind semantics and Fragment/Package closure remain with their original owners.

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
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};
use std::{alloc::Layout, fmt, mem::size_of};

#[derive(Clone, Copy, Debug)]
pub struct NodeProjectionLimits {
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
pub struct NodeProjectionFacts {
    pub input_node_count: usize,
    pub value_reference_count: usize,
    pub list_item_count: usize,
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}
#[derive(Debug)]
pub enum NodeCodecError {
    Control(CompileControlError),
    Properties(PhysicalPropertyCodecError),
    Value(ValueCodecError),
    InvalidShape(&'static str),
}
impl From<CompileControlError> for NodeCodecError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl From<PhysicalPropertyCodecError> for NodeCodecError {
    fn from(error: PhysicalPropertyCodecError) -> Self {
        match error {
            PhysicalPropertyCodecError::Control(cause) => Self::Control(cause),
            error => Self::Properties(error),
        }
    }
}
impl From<ValueCodecError> for NodeCodecError {
    fn from(error: ValueCodecError) -> Self {
        match error {
            ValueCodecError::Control(cause) => Self::Control(cause),
            error => Self::Value(error),
        }
    }
}
impl fmt::Display for NodeCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Properties(e) => e.fmt(f),
            Self::Value(e) => e.fmt(f),
            Self::InvalidShape(s) => f.write_str(s),
        }
    }
}
impl std::error::Error for NodeCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(e) => Some(e),
            Self::Properties(e) => Some(e),
            Self::Value(e) => Some(e),
            Self::InvalidShape(_) => None,
        }
    }
}
type Error = NodeCodecError;
pub(crate) fn invalid(text: &'static str) -> Error {
    Error::InvalidShape(text)
}
pub(crate) fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b)
        .ok_or_else(|| invalid("Repeat resource sum overflow"))
}
pub(crate) fn mul(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_mul(b)
        .ok_or_else(|| invalid("Repeat resource product overflow"))
}
pub(crate) fn bytes<T>(n: usize) -> Result<usize, Error> {
    Layout::array::<T>(n)
        .map(|layout| layout.size())
        .map_err(|_| invalid("Repeat allocation layout is unrepresentable"))
}
pub(crate) fn cap(n: usize, maximum: usize, w: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    let accepted = n <= maximum;
    w.step()?;
    if accepted {
        Ok(())
    } else {
        Err(invalid("Repeat projection envelope exceeded"))
    }
}
pub(crate) fn floor(
    invoice: usize,
    known: usize,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let accepted = invoice >= known;
    w.step()?;
    if accepted {
        Ok(())
    } else {
        Err(invalid("Repeat source invoice omits original backing"))
    }
}
pub(crate) fn finish<T>(w: CompileCheckpoints<'_>, result: Result<T, Error>) -> Result<T, Error> {
    if matches!(&result, Err(Error::Control(_))) {
        return result;
    }
    w.finish()?;
    result
}
pub(crate) fn reserve<T>(n: usize, w: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, Error> {
    bytes::<T>(n)?;
    w.flush()?;
    let mut output = Vec::new();
    let result = output.try_reserve_exact(n);
    reserve_exit::<Error>(result, w)?;
    Ok(output)
}
pub(crate) fn boxed<T>(input: Vec<T>, w: &mut CompileCheckpoints<'_>) -> Result<Box<[T]>, Error> {
    w.flush()?;
    let output = input.into_boxed_slice();
    w.flush()?;
    Ok(output)
}

pub(crate) trait Values {
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
pub(crate) fn reference(
    id: u32,
    values: &impl Values,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
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
pub(crate) fn physical_property_refs(
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
pub(crate) fn wire_property_refs(
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
pub(crate) struct Model {
    pub(crate) inputs: usize,
    pub(crate) refs: usize,
    pub(crate) items: usize,
    pub(crate) requests: usize,
    pub(crate) requested: usize,
    pub(crate) delegated_work: usize,
}
impl Model {
    pub(crate) fn request<T>(&mut self, n: usize, copies: usize) -> Result<(), Error> {
        self.requested = add(self.requested, mul(bytes::<T>(n)?, copies)?)?;
        if n != 0 {
            self.requests = add(self.requests, copies)?;
        }
        Ok(())
    }
    pub(crate) fn property(&mut self, facts: PhysicalPropertyProjectionFacts) -> Result<(), Error> {
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
    pub(crate) fn facts(
        &self,
        source: usize,
        values: usize,
        l: NodeProjectionLimits,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<NodeProjectionFacts, Error> {
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
        let facts = NodeProjectionFacts {
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
pub(crate) fn count_prefix(
    inputs: usize,
    items: usize,
    source: usize,
    known: usize,
    l: NodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    cap(inputs, l.max_input_nodes, w)?;
    cap(items, l.max_list_items, w)?;
    // This lower bound precedes all variable-length numerical counting.
    cap(add(256, mul(add(items, inputs)?, 32)?)?, l.max_work, w)?;
    floor(source, known, w)
}

/// Header contributions only: callers keep their original stage ordering.
pub(crate) fn physical_header_floor(input: &p::PhysicalNode) -> Result<usize, Error> {
    add(
        size_of::<p::PhysicalNode>(),
        add(
            bytes::<p::NodeId>(input.inputs.len())?,
            add(
                bytes::<p::PhysicalProperties>(input.required_inputs.len())?,
                bytes::<p::ValueId>(input.output.columns.len())?,
            )?,
        )?,
    )
}
pub(crate) fn wire_header_floor(
    input: &wire::PhysicalNode,
    port: &wire::OutputPort,
) -> Result<usize, Error> {
    add(
        size_of::<wire::PhysicalNode>(),
        add(
            bytes::<u32>(input.input_node_ids.capacity())?,
            add(
                bytes::<wire::PhysicalProperties>(input.required_inputs.capacity())?,
                bytes::<u32>(port.value_ids.capacity())?,
            )?,
        )?,
    )
}
pub(crate) fn encode_header_requests(
    input: &p::PhysicalNode,
    model: &mut Model,
) -> Result<(), Error> {
    model.request::<u32>(input.inputs.len(), 1)?;
    model.request::<wire::PhysicalProperties>(input.required_inputs.len(), 1)?;
    model.request::<u32>(input.output.columns.len(), 1)
}
pub(crate) fn decode_header_requests(
    input: &wire::PhysicalNode,
    port: &wire::OutputPort,
    model: &mut Model,
) -> Result<(), Error> {
    model.request::<p::NodeId>(input.input_node_ids.len(), 2)?;
    model.request::<p::PhysicalProperties>(input.required_inputs.len(), 2)?;
    model.request::<p::ValueId>(port.value_ids.len(), 2)
}
pub(crate) fn encode_ids(
    input: &[p::ValueId],
    w: &mut CompileCheckpoints<'_>,
) -> Result<Vec<u32>, Error> {
    let mut output = reserve(input.len(), w)?;
    for id in input {
        output.push(id.get());
        w.step()?;
    }
    Ok(output)
}
pub(crate) fn decode_ids(
    input: &[u32],
    w: &mut CompileCheckpoints<'_>,
) -> Result<Box<[p::ValueId]>, Error> {
    let mut output = reserve(input.len(), w)?;
    for id in input {
        output.push(p::ValueId::new(*id));
        w.step()?;
    }
    boxed(output, w)
}
pub(crate) type EncodedHeader = (
    Vec<u32>,
    Vec<wire::PhysicalProperties>,
    wire::PhysicalProperties,
    wire::OutputPort,
);
pub(crate) type DecodedHeader = (
    Box<[p::NodeId]>,
    Box<[p::PhysicalProperties]>,
    p::PhysicalProperties,
    p::OutputPort,
);
pub(crate) fn encode_header(
    input: &p::PhysicalNode,
    source: usize,
    limits: NodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<EncodedHeader, Error> {
    let mut inputs = reserve(input.inputs.len(), w)?;
    for id in &input.inputs {
        inputs.push(id.get());
        w.step()?;
    }
    let mut required = reserve(input.required_inputs.len(), w)?;
    for property in &input.required_inputs {
        required.push(properties::encode_observed(property, source, limits.properties, w)?.0);
        w.step()?;
    }
    let output_properties =
        properties::encode_observed(&input.output_properties, source, limits.properties, w)?.0;
    let output = wire::OutputPort {
        node_id: Some(input.output.node.get()),
        value_ids: encode_ids(&input.output.columns, w)?,
    };
    Ok((inputs, required, output_properties, output))
}
pub(crate) fn decode_header(
    input: &wire::PhysicalNode,
    source: usize,
    limits: NodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<DecodedHeader, Error> {
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
    let mut required = reserve(input.required_inputs.len(), w)?;
    for property in &input.required_inputs {
        required.push(properties::decode_observed(property, source, limits.properties, w)?.0);
        w.step()?;
    }
    let required = boxed(required, w)?;
    let output_properties = properties::decode_observed(
        input
            .output_properties
            .as_ref()
            .ok_or_else(|| invalid("prepared Repeat output properties are absent"))?,
        source,
        limits.properties,
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
    Ok((inputs, required, output_properties, output))
}
