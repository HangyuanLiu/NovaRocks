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

//! Complete row-count assertion node projection through the shared node author.
//! The original Fragment/Package author still checks visibility, key co-location,
//! label arity, output ownership and relational properties. Invoices are caller
//! backing facts and finite projection ceilings, not formal allocator grants.

pub use crate::physical_node_v2::{
    NodeCodecError as AssertRowsNodeCodecError,
    NodeProjectionFacts as AssertRowsNodeProjectionFacts,
    NodeProjectionLimits as AssertRowsNodeProjectionLimits,
};
use crate::{
    allocation_exit_v2::reserve_exit,
    physical_node_v2::*,
    physical_properties_v2 as properties,
    physical_value_v2::{DecodedValues, EncodedValues},
};
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
type Error = AssertRowsNodeCodecError;

fn encode_comparison(value: p::RowCountAssertion) -> wire::RowCountComparison {
    match value {
        p::RowCountAssertion::Eq => wire::RowCountComparison::Eq,
        p::RowCountAssertion::Ne => wire::RowCountComparison::Ne,
        p::RowCountAssertion::Lt => wire::RowCountComparison::Lt,
        p::RowCountAssertion::Le => wire::RowCountComparison::Le,
        p::RowCountAssertion::Gt => wire::RowCountComparison::Gt,
        p::RowCountAssertion::Ge => wire::RowCountComparison::Ge,
    }
}
fn decode_comparison(value: i32) -> Result<p::RowCountAssertion, Error> {
    match wire::RowCountComparison::try_from(value) {
        Ok(wire::RowCountComparison::Eq) => Ok(p::RowCountAssertion::Eq),
        Ok(wire::RowCountComparison::Ne) => Ok(p::RowCountAssertion::Ne),
        Ok(wire::RowCountComparison::Lt) => Ok(p::RowCountAssertion::Lt),
        Ok(wire::RowCountComparison::Le) => Ok(p::RowCountAssertion::Le),
        Ok(wire::RowCountComparison::Gt) => Ok(p::RowCountAssertion::Gt),
        Ok(wire::RowCountComparison::Ge) => Ok(p::RowCountAssertion::Ge),
        _ => Err(invalid("row-count comparison is absent or unknown")),
    }
}
fn physical_outer(
    input: &p::PhysicalNode,
    assertion: &p::RowCountAssertionSpec,
) -> Result<usize, Error> {
    let header = add(input.required_inputs.len(), input.output.columns.len())?;
    match assertion {
        p::RowCountAssertionSpec::Global { .. } => Ok(header),
        p::RowCountAssertionSpec::PerKeyAtMostOne { keys, labels, .. } => {
            add(header, add(keys.len(), labels.len())?)
        }
    }
}
fn wire_outer(
    input: &wire::PhysicalNode,
    port: &wire::OutputPort,
    assertion: &wire::row_count_assertion_node::Kind,
) -> Result<usize, Error> {
    let header = add(input.required_inputs.len(), port.value_ids.len())?;
    match assertion {
        wire::row_count_assertion_node::Kind::Global(_) => Ok(header),
        wire::row_count_assertion_node::Kind::PerKeyAtMostOne(v) => {
            add(header, add(v.key_value_ids.len(), v.labels.len())?)
        }
    }
}
fn prepare_encode(
    input: &p::PhysicalNode,
    values: &impl Values,
    source: usize,
    limits: AssertRowsNodeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AssertRowsNodeProjectionFacts, Error> {
    let p::NodeKind::AssertOneRow(assertion) = &input.kind else {
        return Err(invalid("physical node kind is not AssertOneRow"));
    };
    let outer = physical_outer(input, assertion)?;
    let mut known = physical_header_floor(input)?;
    known = add(
        known,
        match assertion {
            p::RowCountAssertionSpec::Global { subject, .. } => subject.len(),
            p::RowCountAssertionSpec::PerKeyAtMostOne {
                keys,
                labels,
                message,
            } => add(
                bytes::<p::ValueId>(keys.len())?,
                add(bytes::<Box<str>>(labels.len())?, message.len())?,
            )?,
        },
    )?;
    count_prefix(input.inputs.len(), outer, source, known, limits, work)?;
    floor(source, values.retained_floor(work)?, work)?;
    let mut model = Model {
        inputs: input.inputs.len(),
        items: outer,
        refs: input.output.columns.len(),
        ..Model::default()
    };
    encode_header_requests(input, &mut model)?;
    match assertion {
        p::RowCountAssertionSpec::Global { subject, .. } => {
            model.request::<u8>(subject.len(), 1)?;
        }
        p::RowCountAssertionSpec::PerKeyAtMostOne {
            keys,
            labels,
            message,
        } => {
            model.refs = add(model.refs, keys.len())?;
            model.request::<u32>(keys.len(), 1)?;
            model.request::<String>(labels.len(), 1)?;
            model.request::<u8>(message.len(), 1)?;
            for label in labels {
                known = add(known, label.len())?;
                model.request::<u8>(label.len(), 1)?;
                work.step()?;
            }
        }
    }
    floor(source, known, work)?;
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(&input.output_properties))
    {
        model.property(properties::preflight_encode_observed(
            property,
            source,
            limits.properties,
            work,
        )?)?;
        work.step()?;
    }
    let facts = model.facts(source, values.count(), limits, work)?;
    for id in &input.output.columns {
        reference(id.get(), values, work)?;
    }
    if let p::RowCountAssertionSpec::PerKeyAtMostOne { keys, .. } = assertion {
        for id in keys {
            reference(id.get(), values, work)?;
        }
    }
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(&input.output_properties))
    {
        physical_property_refs(property, values, work)?;
    }
    Ok(facts)
}
fn prepare_decode(
    input: &wire::PhysicalNode,
    values: &impl Values,
    source: usize,
    limits: AssertRowsNodeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AssertRowsNodeProjectionFacts, Error> {
    let Some(wire::physical_node::Kind::AssertOneRow(assertion)) = input.kind.as_ref() else {
        return Err(invalid("wire physical node kind is not AssertOneRow"));
    };
    let assertion = assertion
        .kind
        .as_ref()
        .ok_or_else(|| invalid("row-count assertion kind is absent"))?;
    let port = input
        .output
        .as_ref()
        .ok_or_else(|| invalid("AssertOneRow output port is absent"))?;
    port.node_id
        .ok_or_else(|| invalid("AssertOneRow output node ID is absent"))?;
    let output_property = input
        .output_properties
        .as_ref()
        .ok_or_else(|| invalid("AssertOneRow output properties are absent"))?;
    let outer = wire_outer(input, port, assertion)?;
    let mut known = wire_header_floor(input, port)?;
    known = add(
        known,
        match assertion {
            wire::row_count_assertion_node::Kind::Global(v) => v.subject.capacity(),
            wire::row_count_assertion_node::Kind::PerKeyAtMostOne(v) => add(
                bytes::<u32>(v.key_value_ids.capacity())?,
                add(bytes::<String>(v.labels.capacity())?, v.message.capacity())?,
            )?,
        },
    )?;
    count_prefix(
        input.input_node_ids.len(),
        outer,
        source,
        known,
        limits,
        work,
    )?;
    floor(source, values.retained_floor(work)?, work)?;
    let mut model = Model {
        inputs: input.input_node_ids.len(),
        items: outer,
        refs: port.value_ids.len(),
        ..Model::default()
    };
    decode_header_requests(input, port, &mut model)?;
    match assertion {
        wire::row_count_assertion_node::Kind::Global(v) => {
            model.request::<u8>(v.subject.len(), 2)?;
        }
        wire::row_count_assertion_node::Kind::PerKeyAtMostOne(v) => {
            model.refs = add(model.refs, v.key_value_ids.len())?;
            model.request::<p::ValueId>(v.key_value_ids.len(), 2)?;
            model.request::<Box<str>>(v.labels.len(), 2)?;
            model.request::<u8>(v.message.len(), 2)?;
            for label in &v.labels {
                known = add(known, label.capacity())?;
                model.request::<u8>(label.len(), 2)?;
                work.step()?;
            }
        }
    }
    floor(source, known, work)?;
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(output_property))
    {
        model.property(properties::preflight_decode_observed(
            property,
            source,
            limits.properties,
            work,
        )?)?;
        work.step()?;
    }
    let facts = model.facts(source, values.count(), limits, work)?;
    if let wire::row_count_assertion_node::Kind::Global(v) = assertion {
        let comparison = decode_comparison(v.comparison);
        work.step()?;
        comparison?;
    }
    for id in &port.value_ids {
        reference(*id, values, work)?;
    }
    if let wire::row_count_assertion_node::Kind::PerKeyAtMostOne(v) = assertion {
        for id in &v.key_value_ids {
            reference(*id, values, work)?;
        }
    }
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(output_property))
    {
        wire_property_refs(property, values, work)?;
    }
    Ok(facts)
}

fn copy_text(input: &str, work: &mut CompileCheckpoints<'_>) -> Result<String, Error> {
    bytes::<u8>(input.len())?;
    work.flush()?;
    let mut output = String::new();
    let reserved = output.try_reserve_exact(input.len());
    reserve_exit::<Error>(reserved, work)?;
    let mut at = 0;
    while at < input.len() {
        let mut end = at + (input.len() - at).min(256);
        loop {
            let aligned = input.is_char_boundary(end);
            work.step()?;
            if aligned {
                break;
            }
            end -= 1;
        }
        // The complete chunk is copied after the prior checkpoint and then
        // charged as actual completed bytes, never as a prospective prescan.
        work.flush()?;
        output.push_str(&input[at..end]);
        for _ in at..end {
            work.step()?;
        }
        at = end;
    }
    Ok(output)
}
fn copy_boxed_text(input: &str, work: &mut CompileCheckpoints<'_>) -> Result<Box<str>, Error> {
    let text = copy_text(input, work)?;
    work.flush()?;
    let text = text.into_boxed_str();
    work.flush()?;
    Ok(text)
}
fn emit_encode(
    input: &p::PhysicalNode,
    source: usize,
    limits: AssertRowsNodeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::PhysicalNode, Error> {
    let p::NodeKind::AssertOneRow(assertion) = &input.kind else {
        return Err(invalid("prepared physical node kind is not AssertOneRow"));
    };
    let (inputs, required_inputs, output_properties, output) =
        encode_header(input, source, limits, work)?;
    let kind = match assertion {
        p::RowCountAssertionSpec::Global {
            subject,
            desired_rows,
            comparison,
        } => wire::row_count_assertion_node::Kind::Global(wire::GlobalRowCountAssertion {
            subject: copy_text(subject, work)?,
            desired_rows: *desired_rows,
            comparison: encode_comparison(*comparison) as i32,
        }),
        p::RowCountAssertionSpec::PerKeyAtMostOne {
            keys,
            labels,
            message,
        } => {
            let key_value_ids = encode_ids(keys, work)?;
            let mut copied = reserve(labels.len(), work)?;
            for label in labels {
                copied.push(copy_text(label, work)?);
                work.step()?;
            }
            wire::row_count_assertion_node::Kind::PerKeyAtMostOne(wire::PerKeyRowCountAssertion {
                key_value_ids,
                labels: copied,
                message: copy_text(message, work)?,
            })
        }
    };
    let node = wire::PhysicalNode {
        id: input.id.get(),
        input_node_ids: inputs,
        required_inputs,
        output_properties: Some(output_properties),
        output: Some(output),
        kind: Some(wire::physical_node::Kind::AssertOneRow(
            wire::RowCountAssertionNode { kind: Some(kind) },
        )),
    };
    work.step()?;
    Ok(node)
}
fn emit_decode(
    input: &wire::PhysicalNode,
    source: usize,
    limits: AssertRowsNodeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<p::PhysicalNode, Error> {
    let Some(wire::physical_node::Kind::AssertOneRow(assertion)) = input.kind.as_ref() else {
        return Err(invalid(
            "prepared wire physical node kind is not AssertOneRow",
        ));
    };
    let assertion = assertion
        .kind
        .as_ref()
        .ok_or_else(|| invalid("prepared row-count assertion kind is absent"))?;
    let (inputs, required_inputs, output_properties, output) =
        decode_header(input, source, limits, work)?;
    let assertion = match assertion {
        wire::row_count_assertion_node::Kind::Global(v) => p::RowCountAssertionSpec::Global {
            subject: copy_boxed_text(&v.subject, work)?,
            desired_rows: v.desired_rows,
            comparison: decode_comparison(v.comparison)?,
        },
        wire::row_count_assertion_node::Kind::PerKeyAtMostOne(v) => {
            let keys = decode_ids(&v.key_value_ids, work)?;
            let mut labels = reserve(v.labels.len(), work)?;
            for label in &v.labels {
                labels.push(copy_boxed_text(label, work)?);
                work.step()?;
            }
            p::RowCountAssertionSpec::PerKeyAtMostOne {
                keys,
                labels: boxed(labels, work)?,
                message: copy_boxed_text(&v.message, work)?,
            }
        }
    };
    let node = p::PhysicalNode {
        id: p::NodeId::new(input.id),
        inputs,
        required_inputs,
        output_properties,
        output,
        kind: p::NodeKind::AssertOneRow(assertion),
    };
    work.step()?;
    Ok(node)
}

/// Sealed preparation borrowing the exact node and Value emission.
pub struct PreparedAssertRowsNodeEncode<'node, 'namespace, 'loan, 'source, 'control> {
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    source: usize,
    limits: AssertRowsNodeProjectionLimits,
    facts: AssertRowsNodeProjectionFacts,
}
impl PreparedAssertRowsNodeEncode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &AssertRowsNodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(wire::PhysicalNode, AssertRowsNodeProjectionFacts), Error> {
        let mut w =
            CompileCheckpoints::try_new(self.values.original_control(), CompilePhase::Encode)?;
        let result = emit_encode(self.input, self.source, self.limits, &mut w)
            .map(|node| (node, self.facts));
        finish(w, result)
    }
}
pub fn prepare_assert_rows_node_encode<'node, 'namespace, 'loan, 'source, 'control>(
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    source_retained_bytes: usize,
    limits: AssertRowsNodeProjectionLimits,
) -> Result<PreparedAssertRowsNodeEncode<'node, 'namespace, 'loan, 'source, 'control>, Error> {
    let mut w = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Encode)?;
    let result = prepare_encode(input, values, source_retained_bytes, limits, &mut w);
    let facts = finish(w, result)?;
    Ok(PreparedAssertRowsNodeEncode {
        input,
        values,
        source: source_retained_bytes,
        limits,
        facts,
    })
}
/// Sealed preparation borrowing the exact received node and Value owner.
pub struct PreparedAssertRowsNodeDecode<'node, 'namespace, 'loan, 'wire, 'control> {
    input: &'node wire::PhysicalNode,
    values: &'namespace DecodedValues<'loan, 'wire, 'control>,
    source: usize,
    limits: AssertRowsNodeProjectionLimits,
    facts: AssertRowsNodeProjectionFacts,
}
impl PreparedAssertRowsNodeDecode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &AssertRowsNodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(p::PhysicalNode, AssertRowsNodeProjectionFacts), Error> {
        let mut w =
            CompileCheckpoints::try_new(self.values.original_control(), CompilePhase::Decode)?;
        let result = emit_decode(self.input, self.source, self.limits, &mut w)
            .map(|node| (node, self.facts));
        finish(w, result)
    }
}
pub fn prepare_assert_rows_node_decode<'node, 'namespace, 'loan, 'wire, 'control>(
    input: &'node wire::PhysicalNode,
    values: &'namespace DecodedValues<'loan, 'wire, 'control>,
    source_retained_bytes: usize,
    limits: AssertRowsNodeProjectionLimits,
) -> Result<PreparedAssertRowsNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>, Error> {
    let mut w = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Decode)?;
    let result = prepare_decode(input, values, source_retained_bytes, limits, &mut w);
    let facts = finish(w, result)?;
    Ok(PreparedAssertRowsNodeDecode {
        input,
        values,
        source: source_retained_bytes,
        limits,
        facts,
    })
}
/// Single-phase complete projection. Preparation and emission share one meter.
pub fn encode_assert_rows_node(
    input: &p::PhysicalNode,
    values: &EncodedValues<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: AssertRowsNodeProjectionLimits,
) -> Result<(wire::PhysicalNode, AssertRowsNodeProjectionFacts), Error> {
    let mut w = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Encode)?;
    let result = (|| {
        let facts = prepare_encode(input, values, source_retained_bytes, limits, &mut w)?;
        let node = emit_encode(input, source_retained_bytes, limits, &mut w)?;
        Ok((node, facts))
    })();
    finish(w, result)
}
/// Single-phase receiving projection; Fragment/Package checks remain required.
pub fn decode_assert_rows_node(
    input: &wire::PhysicalNode,
    values: &DecodedValues<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: AssertRowsNodeProjectionLimits,
) -> Result<(p::PhysicalNode, AssertRowsNodeProjectionFacts), Error> {
    let mut w = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Decode)?;
    let result = (|| {
        let facts = prepare_decode(input, values, source_retained_bytes, limits, &mut w)?;
        let node = emit_decode(input, source_retained_bytes, limits, &mut w)?;
        Ok((node, facts))
    })();
    finish(w, result)
}

#[cfg(test)]
mod tests;
