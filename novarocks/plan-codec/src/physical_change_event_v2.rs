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

//! Complete ChangeEventExpand representation through original Values and
//! Expressions loans. Fragment owns mutation typing, graph and property proofs.

pub use crate::physical_node_v2::{
    NodeCodecError as ChangeEventNodeCodecError,
    NodeProjectionFacts as ChangeEventNodeProjectionFacts,
    NodeProjectionLimits as ChangeEventNodeProjectionLimits,
};
use crate::{
    physical_expression_v2::{DecodedExpressions, EncodedExpressions},
    physical_node_v2::*,
    physical_properties_v2 as properties,
    physical_value_v2::EncodedValues,
};
use novarocks_connector_contract::ConnectorRowMutationEffect;
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
type Error = ChangeEventNodeCodecError;

/// Sole v2 mapping. Effects describe SQL row mutation, not provider routing.
pub(crate) fn encode_mutation_effect(effect: ConnectorRowMutationEffect) -> i32 {
    match effect {
        ConnectorRowMutationEffect::Delete => wire::RowMutationEffect::Delete as i32,
        ConnectorRowMutationEffect::Replace => wire::RowMutationEffect::Replace as i32,
        ConnectorRowMutationEffect::Insert => wire::RowMutationEffect::Insert as i32,
    }
}
pub(crate) fn decode_mutation_effect(
    effect: i32,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ConnectorRowMutationEffect, Error> {
    let result = match wire::RowMutationEffect::try_from(effect) {
        Ok(wire::RowMutationEffect::Delete) => Ok(ConnectorRowMutationEffect::Delete),
        Ok(wire::RowMutationEffect::Replace) => Ok(ConnectorRowMutationEffect::Replace),
        Ok(wire::RowMutationEffect::Insert) => Ok(ConnectorRowMutationEffect::Insert),
        _ => Err(invalid("change-event effect is unspecified or unknown")),
    };
    work.step()?;
    result
}
fn required(id: Option<u32>, work: &mut CompileCheckpoints<'_>) -> Result<u32, Error> {
    let result = id.ok_or_else(|| invalid("change-event required reference is absent"));
    work.step()?;
    result
}
fn expression_encode(
    id: p::ExprId,
    expressions: &EncodedExpressions<'_, '_, '_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let found = expressions.expression_observed(id.get(), work)?.is_some();
    work.step()?;
    if found {
        Ok(())
    } else {
        Err(invalid(
            "change-event expression is absent from original emission",
        ))
    }
}
fn expression_decode(
    id: u32,
    expressions: &DecodedExpressions<'_, '_, '_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let found = expressions.definition_observed(id, work)?.is_some();
    work.step()?;
    if found {
        Ok(())
    } else {
        Err(invalid(
            "change-event expression is absent from original reception",
        ))
    }
}
fn physical_payload(input: &p::PhysicalNode) -> Result<(&[p::ChangeEventSpec], p::ValueId), Error> {
    match &input.kind {
        p::NodeKind::ChangeEventExpand {
            events,
            effect_output,
        } => Ok((events, *effect_output)),
        _ => Err(invalid("physical node is not change-event expansion")),
    }
}
fn wire_payload(input: &wire::PhysicalNode) -> Result<&wire::ChangeEventExpandNode, Error> {
    match input.kind.as_ref() {
        Some(wire::physical_node::Kind::ChangeEventExpand(v)) => Ok(v),
        _ => Err(invalid("wire node is not change-event expansion")),
    }
}
fn prepare_encode(
    input: &p::PhysicalNode,
    values: &EncodedValues<'_, '_, '_>,
    expressions: &EncodedExpressions<'_, '_, '_>,
    source: usize,
    limits: ChangeEventNodeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ChangeEventNodeProjectionFacts, Error> {
    let same = std::ptr::eq(values.types(), expressions.types())
        && std::ptr::eq(values.original_control(), expressions.original_control());
    work.step()?;
    if !same {
        return Err(invalid(
            "change-event namespaces differ in original type table or control",
        ));
    }
    let (events, effect_output) = physical_payload(input)?;
    let mut known = add(
        physical_header_floor(input)?,
        bytes::<p::ChangeEventSpec>(events.len())?,
    )?;
    let mut model = Model {
        inputs: input.inputs.len(),
        items: add(
            add(input.required_inputs.len(), input.output.columns.len())?,
            events.len(),
        )?,
        refs: add(input.output.columns.len(), 1)?,
        ..Model::default()
    };
    count_prefix(model.inputs, model.items, source, known, limits, work)?;
    floor(source, values.retained_floor(work)?, work)?;
    floor(source, expressions.retained_floor_observed(work)?, work)?;
    encode_header_requests(input, &mut model)?;
    model.request::<wire::ChangeEvent>(events.len(), 1)?;
    let mut occurrences = 0;
    for event in events {
        model.items = add(model.items, event.assignments.len())?;
        model.refs = add(model.refs, event.assignments.len())?;
        known = add(
            known,
            bytes::<(p::ValueId, Option<p::ExprId>)>(event.assignments.len())?,
        )?;
        model.request::<wire::ChangeAssignment>(event.assignments.len(), 1)?;
        count_prefix(model.inputs, model.items, source, known, limits, work)?;
        occurrences = add(occurrences, usize::from(event.predicate.is_some()))?;
        work.step()?;
        for (_, expression) in &event.assignments {
            occurrences = add(occurrences, usize::from(expression.is_some()))?;
            work.step()?;
        }
    }
    model.delegated_work = mul(occurrences, expressions.lookup_work_upper_bound()?)?;
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
    reference(effect_output.get(), values, work)?;
    for event in events {
        if let Some(predicate) = event.predicate {
            expression_encode(predicate, expressions, work)?;
        }
        for (output, expression) in &event.assignments {
            reference(output.get(), values, work)?;
            if let Some(expression) = expression {
                expression_encode(*expression, expressions, work)?;
            }
            work.step()?;
        }
        work.step()?;
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
    expressions: &DecodedExpressions<'_, '_, '_>,
    source: usize,
    limits: ChangeEventNodeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ChangeEventNodeProjectionFacts, Error> {
    let values = expressions.values();
    let payload = wire_payload(input)?;
    let port = input
        .output
        .as_ref()
        .ok_or_else(|| invalid("change-event output port is absent"))?;
    required(port.node_id, work)?;
    let output_property = input
        .output_properties
        .as_ref()
        .ok_or_else(|| invalid("change-event output properties are absent"))?;
    let mut known = add(
        wire_header_floor(input, port)?,
        bytes::<wire::ChangeEvent>(payload.events.capacity())?,
    )?;
    let mut model = Model {
        inputs: input.input_node_ids.len(),
        items: add(
            add(input.required_inputs.len(), port.value_ids.len())?,
            payload.events.len(),
        )?,
        refs: add(port.value_ids.len(), 1)?,
        ..Model::default()
    };
    count_prefix(model.inputs, model.items, source, known, limits, work)?;
    floor(source, values.retained_floor(work)?, work)?;
    floor(source, expressions.retained_floor_observed(work)?, work)?;
    decode_header_requests(input, port, &mut model)?;
    model.request::<p::ChangeEventSpec>(payload.events.len(), 2)?;
    let mut occurrences = 0;
    for event in &payload.events {
        model.items = add(model.items, event.assignments.len())?;
        model.refs = add(model.refs, event.assignments.len())?;
        known = add(
            known,
            bytes::<wire::ChangeAssignment>(event.assignments.capacity())?,
        )?;
        model.request::<(p::ValueId, Option<p::ExprId>)>(event.assignments.len(), 2)?;
        count_prefix(model.inputs, model.items, source, known, limits, work)?;
        occurrences = add(occurrences, usize::from(event.predicate_expr_id.is_some()))?;
        work.step()?;
        for assignment in &event.assignments {
            occurrences = add(occurrences, usize::from(assignment.expr_id.is_some()))?;
            work.step()?;
        }
    }
    model.delegated_work = mul(occurrences, expressions.lookup_work_upper_bound()?)?;
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
    for id in &port.value_ids {
        reference(*id, values, work)?;
    }
    reference(
        required(payload.effect_output_value_id, work)?,
        values,
        work,
    )?;
    for event in &payload.events {
        decode_mutation_effect(event.effect, work)?;
        if let Some(predicate) = event.predicate_expr_id {
            expression_decode(predicate, expressions, work)?;
        }
        for assignment in &event.assignments {
            reference(required(assignment.value_id, work)?, values, work)?;
            if let Some(expression) = assignment.expr_id {
                expression_decode(expression, expressions, work)?;
            }
            work.step()?;
        }
        work.step()?;
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
fn emit_encode(
    input: &p::PhysicalNode,
    source: usize,
    limits: ChangeEventNodeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::PhysicalNode, Error> {
    let (inputs, required_inputs, output_properties, output) =
        encode_header(input, source, limits, work)?;
    let (events, effect_output) = physical_payload(input)?;
    let mut emitted = reserve(events.len(), work)?;
    for event in events {
        let mut assignments = reserve(event.assignments.len(), work)?;
        for (value, expression) in &event.assignments {
            assignments.push(wire::ChangeAssignment {
                value_id: Some(value.get()),
                expr_id: expression.map(p::ExprId::get),
            });
            work.step()?;
        }
        emitted.push(wire::ChangeEvent {
            predicate_expr_id: event.predicate.map(p::ExprId::get),
            effect: encode_mutation_effect(event.effect),
            assignments,
        });
        work.step()?;
    }
    let node = wire::PhysicalNode {
        id: input.id.get(),
        input_node_ids: inputs,
        required_inputs,
        output_properties: Some(output_properties),
        output: Some(output),
        kind: Some(wire::physical_node::Kind::ChangeEventExpand(
            wire::ChangeEventExpandNode {
                events: emitted,
                effect_output_value_id: Some(effect_output.get()),
            },
        )),
    };
    work.step()?;
    Ok(node)
}
fn emit_decode(
    input: &wire::PhysicalNode,
    source: usize,
    limits: ChangeEventNodeProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<p::PhysicalNode, Error> {
    let (inputs, required_inputs, output_properties, output) =
        decode_header(input, source, limits, work)?;
    let payload = wire_payload(input)?;
    let mut emitted = reserve(payload.events.len(), work)?;
    for event in &payload.events {
        let mut assignments = reserve(event.assignments.len(), work)?;
        for assignment in &event.assignments {
            assignments.push((
                p::ValueId::new(required(assignment.value_id, work)?),
                assignment.expr_id.map(p::ExprId::new),
            ));
            work.step()?;
        }
        emitted.push(p::ChangeEventSpec {
            predicate: event.predicate_expr_id.map(p::ExprId::new),
            effect: decode_mutation_effect(event.effect, work)?,
            assignments: boxed(assignments, work)?,
        });
        work.step()?;
    }
    let node = p::PhysicalNode {
        id: p::NodeId::new(input.id),
        inputs,
        required_inputs,
        output_properties,
        output,
        kind: p::NodeKind::ChangeEventExpand {
            events: boxed(emitted, work)?,
            effect_output: p::ValueId::new(required(payload.effect_output_value_id, work)?),
        },
    };
    work.step()?;
    Ok(node)
}

/// Immutable preparation retains both actual emission loans. It does not rerun
/// source counting or reference validation when consumed.
pub struct PreparedChangeEventNodeEncode<'node, 'namespace, 'loan, 'source, 'control> {
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    source: usize,
    limits: ChangeEventNodeProjectionLimits,
    facts: ChangeEventNodeProjectionFacts,
}
impl PreparedChangeEventNodeEncode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &ChangeEventNodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(wire::PhysicalNode, ChangeEventNodeProjectionFacts), Error> {
        // Retaining this Values loan prevents exchanging the admitted namespace.
        let mut work =
            CompileCheckpoints::try_new(self.values.original_control(), CompilePhase::Encode)?;
        debug_assert!(std::ptr::eq(
            self.values.original_control(),
            self.expressions.original_control()
        ));
        let result = emit_encode(self.input, self.source, self.limits, &mut work)
            .map(|node| (node, self.facts));
        finish(work, result)
    }
}
pub fn prepare_change_event_node_encode<'node, 'namespace, 'loan, 'source, 'control>(
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    source_retained_bytes: usize,
    limits: ChangeEventNodeProjectionLimits,
) -> Result<PreparedChangeEventNodeEncode<'node, 'namespace, 'loan, 'source, 'control>, Error> {
    let mut work = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Encode)?;
    let result = prepare_encode(
        input,
        values,
        expressions,
        source_retained_bytes,
        limits,
        &mut work,
    );
    let facts = finish(work, result)?;
    Ok(PreparedChangeEventNodeEncode {
        input,
        values,
        expressions,
        source: source_retained_bytes,
        limits,
        facts,
    })
}
pub struct PreparedChangeEventNodeDecode<'node, 'namespace, 'loan, 'wire, 'control> {
    input: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    source: usize,
    limits: ChangeEventNodeProjectionLimits,
    facts: ChangeEventNodeProjectionFacts,
}
impl PreparedChangeEventNodeDecode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &ChangeEventNodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(p::PhysicalNode, ChangeEventNodeProjectionFacts), Error> {
        let mut work =
            CompileCheckpoints::try_new(self.expressions.original_control(), CompilePhase::Decode)?;
        let result = emit_decode(self.input, self.source, self.limits, &mut work)
            .map(|node| (node, self.facts));
        finish(work, result)
    }
}
pub fn prepare_change_event_node_decode<'node, 'namespace, 'loan, 'wire, 'control>(
    input: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    source_retained_bytes: usize,
    limits: ChangeEventNodeProjectionLimits,
) -> Result<PreparedChangeEventNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>, Error> {
    let mut work =
        CompileCheckpoints::try_new(expressions.original_control(), CompilePhase::Decode)?;
    let result = prepare_decode(input, expressions, source_retained_bytes, limits, &mut work);
    let facts = finish(work, result)?;
    Ok(PreparedChangeEventNodeDecode {
        input,
        expressions,
        source: source_retained_bytes,
        limits,
        facts,
    })
}
pub fn encode_change_event_node(
    input: &p::PhysicalNode,
    values: &EncodedValues<'_, '_, '_>,
    expressions: &EncodedExpressions<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: ChangeEventNodeProjectionLimits,
) -> Result<(wire::PhysicalNode, ChangeEventNodeProjectionFacts), Error> {
    let mut work = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Encode)?;
    let result = (|| {
        let facts = prepare_encode(
            input,
            values,
            expressions,
            source_retained_bytes,
            limits,
            &mut work,
        )?;
        let node = emit_encode(input, source_retained_bytes, limits, &mut work)?;
        Ok((node, facts))
    })();
    finish(work, result)
}
pub fn decode_change_event_node(
    input: &wire::PhysicalNode,
    expressions: &DecodedExpressions<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: ChangeEventNodeProjectionLimits,
) -> Result<(p::PhysicalNode, ChangeEventNodeProjectionFacts), Error> {
    let mut work =
        CompileCheckpoints::try_new(expressions.original_control(), CompilePhase::Decode)?;
    let result = (|| {
        let facts = prepare_decode(input, expressions, source_retained_bytes, limits, &mut work)?;
        let node = emit_decode(input, source_retained_bytes, limits, &mut work)?;
        Ok((node, facts))
    })();
    finish(work, result)
}
#[cfg(test)]
mod tests;
