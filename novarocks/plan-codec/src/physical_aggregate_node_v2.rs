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

//! Complete Aggregate node representation with original namespace loans.
//! Group completeness, phase/state compatibility, output roles and installed
//! implementations remain the original Fragment/Package owners' obligations.

pub use crate::physical_node_v2::{
    NodeCodecError as AggregateNodeCodecError, NodeProjectionFacts as AggregateNodeProjectionFacts,
};
use crate::{
    physical_aggregate_binding_v2::{
        MaterializedAggregateBindings, copy_aggregate_binding_observed,
        preflight_aggregate_binding_copy_counts, preflight_aggregate_binding_copy_types,
    },
    physical_binding_v2::{BindingProjectionLimits, MaterializationModel},
    physical_expression_v2::{DecodedExpressions, EncodedExpressions},
    physical_node_v2::*,
    physical_properties_v2 as properties,
    physical_relational_nodes_v2::{decode_sorts, encode_sorts},
    physical_value_v2::EncodedValues,
};
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
type Error = AggregateNodeCodecError;

#[derive(Clone, Copy, Debug)]
pub struct AggregateNodeProjectionLimits {
    pub node: NodeProjectionLimits,
    pub binding: BindingProjectionLimits,
}
fn completed<T>(r: Result<T, Error>, w: &mut CompileCheckpoints<'_>) -> Result<T, Error> {
    if matches!(&r, Err(Error::Control(_))) {
        return r;
    }
    w.step()?;
    r
}
fn required(id: Option<u32>, w: &mut CompileCheckpoints<'_>) -> Result<u32, Error> {
    completed(
        id.ok_or_else(|| invalid("Aggregate required reference is absent")),
        w,
    )
}
type AggregateBody<'a> = (
    &'a [(p::ExprId, p::ValueId)],
    &'a [p::AggregateCall],
    p::AggregateGrouping,
);
fn physical(input: &p::PhysicalNode) -> Result<AggregateBody<'_>, Error> {
    match &input.kind {
        p::NodeKind::Aggregate {
            group_by,
            calls,
            grouping,
        } => Ok((group_by, calls, *grouping)),
        _ => Err(invalid("physical node is not Aggregate")),
    }
}
fn raw(input: &wire::PhysicalNode) -> Result<&wire::AggregateNode, Error> {
    match input.kind.as_ref() {
        Some(wire::physical_node::Kind::Aggregate(body)) => Ok(body),
        _ => Err(invalid("wire node is not Aggregate")),
    }
}
fn encode_grouping(g: p::AggregateGrouping) -> i32 {
    match g {
        p::AggregateGrouping::Partial => wire::AggregateGrouping::Partial as i32,
        p::AggregateGrouping::Complete => wire::AggregateGrouping::Complete as i32,
    }
}
fn decode_grouping(g: i32) -> Result<p::AggregateGrouping, Error> {
    match wire::AggregateGrouping::try_from(g) {
        Ok(wire::AggregateGrouping::Partial) => Ok(p::AggregateGrouping::Partial),
        Ok(wire::AggregateGrouping::Complete) => Ok(p::AggregateGrouping::Complete),
        Ok(wire::AggregateGrouping::Unspecified) | Err(_) => {
            Err(invalid("Aggregate grouping is unknown or unspecified"))
        }
    }
}
fn encoded_expr(
    id: u32,
    e: &EncodedExpressions<'_, '_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let present = e.expression_observed(id, w)?.is_some();
    completed(
        if present {
            Ok(())
        } else {
            Err(invalid("Aggregate expression is not in original namespace"))
        },
        w,
    )
}
fn decoded_expr(
    id: u32,
    e: &DecodedExpressions<'_, '_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let present = e.definition_observed(id, w)?.is_some();
    completed(
        if present {
            Ok(())
        } else {
            Err(invalid("Aggregate expression is not in original namespace"))
        },
        w,
    )
}
fn binding<'a>(
    id: u32,
    a: &'a MaterializedAggregateBindings<'_, '_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<&'a p::AggregateBinding, Error> {
    let found = a.definition_observed(id, w)?;
    completed(
        found.ok_or_else(|| invalid("Aggregate binding is not in original owned namespace")),
        w,
    )
}
fn base_model(
    inputs: usize,
    req: usize,
    outputs: usize,
    groups: usize,
    calls: usize,
) -> Result<Model, Error> {
    Ok(Model {
        inputs,
        refs: add(outputs, add(groups, calls)?)?,
        items: add(add(req, outputs)?, add(groups, calls)?)?,
        ..Model::default()
    })
}
fn prepare_encode(
    input: &p::PhysicalNode,
    values: &EncodedValues<'_, '_, '_>,
    e: &EncodedExpressions<'_, '_, '_>,
    source: usize,
    l: AggregateNodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<AggregateNodeProjectionFacts, Error> {
    let same = std::ptr::eq(values.types(), e.types())
        && std::ptr::eq(values.original_control(), e.original_control());
    completed(
        if same {
            Ok(())
        } else {
            Err(invalid(
                "Aggregate namespaces have different type or control loans",
            ))
        },
        w,
    )?;
    let (groups, calls, _) = completed(physical(input), w)?;
    let mut model = base_model(
        input.inputs.len(),
        input.required_inputs.len(),
        input.output.columns.len(),
        groups.len(),
        calls.len(),
    )?;
    let mut known = add(
        physical_header_floor(input)?,
        add(
            bytes::<(p::ExprId, p::ValueId)>(groups.len())?,
            bytes::<p::AggregateCall>(calls.len())?,
        )?,
    )?;
    // This namespace count is admitted before its source lookup loop. The
    // original expression invoice includes the same aggregate source owners.
    let binding_work = mul(mul(2, calls.len())?, e.aggregates().source_counts())?;
    let expression_lookup = e.lookup_work_upper_bound()?;
    let mut exprs = groups.len();
    model.delegated_work = add(binding_work, mul(exprs, expression_lookup)?)?;
    model.numerical_facts(source, values.count(), l.node)?;
    count_prefix(model.inputs, model.items, source, known, l.node, w)?;
    model.facts(source, values.count(), l.node, w)?;
    floor(source, values.retained_floor(w)?, w)?;
    floor(source, e.retained_floor_observed(w)?, w)?;
    encode_header_requests(input, &mut model)?;
    model.request::<wire::ExpressionOutput>(groups.len(), 1)?;
    model.request::<wire::AggregateCall>(calls.len(), 1)?;
    model.numerical_facts(source, values.count(), l.node)?;
    for call in calls {
        let nested = add(call.arguments.len(), call.order_by.len())?;
        model.items = add(model.items, nested)?;
        known = add(
            known,
            add(
                bytes::<p::ExprId>(call.arguments.len())?,
                bytes::<p::SortExpr>(call.order_by.len())?,
            )?,
        )?;
        exprs = add(exprs, nested)?;
        model.request::<u32>(call.arguments.len(), 1)?;
        model.request::<wire::SortExpression>(call.order_by.len(), 1)?;
        model.delegated_work = add(binding_work, mul(exprs, expression_lookup)?)?;
        model.numerical_facts(source, values.count(), l.node)?;
        count_prefix(model.inputs, model.items, source, known, l.node, w)?;
        w.step()?;
    }
    model.facts(source, values.count(), l.node, w)?;
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(&input.output_properties))
    {
        model.property(properties::preflight_encode_observed(
            property,
            source,
            l.node.properties,
            w,
        )?)?;
        model.numerical_facts(source, values.count(), l.node)?;
        w.step()?;
    }
    let facts = model.facts(source, values.count(), l.node, w)?;
    for (expr, value) in groups {
        encoded_expr(expr.get(), e, w)?;
        reference(value.get(), values, w)?;
        w.step()?;
    }
    for call in calls {
        e.aggregates().source_id_observed(&call.binding, w)?;
        for arg in &call.arguments {
            encoded_expr(arg.get(), e, w)?;
            w.step()?;
        }
        for sort in &call.order_by {
            encoded_expr(sort.expr.get(), e, w)?;
            w.step()?;
        }
        reference(call.output.get(), values, w)?;
        w.step()?;
    }
    for value in &input.output.columns {
        reference(value.get(), values, w)?;
        w.step()?;
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
    e: &DecodedExpressions<'_, '_, '_>,
    a: &MaterializedAggregateBindings<'_, '_, '_>,
    source: usize,
    l: AggregateNodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<AggregateNodeProjectionFacts, Error> {
    let same = std::ptr::eq(a.headers(), e.aggregates())
        && std::ptr::eq(a.functions().headers(), e.functions())
        && std::ptr::eq(a.original_control(), e.original_control());
    completed(
        if same {
            Ok(())
        } else {
            Err(invalid(
                "Aggregate owned namespaces have different original loans",
            ))
        },
        w,
    )?;
    let body = completed(raw(input), w)?;
    completed(decode_grouping(body.grouping), w)?;
    let port = completed(
        input
            .output
            .as_ref()
            .ok_or_else(|| invalid("Aggregate output port is absent")),
        w,
    )?;
    required(port.node_id, w)?;
    let props = completed(
        input
            .output_properties
            .as_ref()
            .ok_or_else(|| invalid("Aggregate output properties are absent")),
        w,
    )?;
    let mut base = base_model(
        input.input_node_ids.len(),
        input.required_inputs.len(),
        port.value_ids.len(),
        body.group_by.len(),
        body.calls.len(),
    )?;
    let mut known = add(
        wire_header_floor(input, port)?,
        add(
            bytes::<wire::ExpressionOutput>(body.group_by.capacity())?,
            bytes::<wire::AggregateCall>(body.calls.capacity())?,
        )?,
    )?;
    // Counts, types and emit each use the same actual count-sized lookup.
    // Admit all three passes before lending this immutable base to the child.
    let binding_work = mul(mul(3, body.calls.len())?, add(a.definitions().len(), 2)?)?;
    let expression_lookup = e.lookup_work_upper_bound()?;
    let mut exprs = body.group_by.len();
    base.delegated_work = add(binding_work, mul(exprs, expression_lookup)?)?;
    base.numerical_facts(source, e.values().count(), l.node)?;
    count_prefix(base.inputs, base.items, source, known, l.node, w)?;
    base.facts(source, e.values().count(), l.node, w)?;
    let borrowed = e.retained_floor_observed(w)?;
    let owned = add(
        a.functions().retained_output_floor()?,
        a.retained_output_floor()?,
    )?;
    let dependency = add(borrowed, owned)?;
    floor(source, dependency, w)?;
    decode_header_requests(input, port, &mut base)?;
    base.request::<(p::ExprId, p::ValueId)>(body.group_by.len(), 2)?;
    base.request::<p::AggregateCall>(body.calls.len(), 2)?;
    base.numerical_facts(source, e.values().count(), l.node)?;
    for call in &body.calls {
        let nested = add(call.argument_expr_ids.len(), call.order_by.len())?;
        base.items = add(base.items, nested)?;
        known = add(
            known,
            add(
                bytes::<u32>(call.argument_expr_ids.capacity())?,
                bytes::<wire::SortExpression>(call.order_by.capacity())?,
            )?,
        )?;
        exprs = add(exprs, nested)?;
        base.request::<p::ExprId>(call.argument_expr_ids.len(), 2)?;
        base.request::<p::SortExpr>(call.order_by.len(), 2)?;
        base.delegated_work = add(binding_work, mul(exprs, expression_lookup)?)?;
        base.numerical_facts(source, e.values().count(), l.node)?;
        count_prefix(base.inputs, base.items, source, known, l.node, w)?;
        w.step()?;
    }
    base.facts(source, e.values().count(), l.node, w)?;
    for property in input.required_inputs.iter().chain(std::iter::once(props)) {
        base.property(properties::preflight_decode_observed(
            property,
            source,
            l.node.properties,
            w,
        )?)?;
        base.numerical_facts(source, e.values().count(), l.node)?;
        w.step()?;
    }
    let mut child = MaterializationModel::for_composition(
        body.calls.len(),
        add(e.types().value_types().len(), 1)?,
        source,
        dependency,
    );
    child.compose_in_node(base, e.values().count(), l.node, l.binding)?;
    // Every delegated count/type check gates the same containing node before
    // its next observer. All signatures are counted before any full type walk.
    for call in &body.calls {
        let source_binding = binding(required(call.aggregate_binding_id, w)?, a, w)?;
        preflight_aggregate_binding_copy_counts(source_binding, &mut child, l.binding, w)?;
        w.step()?;
    }
    child.node_facts(0, w)?;
    for call in &body.calls {
        let source_binding = binding(required(call.aggregate_binding_id, w)?, a, w)?;
        preflight_aggregate_binding_copy_types(source_binding, &mut child, l.binding, w)?;
        w.step()?;
    }
    let copy_work = mul(2, child.facts.cumulative_work_upper_bound)?;
    let facts = child.node_facts(copy_work, w)?;
    for group in &body.group_by {
        decoded_expr(required(group.expr_id, w)?, e, w)?;
        reference(required(group.value_id, w)?, e.values(), w)?;
        w.step()?;
    }
    for call in &body.calls {
        for arg in &call.argument_expr_ids {
            decoded_expr(*arg, e, w)?;
            w.step()?;
        }
        for sort in &call.order_by {
            decoded_expr(required(sort.expr_id, w)?, e, w)?;
            completed(
                properties::decode_direction(sort.direction).map_err(Error::from),
                w,
            )?;
            completed(
                properties::decode_nulls(sort.null_ordering).map_err(Error::from),
                w,
            )?;
            w.step()?;
        }
        reference(required(call.output_value_id, w)?, e.values(), w)?;
        w.step()?;
    }
    for value in &port.value_ids {
        reference(*value, e.values(), w)?;
        w.step()?;
    }
    for property in input.required_inputs.iter().chain(std::iter::once(props)) {
        wire_property_refs(property, e.values(), w)?;
    }
    Ok(facts)
}
fn emit_encode(
    input: &p::PhysicalNode,
    e: &EncodedExpressions<'_, '_, '_>,
    source: usize,
    l: AggregateNodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<wire::PhysicalNode, Error> {
    let (inputs, required_inputs, output_properties, output) =
        encode_header(input, source, l.node, w)?;
    let (groups, calls, grouping) = physical(input)?;
    let mut group_by = reserve(groups.len(), w)?;
    for (expr, value) in groups {
        group_by.push(wire::ExpressionOutput {
            expr_id: Some(expr.get()),
            value_id: Some(value.get()),
        });
        w.step()?;
    }
    let mut emitted = reserve(calls.len(), w)?;
    for call in calls {
        let id = e.aggregates().source_id_observed(&call.binding, w)?;
        let mut args = reserve(call.arguments.len(), w)?;
        for arg in &call.arguments {
            args.push(arg.get());
            w.step()?;
        }
        emitted.push(wire::AggregateCall {
            id: call.id.get(),
            aggregate_binding_id: Some(id),
            argument_expr_ids: args,
            distinct: call.distinct,
            order_by: encode_sorts(&call.order_by, w)?,
            output_value_id: Some(call.output.get()),
        });
        w.step()?;
    }
    let output = wire::PhysicalNode {
        id: input.id.get(),
        input_node_ids: inputs,
        required_inputs,
        output_properties: Some(output_properties),
        output: Some(output),
        kind: Some(wire::physical_node::Kind::Aggregate(wire::AggregateNode {
            group_by,
            calls: emitted,
            grouping: encode_grouping(grouping),
        })),
    };
    w.step()?;
    Ok(output)
}
fn emit_decode(
    input: &wire::PhysicalNode,
    e: &DecodedExpressions<'_, '_, '_>,
    a: &MaterializedAggregateBindings<'_, '_, '_>,
    source: usize,
    l: AggregateNodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::PhysicalNode, Error> {
    let (inputs, required_inputs, output_properties, output) =
        decode_header(input, source, l.node, w)?;
    let body = raw(input)?;
    let mut group_by = reserve(body.group_by.len(), w)?;
    for group in &body.group_by {
        group_by.push((
            p::ExprId::new(required(group.expr_id, w)?),
            p::ValueId::new(required(group.value_id, w)?),
        ));
        w.step()?;
    }
    let mut calls = reserve(body.calls.len(), w)?;
    for call in &body.calls {
        let binding = copy_aggregate_binding_observed(
            binding(required(call.aggregate_binding_id, w)?, a, w)?,
            w,
        )?;
        let mut args = reserve(call.argument_expr_ids.len(), w)?;
        for id in &call.argument_expr_ids {
            args.push(p::ExprId::new(*id));
            w.step()?;
        }
        calls.push(p::AggregateCall {
            id: p::AggregateCallId::new(call.id),
            binding,
            arguments: boxed(args, w)?,
            distinct: call.distinct,
            order_by: decode_sorts(&call.order_by, w)?,
            output: p::ValueId::new(required(call.output_value_id, w)?),
        });
        w.step()?;
    }
    let output = p::PhysicalNode {
        id: p::NodeId::new(input.id),
        inputs,
        required_inputs,
        output_properties,
        output,
        kind: p::NodeKind::Aggregate {
            group_by: boxed(group_by, w)?,
            calls: boxed(calls, w)?,
            grouping: decode_grouping(body.grouping)?,
        },
    };
    // The AggregateBinding lives inline in each AggregateCall. No outer Box is
    // requested here, unlike an aggregate WindowCall expression.
    w.step()?;
    let _ = e;
    Ok(output)
}

pub struct PreparedAggregateNodeEncode<'node, 'namespace, 'loan, 'source, 'control> {
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    source: usize,
    limits: AggregateNodeProjectionLimits,
    facts: AggregateNodeProjectionFacts,
}
impl PreparedAggregateNodeEncode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &AggregateNodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(wire::PhysicalNode, AggregateNodeProjectionFacts), Error> {
        let mut w =
            CompileCheckpoints::try_new(self.values.original_control(), CompilePhase::Encode)?;
        let r = emit_encode(
            self.input,
            self.expressions,
            self.source,
            self.limits,
            &mut w,
        )
        .map(|n| (n, self.facts));
        finish(w, r)
    }
}
pub fn prepare_aggregate_node_encode<'node, 'namespace, 'loan, 'source, 'control>(
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    source_retained_bytes: usize,
    limits: AggregateNodeProjectionLimits,
) -> Result<PreparedAggregateNodeEncode<'node, 'namespace, 'loan, 'source, 'control>, Error> {
    let mut w = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Encode)?;
    let r = prepare_encode(
        input,
        values,
        expressions,
        source_retained_bytes,
        limits,
        &mut w,
    );
    let facts = finish(w, r)?;
    Ok(PreparedAggregateNodeEncode {
        input,
        values,
        expressions,
        source: source_retained_bytes,
        limits,
        facts,
    })
}
pub struct PreparedAggregateNodeDecode<'node, 'namespace, 'loan, 'wire, 'control> {
    input: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    aggregates: &'namespace MaterializedAggregateBindings<'namespace, 'loan, 'wire>,
    source: usize,
    limits: AggregateNodeProjectionLimits,
    facts: AggregateNodeProjectionFacts,
}
impl PreparedAggregateNodeDecode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &AggregateNodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(p::PhysicalNode, AggregateNodeProjectionFacts), Error> {
        let mut w =
            CompileCheckpoints::try_new(self.expressions.original_control(), CompilePhase::Decode)?;
        let r = emit_decode(
            self.input,
            self.expressions,
            self.aggregates,
            self.source,
            self.limits,
            &mut w,
        )
        .map(|n| (n, self.facts));
        finish(w, r)
    }
}
pub fn prepare_aggregate_node_decode<'node, 'namespace, 'loan, 'wire, 'control>(
    input: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    aggregates: &'namespace MaterializedAggregateBindings<'namespace, 'loan, 'wire>,
    source_retained_bytes: usize,
    limits: AggregateNodeProjectionLimits,
) -> Result<PreparedAggregateNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>, Error> {
    let mut w = CompileCheckpoints::try_new(expressions.original_control(), CompilePhase::Decode)?;
    let r = prepare_decode(
        input,
        expressions,
        aggregates,
        source_retained_bytes,
        limits,
        &mut w,
    );
    let facts = finish(w, r)?;
    Ok(PreparedAggregateNodeDecode {
        input,
        expressions,
        aggregates,
        source: source_retained_bytes,
        limits,
        facts,
    })
}
pub fn encode_aggregate_node(
    input: &p::PhysicalNode,
    values: &EncodedValues<'_, '_, '_>,
    expressions: &EncodedExpressions<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: AggregateNodeProjectionLimits,
) -> Result<(wire::PhysicalNode, AggregateNodeProjectionFacts), Error> {
    let mut w = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Encode)?;
    let r = (|| {
        let facts = prepare_encode(
            input,
            values,
            expressions,
            source_retained_bytes,
            limits,
            &mut w,
        )?;
        Ok((
            emit_encode(input, expressions, source_retained_bytes, limits, &mut w)?,
            facts,
        ))
    })();
    finish(w, r)
}
pub fn decode_aggregate_node(
    input: &wire::PhysicalNode,
    expressions: &DecodedExpressions<'_, '_, '_>,
    aggregates: &MaterializedAggregateBindings<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: AggregateNodeProjectionLimits,
) -> Result<(p::PhysicalNode, AggregateNodeProjectionFacts), Error> {
    let mut w = CompileCheckpoints::try_new(expressions.original_control(), CompilePhase::Decode)?;
    let r = (|| {
        let facts = prepare_decode(
            input,
            expressions,
            aggregates,
            source_retained_bytes,
            limits,
            &mut w,
        )?;
        Ok((
            emit_decode(
                input,
                expressions,
                aggregates,
                source_retained_bytes,
                limits,
                &mut w,
            )?,
            facts,
        ))
    })();
    finish(w, r)
}
#[cfg(test)]
mod tests;
