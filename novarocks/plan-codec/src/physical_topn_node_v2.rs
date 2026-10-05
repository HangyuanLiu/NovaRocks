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

//! Complete TopN representation, borrowing the original admitted namespaces.
//! Phase graph, state compatibility, comparator capabilities and output laws
//! remain the original Fragment/Package authorities' obligations.

pub use crate::physical_node_v2::{
    NodeCodecError as TopNNodeCodecError, NodeProjectionFacts as TopNNodeProjectionFacts,
};
use crate::{
    physical_aggregate_binding_v2::MaterializedAggregateBindings,
    physical_aggregate_node_v2::{
        CollectionProjection, decode_collection_lookup_work, decode_collection_references,
        decode_collections, encode_collection_lookup_work, encode_collection_references,
        encode_collections, preflight_collection_binding_copies,
    },
    physical_binding_v2::BindingProjectionLimits,
    physical_expression_v2::{DecodedExpressions, EncodedExpressions},
    physical_node_v2::*,
    physical_properties_v2 as properties,
    physical_relational_nodes_v2::{decode_sorts, encode_sorts},
    physical_value_v2::EncodedValues,
};
use novarocks_physical_plan as p;
use novarocks_proto_models::{physical_control_v2::Empty, physical_package_v2 as wire};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, OrderedComparisonAlgorithm};
type Error = TopNNodeCodecError;

#[derive(Clone, Copy, Debug)]
pub struct TopNNodeProjectionLimits {
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
        id.ok_or_else(|| invalid("TopN required reference is absent")),
        w,
    )
}
type TopNBody<'a> = (
    &'a [p::SortExpr],
    u64,
    u64,
    p::TopNPhase,
    &'a p::TopNReduction,
);
fn physical(input: &p::PhysicalNode) -> Result<TopNBody<'_>, Error> {
    match &input.kind {
        p::NodeKind::TopN {
            order_by,
            limit,
            offset,
            phase,
            reduction,
        } => Ok((order_by, *limit, *offset, *phase, reduction)),
        _ => Err(invalid("node is not TopN")),
    }
}
fn raw(input: &wire::PhysicalNode) -> Result<&wire::TopNNode, Error> {
    match &input.kind {
        Some(wire::physical_node::Kind::TopN(v)) => Ok(v),
        _ => Err(invalid("node is not TopN")),
    }
}
type Collections<'a> = (&'a [(p::ExprId, p::ValueId)], &'a [p::AggregateCall]);
fn physical_collections(reduction: &p::TopNReduction) -> Collections<'_> {
    match reduction {
        p::TopNReduction::Rows => (&[], &[]),
        p::TopNReduction::GroupedStates {
            group_by, calls, ..
        } => (group_by, calls),
    }
}
fn encode_phase(phase: p::TopNPhase) -> wire::TopNPhase {
    let kind = match phase {
        p::TopNPhase::Single => wire::top_n_phase::Kind::Single(Empty {}),
        p::TopNPhase::Partial { sequence } => {
            wire::top_n_phase::Kind::PartialSequenceId(sequence.get())
        }
        p::TopNPhase::Final { sequence } => {
            wire::top_n_phase::Kind::FinalSequenceId(sequence.get())
        }
    };
    wire::TopNPhase { kind: Some(kind) }
}
fn decode_phase(phase: Option<&wire::TopNPhase>) -> Result<p::TopNPhase, Error> {
    match phase.and_then(|p| p.kind.as_ref()) {
        Some(wire::top_n_phase::Kind::Single(_)) => Ok(p::TopNPhase::Single),
        Some(wire::top_n_phase::Kind::PartialSequenceId(id)) => Ok(p::TopNPhase::Partial {
            sequence: p::TopNSequenceId::new(*id),
        }),
        Some(wire::top_n_phase::Kind::FinalSequenceId(id)) => Ok(p::TopNPhase::Final {
            sequence: p::TopNSequenceId::new(*id),
        }),
        None => Err(invalid("TopN phase or phase kind is absent")),
    }
}
fn encode_comparator(comparator: OrderedComparisonAlgorithm) -> i32 {
    match comparator {
        OrderedComparisonAlgorithm::NativeScalarOrderV1 => {
            wire::OrderedComparisonAlgorithm::NativeScalarOrderV1 as i32
        }
    }
}
fn decode_comparator(comparator: i32) -> Result<OrderedComparisonAlgorithm, Error> {
    match wire::OrderedComparisonAlgorithm::try_from(comparator) {
        Ok(wire::OrderedComparisonAlgorithm::NativeScalarOrderV1) => {
            Ok(OrderedComparisonAlgorithm::NativeScalarOrderV1)
        }
        Ok(wire::OrderedComparisonAlgorithm::Unspecified) | Err(_) => {
            Err(invalid("TopN comparator is unknown or unspecified"))
        }
    }
}
fn raw_reduction(body: &wire::TopNNode) -> Result<&wire::top_n_reduction::Kind, Error> {
    body.reduction
        .as_ref()
        .and_then(|r| r.kind.as_ref())
        .ok_or_else(|| invalid("TopN reduction or reduction kind is absent"))
}
type WireCollections<'a> = (&'a [wire::ExpressionOutput], &'a [wire::AggregateCall]);
fn wire_collections(reduction: &wire::top_n_reduction::Kind) -> Result<WireCollections<'_>, Error> {
    match reduction {
        wire::top_n_reduction::Kind::Rows(_) => Ok((&[], &[])),
        wire::top_n_reduction::Kind::GroupedStates(g) => {
            decode_comparator(g.comparator)?;
            Ok((&g.group_by, &g.calls))
        }
    }
}
fn encoded_expr(
    id: u32,
    e: &EncodedExpressions<'_, '_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let found = e.expression_observed(id, w)?.is_some();
    completed(
        if found {
            Ok(())
        } else {
            Err(invalid("TopN expression is not in original namespace"))
        },
        w,
    )
}
fn decoded_expr(
    id: u32,
    e: &DecodedExpressions<'_, '_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let found = e.definition_observed(id, w)?.is_some();
    completed(
        if found {
            Ok(())
        } else {
            Err(invalid("TopN expression is not in original namespace"))
        },
        w,
    )
}
fn base_model(
    input_count: usize,
    required_count: usize,
    output_count: usize,
    keys: usize,
    groups: usize,
    calls: usize,
) -> Result<Model, Error> {
    Ok(Model {
        inputs: input_count,
        refs: add(output_count, add(groups, calls)?)?,
        items: add(
            add(required_count, output_count)?,
            add(keys, add(groups, calls)?)?,
        )?,
        ..Model::default()
    })
}
fn prepare_encode(
    input: &p::PhysicalNode,
    values: &EncodedValues<'_, '_, '_>,
    e: &EncodedExpressions<'_, '_, '_>,
    source: usize,
    l: TopNNodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<TopNNodeProjectionFacts, Error> {
    let same = std::ptr::eq(values.types(), e.types())
        && std::ptr::eq(values.original_control(), e.original_control());
    completed(
        if same {
            Ok(())
        } else {
            Err(invalid(
                "TopN namespaces have different type or control loans",
            ))
        },
        w,
    )?;
    let (keys, _, _, _, reduction) = completed(physical(input), w)?;
    let (groups, calls) = physical_collections(reduction);
    let mut model = base_model(
        input.inputs.len(),
        input.required_inputs.len(),
        input.output.columns.len(),
        keys.len(),
        groups.len(),
        calls.len(),
    )?;
    let known = add(
        physical_header_floor(input)?,
        add(
            bytes::<p::SortExpr>(keys.len())?,
            add(
                bytes::<(p::ExprId, p::ValueId)>(groups.len())?,
                bytes::<p::AggregateCall>(calls.len())?,
            )?,
        )?,
    )?;
    model.delegated_work = add(
        encode_collection_lookup_work(groups, calls, e)?,
        mul(keys.len(), e.lookup_work_upper_bound()?)?,
    )?;
    model.numerical_facts(source, values.count(), l.node)?;
    count_prefix(model.inputs, model.items, source, known, l.node, w)?;
    model.facts(source, values.count(), l.node, w)?;
    floor(source, values.retained_floor(w)?, w)?;
    floor(source, e.retained_floor_observed(w)?, w)?;
    encode_header_requests(input, &mut model)?;
    model.request::<wire::SortExpression>(keys.len(), 1)?;
    model.request::<wire::ExpressionOutput>(groups.len(), 1)?;
    model.request::<wire::AggregateCall>(calls.len(), 1)?;
    model.numerical_facts(source, values.count(), l.node)?;
    CollectionProjection {
        model: &mut model,
        known,
        source,
        values: values.count(),
        limits: l.node,
    }
    .count_encode(calls, e, w)?;
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
    for key in keys {
        encoded_expr(key.expr.get(), e, w)?;
        w.step()?;
    }
    encode_collection_references(groups, calls, values, e, w)?;
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
    l: TopNNodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<TopNNodeProjectionFacts, Error> {
    let same = std::ptr::eq(a.headers(), e.aggregates())
        && std::ptr::eq(a.functions().headers(), e.functions())
        && std::ptr::eq(a.original_control(), e.original_control());
    completed(
        if same {
            Ok(())
        } else {
            Err(invalid(
                "TopN owned namespaces have different original loans",
            ))
        },
        w,
    )?;
    let b = completed(raw(input), w)?;
    completed(decode_phase(b.phase.as_ref()), w)?;
    let reduction = completed(raw_reduction(b), w)?;
    let (groups, calls) = completed(wire_collections(reduction), w)?;
    let port = completed(
        input
            .output
            .as_ref()
            .ok_or_else(|| invalid("TopN output port is absent")),
        w,
    )?;
    required(port.node_id, w)?;
    let props = completed(
        input
            .output_properties
            .as_ref()
            .ok_or_else(|| invalid("TopN output properties are absent")),
        w,
    )?;
    let mut model = base_model(
        input.input_node_ids.len(),
        input.required_inputs.len(),
        port.value_ids.len(),
        b.order_by.len(),
        groups.len(),
        calls.len(),
    )?;
    let collection_backing = match reduction {
        wire::top_n_reduction::Kind::Rows(_) => 0,
        wire::top_n_reduction::Kind::GroupedStates(g) => add(
            bytes::<wire::ExpressionOutput>(g.group_by.capacity())?,
            bytes::<wire::AggregateCall>(g.calls.capacity())?,
        )?,
    };
    let known = add(
        wire_header_floor(input, port)?,
        add(
            bytes::<wire::SortExpression>(b.order_by.capacity())?,
            collection_backing,
        )?,
    )?;
    model.delegated_work = add(
        decode_collection_lookup_work(groups, calls, e, a)?,
        mul(b.order_by.len(), e.lookup_work_upper_bound()?)?,
    )?;
    model.numerical_facts(source, e.values().count(), l.node)?;
    count_prefix(model.inputs, model.items, source, known, l.node, w)?;
    model.facts(source, e.values().count(), l.node, w)?;
    let borrowed = e.retained_floor_observed(w)?;
    let owned = add(
        a.functions().retained_output_floor()?,
        a.retained_output_floor()?,
    )?;
    let dependency = add(borrowed, owned)?;
    floor(source, dependency, w)?;
    decode_header_requests(input, port, &mut model)?;
    model.request::<p::SortExpr>(b.order_by.len(), 2)?;
    model.request::<(p::ExprId, p::ValueId)>(groups.len(), 2)?;
    model.request::<p::AggregateCall>(calls.len(), 2)?;
    model.numerical_facts(source, e.values().count(), l.node)?;
    CollectionProjection {
        model: &mut model,
        known,
        source,
        values: e.values().count(),
        limits: l.node,
    }
    .count_decode(calls, e, w)?;
    model.facts(source, e.values().count(), l.node, w)?;
    for property in input.required_inputs.iter().chain(std::iter::once(props)) {
        model.property(properties::preflight_decode_observed(
            property,
            source,
            l.node.properties,
            w,
        )?)?;
        model.numerical_facts(source, e.values().count(), l.node)?;
        w.step()?;
    }
    let facts = if matches!(reduction, wire::top_n_reduction::Kind::GroupedStates(_)) {
        preflight_collection_binding_copies(
            calls,
            e,
            a,
            CollectionProjection {
                model: &mut model,
                known,
                source,
                values: e.values().count(),
                limits: l.node,
            },
            dependency,
            l.binding,
            w,
        )?
    } else {
        model.facts(source, e.values().count(), l.node, w)?
    };
    for key in &b.order_by {
        decoded_expr(required(key.expr_id, w)?, e, w)?;
        completed(
            properties::decode_direction(key.direction).map_err(Error::from),
            w,
        )?;
        completed(
            properties::decode_nulls(key.null_ordering).map_err(Error::from),
            w,
        )?;
        w.step()?;
    }
    decode_collection_references(groups, calls, e, w)?;
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
    l: TopNNodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<wire::PhysicalNode, Error> {
    let (inputs, required_inputs, output_properties, output) =
        encode_header(input, source, l.node, w)?;
    let (keys, limit, offset, phase, reduction) = physical(input)?;
    let reduction = match reduction {
        p::TopNReduction::Rows => wire::top_n_reduction::Kind::Rows(Empty {}),
        p::TopNReduction::GroupedStates {
            group_by,
            calls,
            comparator,
        } => {
            let (group_by, calls) = encode_collections(group_by, calls, e, w)?;
            wire::top_n_reduction::Kind::GroupedStates(wire::GroupedStates {
                group_by,
                calls,
                comparator: encode_comparator(*comparator),
            })
        }
    };
    let output = wire::PhysicalNode {
        id: input.id.get(),
        input_node_ids: inputs,
        required_inputs,
        output_properties: Some(output_properties),
        output: Some(output),
        kind: Some(wire::physical_node::Kind::TopN(wire::TopNNode {
            order_by: encode_sorts(keys, w)?,
            limit,
            offset,
            phase: Some(encode_phase(phase)),
            reduction: Some(wire::TopNReduction {
                kind: Some(reduction),
            }),
        })),
    };
    w.step()?;
    Ok(output)
}
fn emit_decode(
    input: &wire::PhysicalNode,
    a: &MaterializedAggregateBindings<'_, '_, '_>,
    source: usize,
    l: TopNNodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::PhysicalNode, Error> {
    let (inputs, required_inputs, output_properties, output) =
        decode_header(input, source, l.node, w)?;
    let b = raw(input)?;
    let order_by = decode_sorts(&b.order_by, w)?;
    let reduction = match raw_reduction(b)? {
        wire::top_n_reduction::Kind::Rows(_) => p::TopNReduction::Rows,
        wire::top_n_reduction::Kind::GroupedStates(g) => {
            let (group_by, calls) = decode_collections(&g.group_by, &g.calls, a, w)?;
            p::TopNReduction::GroupedStates {
                group_by: boxed(group_by, w)?,
                calls: boxed(calls, w)?,
                comparator: decode_comparator(g.comparator)?,
            }
        }
    };
    let output = p::PhysicalNode {
        id: p::NodeId::new(input.id),
        inputs,
        required_inputs,
        output_properties,
        output,
        kind: p::NodeKind::TopN {
            order_by,
            limit: b.limit,
            offset: b.offset,
            phase: decode_phase(b.phase.as_ref())?,
            reduction,
        },
    };
    w.step()?;
    Ok(output)
}

pub struct PreparedTopNNodeEncode<'node, 'namespace, 'loan, 'source, 'control> {
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    source: usize,
    limits: TopNNodeProjectionLimits,
    facts: TopNNodeProjectionFacts,
}
impl PreparedTopNNodeEncode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &TopNNodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(wire::PhysicalNode, TopNNodeProjectionFacts), Error> {
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
pub fn prepare_topn_node_encode<'node, 'namespace, 'loan, 'source, 'control>(
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    source_retained_bytes: usize,
    limits: TopNNodeProjectionLimits,
) -> Result<PreparedTopNNodeEncode<'node, 'namespace, 'loan, 'source, 'control>, Error> {
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
    Ok(PreparedTopNNodeEncode {
        input,
        values,
        expressions,
        source: source_retained_bytes,
        limits,
        facts,
    })
}
pub struct PreparedTopNNodeDecode<'node, 'namespace, 'loan, 'wire, 'control> {
    input: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    aggregates: &'namespace MaterializedAggregateBindings<'namespace, 'loan, 'wire>,
    source: usize,
    limits: TopNNodeProjectionLimits,
    facts: TopNNodeProjectionFacts,
}
impl PreparedTopNNodeDecode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &TopNNodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(p::PhysicalNode, TopNNodeProjectionFacts), Error> {
        let mut w =
            CompileCheckpoints::try_new(self.expressions.original_control(), CompilePhase::Decode)?;
        let r = emit_decode(
            self.input,
            self.aggregates,
            self.source,
            self.limits,
            &mut w,
        )
        .map(|n| (n, self.facts));
        finish(w, r)
    }
}
pub fn prepare_topn_node_decode<'node, 'namespace, 'loan, 'wire, 'control>(
    input: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    aggregates: &'namespace MaterializedAggregateBindings<'namespace, 'loan, 'wire>,
    source_retained_bytes: usize,
    limits: TopNNodeProjectionLimits,
) -> Result<PreparedTopNNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>, Error> {
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
    Ok(PreparedTopNNodeDecode {
        input,
        expressions,
        aggregates,
        source: source_retained_bytes,
        limits,
        facts,
    })
}
pub fn encode_topn_node(
    input: &p::PhysicalNode,
    values: &EncodedValues<'_, '_, '_>,
    expressions: &EncodedExpressions<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: TopNNodeProjectionLimits,
) -> Result<(wire::PhysicalNode, TopNNodeProjectionFacts), Error> {
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
pub fn decode_topn_node(
    input: &wire::PhysicalNode,
    expressions: &DecodedExpressions<'_, '_, '_>,
    aggregates: &MaterializedAggregateBindings<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: TopNNodeProjectionLimits,
) -> Result<(p::PhysicalNode, TopNNodeProjectionFacts), Error> {
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
            emit_decode(input, aggregates, source_retained_bytes, limits, &mut w)?,
            facts,
        ))
    })();
    finish(w, r)
}
#[cfg(test)]
mod tests;
