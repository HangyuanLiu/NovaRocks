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

//! Fragment envelope vocabulary only; no Fragment/Package certification.
//! The caller invoices the entire original owner. Known floors here cover only
//! borrowed header/sink backing; other components retain their original gates.

use crate::physical_change_event_v2::{decode_mutation_effect, encode_mutation_effect};
use crate::physical_node_v2::*;
use novarocks_connector_contract::{
    ConnectorRowMutationEffect, ConnectorWriteFieldToken, ConnectorWriteRouteId, WriteTargetOrdinal,
};
use novarocks_physical_plan as p;
use novarocks_proto_models::{physical_control_v2::Empty, physical_package_v2 as wire};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};
use std::mem::size_of;

pub(crate) use crate::physical_node_v2::{
    NodeProjectionFacts as FragmentEnvelopeProjectionFacts,
    NodeProjectionLimits as FragmentEnvelopeProjectionLimits,
};
type Error = NodeCodecError;

/// These owned fields must be combined with all other original component owners.
/// A sink vocabulary projection never proves its routes, DOP or references legal.
#[derive(Debug, PartialEq)]
pub(crate) struct DecodedFragmentEnvelope {
    pub(crate) id: p::FragmentId,
    pub(crate) root: p::NodeId,
    pub(crate) sink: p::FragmentSink,
    pub(crate) dop_domain: p::PipelineDopDomain,
    pub(crate) runtime_filters: Box<[p::RuntimeFilterId]>,
}
#[derive(Debug, PartialEq)]
pub(crate) struct EncodedFragmentEnvelope {
    pub(crate) id: u32,
    pub(crate) root_node_id: u32,
    pub(crate) sink: wire::FragmentSink,
    pub(crate) dop_domain: wire::PipelineDopDomain,
    pub(crate) runtime_filter_ids: Vec<u32>,
}
fn required<T>(
    value: Option<T>,
    text: &'static str,
    w: &mut CompileCheckpoints<'_>,
) -> Result<T, Error> {
    let result = value.ok_or_else(|| invalid(text));
    w.step()?;
    result
}
fn token32(bytes: &[u8], w: &mut CompileCheckpoints<'_>) -> Result<[u8; 32], Error> {
    let correct_length = bytes.len() == 32;
    w.step()?;
    if !correct_length {
        return Err(invalid(
            "fragment route/field token must be exactly 32 bytes",
        ));
    }
    let mut output = [0; 32];
    for (target, source) in output.iter_mut().zip(bytes) {
        *target = *source;
        w.step()?;
    }
    Ok(output)
}
fn ordinal(raw: u32, w: &mut CompileCheckpoints<'_>) -> Result<WriteTargetOrdinal, Error> {
    let result = WriteTargetOrdinal::try_new(raw).map_err(Error::Identity);
    w.step()?;
    result
}
fn list<T, U>(
    input: &[T],
    w: &mut CompileCheckpoints<'_>,
    mut convert: impl FnMut(&T, &mut CompileCheckpoints<'_>) -> Result<U, Error>,
) -> Result<Vec<U>, Error> {
    let mut output = reserve(input.len(), w)?;
    for value in input {
        output.push(convert(value, w)?);
        w.step()?;
    }
    Ok(output)
}
fn token_vec(bytes: [u8; 32], w: &mut CompileCheckpoints<'_>) -> Result<Vec<u8>, Error> {
    list(&bytes, w, |v, _| Ok(*v))
}
fn physical_outer(input: &p::Fragment) -> Result<usize, Error> {
    add(
        input.runtime_filters().len(),
        match input.sink() {
            p::FragmentSink::Multicast { edges } => edges.len(),
            p::FragmentSink::Router { routes, .. } => routes.len(),
            _ => 0,
        },
    )
}
fn raw_kind<'a>(
    input: &'a wire::Fragment,
    w: &mut CompileCheckpoints<'_>,
) -> Result<&'a wire::fragment_sink::Kind, Error> {
    required(input.root_node_id, "fragment root ID is absent", w)?;
    required(
        input.dop_domain.as_ref(),
        "fragment DOP domain is absent",
        w,
    )?;
    let sink = required(input.sink.as_ref(), "fragment sink is absent", w)?;
    required(sink.kind.as_ref(), "fragment sink kind is absent", w)
}
fn wire_outer(input: &wire::Fragment, kind: &wire::fragment_sink::Kind) -> Result<usize, Error> {
    add(
        input.runtime_filter_ids.len(),
        match kind {
            wire::fragment_sink::Kind::Multicast(v) => v.edge_ids.len(),
            wire::fragment_sink::Kind::Router(v) => v.routes.len(),
            _ => 0,
        },
    )
}
fn preflight_encode(
    input: &p::Fragment,
    source: usize,
    l: FragmentEnvelopeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<FragmentEnvelopeProjectionFacts, Error> {
    let outer = physical_outer(input)?;
    let mut known = add(
        size_of::<p::Fragment>(),
        bytes::<p::RuntimeFilterId>(input.runtime_filters().len())?,
    )?;
    known = add(
        known,
        match input.sink() {
            p::FragmentSink::Multicast { edges } => bytes::<p::EdgeId>(edges.len())?,
            p::FragmentSink::Router { routes, .. } => bytes::<p::ChangeStreamRoute>(routes.len())?,
            _ => 0,
        },
    )?;
    count_prefix(0, outer, source, known, l, w)?;
    let mut model = Model {
        items: outer,
        ..Model::default()
    };
    model.request::<u32>(input.runtime_filters().len(), 1)?;
    model.facts(source, 0, l, w)?;
    match input.sink() {
        p::FragmentSink::Multicast { edges } => model.request::<u32>(edges.len(), 1)?,
        p::FragmentSink::Router { routes, .. } => {
            model.refs = 1;
            model.request::<wire::ChangeStreamRoute>(routes.len(), 1)?;
            model.facts(source, 0, l, w)?;
            for route in routes {
                let inner = add(
                    route.accepted_effects.len(),
                    add(route.input_mapping.len(), route.partition_by.len())?,
                )?;
                model.items = add(model.items, inner)?;
                count_prefix(0, model.items, source, known, l, w)?;
                model.refs = add(
                    model.refs,
                    add(route.input_mapping.len(), route.partition_by.len())?,
                )?;
                cap(model.refs, l.max_value_references, w)?;
                known = add(
                    known,
                    add(
                        bytes::<ConnectorRowMutationEffect>(route.accepted_effects.len())?,
                        add(
                            bytes::<(ConnectorWriteFieldToken, p::ValueId)>(
                                route.input_mapping.len(),
                            )?,
                            bytes::<p::ValueId>(route.partition_by.len())?,
                        )?,
                    )?,
                )?;
                model.request::<u8>(32, 1)?;
                model.request::<i32>(route.accepted_effects.len(), 1)?;
                model.request::<wire::WriteInputMapping>(route.input_mapping.len(), 1)?;
                model.request::<u32>(route.partition_by.len(), 1)?;
                // Each field token is an independent generated Vec allocation;
                // copies counts these requests through the sole Model author.
                model.request::<u8>(32, route.input_mapping.len())?;
                model.facts(source, 0, l, w)?;
                floor(source, known, w)?;
                w.step()?;
            }
        }
        _ => {}
    }
    floor(source, known, w)?;
    model.facts(source, 0, l, w)
}
fn preflight_decode(
    input: &wire::Fragment,
    source: usize,
    l: FragmentEnvelopeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<FragmentEnvelopeProjectionFacts, Error> {
    let kind = raw_kind(input, w)?;
    let outer = wire_outer(input, kind)?;
    let mut known = add(
        size_of::<wire::Fragment>(),
        bytes::<u32>(input.runtime_filter_ids.capacity())?,
    )?;
    known = add(
        known,
        match kind {
            wire::fragment_sink::Kind::Multicast(v) => bytes::<u32>(v.edge_ids.capacity())?,
            wire::fragment_sink::Kind::Router(v) => {
                bytes::<wire::ChangeStreamRoute>(v.routes.capacity())?
            }
            _ => 0,
        },
    )?;
    count_prefix(0, outer, source, known, l, w)?;
    let mut model = Model {
        items: outer,
        ..Model::default()
    };
    model.request::<p::RuntimeFilterId>(input.runtime_filter_ids.len(), 2)?;
    model.facts(source, 0, l, w)?;
    match kind {
        wire::fragment_sink::Kind::Multicast(v) => {
            model.request::<p::EdgeId>(v.edge_ids.len(), 2)?
        }
        wire::fragment_sink::Kind::Router(v) => {
            required(v.effect_value_id, "router effect ValueId is absent", w)?;
            model.refs = 1;
            model.request::<p::ChangeStreamRoute>(v.routes.len(), 2)?;
            model.facts(source, 0, l, w)?;
            for route in &v.routes {
                let inner = add(
                    route.accepted_effects.len(),
                    add(route.input_mapping.len(), route.partition_value_ids.len())?,
                )?;
                model.items = add(model.items, inner)?;
                count_prefix(0, model.items, source, known, l, w)?;
                model.refs = add(
                    model.refs,
                    add(route.input_mapping.len(), route.partition_value_ids.len())?,
                )?;
                cap(model.refs, l.max_value_references, w)?;
                known = add(
                    known,
                    add(
                        route.route_id.capacity(),
                        add(
                            bytes::<i32>(route.accepted_effects.capacity())?,
                            add(
                                bytes::<wire::WriteInputMapping>(route.input_mapping.capacity())?,
                                bytes::<u32>(route.partition_value_ids.capacity())?,
                            )?,
                        )?,
                    )?,
                )?;
                model.request::<ConnectorRowMutationEffect>(route.accepted_effects.len(), 2)?;
                model.request::<(ConnectorWriteFieldToken, p::ValueId)>(
                    route.input_mapping.len(),
                    2,
                )?;
                model.request::<p::ValueId>(route.partition_value_ids.len(), 2)?;
                model.facts(source, 0, l, w)?;
                floor(source, known, w)?;
                token32(&route.route_id, w)?;
                ordinal(route.write_target_ordinal, w)?;
                required(route.edge_id, "router route edge ID is absent", w)?;
                for effect in &route.accepted_effects {
                    decode_mutation_effect(*effect, w)?;
                }
                for mapping in &route.input_mapping {
                    known = add(known, mapping.field_token.capacity())?;
                    floor(source, known, w)?;
                    token32(&mapping.field_token, w)?;
                    required(mapping.value_id, "router mapping ValueId is absent", w)?;
                    w.step()?;
                }
                w.step()?;
            }
        }
        _ => {}
    }
    floor(source, known, w)?;
    model.facts(source, 0, l, w)
}
fn emit_encode(
    input: &p::Fragment,
    w: &mut CompileCheckpoints<'_>,
) -> Result<EncodedFragmentEnvelope, Error> {
    use wire::fragment_sink::Kind;
    let kind = match input.sink() {
        p::FragmentSink::Result => Kind::Result(Empty {}),
        p::FragmentSink::Noop => Kind::Noop(Empty {}),
        p::FragmentSink::Stream { edge } => Kind::StreamEdgeId(edge.get()),
        p::FragmentSink::Multicast { edges } => Kind::Multicast(wire::MulticastSink {
            edge_ids: list(edges, w, |id, _| Ok(id.get()))?,
        }),
        p::FragmentSink::Router { effect, routes } => Kind::Router(wire::RouterSink {
            effect_value_id: Some(effect.get()),
            routes: list(routes, w, |route, w| {
                Ok(wire::ChangeStreamRoute {
                    route_id: token_vec(route.route_id.to_bytes(), w)?,
                    write_target_ordinal: route.write_target_ordinal.get(),
                    accepted_effects: list(&route.accepted_effects, w, |effect, _| {
                        Ok(encode_mutation_effect(*effect))
                    })?,
                    input_mapping: list(&route.input_mapping, w, |(token, value), w| {
                        Ok(wire::WriteInputMapping {
                            field_token: token_vec(token.to_bytes(), w)?,
                            value_id: Some(value.get()),
                        })
                    })?,
                    partition_value_ids: list(&route.partition_by, w, |id, _| Ok(id.get()))?,
                    edge_id: Some(route.edge.get()),
                })
            })?,
        }),
    };
    let dop = input.dop_domain();
    let output = EncodedFragmentEnvelope {
        id: input.id().get(),
        root_node_id: input.root().get(),
        sink: wire::FragmentSink { kind: Some(kind) },
        dop_domain: wire::PipelineDopDomain {
            min: dop.min,
            max: dop.max,
            requires_power_of_two: dop.requires_power_of_two,
        },
        runtime_filter_ids: list(input.runtime_filters(), w, |id, _| Ok(id.get()))?,
    };
    w.step()?;
    Ok(output)
}
fn emit_decode(
    input: &wire::Fragment,
    w: &mut CompileCheckpoints<'_>,
) -> Result<DecodedFragmentEnvelope, Error> {
    use wire::fragment_sink::Kind;
    let kind = raw_kind(input, w)?;
    let sink = match kind {
        Kind::Result(_) => p::FragmentSink::Result,
        Kind::Noop(_) => p::FragmentSink::Noop,
        Kind::StreamEdgeId(id) => p::FragmentSink::Stream {
            edge: p::EdgeId::new(*id),
        },
        Kind::Multicast(v) => p::FragmentSink::Multicast {
            edges: boxed(list(&v.edge_ids, w, |id, _| Ok(p::EdgeId::new(*id)))?, w)?,
        },
        Kind::Router(v) => p::FragmentSink::Router {
            effect: p::ValueId::new(required(
                v.effect_value_id,
                "prepared router effect ID is absent",
                w,
            )?),
            routes: boxed(
                list(&v.routes, w, |route, w| {
                    let route_id = ConnectorWriteRouteId::from_bytes(token32(&route.route_id, w)?);
                    let write_target_ordinal = ordinal(route.write_target_ordinal, w)?;
                    let accepted_effects = boxed(
                        list(&route.accepted_effects, w, |v, w| {
                            decode_mutation_effect(*v, w)
                        })?,
                        w,
                    )?;
                    let input_mapping = boxed(
                        list(&route.input_mapping, w, |mapping, w| {
                            let token = ConnectorWriteFieldToken::from_bytes(token32(
                                &mapping.field_token,
                                w,
                            )?);
                            let id = p::ValueId::new(required(
                                mapping.value_id,
                                "prepared router mapping ID is absent",
                                w,
                            )?);
                            Ok((token, id))
                        })?,
                        w,
                    )?;
                    let partition_by = boxed(
                        list(&route.partition_value_ids, w, |id, _| {
                            Ok(p::ValueId::new(*id))
                        })?,
                        w,
                    )?;
                    Ok(p::ChangeStreamRoute {
                        route_id,
                        write_target_ordinal,
                        accepted_effects,
                        input_mapping,
                        partition_by,
                        edge: p::EdgeId::new(required(
                            route.edge_id,
                            "prepared router edge ID is absent",
                            w,
                        )?),
                    })
                })?,
                w,
            )?,
        },
    };
    let dop = required(input.dop_domain.as_ref(), "prepared DOP is absent", w)?;
    let output = DecodedFragmentEnvelope {
        id: p::FragmentId::new(input.id),
        root: p::NodeId::new(required(
            input.root_node_id,
            "prepared fragment root is absent",
            w,
        )?),
        sink,
        dop_domain: p::PipelineDopDomain {
            min: dop.min,
            max: dop.max,
            requires_power_of_two: dop.requires_power_of_two,
        },
        runtime_filters: boxed(
            list(&input.runtime_filter_ids, w, |id, _| {
                Ok(p::RuntimeFilterId::new(*id))
            })?,
            w,
        )?,
    };
    w.step()?;
    Ok(output)
}

pub(crate) struct PreparedFragmentEnvelopeEncode<'a> {
    input: &'a p::Fragment,
    control: &'a dyn PureCompileControl,
    facts: FragmentEnvelopeProjectionFacts,
}
impl PreparedFragmentEnvelopeEncode<'_> {
    pub(crate) fn facts(&self) -> &FragmentEnvelopeProjectionFacts {
        &self.facts
    }
    pub(crate) fn emit(
        self,
    ) -> Result<(EncodedFragmentEnvelope, FragmentEnvelopeProjectionFacts), Error> {
        let mut w = CompileCheckpoints::try_new(self.control, CompilePhase::Encode)?;
        let result = emit_encode(self.input, &mut w).map(|output| (output, self.facts));
        finish(w, result)
    }
}
pub(crate) fn prepare_fragment_envelope_encode<'a>(
    input: &'a p::Fragment,
    source_retained_bytes: usize,
    limits: FragmentEnvelopeProjectionLimits,
    control: &'a dyn PureCompileControl,
) -> Result<PreparedFragmentEnvelopeEncode<'a>, Error> {
    let mut w = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = preflight_encode(input, source_retained_bytes, limits, &mut w).map(|facts| {
        PreparedFragmentEnvelopeEncode {
            input,
            control,
            facts,
        }
    });
    finish(w, result)
}
pub(crate) struct PreparedFragmentEnvelopeDecode<'a> {
    input: &'a wire::Fragment,
    control: &'a dyn PureCompileControl,
    facts: FragmentEnvelopeProjectionFacts,
}
impl PreparedFragmentEnvelopeDecode<'_> {
    pub(crate) fn facts(&self) -> &FragmentEnvelopeProjectionFacts {
        &self.facts
    }
    pub(crate) fn emit(
        self,
    ) -> Result<(DecodedFragmentEnvelope, FragmentEnvelopeProjectionFacts), Error> {
        let mut w = CompileCheckpoints::try_new(self.control, CompilePhase::Decode)?;
        let result = emit_decode(self.input, &mut w).map(|output| (output, self.facts));
        finish(w, result)
    }
}
pub(crate) fn prepare_fragment_envelope_decode<'a>(
    input: &'a wire::Fragment,
    source_retained_bytes: usize,
    limits: FragmentEnvelopeProjectionLimits,
    control: &'a dyn PureCompileControl,
) -> Result<PreparedFragmentEnvelopeDecode<'a>, Error> {
    let mut w = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = preflight_decode(input, source_retained_bytes, limits, &mut w).map(|facts| {
        PreparedFragmentEnvelopeDecode {
            input,
            control,
            facts,
        }
    });
    finish(w, result)
}
#[cfg(test)]
#[path = "physical_fragment_envelope_v2/tests.rs"]
mod tests;
