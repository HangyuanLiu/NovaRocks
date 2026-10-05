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
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
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
type EnvelopeAdmit<'a> =
    dyn FnMut(&FragmentEnvelopeProjectionFacts) -> Result<(), CompileControlError> + 'a;
struct EnvelopeAdmission<'a, 'callback> {
    parent: Option<&'a mut EnvelopeAdmit<'callback>>,
}
impl EnvelopeAdmission<'_, '_> {
    fn observed(&self) -> bool {
        self.parent.is_some()
    }
    fn numeric<T>(&self, result: Result<T, Error>) -> Result<T, Error> {
        match result {
            Err(Error::InvalidShape(_)) if self.observed() => {
                Err(CompileControlError::ResourceExhausted.into())
            }
            result => result,
        }
    }
    fn sum(&self, left: usize, right: usize) -> Result<usize, Error> {
        self.numeric(add(left, right))
    }
    fn request<T>(&self, model: &mut Model, n: usize, copies: usize) -> Result<(), Error> {
        self.numeric(model.request::<T>(n, copies))
    }
    fn gate(
        &mut self,
        model: &Model,
        source: usize,
        limits: FragmentEnvelopeProjectionLimits,
    ) -> Result<(), Error> {
        if self.observed() {
            let facts = self.numeric(model.numerical_facts(source, 0, limits))?;
            (self.parent.as_mut().expect("observed parent"))(&facts)?;
        }
        Ok(())
    }
    fn facts(
        &mut self,
        model: &Model,
        source: usize,
        limits: FragmentEnvelopeProjectionLimits,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<FragmentEnvelopeProjectionFacts, Error> {
        self.gate(model, source, limits)?;
        self.numeric(model.facts(source, 0, limits, work))
    }
}
// These are the original root request authors. Observed preparation consumes
// them before its first completed callback; Plain keeps its original order.
fn encode_sink_requests(
    sink: &p::FragmentSink,
    model: &mut Model,
    admission: &EnvelopeAdmission<'_, '_>,
) -> Result<(), Error> {
    match sink {
        p::FragmentSink::Multicast { edges } => admission.request::<u32>(model, edges.len(), 1)?,
        p::FragmentSink::Router { routes, .. } => {
            model.refs = 1;
            admission.request::<wire::ChangeStreamRoute>(model, routes.len(), 1)?;
        }
        _ => {}
    }
    Ok(())
}
fn decode_sink_requests(
    kind: Option<&wire::fragment_sink::Kind>,
    model: &mut Model,
    admission: &EnvelopeAdmission<'_, '_>,
) -> Result<(), Error> {
    match kind {
        Some(wire::fragment_sink::Kind::Multicast(v)) => {
            admission.request::<p::EdgeId>(model, v.edge_ids.len(), 2)?;
        }
        Some(wire::fragment_sink::Kind::Router(v)) => {
            model.refs = 1;
            admission.request::<p::ChangeStreamRoute>(model, v.routes.len(), 2)?;
        }
        _ => {}
    }
    Ok(())
}
fn encode_route_requests(
    route: &p::ChangeStreamRoute,
    model: &mut Model,
    admission: &EnvelopeAdmission<'_, '_>,
) -> Result<(), Error> {
    admission.request::<u8>(model, 32, 1)?;
    admission.request::<i32>(model, route.accepted_effects.len(), 1)?;
    admission.request::<wire::WriteInputMapping>(model, route.input_mapping.len(), 1)?;
    admission.request::<u32>(model, route.partition_by.len(), 1)?;
    admission.request::<u8>(model, 32, route.input_mapping.len())
}
fn decode_route_requests(
    route: &wire::ChangeStreamRoute,
    model: &mut Model,
    admission: &EnvelopeAdmission<'_, '_>,
) -> Result<(), Error> {
    admission.request::<ConnectorRowMutationEffect>(model, route.accepted_effects.len(), 2)?;
    admission.request::<(ConnectorWriteFieldToken, p::ValueId)>(
        model,
        route.input_mapping.len(),
        2,
    )?;
    admission.request::<p::ValueId>(model, route.partition_value_ids.len(), 2)
}
fn preflight_encode(
    input: &p::Fragment,
    source: usize,
    l: FragmentEnvelopeProjectionLimits,
    admission: &mut EnvelopeAdmission<'_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<FragmentEnvelopeProjectionFacts, Error> {
    let outer = admission.numeric(physical_outer(input))?;
    let mut model = Model {
        items: outer,
        ..Model::default()
    };
    if admission.observed() {
        admission.request::<u32>(&mut model, input.runtime_filters().len(), 1)?;
        encode_sink_requests(input.sink(), &mut model, admission)?;
        admission.gate(&model, source, l)?;
    }
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
    if !admission.observed() {
        admission.request::<u32>(&mut model, input.runtime_filters().len(), 1)?;
    }
    admission.facts(&model, source, l, w)?;
    if !admission.observed() {
        encode_sink_requests(input.sink(), &mut model, admission)?;
    }
    if let p::FragmentSink::Router { routes, .. } = input.sink() {
        admission.facts(&model, source, l, w)?;
        for route in routes {
            let inner = admission.sum(
                route.accepted_effects.len(),
                admission.sum(route.input_mapping.len(), route.partition_by.len())?,
            )?;
            model.items = admission.sum(model.items, inner)?;
            let references = || {
                admission.sum(
                    model.refs,
                    admission.sum(route.input_mapping.len(), route.partition_by.len())?,
                )
            };
            if admission.observed() {
                model.refs = references()?;
                encode_route_requests(route, &mut model, admission)?;
                admission.gate(&model, source, l)?;
            }
            count_prefix(0, model.items, source, known, l, w)?;
            if !admission.observed() {
                model.refs = admission.sum(
                    model.refs,
                    admission.sum(route.input_mapping.len(), route.partition_by.len())?,
                )?;
            }
            cap(model.refs, l.max_value_references, w)?;
            known = add(
                known,
                add(
                    bytes::<ConnectorRowMutationEffect>(route.accepted_effects.len())?,
                    add(
                        bytes::<(ConnectorWriteFieldToken, p::ValueId)>(route.input_mapping.len())?,
                        bytes::<p::ValueId>(route.partition_by.len())?,
                    )?,
                )?,
            )?;
            if !admission.observed() {
                encode_route_requests(route, &mut model, admission)?;
            }
            admission.facts(&model, source, l, w)?;
            floor(source, known, w)?;
            w.step()?;
        }
    }
    floor(source, known, w)?;
    admission.facts(&model, source, l, w)
}
fn preflight_decode(
    input: &wire::Fragment,
    source: usize,
    l: FragmentEnvelopeProjectionLimits,
    admission: &mut EnvelopeAdmission<'_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<FragmentEnvelopeProjectionFacts, Error> {
    let mut initial = None;
    if admission.observed() {
        // Presence belongs to raw_kind below. These are only requests already
        // exposed by the actual borrowed raw header; no sink is synthesized.
        let captured = input.sink.as_ref().and_then(|sink| sink.kind.as_ref());
        let outer = match captured {
            Some(kind) => admission.numeric(wire_outer(input, kind))?,
            None => input.runtime_filter_ids.len(),
        };
        let mut model = Model {
            items: outer,
            ..Model::default()
        };
        admission.request::<p::RuntimeFilterId>(&mut model, input.runtime_filter_ids.len(), 2)?;
        decode_sink_requests(captured, &mut model, admission)?;
        admission.gate(&model, source, l)?;
        initial = Some(model);
    }
    let kind = raw_kind(input, w)?;
    let outer = admission.numeric(wire_outer(input, kind))?;
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
    let mut model = initial.unwrap_or(Model {
        items: outer,
        ..Model::default()
    });
    if !admission.observed() {
        admission.request::<p::RuntimeFilterId>(&mut model, input.runtime_filter_ids.len(), 2)?;
    }
    admission.facts(&model, source, l, w)?;
    match kind {
        wire::fragment_sink::Kind::Multicast(_) => {
            if !admission.observed() {
                decode_sink_requests(Some(kind), &mut model, admission)?;
            }
        }
        wire::fragment_sink::Kind::Router(v) => {
            required(v.effect_value_id, "router effect ValueId is absent", w)?;
            if !admission.observed() {
                decode_sink_requests(Some(kind), &mut model, admission)?;
            }
            admission.facts(&model, source, l, w)?;
            for route in &v.routes {
                let inner = admission.sum(
                    route.accepted_effects.len(),
                    admission.sum(route.input_mapping.len(), route.partition_value_ids.len())?,
                )?;
                model.items = admission.sum(model.items, inner)?;
                let references = || {
                    admission.sum(
                        model.refs,
                        admission
                            .sum(route.input_mapping.len(), route.partition_value_ids.len())?,
                    )
                };
                if admission.observed() {
                    model.refs = references()?;
                    decode_route_requests(route, &mut model, admission)?;
                    admission.gate(&model, source, l)?;
                }
                count_prefix(0, model.items, source, known, l, w)?;
                if !admission.observed() {
                    model.refs = admission.sum(
                        model.refs,
                        admission
                            .sum(route.input_mapping.len(), route.partition_value_ids.len())?,
                    )?;
                }
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
                if !admission.observed() {
                    decode_route_requests(route, &mut model, admission)?;
                }
                admission.facts(&model, source, l, w)?;
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
    admission.facts(&model, source, l, w)
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

    /// Consume this exact token in the caller's scope. Preparation and emission
    /// replace one contribution; the source invoice and requests are not added twice.
    pub(crate) fn emit_in(
        self,
        admit: &mut impl FnMut(&FragmentEnvelopeProjectionFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(EncodedFragmentEnvelope, FragmentEnvelopeProjectionFacts), Error> {
        if !std::ptr::addr_eq(self.control, work.control()) {
            return Err(invalid(
                "fragment envelope emission has a different original control",
            ));
        }
        admit(&self.facts)?;
        emit_encode(self.input, work).map(|output| (output, self.facts))
    }
}
pub(crate) fn prepare_fragment_envelope_encode<'a>(
    input: &'a p::Fragment,
    source_retained_bytes: usize,
    limits: FragmentEnvelopeProjectionLimits,
    control: &'a dyn PureCompileControl,
) -> Result<PreparedFragmentEnvelopeEncode<'a>, Error> {
    let mut w = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = preflight_encode(
        input,
        source_retained_bytes,
        limits,
        &mut EnvelopeAdmission { parent: None },
        &mut w,
    )
    .map(|facts| PreparedFragmentEnvelopeEncode {
        input,
        control,
        facts,
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

    /// Consume this exact token in the caller's scope. Preparation and emission
    /// replace one contribution; the source invoice and requests are not added twice.
    pub(crate) fn emit_in(
        self,
        admit: &mut impl FnMut(&FragmentEnvelopeProjectionFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(DecodedFragmentEnvelope, FragmentEnvelopeProjectionFacts), Error> {
        if !std::ptr::addr_eq(self.control, work.control()) {
            return Err(invalid(
                "fragment envelope emission has a different original control",
            ));
        }
        admit(&self.facts)?;
        emit_decode(self.input, work).map(|output| (output, self.facts))
    }
}
pub(crate) fn prepare_fragment_envelope_decode<'a>(
    input: &'a wire::Fragment,
    source_retained_bytes: usize,
    limits: FragmentEnvelopeProjectionLimits,
    control: &'a dyn PureCompileControl,
) -> Result<PreparedFragmentEnvelopeDecode<'a>, Error> {
    let mut w = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = preflight_decode(
        input,
        source_retained_bytes,
        limits,
        &mut EnvelopeAdmission { parent: None },
        &mut w,
    )
    .map(|facts| PreparedFragmentEnvelopeDecode {
        input,
        control,
        facts,
    });
    finish(w, result)
}

/// Borrow the exact original header and the caller's control/work owner.
pub(crate) fn prepare_fragment_envelope_encode_in<'a, 'control: 'a>(
    input: &'a p::Fragment,
    source_retained_bytes: usize,
    limits: FragmentEnvelopeProjectionLimits,
    admit: &mut impl FnMut(&FragmentEnvelopeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedFragmentEnvelopeEncode<'a>, Error> {
    let facts = preflight_encode(
        input,
        source_retained_bytes,
        limits,
        &mut EnvelopeAdmission {
            parent: Some(admit),
        },
        work,
    )?;
    Ok(PreparedFragmentEnvelopeEncode {
        input,
        control: work.control(),
        facts,
    })
}

/// Borrow the exact original header and the caller's control/work owner.
pub(crate) fn prepare_fragment_envelope_decode_in<'a, 'control: 'a>(
    input: &'a wire::Fragment,
    source_retained_bytes: usize,
    limits: FragmentEnvelopeProjectionLimits,
    admit: &mut impl FnMut(&FragmentEnvelopeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedFragmentEnvelopeDecode<'a>, Error> {
    let facts = preflight_decode(
        input,
        source_retained_bytes,
        limits,
        &mut EnvelopeAdmission {
            parent: Some(admit),
        },
        work,
    )?;
    Ok(PreparedFragmentEnvelopeDecode {
        input,
        control: work.control(),
        facts,
    })
}
#[cfg(test)]
#[path = "physical_fragment_envelope_v2/tests.rs"]
mod tests;
