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

//! Full neutral read-reference namespace through two sealed original sources.
//! Purpose, provider-private interpretation and Fragment closure remain at their
//! actual checked owners. The caller invoice covers ALL coexisting sources;
//! namespace floors below are only alias-safe necessary lower bounds.

use crate::{
    allocation_exit_v2::reserve_exit,
    binding_index_v2::BindingIndex,
    physical_binding_v2::BindingCodecError,
    physical_connector_payload_v2::{
        ConnectorPayloadCodecError, DecodedConnectorPayloads, EncodedConnectorPayloads,
        arc_u8_slice_bytes, bytes_shared_upper,
    },
    physical_provider_binding_v2::{
        DecodedProviderBindings, EncodedProviderBindings, ProviderBindingCodecError,
    },
};
use novarocks_connector_contract::{
    ConnectorError, ConnectorReadInputVersion, ConnectorReadRelationKind,
    ConnectorReadRelationPayload,
};
use novarocks_physical_plan::ProviderReadReference;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::{alloc::Layout, fmt, mem::size_of, sync::Arc};

#[derive(Clone, Copy, Debug)]
pub struct ProviderReadProjectionLimits {
    pub max_definitions: usize,
    pub max_input_version_bytes: usize,
    pub max_allocation_requests: usize,
    pub max_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_work: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderReadProjectionFacts {
    pub definition_count: usize,
    pub input_version_bytes: usize,
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}
#[derive(Debug)]
pub enum ProviderReadCodecError {
    Control(CompileControlError),
    Binding(ProviderBindingCodecError),
    Payload(ConnectorPayloadCodecError),
    Index(BindingCodecError),
    Contract(ConnectorError),
    InvalidShape(&'static str),
}
impl From<CompileControlError> for ProviderReadCodecError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl From<ProviderBindingCodecError> for ProviderReadCodecError {
    fn from(e: ProviderBindingCodecError) -> Self {
        match e {
            ProviderBindingCodecError::Control(c) => Self::Control(c),
            e => Self::Binding(e),
        }
    }
}
impl From<ConnectorPayloadCodecError> for ProviderReadCodecError {
    fn from(e: ConnectorPayloadCodecError) -> Self {
        match e {
            ConnectorPayloadCodecError::Control(c) => Self::Control(c),
            e => Self::Payload(e),
        }
    }
}
impl From<BindingCodecError> for ProviderReadCodecError {
    fn from(e: BindingCodecError) -> Self {
        match e {
            BindingCodecError::Control(c) => Self::Control(c),
            e => Self::Index(e),
        }
    }
}
impl fmt::Display for ProviderReadCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Binding(e) => e.fmt(f),
            Self::Payload(e) => e.fmt(f),
            Self::Index(e) => e.fmt(f),
            Self::Contract(e) => e.fmt(f),
            Self::InvalidShape(s) => f.write_str(s),
        }
    }
}
impl std::error::Error for ProviderReadCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(e) => Some(e),
            Self::Binding(e) => Some(e),
            Self::Payload(e) => Some(e),
            Self::Index(e) => Some(e),
            Self::Contract(e) => Some(e),
            Self::InvalidShape(_) => None,
        }
    }
}
type Error = ProviderReadCodecError;
fn invalid(s: &'static str) -> Error {
    Error::InvalidShape(s)
}
fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b)
        .ok_or_else(|| invalid("provider read resource sum overflow"))
}
fn mul(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_mul(b)
        .ok_or_else(|| invalid("provider read resource product overflow"))
}
fn bytes<T>(n: usize) -> Result<usize, Error> {
    Layout::array::<T>(n)
        .map(|v| v.size())
        .map_err(|_| invalid("provider read allocation layout is unrepresentable"))
}
fn cap(n: usize, max: usize, work: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    let allowed = n <= max;
    work.step()?;
    if allowed {
        Ok(())
    } else {
        Err(invalid("provider read projection envelope exceeded"))
    }
}
fn floor(source: usize, known: usize, work: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    let allowed = source >= known;
    work.step()?;
    if allowed {
        Ok(())
    } else {
        Err(invalid(
            "provider read source invoice omits original backing",
        ))
    }
}
fn finish<T>(result: Result<T, Error>, work: CompileCheckpoints<'_>) -> Result<T, Error> {
    if matches!(&result, Err(Error::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn reserve<T>(n: usize, work: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, Error> {
    bytes::<T>(n)?;
    work.flush()?;
    let mut out = Vec::new();
    let outcome = out.try_reserve_exact(n);
    reserve_exit::<Error>(outcome, work)?;
    Ok(out)
}
fn copy(input: &[u8], work: &mut CompileCheckpoints<'_>) -> Result<Vec<u8>, Error> {
    let mut out = reserve(input.len(), work)?;
    for chunk in input.chunks(1024) {
        out.extend_from_slice(chunk);
        work.step()?;
    }
    Ok(out)
}
fn same_control(
    left: &dyn PureCompileControl,
    right: &dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let same = std::ptr::eq(left, right);
    work.step()?;
    if same {
        Ok(())
    } else {
        Err(invalid(
            "provider read namespaces have different original controls",
        ))
    }
}
pub(crate) const fn encode_relation_kind(
    kind: ConnectorReadRelationKind,
) -> wire::ConnectorRelationKind {
    match kind {
        ConnectorReadRelationKind::Table => wire::ConnectorRelationKind::Table,
        ConnectorReadRelationKind::TableFunction => wire::ConnectorRelationKind::TableFunction,
        ConnectorReadRelationKind::ChangeWindow => wire::ConnectorRelationKind::ChangeWindow,
        ConnectorReadRelationKind::SystemTable => wire::ConnectorRelationKind::SystemTable,
        ConnectorReadRelationKind::TableExecute => wire::ConnectorRelationKind::TableExecute,
        ConnectorReadRelationKind::MergeTable => wire::ConnectorRelationKind::MergeTable,
    }
}
pub(crate) fn decode_relation_kind(kind: i32) -> Result<ConnectorReadRelationKind, Error> {
    Ok(
        match wire::ConnectorRelationKind::try_from(kind)
            .map_err(|_| invalid("unknown connector relation kind"))?
        {
            wire::ConnectorRelationKind::Table => ConnectorReadRelationKind::Table,
            wire::ConnectorRelationKind::TableFunction => ConnectorReadRelationKind::TableFunction,
            wire::ConnectorRelationKind::ChangeWindow => ConnectorReadRelationKind::ChangeWindow,
            wire::ConnectorRelationKind::SystemTable => ConnectorReadRelationKind::SystemTable,
            wire::ConnectorRelationKind::TableExecute => ConnectorReadRelationKind::TableExecute,
            wire::ConnectorRelationKind::MergeTable => ConnectorReadRelationKind::MergeTable,
            wire::ConnectorRelationKind::Unspecified => {
                return Err(invalid("unspecified connector relation kind"));
            }
        },
    )
}
fn required(value: Option<u32>, work: &mut CompileCheckpoints<'_>) -> Result<u32, Error> {
    work.step()?;
    value.ok_or_else(|| invalid("provider read reference ID is absent"))
}
#[derive(Default)]
struct Requests {
    count: usize,
    bytes: usize,
}
impl Requests {
    fn record(&mut self, n: usize) -> Result<(), Error> {
        if n != 0 {
            self.count = add(self.count, 1)?;
            self.bytes = add(self.bytes, n)?;
        }
        Ok(())
    }
}
fn bound(
    n: usize,
    bindings: usize,
    payloads: usize,
    input: usize,
    requests: &Requests,
) -> Result<usize, Error> {
    let height = (usize::BITS - n.leading_zeros()) as usize;
    // Index sort is <=4 operations/heap level. Encoding visits every source
    // once in preflight and again while emitting to preserve pointer identity;
    // decoding does two binary lookups per payload and binding. The linear
    // source-count bound covers both, plus each prior floor walk ONCE. Native
    // Arc/Bytes/constructor work is included numerically and bracketed, not
    // claimed to cooperate inside their opaque library calls.
    let namespaces = add(bindings, mul(payloads, 2)?)?;
    add(
        512,
        add(
            mul(
                n,
                add(128, add(mul(height + 1, 16)?, mul(namespaces, 4)?)?)?,
            )?,
            add(
                mul(add(bindings, payloads)?, 16)?,
                add(
                    mul(input, 4)?,
                    add(mul(requests.bytes, 4)?, requests.count)?,
                )?,
            )?,
        )?,
    )
}
fn facts(
    n: usize,
    input: usize,
    requests: Requests,
    source: usize,
    namespace_counts: (usize, usize),
    limits: ProviderReadProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ProviderReadProjectionFacts, Error> {
    let (bindings, payloads) = namespace_counts;
    let bound = bound(n, bindings, payloads, input, &requests)?;
    let facts = ProviderReadProjectionFacts {
        definition_count: n,
        input_version_bytes: input,
        allocation_requests_upper_bound: requests.count,
        allocation_request_bytes_upper_bound: requests.bytes,
        coexisting_source_and_request_bytes_upper_bound: add(source, requests.bytes)?,
        cumulative_work_upper_bound: bound,
    };
    cap(input, limits.max_input_version_bytes, work)?;
    cap(requests.count, limits.max_allocation_requests, work)?;
    cap(requests.bytes, limits.max_allocation_request_bytes, work)?;
    cap(
        facts.coexisting_source_and_request_bytes_upper_bound,
        limits.max_coexisting_source_and_request_bytes,
        work,
    )?;
    cap(bound, limits.max_work, work)?;
    Ok(facts)
}
fn prior_work(
    n: usize,
    bindings: usize,
    payloads: usize,
    limits: ProviderReadProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    // Admit floor/source-ID/lookup scans before any potentially long walk. The
    // complete request/copy bound is gated separately before first reserve.
    cap(
        bound(n, bindings, payloads, 0, &Requests::default())?,
        limits.max_work,
        work,
    )
}

pub struct EncodedProviderReads<'source, 'control> {
    inputs: &'source [(u32, &'source ProviderReadReference)],
    bindings: &'source EncodedProviderBindings<'source, 'control>,
    payloads: &'source EncodedConnectorPayloads<'source, 'control>,
    wire: Vec<wire::ProviderReadReferenceDefinition>,
    indices: BindingIndex,
    facts: ProviderReadProjectionFacts,
    original_source_bytes: usize,
}
impl<'source, 'control> EncodedProviderReads<'source, 'control> {
    pub fn as_wire(&self) -> &[wire::ProviderReadReferenceDefinition] {
        &self.wire
    }
    pub fn into_wire(self) -> Vec<wire::ProviderReadReferenceDefinition> {
        self.wire
    }
    pub fn facts(&self) -> &ProviderReadProjectionFacts {
        &self.facts
    }
    pub fn source_count(&self) -> usize {
        self.inputs.len()
    }
    pub fn bindings(&self) -> &EncodedProviderBindings<'source, 'control> {
        self.bindings
    }
    pub fn payloads(&self) -> &EncodedConnectorPayloads<'source, 'control> {
        self.payloads
    }
    pub(crate) fn original_control(&self) -> &'control dyn PureCompileControl {
        self.bindings.original_control()
    }
    pub fn read(&self, id: u32) -> Result<Option<&'source ProviderReadReference>, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.read_observed(id, &mut work);
        finish(result, work)
    }
    pub(crate) fn read_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source ProviderReadReference>, Error> {
        Ok(self
            .indices
            .find(id, |at| self.inputs[at].0, work)?
            .map(|at| self.inputs[at].1))
    }
    pub fn source_id(&self, source: &ProviderReadReference) -> Result<u32, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.source_id_observed(source, &mut work);
        finish(result, work)
    }
    pub(crate) fn source_id_observed(
        &self,
        source: &ProviderReadReference,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, Error> {
        let mut found = None;
        for (id, read) in self.inputs {
            let same = std::ptr::eq(*read, source);
            work.step()?;
            if same {
                if found.is_some() {
                    return Err(invalid("provider read source association is ambiguous"));
                }
                found = Some(*id);
            }
        }
        found.ok_or_else(|| invalid("provider read source owner is not in this namespace"))
    }
    pub fn retained_invoice_floor(&self) -> Result<usize, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.retained_floor_observed(&mut work);
        finish(result, work)
    }
    pub(crate) fn retained_floor_observed(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        let mut n = add(self.original_source_bytes, size_of::<Self>())?;
        n = add(n, self.indices.backing_bytes()?)?;
        n = add(
            n,
            bytes::<wire::ProviderReadReferenceDefinition>(self.wire.capacity())?,
        )?;
        work.step()?;
        for def in &self.wire {
            n = add(n, def.input_version.capacity())?;
            work.step()?;
        }
        Ok(n)
    }
}
pub struct DecodedProviderReads<'wire, 'control> {
    wire: &'wire [wire::ProviderReadReferenceDefinition],
    bindings: &'wire DecodedProviderBindings<'wire, 'control>,
    payloads: &'wire DecodedConnectorPayloads<'wire, 'control>,
    reads: Vec<ProviderReadReference>,
    indices: BindingIndex,
    facts: ProviderReadProjectionFacts,
    original_source_bytes: usize,
}
impl<'wire, 'control> DecodedProviderReads<'wire, 'control> {
    pub fn as_wire(&self) -> &'wire [wire::ProviderReadReferenceDefinition] {
        self.wire
    }
    pub fn facts(&self) -> &ProviderReadProjectionFacts {
        &self.facts
    }
    pub fn source_count(&self) -> usize {
        self.wire.len()
    }
    pub fn bindings(&self) -> &DecodedProviderBindings<'wire, 'control> {
        self.bindings
    }
    pub fn payloads(&self) -> &DecodedConnectorPayloads<'wire, 'control> {
        self.payloads
    }
    pub(crate) fn original_control(&self) -> &'control dyn PureCompileControl {
        self.bindings.original_control()
    }
    pub fn read(&self, id: u32) -> Result<Option<&ProviderReadReference>, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Decode)?;
        let result = self.read_observed(id, &mut work);
        finish(result, work)
    }
    pub(crate) fn read_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&ProviderReadReference>, Error> {
        Ok(self
            .indices
            .find(id, |at| self.wire[at].id, work)?
            .map(|at| &self.reads[at]))
    }
    pub fn retained_invoice_floor(&self) -> Result<usize, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Decode)?;
        let result = self.retained_floor_observed(&mut work);
        finish(result, work)
    }
    pub(crate) fn retained_floor_observed(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        let mut n = add(self.original_source_bytes, size_of::<Self>())?;
        n = add(n, self.indices.backing_bytes()?)?;
        n = add(n, bytes::<ProviderReadReference>(self.reads.capacity())?)?;
        work.step()?;
        for read in &self.reads {
            n = add(n, arc_u8_slice_bytes(read.input_version.as_bytes().len())?)?;
            work.step()?;
        }
        // Binding/payload backing aliases the prior namespaces already included
        // in the caller invoice. Potential new Bytes Shared blocks are request
        // upper bounds, not observable retained lower floors.
        Ok(n)
    }
}

pub fn encode_provider_reads<'source, 'control>(
    inputs: &'source [(u32, &'source ProviderReadReference)],
    bindings: &'source EncodedProviderBindings<'source, 'control>,
    payloads: &'source EncodedConnectorPayloads<'source, 'control>,
    source_retained_bytes: usize,
    limits: ProviderReadProjectionLimits,
) -> Result<EncodedProviderReads<'source, 'control>, Error> {
    let control = bindings.original_control();
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = encode_core(
        inputs,
        bindings,
        payloads,
        source_retained_bytes,
        limits,
        &mut work,
    );
    finish(result, work)
}
fn encode_core<'source, 'control>(
    inputs: &'source [(u32, &'source ProviderReadReference)],
    bindings: &'source EncodedProviderBindings<'source, 'control>,
    payloads: &'source EncodedConnectorPayloads<'source, 'control>,
    source: usize,
    limits: ProviderReadProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<EncodedProviderReads<'source, 'control>, Error> {
    same_control(
        bindings.original_control(),
        payloads.original_control(),
        work,
    )?;
    cap(inputs.len(), limits.max_definitions, work)?;
    prior_work(
        inputs.len(),
        bindings.source_count(),
        payloads.source_count(),
        limits,
        work,
    )?;
    let known = bindings
        .retained_floor_observed(work)?
        .max(payloads.retained_floor_observed(work)?);
    floor(source, known, work)?;
    let roots = bytes::<(u32, &ProviderReadReference)>(inputs.len())?;
    floor(source, roots, work)?;
    let mut req = Requests::default();
    req.record(bytes::<usize>(inputs.len())?)?;
    req.record(bytes::<wire::ProviderReadReferenceDefinition>(
        inputs.len(),
    )?)?;
    let mut input = 0;
    let mut individual = 0;
    for (_, read) in inputs {
        let n = read.input_version.as_bytes().len();
        input = add(input, n)?;
        individual = individual.max(add(
            size_of::<ProviderReadReference>(),
            arc_u8_slice_bytes(n)?,
        )?);
        req.record(bytes::<u8>(n)?)?;
        work.step()?;
        bindings.source_id_observed(&read.binding, work)?;
        payloads.source_id_observed(read.relation.table(), work)?;
        payloads.source_id_observed(read.relation.view(), work)?;
    }
    floor(source, add(roots, individual)?, work)?;
    let facts = facts(
        inputs.len(),
        input,
        req,
        source,
        (bindings.source_count(), payloads.source_count()),
        limits,
        work,
    )?;
    let indices = BindingIndex::prepare(inputs.len(), |at| inputs[at].0, work)?;
    let mut output = reserve(inputs.len(), work)?;
    for (id, read) in inputs {
        output.push(wire::ProviderReadReferenceDefinition {
            id: *id,
            provider_binding_id: Some(bindings.source_id_observed(&read.binding, work)?),
            input_version: copy(read.input_version.as_bytes(), work)?,
            kind: encode_relation_kind(read.relation.kind()) as i32,
            table_payload_id: Some(payloads.source_id_observed(read.relation.table(), work)?),
            view_payload_id: Some(payloads.source_id_observed(read.relation.view(), work)?),
        });
        work.step()?;
    }
    Ok(EncodedProviderReads {
        inputs,
        bindings,
        payloads,
        wire: output,
        indices,
        facts,
        original_source_bytes: source,
    })
}
fn decode_refs<'a>(
    def: &wire::ProviderReadReferenceDefinition,
    bindings: &'a DecodedProviderBindings<'_, '_>,
    payloads: &'a DecodedConnectorPayloads<'_, '_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    (
        &'a novarocks_connector_contract::ConnectorReadBinding,
        &'a novarocks_connector_contract::ConnectorEncodedPayload,
        &'a novarocks_connector_contract::ConnectorEncodedPayload,
    ),
    Error,
> {
    let id = required(def.provider_binding_id, work)?;
    let binding = bindings
        .binding_observed(id, work)?
        .ok_or_else(|| invalid("provider read binding ID is unknown"))?;
    let id = required(def.table_payload_id, work)?;
    let table = payloads
        .payload_observed(id, work)?
        .ok_or_else(|| invalid("provider read table payload ID is unknown"))?;
    let id = required(def.view_payload_id, work)?;
    let view = payloads
        .payload_observed(id, work)?
        .ok_or_else(|| invalid("provider read view payload ID is unknown"))?;
    Ok((binding, table, view))
}
pub fn decode_provider_reads<'wire, 'control>(
    definitions: &'wire [wire::ProviderReadReferenceDefinition],
    bindings: &'wire DecodedProviderBindings<'wire, 'control>,
    payloads: &'wire DecodedConnectorPayloads<'wire, 'control>,
    source_retained_bytes: usize,
    limits: ProviderReadProjectionLimits,
) -> Result<DecodedProviderReads<'wire, 'control>, Error> {
    let mut work = CompileCheckpoints::try_new(bindings.original_control(), CompilePhase::Decode)?;
    let result = decode_core(
        definitions,
        bindings,
        payloads,
        source_retained_bytes,
        limits,
        &mut work,
    );
    finish(result, work)
}
fn decode_core<'wire, 'control>(
    definitions: &'wire [wire::ProviderReadReferenceDefinition],
    bindings: &'wire DecodedProviderBindings<'wire, 'control>,
    payloads: &'wire DecodedConnectorPayloads<'wire, 'control>,
    source: usize,
    limits: ProviderReadProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedProviderReads<'wire, 'control>, Error> {
    same_control(
        bindings.original_control(),
        payloads.original_control(),
        work,
    )?;
    cap(definitions.len(), limits.max_definitions, work)?;
    prior_work(
        definitions.len(),
        bindings.source_count(),
        payloads.source_count(),
        limits,
        work,
    )?;
    let known = bindings
        .retained_floor_observed(work)?
        .max(payloads.retained_floor_observed(work)?);
    floor(source, known, work)?;
    let mut known = bytes::<wire::ProviderReadReferenceDefinition>(definitions.len())?;
    floor(source, known, work)?;
    let mut req = Requests::default();
    req.record(bytes::<usize>(definitions.len())?)?;
    req.record(bytes::<ProviderReadReference>(definitions.len())?)?;
    let mut input = 0;
    for def in definitions {
        let kind = decode_relation_kind(def.kind);
        work.step()?;
        kind?;
        let (_, table, view) = decode_refs(def, bindings, payloads, work)?;
        input = add(input, def.input_version.len())?;
        known = add(known, def.input_version.capacity())?;
        req.record(arc_u8_slice_bytes(def.input_version.len())?)?;
        for payload in [table, view] {
            if !payload.payload().is_empty() {
                req.record(bytes_shared_upper()?)?;
            }
            work.step()?;
        }
        work.step()?;
    }
    // Sole InputVersion::try_new converts to Arc THEN validates. Its one
    // possible error is ConnectorError::new with this exact owned String;
    // cleanup_context is None and no other fields allocate (read_facts.rs).
    // Reserve ONE possible terminal diagnostic, not one per successful input.
    if !definitions.is_empty() {
        req.record(bytes::<u8>(
            ConnectorReadInputVersion::invalid_length_diagnostic().len(),
        )?)?;
    }
    floor(source, known, work)?;
    let facts = facts(
        definitions.len(),
        input,
        req,
        source,
        (bindings.source_count(), payloads.source_count()),
        limits,
        work,
    )?;
    let indices = BindingIndex::prepare(definitions.len(), |at| definitions[at].id, work)?;
    let mut output = reserve(definitions.len(), work)?;
    for def in definitions {
        let kind = decode_relation_kind(def.kind)?;
        let (binding, table, view) = decode_refs(def, bindings, payloads, work)?;
        work.flush()?;
        let version =
            ConnectorReadInputVersion::try_new(Arc::<[u8]>::from(def.input_version.as_slice()));
        work.flush()?;
        let version = version.map_err(Error::Contract)?;
        work.flush()?;
        let read = ProviderReadReference {
            binding: binding.clone(),
            input_version: version,
            relation: ConnectorReadRelationPayload::new(kind, table.clone(), view.clone()),
        };
        work.flush()?;
        output.push(read);
        work.step()?;
    }
    Ok(DecodedProviderReads {
        wire: definitions,
        bindings,
        payloads,
        reads: output,
        indices,
        facts,
        original_source_bytes: source,
    })
}

#[cfg(test)]
mod tests;
