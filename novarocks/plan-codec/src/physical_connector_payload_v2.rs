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

//! Complete neutral connector-payload namespace projection. Private provider
//! bytes remain opaque. These tokens preserve namespace sources; they do not
//! authenticate installed capabilities or replace Fragment/package closure.
//! Request bounds precede output allocations, but are not formal MEM grants.

use crate::{
    allocation_exit_v2::reserve_exit, binding_index_v2::BindingIndex,
    physical_binding_v2::BindingCodecError,
};
use novarocks_connector_contract::{
    CatalogHandle, CatalogVersion, ConnectorCodecContractError, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorIdentityError, ConnectorInstanceId,
    ConnectorProviderId,
};
use novarocks_proto_models::{catalog, connector_common as dto, physical_package_v2 as wire};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::{alloc::Layout, fmt, mem::size_of};

#[derive(Clone, Copy, Debug)]
pub struct ConnectorPayloadProjectionLimits {
    pub max_definitions: usize,
    pub max_payload_bytes: usize,
    pub max_allocation_requests: usize,
    pub max_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_work: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorPayloadProjectionFacts {
    pub definition_count: usize,
    pub payload_bytes: usize,
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}
#[derive(Debug)]
pub enum ConnectorPayloadCodecError {
    Control(CompileControlError),
    Identity(ConnectorIdentityError),
    Contract(ConnectorCodecContractError),
    InvalidShape(&'static str),
}
impl From<CompileControlError> for ConnectorPayloadCodecError {
    fn from(value: CompileControlError) -> Self {
        Self::Control(value)
    }
}
impl From<BindingCodecError> for ConnectorPayloadCodecError {
    fn from(value: BindingCodecError) -> Self {
        match value {
            BindingCodecError::Control(cause) => Self::Control(cause),
            _ => Self::InvalidShape("connector payload namespace index is invalid"),
        }
    }
}
impl fmt::Display for ConnectorPayloadCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Identity(e) => e.fmt(f),
            Self::Contract(e) => e.fmt(f),
            Self::InvalidShape(s) => f.write_str(s),
        }
    }
}
impl std::error::Error for ConnectorPayloadCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(e) => Some(e),
            Self::Identity(e) => Some(e),
            Self::Contract(e) => Some(e),
            Self::InvalidShape(_) => None,
        }
    }
}
type Error = ConnectorPayloadCodecError;
fn invalid(s: &'static str) -> Error {
    Error::InvalidShape(s)
}
fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b)
        .ok_or_else(|| invalid("connector payload resource sum overflow"))
}
fn mul(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_mul(b)
        .ok_or_else(|| invalid("connector payload resource product overflow"))
}
fn bytes<T>(n: usize) -> Result<usize, Error> {
    Layout::array::<T>(n)
        .map(|v| v.size())
        .map_err(|_| invalid("connector payload allocation layout is unrepresentable"))
}
fn cap(n: usize, max: usize, work: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    let admitted = n <= max;
    work.step()?;
    if admitted {
        Ok(())
    } else {
        Err(invalid("connector payload projection envelope exceeded"))
    }
}
fn source_floor(n: usize, known: usize, work: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    let valid = n >= known;
    work.step()?;
    if valid {
        Ok(())
    } else {
        Err(invalid(
            "connector payload source invoice omits original backing",
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
    let mut values = Vec::new();
    let outcome = values.try_reserve_exact(n);
    reserve_exit::<Error>(outcome, work)?;
    Ok(values)
}
fn copy(input: &[u8], work: &mut CompileCheckpoints<'_>) -> Result<Vec<u8>, Error> {
    let mut output = reserve(input.len(), work)?;
    for chunk in input.chunks(1024) {
        output.extend_from_slice(chunk);
        work.step()?;
    }
    Ok(output)
}
fn string(input: &str, work: &mut CompileCheckpoints<'_>) -> Result<String, Error> {
    let bytes = copy(input.as_bytes(), work)?;
    work.flush()?;
    let outcome = String::from_utf8(bytes);
    work.flush()?;
    outcome.map_err(|_| invalid("connector payload authored string is not UTF-8"))
}
fn request_model() -> Result<(), Error> {
    crate::library_profile_v2::require_locked_bytes_request_model();
    if !crate::resource_source_model::LOCKED_TOOLCHAIN {
        return Err(invalid(
            "connector payload request model requires Rust 1.92",
        ));
    }
    Ok(())
}
pub(crate) fn arc_str_bytes(n: usize) -> Result<usize, Error> {
    arc_u8_slice_bytes(n)
}
pub(crate) fn arc_u8_slice_bytes(n: usize) -> Result<usize, Error> {
    // Rust 1.92 alloc/sync.rs ArcInner is repr(C): two AtomicUsize counters
    // followed by byte-aligned str/[u8] data. From<&str>/From<&[u8]>
    // allocate once, without an intermediate String or Vec.
    let data = Layout::array::<u8>(n).map_err(|_| invalid("connector identity layout overflow"))?;
    novarocks_type_contract::owned_resources::layout::arc_layout(data)
        .map(|layout| layout.size())
        .map_err(|_| invalid("connector identity Arc layout overflow"))
}
pub(crate) fn bytes_shared_upper() -> Result<usize, Error> {
    novarocks_type_contract::owned_resources::layout::bytes_shared_upper()
        .map_err(|_| invalid("connector Bytes shared layout overflow"))
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
fn facts(
    n: usize,
    payload: usize,
    input_bytes: usize,
    requests: Requests,
    source: usize,
    limits: ConnectorPayloadProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ConnectorPayloadProjectionFacts, Error> {
    let height = (usize::BITS - n.leading_zeros()) as usize;
    // BindingIndex uses at most four completed operations per heap level.
    // This covers heap construction/extraction, dedup, gates/header work,
    // constructors, copying and requested-backing initialization/moves. Raw
    // identity validation and std/Bytes allocation internals are opaque,
    // bracketed boundaries, not an internal 256-operation cooperation proof.
    let bound = add(
        256,
        add(
            mul(n, add(128, mul(height + 1, 16)?)?)?,
            add(
                mul(input_bytes, 4)?,
                add(mul(requests.bytes, 4)?, requests.count)?,
            )?,
        )?,
    )?;
    let result = ConnectorPayloadProjectionFacts {
        definition_count: n,
        payload_bytes: payload,
        allocation_requests_upper_bound: requests.count,
        allocation_request_bytes_upper_bound: requests.bytes,
        coexisting_source_and_request_bytes_upper_bound: add(source, requests.bytes)?,
        cumulative_work_upper_bound: bound,
    };
    cap(payload, limits.max_payload_bytes, work)?;
    cap(
        result.allocation_requests_upper_bound,
        limits.max_allocation_requests,
        work,
    )?;
    cap(
        result.allocation_request_bytes_upper_bound,
        limits.max_allocation_request_bytes,
        work,
    )?;
    cap(
        result.coexisting_source_and_request_bytes_upper_bound,
        limits.max_coexisting_source_and_request_bytes,
        work,
    )?;
    cap(bound, limits.max_work, work)?;
    Ok(result)
}
fn raw_header<'a>(
    value: &'a dto::ConnectorEncodedPayload,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(&'a dto::ConnectorEnvelopeHeader, &'a catalog::CatalogHandle), Error> {
    let header = value.header.as_ref();
    work.step()?;
    let header = header.ok_or_else(|| invalid("connector payload header is missing"))?;
    let catalog = header.catalog.as_ref();
    work.step()?;
    let catalog = catalog.ok_or_else(|| invalid("connector payload catalog is missing"))?;
    Ok((header, catalog))
}
fn check_header(
    header: &dto::ConnectorEnvelopeHeader,
    catalog: &catalog::CatalogHandle,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let category =
        novarocks_proto_codec::connector_common::decode_connector_category(header.category);
    let version = catalog.version.len() == 32;
    let revision = ConnectorCodecRevision::try_new(header.codec_revision);
    work.step()?;
    category.ok_or_else(|| invalid("connector payload category is unknown or unspecified"))?;
    if !version {
        return Err(invalid(
            "connector payload catalog version is not exactly 32 bytes",
        ));
    }
    revision.map_err(Error::Contract)?;
    Ok(())
}

/// Same-emission source loan. Immutable wire output cannot be modified behind
/// this token; sparse IDs borrow the exact original payload owner.
pub struct EncodedConnectorPayloads<'source, 'control> {
    inputs: &'source [(u32, &'source ConnectorEncodedPayload)],
    wire: Vec<wire::ConnectorPayloadDefinition>,
    indices: BindingIndex,
    facts: ConnectorPayloadProjectionFacts,
    control: &'control dyn PureCompileControl,
    original_source_bytes: usize,
}
impl<'source, 'control> EncodedConnectorPayloads<'source, 'control> {
    pub(crate) fn original_control(&self) -> &'control dyn PureCompileControl {
        self.control
    }

    /// This lower floor counts the original invoice once and current owned
    /// backing. It is not an allocator-capacity upper bound or a host grant.
    // This is the necessary known header floor, not the original source B
    // again and not a host grant. All operations are pure layout/arithmetic.
    fn retained_floor_header(&self) -> Result<usize, Error> {
        let mut floor = add(self.original_source_bytes, size_of::<Self>())?;
        floor = add(floor, self.indices.backing_bytes()?)?;
        add(
            floor,
            bytes::<wire::ConnectorPayloadDefinition>(self.wire.capacity())?,
        )
    }
    pub(crate) fn retained_floor_header_admitted(&self) -> Result<usize, CompileControlError> {
        self.retained_floor_header()
            .map_err(|_| CompileControlError::ResourceExhausted)
    }
    pub(crate) fn retained_floor_observed(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        let mut floor = self.retained_floor_header()?;
        work.step()?;
        for definition in &self.wire {
            let payload = definition
                .payload
                .as_ref()
                .ok_or_else(|| invalid("sealed connector payload source is absent"))?;
            let (header, catalog) = raw_header(payload, work)?;
            for n in [
                payload.payload.capacity(),
                header.provider_id.capacity(),
                catalog.catalog_name.capacity(),
                catalog.version.capacity(),
            ] {
                floor = add(floor, n)?;
            }
            work.step()?;
        }
        Ok(floor)
    }

    /// Resolve the exact original owner. An alias emitted under multiple IDs
    /// requires a later explicit sealed-reference author, never a first match.
    pub(crate) fn source_id_observed(
        &self,
        source: &ConnectorEncodedPayload,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, Error> {
        let mut found = None;
        for (id, payload) in self.inputs {
            let same = std::ptr::eq(*payload, source);
            work.step()?;
            if same {
                if found.is_some() {
                    return Err(invalid("connector payload source association is ambiguous"));
                }
                found = Some(*id);
            }
        }
        found.ok_or_else(|| invalid("connector payload source owner is not in this namespace"))
    }
    pub fn as_wire(&self) -> &[wire::ConnectorPayloadDefinition] {
        &self.wire
    }
    pub fn into_wire(self) -> Vec<wire::ConnectorPayloadDefinition> {
        self.wire
    }
    pub fn facts(&self) -> &ConnectorPayloadProjectionFacts {
        &self.facts
    }
    pub fn payload(&self, id: u32) -> Result<Option<&'source ConnectorEncodedPayload>, Error> {
        let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Encode)?;
        let result = self.payload_observed(id, &mut work);
        finish(result, work)
    }
    pub(crate) fn payload_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source ConnectorEncodedPayload>, Error> {
        let at = self.indices.find(id, |at| self.inputs[at].0, work)?;
        Ok(at.map(|at| self.inputs[at].1))
    }
    pub fn source_count(&self) -> usize {
        self.inputs.len()
    }
}
/// Full typed-neutral receiving owner. Provider-private decoding and namespace
/// reference closure still belong to their original public owners.
pub struct DecodedConnectorPayloads<'wire, 'control> {
    wire: &'wire [wire::ConnectorPayloadDefinition],
    payloads: Vec<ConnectorEncodedPayload>,
    indices: BindingIndex,
    facts: ConnectorPayloadProjectionFacts,
    control: &'control dyn PureCompileControl,
    original_source_bytes: usize,
}
impl<'wire, 'control> DecodedConnectorPayloads<'wire, 'control> {
    pub(crate) fn original_control(&self) -> &'control dyn PureCompileControl {
        self.control
    }
    pub fn source_count(&self) -> usize {
        self.wire.len()
    }
    // This is the necessary known header floor, not the original source B
    // again and not a host grant. All operations are pure layout/arithmetic.
    fn retained_floor_header(&self) -> Result<usize, Error> {
        let mut floor = add(self.original_source_bytes, size_of::<Self>())?;
        floor = add(floor, self.indices.backing_bytes()?)?;
        add(
            floor,
            bytes::<ConnectorEncodedPayload>(self.payloads.capacity())?,
        )
    }
    pub(crate) fn retained_floor_header_admitted(&self) -> Result<usize, CompileControlError> {
        self.retained_floor_header()
            .map_err(|_| CompileControlError::ResourceExhausted)
    }
    pub(crate) fn retained_floor_observed(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        let mut floor = self.retained_floor_header()?;
        work.step()?;
        for payload in &self.payloads {
            let header = payload.header();
            floor = add(floor, arc_str_bytes(header.provider_id().as_str().len())?)?;
            floor = add(
                floor,
                arc_str_bytes(header.catalog().catalog_name().as_str().len())?,
            )?;
            // Bytes' logical length is independently visible; opaque allocator
            // capacity and Shared backing remain in the host's complete invoice.
            floor = add(floor, payload.payload().len())?;
            work.step()?;
        }
        Ok(floor)
    }
    pub fn as_wire(&self) -> &'wire [wire::ConnectorPayloadDefinition] {
        self.wire
    }
    pub fn facts(&self) -> &ConnectorPayloadProjectionFacts {
        &self.facts
    }
    pub fn payload(&self, id: u32) -> Result<Option<&ConnectorEncodedPayload>, Error> {
        let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Decode)?;
        let result = self.payload_observed(id, &mut work);
        finish(result, work)
    }
    pub(crate) fn payload_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&ConnectorEncodedPayload>, Error> {
        let at = self.indices.find(id, |at| self.wire[at].id, work)?;
        Ok(at.map(|at| &self.payloads[at]))
    }
}

pub fn encode_connector_payloads<'source, 'control>(
    inputs: &'source [(u32, &'source ConnectorEncodedPayload)],
    source_retained_bytes: usize,
    limits: ConnectorPayloadProjectionLimits,
    control: &'control dyn PureCompileControl,
) -> Result<EncodedConnectorPayloads<'source, 'control>, Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = encode_core(inputs, source_retained_bytes, limits, control, &mut work);
    finish(result, work)
}
fn encode_core<'source, 'control>(
    inputs: &'source [(u32, &'source ConnectorEncodedPayload)],
    source: usize,
    limits: ConnectorPayloadProjectionLimits,
    control: &'control dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
) -> Result<EncodedConnectorPayloads<'source, 'control>, Error> {
    request_model()?;
    cap(inputs.len(), limits.max_definitions, work)?;
    let roots = bytes::<(u32, &ConnectorEncodedPayload)>(inputs.len())?;
    source_floor(source, roots, work)?;
    let mut requests = Requests::default();
    requests.record(bytes::<usize>(inputs.len())?)?;
    requests.record(bytes::<wire::ConnectorPayloadDefinition>(inputs.len())?)?;
    let mut payload = 0;
    let mut input_bytes = 0;
    let mut individual = 0;
    for (_, value) in inputs {
        let header = value.header();
        let provider = header.provider_id().as_str().len();
        let catalog = header.catalog().catalog_name().as_str().len();
        let body = value.payload().len();
        let content = add(add(provider, catalog)?, body)?;
        payload = add(payload, body)?;
        input_bytes = add(input_bytes, content)?;
        individual = individual.max(add(
            size_of::<ConnectorEncodedPayload>(),
            add(
                body,
                add(arc_str_bytes(provider)?, arc_str_bytes(catalog)?)?,
            )?,
        )?);
        for n in [provider, catalog, 32, body] {
            requests.record(bytes::<u8>(n)?)?;
        }
        work.step()?;
    }
    // Inputs can alias the same payload. The root slice is independent, while
    // only the maximum individual original owner is a safe additive floor.
    source_floor(source, add(roots, individual)?, work)?;
    let facts = facts(
        inputs.len(),
        payload,
        input_bytes,
        requests,
        source,
        limits,
        work,
    )?;
    let indices = BindingIndex::prepare(inputs.len(), |at| inputs[at].0, work)?;
    let mut output = reserve(inputs.len(), work)?;
    for (id, value) in inputs {
        let header = value.header();
        let provider_id = string(header.provider_id().as_str(), work)?;
        let catalog_name = string(header.catalog().catalog_name().as_str(), work)?;
        let version = copy(header.catalog().version().as_bytes(), work)?;
        let payload = copy(value.payload(), work)?;
        output.push(wire::ConnectorPayloadDefinition {
            id: *id,
            payload: Some(dto::ConnectorEncodedPayload {
                header: Some(dto::ConnectorEnvelopeHeader {
                    provider_id,
                    catalog: Some(catalog::CatalogHandle {
                        catalog_name,
                        version,
                    }),
                    category: novarocks_proto_codec::connector_common::encode_connector_category(
                        header.category(),
                    ),
                    codec_revision: header.codec_revision().get(),
                }),
                payload,
            }),
        });
        work.step()?;
    }
    Ok(EncodedConnectorPayloads {
        inputs,
        wire: output,
        indices,
        facts,
        control,
        original_source_bytes: source,
    })
}
pub fn decode_connector_payloads<'wire, 'control>(
    definitions: &'wire [wire::ConnectorPayloadDefinition],
    source_retained_bytes: usize,
    limits: ConnectorPayloadProjectionLimits,
    control: &'control dyn PureCompileControl,
) -> Result<DecodedConnectorPayloads<'wire, 'control>, Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = decode_core(
        definitions,
        source_retained_bytes,
        limits,
        control,
        &mut work,
    );
    finish(result, work)
}
fn decode_core<'wire, 'control>(
    definitions: &'wire [wire::ConnectorPayloadDefinition],
    source: usize,
    limits: ConnectorPayloadProjectionLimits,
    control: &'control dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedConnectorPayloads<'wire, 'control>, Error> {
    request_model()?;
    cap(definitions.len(), limits.max_definitions, work)?;
    let roots = bytes::<wire::ConnectorPayloadDefinition>(definitions.len())?;
    source_floor(source, roots, work)?;
    let mut known = roots;
    let mut requests = Requests::default();
    requests.record(bytes::<usize>(definitions.len())?)?;
    requests.record(bytes::<ConnectorEncodedPayload>(definitions.len())?)?;
    let mut payload = 0;
    let mut input_bytes = 0;
    for definition in definitions {
        let value = definition.payload.as_ref();
        work.step()?;
        let value =
            value.ok_or_else(|| invalid("connector payload definition value is missing"))?;
        let (header, catalog) = raw_header(value, work)?;
        check_header(header, catalog, work)?;
        known = add(
            known,
            add(
                value.payload.capacity(),
                add(
                    header.provider_id.capacity(),
                    add(catalog.catalog_name.capacity(), catalog.version.capacity())?,
                )?,
            )?,
        )?;
        input_bytes = add(
            input_bytes,
            add(
                value.payload.len(),
                add(header.provider_id.len(), catalog.catalog_name.len())?,
            )?,
        )?;
        payload = add(payload, value.payload.len())?;
        requests.record(arc_str_bytes(header.provider_id.len())?)?;
        requests.record(arc_str_bytes(catalog.catalog_name.len())?)?;
        requests.record(bytes::<u8>(value.payload.len())?)?;
        if !value.payload.is_empty() {
            requests.record(bytes_shared_upper()?)?;
        }
        work.step()?;
    }
    source_floor(source, known, work)?;
    let facts = facts(
        definitions.len(),
        payload,
        input_bytes,
        requests,
        source,
        limits,
        work,
    )?;
    let indices = BindingIndex::prepare(definitions.len(), |at| definitions[at].id, work)?;
    let mut payloads = reserve(definitions.len(), work)?;
    for definition in definitions {
        let value = definition
            .payload
            .as_ref()
            .ok_or_else(|| invalid("connector payload definition value is missing"))?;
        let (header, catalog) = raw_header(value, work)?;
        work.flush()?;
        let provider = ConnectorProviderId::parse(&header.provider_id);
        work.flush()?;
        let provider = provider.map_err(Error::Identity)?;
        work.flush()?;
        let name = ConnectorInstanceId::try_from_canonical(&catalog.catalog_name);
        work.flush()?;
        let name = name.map_err(Error::Identity)?;
        let version =
            CatalogVersion::from_bytes(catalog.version.as_slice().try_into().map_err(|_| {
                invalid("connector payload catalog version is not exactly 32 bytes")
            })?);
        let category =
            novarocks_proto_codec::connector_common::decode_connector_category(header.category)
                .ok_or_else(|| invalid("connector payload category is unknown or unspecified"))?;
        let revision =
            ConnectorCodecRevision::try_new(header.codec_revision).map_err(Error::Contract)?;
        let header = ConnectorEnvelopeHeader::new(
            provider,
            CatalogHandle::new(name, version),
            category,
            revision,
        );
        let body = copy(&value.payload, work)?;
        work.flush()?;
        let value = ConnectorEncodedPayload::new(header, body.into());
        work.flush()?;
        payloads.push(value);
        work.step()?;
    }
    Ok(DecodedConnectorPayloads {
        wire: definitions,
        payloads,
        indices,
        facts,
        control,
        original_source_bytes: source,
    })
}

#[cfg(test)]
mod tests;
