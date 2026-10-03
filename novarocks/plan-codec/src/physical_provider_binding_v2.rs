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

//! Complete neutral provider-binding namespace. Original sources and control
//! are sealed together; no provider capability or read/install proof is minted.
//! Future multi-namespace composition must retain the SAME original control
//! loan, not substitute a new callback owner.

use crate::{
    allocation_exit_v2::reserve_exit, binding_index_v2::BindingIndex,
    physical_binding_v2::BindingCodecError, physical_connector_payload_v2::arc_str_bytes,
};
use novarocks_connector_contract::{
    CatalogHandle, CatalogVersion, ConnectorIdentityError, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorProviderId, ConnectorReadBinding,
};
use novarocks_proto_models::{catalog, physical_package_v2 as wire};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::{alloc::Layout, fmt, mem::size_of};

#[derive(Clone, Copy, Debug)]
pub struct ProviderBindingProjectionLimits {
    pub max_definitions: usize,
    pub max_allocation_requests: usize,
    pub max_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_work: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderBindingProjectionFacts {
    pub definition_count: usize,
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}
#[derive(Debug)]
pub enum ProviderBindingCodecError {
    Control(CompileControlError),
    Identity(ConnectorIdentityError),
    Index(BindingCodecError),
    ArcLayout(crate::physical_connector_payload_v2::ConnectorPayloadCodecError),
    InvalidShape(&'static str),
}
impl From<CompileControlError> for ProviderBindingCodecError {
    fn from(value: CompileControlError) -> Self {
        Self::Control(value)
    }
}
impl From<BindingCodecError> for ProviderBindingCodecError {
    fn from(value: BindingCodecError) -> Self {
        match value {
            BindingCodecError::Control(cause) => Self::Control(cause),
            value => Self::Index(value),
        }
    }
}
impl From<crate::physical_connector_payload_v2::ConnectorPayloadCodecError>
    for ProviderBindingCodecError
{
    fn from(value: crate::physical_connector_payload_v2::ConnectorPayloadCodecError) -> Self {
        match value {
            crate::physical_connector_payload_v2::ConnectorPayloadCodecError::Control(cause) => {
                Self::Control(cause)
            }
            value => Self::ArcLayout(value),
        }
    }
}
impl fmt::Display for ProviderBindingCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Identity(e) => e.fmt(f),
            Self::Index(e) => e.fmt(f),
            Self::ArcLayout(e) => e.fmt(f),
            Self::InvalidShape(s) => f.write_str(s),
        }
    }
}
impl std::error::Error for ProviderBindingCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(e) => Some(e),
            Self::Identity(e) => Some(e),
            Self::Index(e) => Some(e),
            Self::ArcLayout(e) => Some(e),
            Self::InvalidShape(_) => None,
        }
    }
}
type Error = ProviderBindingCodecError;
fn invalid(s: &'static str) -> Error {
    Error::InvalidShape(s)
}
fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b)
        .ok_or_else(|| invalid("provider binding resource sum overflow"))
}
fn mul(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_mul(b)
        .ok_or_else(|| invalid("provider binding resource product overflow"))
}
fn bytes<T>(n: usize) -> Result<usize, Error> {
    Layout::array::<T>(n)
        .map(|v| v.size())
        .map_err(|_| invalid("provider binding allocation layout is unrepresentable"))
}
fn cap(n: usize, max: usize, work: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    let valid = n <= max;
    work.step()?;
    if valid {
        Ok(())
    } else {
        Err(invalid("provider binding projection envelope exceeded"))
    }
}
fn floor(source: usize, known: usize, work: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    let valid = source >= known;
    work.step()?;
    if valid {
        Ok(())
    } else {
        Err(invalid(
            "provider binding source invoice omits original backing",
        ))
    }
}
fn finish<T>(outcome: Result<T, Error>, work: CompileCheckpoints<'_>) -> Result<T, Error> {
    if matches!(&outcome, Err(Error::Control(_))) {
        return outcome;
    }
    work.finish()?;
    outcome
}
fn reserve<T>(n: usize, work: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, Error> {
    bytes::<T>(n)?;
    work.flush()?;
    let mut output = Vec::new();
    let result = output.try_reserve_exact(n);
    reserve_exit::<Error>(result, work)?;
    Ok(output)
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
    let result = String::from_utf8(bytes);
    work.flush()?;
    result.map_err(|_| invalid("provider binding authored identity is not UTF-8"))
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
    input_bytes: usize,
    requests: Requests,
    source: usize,
    limits: ProviderBindingProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ProviderBindingProjectionFacts, Error> {
    let height = (usize::BITS - n.leading_zeros()) as usize;
    // Sole BindingIndex: <=4 completed operations per heap level, plus build,
    // extraction and adjacent dedup. Raw native-identity ASCII validation,
    // Arc construction and String verification are bracketed opaque work;
    // their raw byte lengths and requests are included, not claimed to yield
    // cooperatively inside those libraries. No formal host MEM grant.
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
    let facts = ProviderBindingProjectionFacts {
        definition_count: n,
        allocation_requests_upper_bound: requests.count,
        allocation_request_bytes_upper_bound: requests.bytes,
        coexisting_source_and_request_bytes_upper_bound: add(source, requests.bytes)?,
        cumulative_work_upper_bound: bound,
    };
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
fn catalog<'a>(
    definition: &'a wire::ProviderBindingDefinition,
    work: &mut CompileCheckpoints<'_>,
) -> Result<&'a catalog::CatalogHandle, Error> {
    let result = definition.catalog.as_ref();
    work.step()?;
    result.ok_or_else(|| invalid("provider binding catalog is absent"))
}
fn provider_arc(n: usize) -> Result<usize, Error> {
    Ok(arc_str_bytes(n)?)
}

pub struct EncodedProviderBindings<'source, 'control> {
    inputs: &'source [(u32, &'source ConnectorReadBinding)],
    wire: Vec<wire::ProviderBindingDefinition>,
    indices: BindingIndex,
    facts: ProviderBindingProjectionFacts,
    control: &'control dyn PureCompileControl,
    original_source_bytes: usize,
}
impl<'source, 'control> EncodedProviderBindings<'source, 'control> {
    pub fn as_wire(&self) -> &[wire::ProviderBindingDefinition] {
        &self.wire
    }
    pub fn into_wire(self) -> Vec<wire::ProviderBindingDefinition> {
        self.wire
    }
    pub fn facts(&self) -> &ProviderBindingProjectionFacts {
        &self.facts
    }
    pub fn source_count(&self) -> usize {
        self.inputs.len()
    }
    pub(crate) fn original_control(&self) -> &'control dyn PureCompileControl {
        self.control
    }
    pub fn binding(&self, id: u32) -> Result<Option<&'source ConnectorReadBinding>, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.binding_observed(id, &mut work);
        finish(result, work)
    }
    pub(crate) fn binding_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source ConnectorReadBinding>, Error> {
        Ok(self
            .indices
            .find(id, |at| self.inputs[at].0, work)?
            .map(|at| self.inputs[at].1))
    }
    /// Resolve an exact source owner, not an equivalent reconstructed binding.
    pub fn source_id(&self, source: &ConnectorReadBinding) -> Result<u32, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.source_id_observed(source, &mut work);
        finish(result, work)
    }
    pub(crate) fn source_id_observed(
        &self,
        source: &ConnectorReadBinding,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, Error> {
        let mut found = None;
        for (id, binding) in self.inputs {
            let same = std::ptr::eq(*binding, source);
            work.step()?;
            if same {
                if found.is_some() {
                    return Err(invalid("provider binding source association is ambiguous"));
                }
                found = Some(*id);
            }
        }
        found.ok_or_else(|| invalid("provider binding source owner is not in this namespace"))
    }
    /// Known retained lower floor, not an allocator/live-byte measurement.
    /// Repeated floor/lookup work must be admitted by the consuming scope.
    pub fn retained_invoice_floor(&self) -> Result<usize, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.retained_floor_observed(&mut work);
        finish(result, work)
    }
    pub(crate) fn retained_floor_observed(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        let mut result = add(self.original_source_bytes, size_of::<Self>())?;
        result = add(result, self.indices.backing_bytes()?)?;
        result = add(
            result,
            bytes::<wire::ProviderBindingDefinition>(self.wire.capacity())?,
        )?;
        work.step()?;
        for definition in &self.wire {
            let catalog = catalog(definition, work)?;
            result = add(
                result,
                add(
                    definition.provider_id.capacity(),
                    add(
                        definition.instance_id.capacity(),
                        add(catalog.catalog_name.capacity(), catalog.version.capacity())?,
                    )?,
                )?,
            )?;
            work.step()?;
        }
        Ok(result)
    }
}
pub struct DecodedProviderBindings<'wire, 'control> {
    wire: &'wire [wire::ProviderBindingDefinition],
    bindings: Vec<ConnectorReadBinding>,
    indices: BindingIndex,
    facts: ProviderBindingProjectionFacts,
    control: &'control dyn PureCompileControl,
    original_source_bytes: usize,
}
impl<'wire, 'control> DecodedProviderBindings<'wire, 'control> {
    pub fn as_wire(&self) -> &'wire [wire::ProviderBindingDefinition] {
        self.wire
    }
    pub fn facts(&self) -> &ProviderBindingProjectionFacts {
        &self.facts
    }
    pub fn source_count(&self) -> usize {
        self.wire.len()
    }
    pub(crate) fn original_control(&self) -> &'control dyn PureCompileControl {
        self.control
    }
    pub fn binding(&self, id: u32) -> Result<Option<&ConnectorReadBinding>, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Decode)?;
        let result = self.binding_observed(id, &mut work);
        finish(result, work)
    }
    pub(crate) fn binding_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&ConnectorReadBinding>, Error> {
        Ok(self
            .indices
            .find(id, |at| self.wire[at].id, work)?
            .map(|at| &self.bindings[at]))
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
        let mut result = add(self.original_source_bytes, size_of::<Self>())?;
        result = add(result, self.indices.backing_bytes()?)?;
        result = add(
            result,
            bytes::<ConnectorReadBinding>(self.bindings.capacity())?,
        )?;
        work.step()?;
        for binding in &self.bindings {
            result = add(
                result,
                provider_arc(binding.descriptor().provider_id.as_str().len())?,
            )?;
            // Each receiving constructor creates three independent Arcs. The
            // encoder's potentially aliased instance/catalog source differs.
            result = add(
                result,
                provider_arc(binding.descriptor().instance_id.as_str().len())?,
            )?;
            result = add(
                result,
                provider_arc(binding.catalog_handle().catalog_name().as_str().len())?,
            )?;
            work.step()?;
        }
        Ok(result)
    }
}

pub fn encode_provider_bindings<'source, 'control>(
    inputs: &'source [(u32, &'source ConnectorReadBinding)],
    source_retained_bytes: usize,
    limits: ProviderBindingProjectionLimits,
    control: &'control dyn PureCompileControl,
) -> Result<EncodedProviderBindings<'source, 'control>, Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = encode_core(inputs, source_retained_bytes, limits, control, &mut work);
    finish(result, work)
}
fn encode_core<'source, 'control>(
    inputs: &'source [(u32, &'source ConnectorReadBinding)],
    source: usize,
    limits: ProviderBindingProjectionLimits,
    control: &'control dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
) -> Result<EncodedProviderBindings<'source, 'control>, Error> {
    cap(inputs.len(), limits.max_definitions, work)?;
    let roots = bytes::<(u32, &ConnectorReadBinding)>(inputs.len())?;
    floor(source, roots, work)?;
    let mut requests = Requests::default();
    requests.record(bytes::<usize>(inputs.len())?)?;
    requests.record(bytes::<wire::ProviderBindingDefinition>(inputs.len())?)?;
    let mut input_bytes = 0;
    let mut individual = 0;
    for (_, binding) in inputs {
        let provider = binding.descriptor().provider_id.as_str().len();
        let instance = binding.descriptor().instance_id.as_str().len();
        let catalog = binding.catalog_handle().catalog_name().as_str().len();
        input_bytes = add(input_bytes, add(provider, add(instance, catalog)?)?)?;
        // Instance and catalog identities can clone the SAME Arc. Counting
        // their sum would reject an exactly invoiced lawful source. Max is a
        // safe floor whether those immutable owners alias or are independent.
        individual = individual.max(add(
            size_of::<ConnectorReadBinding>(),
            add(
                provider_arc(provider)?,
                provider_arc(instance)?.max(provider_arc(catalog)?),
            )?,
        )?);
        for n in [provider, instance, catalog, 32] {
            requests.record(bytes::<u8>(n)?)?;
        }
        work.step()?;
    }
    floor(source, add(roots, individual)?, work)?;
    let facts = facts(inputs.len(), input_bytes, requests, source, limits, work)?;
    let indices = BindingIndex::prepare(inputs.len(), |at| inputs[at].0, work)?;
    let mut output = reserve(inputs.len(), work)?;
    for (id, binding) in inputs {
        output.push(wire::ProviderBindingDefinition {
            id: *id,
            provider_id: string(binding.descriptor().provider_id.as_str(), work)?,
            instance_id: string(binding.descriptor().instance_id.as_str(), work)?,
            catalog: Some(catalog::CatalogHandle {
                catalog_name: string(binding.catalog_handle().catalog_name().as_str(), work)?,
                version: copy(binding.catalog_handle().version().as_bytes(), work)?,
            }),
        });
        work.step()?;
    }
    Ok(EncodedProviderBindings {
        inputs,
        wire: output,
        indices,
        facts,
        control,
        original_source_bytes: source,
    })
}
pub fn decode_provider_bindings<'wire, 'control>(
    definitions: &'wire [wire::ProviderBindingDefinition],
    source_retained_bytes: usize,
    limits: ProviderBindingProjectionLimits,
    control: &'control dyn PureCompileControl,
) -> Result<DecodedProviderBindings<'wire, 'control>, Error> {
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
    definitions: &'wire [wire::ProviderBindingDefinition],
    source: usize,
    limits: ProviderBindingProjectionLimits,
    control: &'control dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedProviderBindings<'wire, 'control>, Error> {
    cap(definitions.len(), limits.max_definitions, work)?;
    let roots = bytes::<wire::ProviderBindingDefinition>(definitions.len())?;
    floor(source, roots, work)?;
    let mut known = roots;
    let mut requests = Requests::default();
    requests.record(bytes::<usize>(definitions.len())?)?;
    requests.record(bytes::<ConnectorReadBinding>(definitions.len())?)?;
    let mut input_bytes = 0;
    for definition in definitions {
        let catalog = catalog(definition, work)?;
        let version = catalog.version.len() == 32;
        work.step()?;
        if !version {
            return Err(invalid(
                "provider binding catalog version is not exactly 32 bytes",
            ));
        }
        input_bytes = add(
            input_bytes,
            add(
                definition.provider_id.len(),
                add(definition.instance_id.len(), catalog.catalog_name.len())?,
            )?,
        )?;
        known = add(
            known,
            add(
                definition.provider_id.capacity(),
                add(
                    definition.instance_id.capacity(),
                    add(catalog.catalog_name.capacity(), catalog.version.capacity())?,
                )?,
            )?,
        )?;
        for n in [
            definition.provider_id.len(),
            definition.instance_id.len(),
            catalog.catalog_name.len(),
        ] {
            requests.record(provider_arc(n)?)?;
        }
        work.step()?;
    }
    floor(source, known, work)?;
    let facts = facts(
        definitions.len(),
        input_bytes,
        requests,
        source,
        limits,
        work,
    )?;
    let indices = BindingIndex::prepare(definitions.len(), |at| definitions[at].id, work)?;
    let mut output = reserve(definitions.len(), work)?;
    for definition in definitions {
        let catalog = catalog(definition, work)?;
        work.flush()?;
        let provider_id = ConnectorProviderId::parse(&definition.provider_id);
        work.flush()?;
        let provider_id = provider_id.map_err(Error::Identity)?;
        work.flush()?;
        let instance_id = ConnectorInstanceId::try_from_canonical(&definition.instance_id);
        work.flush()?;
        let instance_id = instance_id.map_err(Error::Identity)?;
        work.flush()?;
        let catalog_name = ConnectorInstanceId::try_from_canonical(&catalog.catalog_name);
        work.flush()?;
        let catalog_name = catalog_name.map_err(Error::Identity)?;
        let version =
            CatalogVersion::from_bytes(catalog.version.as_slice().try_into().map_err(|_| {
                invalid("provider binding catalog version is not exactly 32 bytes")
            })?);
        output.push(ConnectorReadBinding::new(
            ConnectorInstanceDescriptor {
                provider_id,
                instance_id,
            },
            CatalogHandle::new(catalog_name, version),
        ));
        work.step()?;
    }
    Ok(DecodedProviderBindings {
        wire: definitions,
        bindings: output,
        indices,
        facts,
        control,
        original_source_bytes: source,
    })
}

#[cfg(test)]
mod tests;
