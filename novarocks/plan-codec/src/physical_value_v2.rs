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

//! Complete ValueDef namespace through the original type/payload authors.
//! The payload token supplies the original control; TypeTable supplies type
//! authority and currently does not retain a control. Fragment reference,
//! provenance, provider purpose, and installed-owner checks remain mandatory.
//! Source invoices are caller-owned full backing facts; floors below are only
//! necessary lower bounds, never a host allocation grant or RSS measurement.

use crate::{
    allocation_exit_v2::reserve_exit,
    binding_index_v2::BindingIndex,
    borrowed_type_resources::verify_type_binding,
    physical_binding_v2::BindingCodecError,
    physical_connector_payload_v2::{
        ConnectorPayloadCodecError, DecodedConnectorPayloads, EncodedConnectorPayloads,
        arc_str_bytes,
    },
    physical_type_v2::{self, DecodedTypeTable, EncodedTypeTable, TypeCodecError},
    physical_value_origin_v2::{
        self as origins, ValueOriginCodecError, ValueOriginProjectionFacts,
        ValueOriginProjectionLimits,
    },
};
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::{alloc::Layout, fmt, mem::size_of};

#[derive(Clone, Copy)]
pub struct ValueSource<'source> {
    pub source: &'source p::ValueDef,
    pub value_type_id: u32,
}
#[derive(Clone, Copy, Debug)]
pub struct ValueProjectionLimits {
    pub max_definitions: usize,
    pub max_origin_references: usize,
    pub max_allocation_requests: usize,
    pub max_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_work: usize,
    pub origins: ValueOriginProjectionLimits,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValueProjectionFacts {
    pub definition_count: usize,
    pub origin_reference_count: usize,
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}
#[derive(Debug)]
pub enum ValueCodecError {
    Control(CompileControlError),
    Payload(ConnectorPayloadCodecError),
    Type(TypeCodecError),
    Origin(ValueOriginCodecError),
    Index(BindingCodecError),
    InvalidShape(&'static str),
}
impl From<CompileControlError> for ValueCodecError {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
macro_rules! lift {
    ($source:ty, $variant:ident, $control:path) => {
        impl From<$source> for ValueCodecError {
            fn from(e: $source) -> Self {
                match e {
                    $control(cause) => Self::Control(cause),
                    e => Self::$variant(e),
                }
            }
        }
    };
}
lift!(
    ConnectorPayloadCodecError,
    Payload,
    ConnectorPayloadCodecError::Control
);
lift!(TypeCodecError, Type, TypeCodecError::Control);
lift!(
    ValueOriginCodecError,
    Origin,
    ValueOriginCodecError::Control
);
lift!(BindingCodecError, Index, BindingCodecError::Control);
impl fmt::Display for ValueCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Payload(e) => e.fmt(f),
            Self::Type(e) => e.fmt(f),
            Self::Origin(e) => e.fmt(f),
            Self::Index(e) => e.fmt(f),
            Self::InvalidShape(s) => f.write_str(s),
        }
    }
}
impl std::error::Error for ValueCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(e) => Some(e),
            Self::Payload(e) => Some(e),
            Self::Type(e) => Some(e),
            Self::Origin(e) => Some(e),
            Self::Index(e) => Some(e),
            Self::InvalidShape(_) => None,
        }
    }
}
type Error = ValueCodecError;
fn invalid(s: &'static str) -> Error {
    Error::InvalidShape(s)
}
fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b)
        .ok_or_else(|| invalid("value resource sum overflow"))
}
fn mul(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_mul(b)
        .ok_or_else(|| invalid("value resource product overflow"))
}
fn bytes<T>(n: usize) -> Result<usize, Error> {
    Layout::array::<T>(n)
        .map(|layout| layout.size())
        .map_err(|_| invalid("value allocation layout is unrepresentable"))
}
fn cap(n: usize, max: usize, w: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    let admitted = n <= max;
    w.step()?;
    if admitted {
        Ok(())
    } else {
        Err(invalid("value projection envelope exceeded"))
    }
}
fn floor(source: usize, known: usize, w: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    let admitted = source >= known;
    w.step()?;
    if admitted {
        Ok(())
    } else {
        Err(invalid("value source invoice omits original backing"))
    }
}
fn finish<T>(result: Result<T, Error>, w: CompileCheckpoints<'_>) -> Result<T, Error> {
    if matches!(&result, Err(Error::Control(_))) {
        return result;
    }
    w.finish()?;
    result
}
fn reserve<T>(n: usize, w: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, Error> {
    bytes::<T>(n)?;
    w.flush()?;
    let mut out = Vec::new();
    let result = out.try_reserve_exact(n);
    reserve_exit::<Error>(result, w)?;
    Ok(out)
}
fn required(id: Option<u32>, w: &mut CompileCheckpoints<'_>) -> Result<u32, Error> {
    let value = id;
    w.step()?;
    value.ok_or_else(|| invalid("value type reference is absent"))
}
fn origin<'a>(
    def: &'a wire::ValueDefinition,
    w: &mut CompileCheckpoints<'_>,
) -> Result<&'a wire::ValueOrigin, Error> {
    let origin = def.origin.as_ref();
    w.step()?;
    origin.ok_or_else(|| invalid("value origin is absent"))
}
fn typed<'a>(
    types: &'a DecodedTypeTable,
    id: u32,
    w: &mut CompileCheckpoints<'_>,
) -> Result<&'a p::ValueType, Error> {
    // The actual sparse BTreeMap lookup remains opaque. Its conservative
    // source-count work bound is admitted before these original boundaries.
    w.flush()?;
    let value = types.value_type(id);
    w.flush()?;
    value.ok_or_else(|| invalid("value type ID is unknown"))
}

#[derive(Default)]
struct Model {
    references: usize,
    requests: usize,
    bytes: usize,
    dictionary_bytes: usize,
    work: usize,
}
impl Model {
    fn request(&mut self, n: usize) -> Result<(), Error> {
        if n != 0 {
            self.requests = add(self.requests, 1)?;
            self.bytes = add(self.bytes, n)?;
        }
        Ok(())
    }
    fn origin(&mut self, facts: ValueOriginProjectionFacts) -> Result<(), Error> {
        self.references = add(self.references, facts.reference_count)?;
        self.requests = add(self.requests, facts.allocation_requests_upper_bound)?;
        self.bytes = add(self.bytes, facts.allocation_request_bytes_upper_bound)?;
        // Numerical preflight and the sole observed projection each run once.
        // Only projection allocates, so requests are counted ONCE.
        self.work = add(self.work, mul(facts.cumulative_work_upper_bound, 2)?)?;
        Ok(())
    }
    fn clone_type(&mut self, facts: physical_type_v2::ValueTypeCloneFacts) -> Result<(), Error> {
        self.requests = add(self.requests, facts.allocation_requests_upper_bound())?;
        self.bytes = add(self.bytes, facts.allocation_request_bytes_upper_bound())?;
        self.dictionary_bytes = add(
            self.dictionary_bytes,
            facts.allocation_request_bytes_upper_bound(),
        )?;
        self.work = add(self.work, mul(facts.work_upper_bound(), 2)?)?;
        Ok(())
    }
    fn gate(
        &self,
        n: usize,
        source: usize,
        l: ValueProjectionLimits,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<ValueProjectionFacts, Error> {
        cap(self.references, l.max_origin_references, w)?;
        cap(self.requests, l.max_allocation_requests, w)?;
        cap(self.bytes, l.max_allocation_request_bytes, w)?;
        let coexist = add(source, self.bytes)?;
        cap(coexist, l.max_coexisting_source_and_request_bytes, w)?;
        let work = add(self.work, add(mul(self.bytes, 4)?, self.requests)?)?;
        cap(work, l.max_work, w)?;
        Ok(ValueProjectionFacts {
            definition_count: n,
            origin_reference_count: self.references,
            allocation_requests_upper_bound: self.requests,
            allocation_request_bytes_upper_bound: self.bytes,
            coexisting_source_and_request_bytes_upper_bound: coexist,
            cumulative_work_upper_bound: work,
        })
    }
}
fn own_work(n: usize, payloads: usize, types: usize) -> Result<usize, Error> {
    let height = (usize::BITS - n.leading_zeros()) as usize;
    // Original source/count loops, two value-root lookup passes, and the sole
    // index heapsort/dedup. Origin source scans and type comparison/clone work
    // are separately added from their real author facts, not inferred here.
    let per_value = add(128, add(mul(height + 1, 16)?, mul(types, 4)?)?)?;
    add(1024, add(mul(payloads, 16)?, mul(n, per_value)?)?)
}
fn individual_floor(
    value: &p::ValueDef,
    dictionary_bytes: usize,
    w: &mut CompileCheckpoints<'_>,
) -> Result<usize, Error> {
    let mut known = add(size_of::<p::ValueDef>(), dictionary_bytes)?;
    if let p::ValueOrigin::ProviderField { field, .. } = &value.origin {
        let header = field.column_payload.header();
        // Original buffer length is only a known floor. Its opaque capacity,
        // Field/metadata backing and all aliases remain in the host invoice.
        known = add(
            known,
            add(
                field.column_payload.payload().len(),
                add(
                    arc_str_bytes(header.provider_id().as_str().len())?,
                    arc_str_bytes(header.catalog().catalog_name().as_str().len())?,
                )?,
            )?,
        )?;
    }
    w.step()?;
    Ok(known)
}

pub struct EncodedValues<'loan, 'source, 'control> {
    inputs: &'loan [ValueSource<'source>],
    payloads: &'loan EncodedConnectorPayloads<'source, 'control>,
    types: &'loan EncodedTypeTable<'source>,
    wire: Vec<wire::ValueDefinition>,
    indices: BindingIndex,
    facts: ValueProjectionFacts,
    original_source_bytes: usize,
}
impl<'loan, 'source, 'control> EncodedValues<'loan, 'source, 'control> {
    pub fn as_wire(&self) -> &[wire::ValueDefinition] {
        &self.wire
    }
    pub fn into_wire(self) -> Vec<wire::ValueDefinition> {
        self.wire
    }
    pub fn facts(&self) -> &ValueProjectionFacts {
        &self.facts
    }
    pub fn source_count(&self) -> usize {
        self.inputs.len()
    }
    pub fn payloads(&self) -> &EncodedConnectorPayloads<'source, 'control> {
        self.payloads
    }
    pub fn types(&self) -> &EncodedTypeTable<'source> {
        self.types
    }
    pub(crate) fn original_control(&self) -> &'control dyn PureCompileControl {
        self.payloads.original_control()
    }
    pub fn value(&self, id: u32) -> Result<Option<&'source p::ValueDef>, Error> {
        let mut w = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.value_observed(id, &mut w);
        finish(result, w)
    }
    pub(crate) fn value_observed(
        &self,
        id: u32,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source p::ValueDef>, Error> {
        Ok(self
            .indices
            .find(id, |at| self.inputs[at].source.id.get(), w)?
            .map(|at| self.inputs[at].source))
    }
    pub fn source_id(&self, value: &p::ValueDef) -> Result<u32, Error> {
        let mut w = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.source_id_observed(value, &mut w);
        finish(result, w)
    }
    pub(crate) fn source_id_observed(
        &self,
        value: &p::ValueDef,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, Error> {
        let mut found = None;
        for input in self.inputs {
            let same = std::ptr::eq(input.source, value);
            w.step()?;
            if same {
                if found.is_some() {
                    return Err(invalid("value source association is ambiguous"));
                }
                found = Some(value.id.get());
            }
        }
        found.ok_or_else(|| invalid("value source owner is not in this namespace"))
    }
    pub fn retained_invoice_floor(&self) -> Result<usize, Error> {
        let mut w = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.retained_floor_observed(&mut w);
        finish(result, w)
    }
    pub(crate) fn retained_floor_observed(
        &self,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        let known = add(
            self.original_source_bytes,
            add(
                size_of::<Self>(),
                add(
                    self.indices.backing_bytes()?,
                    bytes::<wire::ValueDefinition>(self.wire.capacity())?,
                )?,
            )?,
        )?;
        w.step()?;
        // All origin DTO variants are inline; they own no string/Vec payload.
        Ok(known)
    }
}
pub struct DecodedValues<'loan, 'wire, 'control> {
    wire: &'wire [wire::ValueDefinition],
    payloads: &'loan DecodedConnectorPayloads<'wire, 'control>,
    types: &'loan DecodedTypeTable,
    values: Vec<p::ValueDef>,
    indices: BindingIndex,
    facts: ValueProjectionFacts,
    original_source_bytes: usize,
    owned_dictionary_bytes: usize,
}
impl<'loan, 'wire, 'control> DecodedValues<'loan, 'wire, 'control> {
    pub fn as_wire(&self) -> &'wire [wire::ValueDefinition] {
        self.wire
    }
    pub fn facts(&self) -> &ValueProjectionFacts {
        &self.facts
    }
    pub fn source_count(&self) -> usize {
        self.wire.len()
    }
    pub fn payloads(&self) -> &DecodedConnectorPayloads<'wire, 'control> {
        self.payloads
    }
    pub fn types(&self) -> &DecodedTypeTable {
        self.types
    }
    pub(crate) fn original_control(&self) -> &'control dyn PureCompileControl {
        self.payloads.original_control()
    }
    pub fn value(&self, id: u32) -> Result<Option<&p::ValueDef>, Error> {
        let mut w = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Decode)?;
        let result = self.value_observed(id, &mut w);
        finish(result, w)
    }
    pub(crate) fn value_observed(
        &self,
        id: u32,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&p::ValueDef>, Error> {
        Ok(self
            .indices
            .find(id, |at| self.wire[at].id, w)?
            .map(|at| &self.values[at]))
    }
    pub fn into_values(self) -> Vec<p::ValueDef> {
        self.values
    }
    pub fn retained_invoice_floor(&self) -> Result<usize, Error> {
        let mut w = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Decode)?;
        let result = self.retained_floor_observed(&mut w);
        finish(result, w)
    }
    pub(crate) fn retained_floor_observed(
        &self,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        let known = add(
            self.original_source_bytes,
            add(
                size_of::<Self>(),
                add(
                    self.indices.backing_bytes()?,
                    bytes::<p::ValueDef>(self.values.capacity())?,
                )?,
            )?,
        )?;
        let known = add(known, self.owned_dictionary_bytes)?;
        w.step()?;
        // Shared payload/Field/metadata backing stays in the original invoice;
        // only independently owned output Dictionary boxes are added here.
        Ok(known)
    }
}

pub fn encode_values<'loan, 'source, 'control>(
    inputs: &'loan [ValueSource<'source>],
    payloads: &'loan EncodedConnectorPayloads<'source, 'control>,
    types: &'loan EncodedTypeTable<'source>,
    source: usize,
    limits: ValueProjectionLimits,
) -> Result<EncodedValues<'loan, 'source, 'control>, Error> {
    let mut w = CompileCheckpoints::try_new(payloads.original_control(), CompilePhase::Encode)?;
    let result = encode_core(inputs, payloads, types, source, limits, &mut w);
    finish(result, w)
}
fn encode_core<'loan, 'source, 'control>(
    inputs: &'loan [ValueSource<'source>],
    payloads: &'loan EncodedConnectorPayloads<'source, 'control>,
    types: &'loan EncodedTypeTable<'source>,
    source: usize,
    l: ValueProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<EncodedValues<'loan, 'source, 'control>, Error> {
    cap(inputs.len(), l.max_definitions, w)?;
    let root = bytes::<ValueSource<'_>>(inputs.len())?;
    let mut model = Model::default();
    model.request(bytes::<usize>(inputs.len())?)?;
    model.request(bytes::<wire::ValueDefinition>(inputs.len())?)?;
    model.work = own_work(
        inputs.len(),
        payloads.source_count(),
        types.source_counts().0,
    )?;
    cap(model.work, l.max_work, w)?;
    floor(source, root, w)?;
    floor(source, payloads.retained_floor_observed(w)?, w)?;
    let mut individual = 0usize;
    for input in inputs {
        let original = input.source;
        let source_clone = physical_type_v2::preflight_value_type_clone(&original.ty, w)?;
        individual = individual.max(individual_floor(
            original,
            source_clone.allocation_request_bytes_upper_bound(),
            w,
        )?);
        model.work = add(model.work, source_clone.work_upper_bound())?;
        floor(source, add(root, individual)?, w)?;
        let bound = types
            .value_type_observed(input.value_type_id, w)?
            .ok_or_else(|| invalid("value type ID is unknown"))?;
        let checked = verify_type_binding(
            &original.ty,
            bound,
            source,
            l.max_work
                .checked_sub(model.work)
                .ok_or_else(|| invalid("value work envelope exhausted"))?,
            w,
        )?;
        model.work = add(model.work, checked.work_upper_bound())?;
        if !checked.matches() {
            return Err(invalid("value full source type differs"));
        }
        model.origin(origins::preflight_encode_observed(
            &original.origin,
            payloads,
            source,
            l.origins,
            w,
        )?)?;
        cap(model.work, l.max_work, w)?;
        w.step()?;
    }
    let facts = model.gate(inputs.len(), source, l, w)?;
    let indices = BindingIndex::prepare(inputs.len(), |at| inputs[at].source.id.get(), w)?;
    let mut output = reserve(inputs.len(), w)?;
    for input in inputs {
        let (origin, _) =
            origins::encode_observed(&input.source.origin, payloads, source, l.origins, w)?;
        output.push(wire::ValueDefinition {
            id: input.source.id.get(),
            value_type_id: Some(input.value_type_id),
            origin: Some(origin),
        });
        w.step()?;
    }
    Ok(EncodedValues {
        inputs,
        payloads,
        types,
        wire: output,
        indices,
        facts,
        original_source_bytes: source,
    })
}
pub fn decode_values<'loan, 'wire, 'control>(
    defs: &'wire [wire::ValueDefinition],
    payloads: &'loan DecodedConnectorPayloads<'wire, 'control>,
    types: &'loan DecodedTypeTable,
    source: usize,
    limits: ValueProjectionLimits,
) -> Result<DecodedValues<'loan, 'wire, 'control>, Error> {
    let mut w = CompileCheckpoints::try_new(payloads.original_control(), CompilePhase::Decode)?;
    let result = decode_core(defs, payloads, types, source, limits, &mut w);
    finish(result, w)
}
fn decode_core<'loan, 'wire, 'control>(
    defs: &'wire [wire::ValueDefinition],
    payloads: &'loan DecodedConnectorPayloads<'wire, 'control>,
    types: &'loan DecodedTypeTable,
    source: usize,
    l: ValueProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<DecodedValues<'loan, 'wire, 'control>, Error> {
    cap(defs.len(), l.max_definitions, w)?;
    let mut model = Model::default();
    model.request(bytes::<usize>(defs.len())?)?;
    model.request(bytes::<p::ValueDef>(defs.len())?)?;
    model.work = own_work(
        defs.len(),
        payloads.source_count(),
        types.value_types().len(),
    )?;
    cap(model.work, l.max_work, w)?;
    floor(source, bytes::<wire::ValueDefinition>(defs.len())?, w)?;
    floor(source, payloads.retained_floor_observed(w)?, w)?;
    for def in defs {
        let value = typed(types, required(def.value_type_id, w)?, w)?;
        let clone = physical_type_v2::preflight_value_type_clone(value, w)?;
        model.clone_type(clone)?;
        model.origin(origins::preflight_decode_observed(
            origin(def, w)?,
            payloads,
            source,
            l.origins,
            w,
        )?)?;
        cap(model.work, l.max_work, w)?;
        w.step()?;
    }
    let facts = model.gate(defs.len(), source, l, w)?;
    let indices = BindingIndex::prepare(defs.len(), |at| defs[at].id, w)?;
    let mut output = reserve(defs.len(), w)?;
    for def in defs {
        let ty = physical_type_v2::clone_value_type_observed(
            typed(types, required(def.value_type_id, w)?, w)?,
            w,
        )?;
        let (origin, _) =
            origins::decode_observed(origin(def, w)?, payloads, source, l.origins, w)?;
        output.push(p::ValueDef {
            id: p::ValueId::new(def.id),
            ty,
            origin,
        });
        w.step()?;
    }
    Ok(DecodedValues {
        wire: defs,
        payloads,
        types,
        values: output,
        indices,
        facts,
        original_source_bytes: source,
        owned_dictionary_bytes: model.dictionary_bytes,
    })
}
#[cfg(test)]
mod tests;
