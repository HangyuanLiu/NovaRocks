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

//! Complete writer schema/target-field representation through the original
//! checked type roots. IDs and writer semantics remain Fragment-owned. Types
//! do not retain control provenance; the caller supplies its original control.

use crate::{
    borrowed_type_resources::{preflight_type_binding, verify_type_binding},
    physical_node_v2::{self as resources, Model, NodeProjectionLimits},
    physical_properties_v2::PhysicalPropertyProjectionLimits,
    physical_type_v2::{self, DecodedTypeTable, EncodedTypeTable, TypeCodecError},
};
use novarocks_connector_contract::ConnectorWriteFieldToken;
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::{fmt, mem::size_of};

#[derive(Clone, Copy, Debug)]
pub struct WriterSchemaProjectionLimits {
    pub max_fields: usize,
    pub max_name_bytes: usize,
    pub max_type_references: usize,
    pub max_allocation_requests: usize,
    pub max_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_work: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriterSchemaProjectionFacts {
    pub field_count: usize,
    pub name_bytes: usize,
    pub type_reference_count: usize,
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}
#[derive(Debug)]
pub enum WriterSchemaCodecError {
    Control(CompileControlError),
    Type(TypeCodecError),
    Resources(resources::NodeCodecError),
    InvalidShape(&'static str),
}
impl From<CompileControlError> for WriterSchemaCodecError {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
impl From<TypeCodecError> for WriterSchemaCodecError {
    fn from(e: TypeCodecError) -> Self {
        match e {
            TypeCodecError::Control(c) => Self::Control(c),
            e => Self::Type(e),
        }
    }
}
impl From<resources::NodeCodecError> for WriterSchemaCodecError {
    fn from(e: resources::NodeCodecError) -> Self {
        match e {
            resources::NodeCodecError::Control(c) => Self::Control(c),
            e => Self::Resources(e),
        }
    }
}
impl fmt::Display for WriterSchemaCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Type(e) => e.fmt(f),
            Self::Resources(e) => e.fmt(f),
            Self::InvalidShape(s) => f.write_str(s),
        }
    }
}
impl std::error::Error for WriterSchemaCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(e) => Some(e),
            Self::Type(e) => Some(e),
            Self::Resources(e) => Some(e),
            Self::InvalidShape(_) => None,
        }
    }
}
type Error = WriterSchemaCodecError;
fn shape(s: &'static str) -> Error {
    Error::InvalidShape(s)
}
fn finish<T>(w: CompileCheckpoints<'_>, result: Result<T, Error>) -> Result<T, Error> {
    if matches!(&result, Err(Error::Control(_))) {
        return result;
    }
    w.finish()?;
    result
}
fn required(id: Option<u32>, w: &mut CompileCheckpoints<'_>) -> Result<u32, Error> {
    let result = id.ok_or_else(|| shape("writer field required reference is absent"));
    w.step()?;
    result
}
fn role(value: p::WriterRelationFieldRole) -> i32 {
    use wire::WriterRelationFieldRole as W;
    (match value {
        p::WriterRelationFieldRole::Kind => W::Kind,
        p::WriterRelationFieldRole::TargetOrdinal => W::TargetOrdinal,
        p::WriterRelationFieldRole::RowCount => W::RowCount,
        p::WriterRelationFieldRole::CommitFragment => W::CommitFragment,
        p::WriterRelationFieldRole::Auxiliary => W::Auxiliary,
    }) as i32
}
fn decode_role(value: i32) -> Result<p::WriterRelationFieldRole, Error> {
    use wire::WriterRelationFieldRole as W;
    match W::try_from(value) {
        Ok(W::Kind) => Ok(p::WriterRelationFieldRole::Kind),
        Ok(W::TargetOrdinal) => Ok(p::WriterRelationFieldRole::TargetOrdinal),
        Ok(W::RowCount) => Ok(p::WriterRelationFieldRole::RowCount),
        Ok(W::CommitFragment) => Ok(p::WriterRelationFieldRole::CommitFragment),
        Ok(W::Auxiliary) => Ok(p::WriterRelationFieldRole::Auxiliary),
        Ok(W::Unspecified) | Err(_) => Err(shape("unknown writer relation field role")),
    }
}
impl WriterSchemaProjectionLimits {
    // This private adapter borrows the shared request/work algebra; it does
    // not expose a second caller wallet. No properties or node inputs exist.
    fn resources(self) -> NodeProjectionLimits {
        NodeProjectionLimits {
            max_input_nodes: self.max_fields,
            max_value_references: self.max_type_references,
            max_list_items: self.max_fields,
            max_allocation_requests: self.max_allocation_requests,
            max_allocation_request_bytes: self.max_allocation_request_bytes,
            max_coexisting_source_and_request_bytes: self.max_coexisting_source_and_request_bytes,
            max_work: self.max_work,
            properties: PhysicalPropertyProjectionLimits {
                max_value_references: self.max_type_references,
                max_allocation_requests: self.max_allocation_requests,
                max_allocation_request_bytes: self.max_allocation_request_bytes,
                max_coexisting_source_and_request_bytes: self
                    .max_coexisting_source_and_request_bytes,
                max_work: self.max_work,
            },
        }
    }
}
struct Count {
    model: Model,
    names: usize,
}
impl Count {
    fn new(n: usize) -> Self {
        Self {
            model: Model {
                items: n,
                refs: n,
                ..Model::default()
            },
            names: 0,
        }
    }
    fn facts(
        &self,
        source: usize,
        types: usize,
        limits: WriterSchemaProjectionLimits,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<WriterSchemaProjectionFacts, Error> {
        resources::cap(self.names, limits.max_name_bytes, w)?;
        let f = self.model.facts(source, types, limits.resources(), w)?;
        Ok(WriterSchemaProjectionFacts {
            field_count: f.list_item_count,
            name_bytes: self.names,
            type_reference_count: f.value_reference_count,
            allocation_requests_upper_bound: f.allocation_requests_upper_bound,
            allocation_request_bytes_upper_bound: f.allocation_request_bytes_upper_bound,
            coexisting_source_and_request_bytes_upper_bound: f
                .coexisting_source_and_request_bytes_upper_bound,
            cumulative_work_upper_bound: f.cumulative_work_upper_bound,
        })
    }
    fn remaining(
        &self,
        source: usize,
        types: usize,
        limits: WriterSchemaProjectionLimits,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        let f = self.facts(source, types, limits, w)?;
        limits
            .max_work
            .checked_sub(f.cumulative_work_upper_bound)
            .ok_or_else(|| shape("writer type has no remaining work envelope"))
    }
}
#[derive(Clone, Copy)]
enum Source<'a> {
    Schema(&'a p::WriterRelationSchema),
    Targets(&'a [p::WriterTargetField]),
}
impl<'a> Source<'a> {
    fn len(self) -> usize {
        match self {
            Self::Schema(s) => s.fields.len(),
            Self::Targets(s) => s.len(),
        }
    }
    fn ty(self, i: usize) -> &'a FunctionValueType {
        match self {
            Self::Schema(s) => &s.fields[i].ty,
            Self::Targets(s) => &s[i].ty,
        }
    }
    fn name(self, i: usize) -> &'a str {
        match self {
            Self::Schema(s) => &s.fields[i].name,
            Self::Targets(s) => &s[i].provider_name,
        }
    }
    fn floor(self) -> Result<usize, Error> {
        Ok(match self {
            Self::Schema(s) => resources::add(
                size_of::<p::WriterRelationSchema>(),
                resources::bytes::<p::WriterRelationField>(s.fields.len())?,
            )?,
            Self::Targets(s) => resources::add(
                size_of::<&[p::WriterTargetField]>(),
                resources::bytes::<p::WriterTargetField>(s.len())?,
            )?,
        })
    }
}
#[derive(Clone, Copy)]
enum Raw<'a> {
    Schema(&'a wire::WriterRelationSchema),
    Targets(&'a Vec<wire::WriterTargetField>),
}
impl<'a> Raw<'a> {
    fn len(self) -> usize {
        match self {
            Self::Schema(s) => s.fields.len(),
            Self::Targets(s) => s.len(),
        }
    }
    fn name(self, i: usize) -> &'a String {
        match self {
            Self::Schema(s) => &s.fields[i].name,
            Self::Targets(s) => &s[i].provider_name,
        }
    }
    fn type_id(self, i: usize) -> Option<u32> {
        match self {
            Self::Schema(s) => s.fields[i].value_type_id,
            Self::Targets(s) => s[i].value_type_id,
        }
    }
    fn floor(self) -> Result<usize, Error> {
        Ok(match self {
            Self::Schema(s) => resources::add(
                size_of::<wire::WriterRelationSchema>(),
                resources::bytes::<wire::WriterRelationField>(s.fields.capacity())?,
            )?,
            Self::Targets(s) => resources::add(
                size_of::<Vec<wire::WriterTargetField>>(),
                resources::bytes::<wire::WriterTargetField>(s.capacity())?,
            )?,
        })
    }
}
fn typed<'a>(
    types: &'a DecodedTypeTable,
    id: u32,
    w: &mut CompileCheckpoints<'_>,
) -> Result<&'a FunctionValueType, Error> {
    w.flush()?;
    let result = types.value_type(id);
    w.flush()?;
    w.step()?;
    result.ok_or_else(|| shape("writer field type reference is unknown"))
}
fn preflight_encode(
    source: Source<'_>,
    ids: &[u32],
    types: &EncodedTypeTable<'_>,
    invoice: usize,
    l: WriterSchemaProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<WriterSchemaProjectionFacts, Error> {
    let n = source.len();
    let roots = types.source_counts().0;
    let mut c = Count::new(n);
    // Original linear root lookups run once here; emitted IDs need no lookup.
    c.model.delegated_work = resources::mul(n, resources::add(roots, 8)?)?;
    c.facts(invoice, roots, l, w)?;
    let mut known = resources::add(source.floor()?, resources::bytes::<u32>(ids.len())?)?;
    resources::floor(
        invoice,
        resources::add(known, resources::bytes::<FunctionValueType>(roots)?)?,
        w,
    )?;
    let valid = ids.len() == n;
    w.step()?;
    if !valid {
        return Err(shape(
            "writer field type ID count differs from original fields",
        ));
    }
    match source {
        Source::Schema(_) => c.model.request::<wire::WriterRelationField>(n, 1)?,
        Source::Targets(_) => c.model.request::<wire::WriterTargetField>(n, 1)?,
    }
    for i in 0..n {
        let bytes = source.name(i).len();
        c.names = resources::add(c.names, bytes)?;
        known = resources::add(known, bytes)?;
        c.model.request::<u8>(bytes, 1)?;
        if let Source::Targets(_) = source {
            c.model.request::<u8>(32, 1)?;
        }
        w.step()?;
        resources::floor(invoice, known, w)?;
        c.facts(invoice, roots, l, w)?;
    }
    for (i, id) in ids.iter().enumerate() {
        let ty = types
            .value_type_observed(*id, w)?
            .ok_or_else(|| shape("writer field type reference is unknown"))?;
        let bound = verify_type_binding(
            source.ty(i),
            ty,
            invoice,
            c.remaining(invoice, roots, l, w)?,
            w,
        )?;
        c.model.delegated_work = resources::add(c.model.delegated_work, bound.work_upper_bound())?;
        w.step()?;
        if !bound.matches() {
            return Err(shape(
                "writer field full type differs from authored type root",
            ));
        }
        // Root-owned Dictionary Boxes are retained even when encoding; reuse
        // the sole clone topology for their necessary source backing floor.
        let clone = physical_type_v2::preflight_value_type_clone(source.ty(i), w)?;
        known = resources::add(known, clone.allocation_request_bytes_upper_bound())?;
        c.model.delegated_work = resources::add(c.model.delegated_work, clone.work_upper_bound())?;
        resources::floor(invoice, known, w)?;
        c.facts(invoice, roots, l, w)?;
    }
    c.facts(invoice, roots, l, w)
}
fn preflight_decode(
    raw: Raw<'_>,
    types: &DecodedTypeTable,
    invoice: usize,
    l: WriterSchemaProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<WriterSchemaProjectionFacts, Error> {
    let n = raw.len();
    let roots = types.value_types().len();
    let mut c = Count::new(n);
    let lookups = crate::btree_resources_v2::lookup_work(roots).map_err(shape)?;
    c.model.delegated_work = resources::mul(n, resources::mul(lookups, 2)?)?;
    c.facts(invoice, roots, l, w)?;
    let mut known = raw.floor()?;
    resources::floor(
        invoice,
        resources::add(known, resources::bytes::<FunctionValueType>(roots)?)?,
        w,
    )?;
    match raw {
        Raw::Schema(_) => c.model.request::<p::WriterRelationField>(n, 2)?,
        Raw::Targets(_) => c.model.request::<p::WriterTargetField>(n, 2)?,
    }
    for i in 0..n {
        let name = raw.name(i);
        c.names = resources::add(c.names, name.len())?;
        known = resources::add(known, name.capacity())?;
        c.model.request::<u8>(name.len(), 2)?;
        match raw {
            Raw::Schema(s) => {
                required(s.fields[i].value_id, w)?;
                let result = decode_role(s.fields[i].role);
                w.step()?;
                result?;
            }
            Raw::Targets(s) => {
                required(s[i].input_value_id, w)?;
                known = resources::add(known, s[i].token.capacity())?;
                let valid = s[i].token.len() == 32;
                w.step()?;
                if !valid {
                    return Err(shape("writer field token is not exactly 32 bytes"));
                }
            }
        }
        required(raw.type_id(i), w)?;
        w.step()?;
        resources::floor(invoice, known, w)?;
        c.facts(invoice, roots, l, w)?;
    }
    for i in 0..n {
        let ty = typed(types, required(raw.type_id(i), w)?, w)?;
        // Borrowed fixed-scratch admission supplies an existing full-type
        // numerical bound before the sole owned-Dictionary clone topology.
        let bound = preflight_type_binding(ty, ty, invoice, c.remaining(invoice, roots, l, w)?, w)?;
        c.model.delegated_work = resources::add(c.model.delegated_work, bound.work_upper_bound())?;
        let clone = physical_type_v2::preflight_value_type_clone(ty, w)?;
        c.model.requests =
            resources::add(c.model.requests, clone.allocation_requests_upper_bound())?;
        c.model.requested = resources::add(
            c.model.requested,
            clone.allocation_request_bytes_upper_bound(),
        )?;
        c.model.delegated_work = resources::add(
            c.model.delegated_work,
            resources::mul(clone.work_upper_bound(), 2)?,
        )?;
        w.step()?;
        c.facts(invoice, roots, l, w)?;
    }
    c.facts(invoice, roots, l, w)
}
fn text(input: &str, w: &mut CompileCheckpoints<'_>) -> Result<String, Error> {
    let mut bytes = resources::reserve::<u8>(input.len(), w)?;
    for byte in input.as_bytes() {
        bytes.push(*byte);
        w.step()?;
    }
    w.flush()?;
    let result = String::from_utf8(bytes);
    w.flush()?;
    result.map_err(|_| shape("copied writer field name is invalid UTF8"))
}
fn name(input: &str, w: &mut CompileCheckpoints<'_>) -> Result<Box<str>, Error> {
    let text = text(input, w)?;
    w.flush()?;
    let output = text.into_boxed_str();
    w.flush()?;
    Ok(output)
}
fn emit_schema(
    source: &p::WriterRelationSchema,
    ids: &[u32],
    w: &mut CompileCheckpoints<'_>,
) -> Result<wire::WriterRelationSchema, Error> {
    let mut fields = resources::reserve(source.fields.len(), w)?;
    for (f, id) in source.fields.iter().zip(ids) {
        fields.push(wire::WriterRelationField {
            value_id: Some(f.value.get()),
            name: text(&f.name, w)?,
            value_type_id: Some(*id),
            role: role(f.role),
        });
        w.step()?;
    }
    Ok(wire::WriterRelationSchema {
        revision: source.revision,
        fields,
    })
}
fn emit_targets(
    source: &[p::WriterTargetField],
    ids: &[u32],
    w: &mut CompileCheckpoints<'_>,
) -> Result<Vec<wire::WriterTargetField>, Error> {
    let mut fields = resources::reserve(source.len(), w)?;
    for (f, id) in source.iter().zip(ids) {
        let mut token = resources::reserve(32, w)?;
        for byte in f.token.to_bytes() {
            token.push(byte);
            w.step()?;
        }
        fields.push(wire::WriterTargetField {
            token,
            provider_name: text(&f.provider_name, w)?,
            input_value_id: Some(f.input.get()),
            value_type_id: Some(*id),
            hidden: f.hidden,
        });
        w.step()?;
    }
    Ok(fields)
}
fn read_schema(
    raw: &wire::WriterRelationSchema,
    types: &DecodedTypeTable,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::WriterRelationSchema, Error> {
    let mut fields = resources::reserve(raw.fields.len(), w)?;
    for f in &raw.fields {
        fields.push(p::WriterRelationField {
            value: p::ValueId::new(required(f.value_id, w)?),
            name: name(&f.name, w)?,
            ty: physical_type_v2::clone_value_type_observed(
                typed(types, required(f.value_type_id, w)?, w)?,
                w,
            )?,
            role: decode_role(f.role)?,
        });
        w.step()?;
    }
    Ok(p::WriterRelationSchema {
        revision: raw.revision,
        fields: resources::boxed(fields, w)?,
    })
}
fn read_targets(
    raw: &[wire::WriterTargetField],
    types: &DecodedTypeTable,
    w: &mut CompileCheckpoints<'_>,
) -> Result<Box<[p::WriterTargetField]>, Error> {
    let mut fields = resources::reserve(raw.len(), w)?;
    for f in raw {
        let token: [u8; 32] = f
            .token
            .as_slice()
            .try_into()
            .map_err(|_| shape("prepared writer field token is not 32 bytes"))?;
        w.step()?;
        fields.push(p::WriterTargetField {
            token: ConnectorWriteFieldToken::from_bytes(token),
            provider_name: name(&f.provider_name, w)?,
            input: p::ValueId::new(required(f.input_value_id, w)?),
            ty: physical_type_v2::clone_value_type_observed(
                typed(types, required(f.value_type_id, w)?, w)?,
                w,
            )?,
            hidden: f.hidden,
        });
        w.step()?;
    }
    Ok(resources::boxed(fields, w)?)
}

macro_rules! encoding_api {
    ($token:ident,$prepare:ident,$encode:ident,$input:ty,$output:ty,$variant:ident,$emit:ident) => {
        pub struct $token<'input, 'loan, 'source, 'control> {
            input: &'input $input,
            type_ids: &'input [u32],
            types: &'loan EncodedTypeTable<'source>,
            control: &'control dyn PureCompileControl,
            facts: WriterSchemaProjectionFacts,
        }
        impl $token<'_, '_, '_, '_> {
            pub fn facts(&self) -> &WriterSchemaProjectionFacts {
                &self.facts
            }
            pub fn emit(self) -> Result<($output, WriterSchemaProjectionFacts), Error> {
                let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Encode)?;
                let result =
                    $emit(self.input, self.type_ids, &mut work).map(|output| (output, self.facts));
                finish(work, result)
            }
            pub fn types(&self) -> &EncodedTypeTable<'_> {
                self.types
            }
        }
        pub fn $prepare<'input, 'loan, 'source, 'control>(
            input: &'input $input,
            type_ids: &'input [u32],
            types: &'loan EncodedTypeTable<'source>,
            source_retained_bytes: usize,
            limits: WriterSchemaProjectionLimits,
            control: &'control dyn PureCompileControl,
        ) -> Result<$token<'input, 'loan, 'source, 'control>, Error> {
            let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
            let result = preflight_encode(
                Source::$variant(input),
                type_ids,
                types,
                source_retained_bytes,
                limits,
                &mut work,
            );
            let facts = finish(work, result)?;
            Ok($token {
                input,
                type_ids,
                types,
                control,
                facts,
            })
        }
        pub fn $encode(
            input: &$input,
            type_ids: &[u32],
            types: &EncodedTypeTable<'_>,
            source_retained_bytes: usize,
            limits: WriterSchemaProjectionLimits,
            control: &dyn PureCompileControl,
        ) -> Result<($output, WriterSchemaProjectionFacts), Error> {
            let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
            let result = (|| {
                let facts = preflight_encode(
                    Source::$variant(input),
                    type_ids,
                    types,
                    source_retained_bytes,
                    limits,
                    &mut work,
                )?;
                let output = $emit(input, type_ids, &mut work)?;
                Ok((output, facts))
            })();
            finish(work, result)
        }
    };
}
encoding_api!(
    PreparedWriterSchemaEncode,
    prepare_writer_schema_encode,
    encode_writer_schema,
    p::WriterRelationSchema,
    wire::WriterRelationSchema,
    Schema,
    emit_schema
);
encoding_api!(
    PreparedWriterTargetFieldsEncode,
    prepare_writer_target_fields_encode,
    encode_writer_target_fields,
    [p::WriterTargetField],
    Vec<wire::WriterTargetField>,
    Targets,
    emit_targets
);

macro_rules! decoding_api {
    ($token:ident,$prepare:ident,$decode:ident,$input:ty,$output:ty,$variant:ident,$emit:ident) => {
        pub struct $token<'input, 'types, 'control> {
            input: &'input $input,
            types: &'types DecodedTypeTable,
            control: &'control dyn PureCompileControl,
            facts: WriterSchemaProjectionFacts,
        }
        impl $token<'_, '_, '_> {
            pub fn facts(&self) -> &WriterSchemaProjectionFacts {
                &self.facts
            }
            pub fn types(&self) -> &DecodedTypeTable {
                self.types
            }
            pub fn emit(self) -> Result<($output, WriterSchemaProjectionFacts), Error> {
                let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Decode)?;
                let result =
                    $emit(self.input, self.types, &mut work).map(|output| (output, self.facts));
                finish(work, result)
            }
        }
        pub fn $prepare<'input, 'types, 'control>(
            input: &'input $input,
            types: &'types DecodedTypeTable,
            source_retained_bytes: usize,
            limits: WriterSchemaProjectionLimits,
            control: &'control dyn PureCompileControl,
        ) -> Result<$token<'input, 'types, 'control>, Error> {
            let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
            let result = preflight_decode(
                Raw::$variant(input),
                types,
                source_retained_bytes,
                limits,
                &mut work,
            );
            let facts = finish(work, result)?;
            Ok($token {
                input,
                types,
                control,
                facts,
            })
        }
        pub fn $decode(
            input: &$input,
            types: &DecodedTypeTable,
            source_retained_bytes: usize,
            limits: WriterSchemaProjectionLimits,
            control: &dyn PureCompileControl,
        ) -> Result<($output, WriterSchemaProjectionFacts), Error> {
            let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
            let result = (|| {
                let facts = preflight_decode(
                    Raw::$variant(input),
                    types,
                    source_retained_bytes,
                    limits,
                    &mut work,
                )?;
                let output = $emit(input, types, &mut work)?;
                Ok((output, facts))
            })();
            finish(work, result)
        }
    };
}
decoding_api!(
    PreparedWriterSchemaDecode,
    prepare_writer_schema_decode,
    decode_writer_schema,
    wire::WriterRelationSchema,
    p::WriterRelationSchema,
    Schema,
    read_schema
);
decoding_api!(
    PreparedWriterTargetFieldsDecode,
    prepare_writer_target_fields_decode,
    decode_writer_target_fields,
    Vec<wire::WriterTargetField>,
    Box<[p::WriterTargetField]>,
    Targets,
    read_targets
);

#[cfg(test)]
#[path = "physical_writer_schema_v2/tests.rs"]
mod tests;
