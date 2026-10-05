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

//! Complete Arrow Schema namespace representation. Field/type semantics and
//! Connector public read facts remain with their original owners. Numerical
//! request projections are not grants for opaque std/Arrow allocations.

use crate::{
    allocation_exit_v2::reserve_exit,
    arrow_metadata_v2::{copy_string, encode_metadata, ordered_key},
    binding_index_v2::{BindingIndex, prepare_work_upper_bound},
    hashmap_resources_v2::{self as maps, HashMapResourceError},
    physical_binding_v2::BindingCodecError,
    physical_node_v2::{
        self as resources, Model, NodeCodecError, NodeProjectionFacts, NodeProjectionLimits,
    },
    physical_type_v2::{DecodedTypeTable, EncodedTypeTable, TypeCodecError},
};
use arrow::datatypes::{Field, Fields, Schema};
use novarocks_proto_models::{physical_package_v2 as wire, plan::ArrowFieldMetadataEntry};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::{alloc::Layout, collections::HashMap, fmt, mem::size_of, sync::Arc};

#[derive(Debug)]
pub enum SchemaCodecError {
    Control(CompileControlError),
    Type(TypeCodecError),
    Binding(BindingCodecError),
    Node(NodeCodecError),
    SourceModel(&'static str),
    InvalidShape(&'static str),
}
impl fmt::Display for SchemaCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Type(e) => e.fmt(f),
            Self::Binding(e) => e.fmt(f),
            Self::Node(e) => e.fmt(f),
            Self::SourceModel(s) | Self::InvalidShape(s) => f.write_str(s),
        }
    }
}
impl std::error::Error for SchemaCodecError {}
impl From<CompileControlError> for SchemaCodecError {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
impl From<TypeCodecError> for SchemaCodecError {
    fn from(e: TypeCodecError) -> Self {
        match e {
            TypeCodecError::Control(c) => Self::Control(c),
            e => Self::Type(e),
        }
    }
}
impl From<BindingCodecError> for SchemaCodecError {
    fn from(e: BindingCodecError) -> Self {
        match e {
            BindingCodecError::Control(c) => Self::Control(c),
            e => Self::Binding(e),
        }
    }
}
impl From<NodeCodecError> for SchemaCodecError {
    fn from(e: NodeCodecError) -> Self {
        match e {
            NodeCodecError::Control(c) => Self::Control(c),
            e => Self::Node(e),
        }
    }
}
impl From<HashMapResourceError> for SchemaCodecError {
    fn from(e: HashMapResourceError) -> Self {
        match e {
            HashMapResourceError::Arithmetic(_) => CompileControlError::ResourceExhausted.into(),
            HashMapResourceError::SourceModel(s) => Self::SourceModel(s),
        }
    }
}
type Error = SchemaCodecError;
type OwnedSchemaDefinitions = Box<[(u32, Schema)]>;
fn invalid(s: &'static str) -> Error {
    Error::InvalidShape(s)
}
fn add(a: usize, b: usize) -> Result<usize, Error> {
    Ok(resources::add(a, b)?)
}
fn mul(a: usize, b: usize) -> Result<usize, Error> {
    Ok(resources::mul(a, b)?)
}
fn bytes<T>(n: usize) -> Result<usize, Error> {
    Ok(resources::bytes::<T>(n)?)
}
fn finish<T>(work: CompileCheckpoints<'_>, result: Result<T, Error>) -> Result<T, Error> {
    if matches!(&result, Err(Error::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn fields_layout(n: usize) -> Result<Layout, Error> {
    let payload =
        Layout::array::<Arc<Field>>(n).map_err(|_| CompileControlError::ResourceExhausted)?;
    // This sole port has checked Layout/arithmetic errors only, never a source
    // grammar diagnostic. Empty Fields still have a real Arc header request.
    crate::ipc_flat_stream_v2::reader_allocations::arc_layout(payload)
        .map_err(|_| CompileControlError::ResourceExhausted.into())
}
fn shape_equal(
    equal: bool,
    work: &mut CompileCheckpoints<'_>,
    message: &'static str,
) -> Result<(), Error> {
    work.step()?;
    if equal { Ok(()) } else { Err(invalid(message)) }
}
fn map_lower_floor(n: usize) -> Result<usize, Error> {
    // Occupied inline pairs only: no claim about deleted buckets/private CAP.
    bytes::<(String, String)>(n)
}
fn string_requests(model: &mut Model, key: &str, value: &str) -> Result<(), Error> {
    model.request::<u8>(key.len(), 1)?;
    model.request::<u8>(value.len(), 1)?;
    Ok(())
}
fn sorting_work(entries: usize, max_key: usize) -> Result<usize, Error> {
    mul(mul(entries, entries)?, add(mul(max_key, 2)?, 64)?)
}
#[derive(Clone, Copy)]
pub struct SchemaSource<'source> {
    pub id: u32,
    pub source: &'source Schema,
    pub field_ids: &'source [u32],
}

type Admission<'a> = dyn FnMut(&NodeProjectionFacts) -> Result<(), CompileControlError> + 'a;

// Parent admission shares the original numerical author and precedes every
// completed gate observation. It creates neither a budget nor a scope.
fn admit_model(
    model: &Model,
    source: usize,
    fields: usize,
    limits: NodeProjectionLimits,
    admit: &mut Admission<'_>,
) -> Result<NodeProjectionFacts, Error> {
    let facts = model.numerical_facts(source, fields, limits)?;
    admit(&facts)?;
    Ok(facts)
}
fn admitted_facts(
    model: &Model,
    source: usize,
    fields: usize,
    limits: NodeProjectionLimits,
    admit: &mut Admission<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<NodeProjectionFacts, Error> {
    admit_model(model, source, fields, limits, admit)?;
    Ok(model.facts(source, fields, limits, work)?)
}

fn encode_preflight(
    sources: &[SchemaSource<'_>],
    types: &EncodedTypeTable<'_>,
    source: usize,
    limits: NodeProjectionLimits,
    admit: &mut Admission<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<NodeProjectionFacts, Error> {
    let mut model = Model {
        items: sources.len(),
        ..Model::default()
    };
    model.request::<usize>(sources.len(), 1)?;
    model.request::<wire::SchemaDefinition>(sources.len(), 1)?;
    model.delegated_work = prepare_work_upper_bound(sources.len())?;
    admitted_facts(&model, source, types.source_counts().1, limits, admit, work)?;
    let source_slice = bytes::<SchemaSource<'_>>(sources.len())?;
    let types_floor = add(
        size_of::<EncodedTypeTable<'_>>(),
        bytes::<(u32, Arc<Field>)>(types.source_counts().1)?,
    )?;
    resources::floor(source, source_slice.max(types_floor), work)?;
    let mut max_schema_floor = 0;
    for entry in sources {
        let fields = entry.source.fields();
        let metadata = entry.source.metadata();
        model.refs = add(model.refs, entry.field_ids.len())?;
        model.items = add(model.items, add(entry.field_ids.len(), metadata.len())?)?;
        model.request::<u32>(entry.field_ids.len(), 1)?;
        model.request::<(&str, &str)>(metadata.len(), 1)?;
        model.request::<ArrowFieldMetadataEntry>(metadata.len(), 1)?;
        // The retained source invoice bounds raw control-tag iteration even
        // after removals. Admit it before creating/advancing the real iterator.
        let iteration = maps::source_iterator_work_upper_bound(source, metadata.len())?;
        model.delegated_work = add(model.delegated_work, mul(iteration, 2)?)?;
        model.delegated_work = add(
            model.delegated_work,
            mul(entry.field_ids.len(), add(types.source_counts().1, 1)?)?,
        )?;
        admitted_facts(&model, source, types.source_counts().1, limits, admit, work)?;
        shape_equal(
            fields.len() == entry.field_ids.len(),
            work,
            "Schema field occurrence count differs from original field IDs",
        )?;
        let mut known = add(
            size_of::<Schema>(),
            add(
                fields_layout(fields.len())?.size(),
                add(
                    bytes::<u32>(entry.field_ids.len())?,
                    map_lower_floor(metadata.len())?,
                )?,
            )?,
        )?;
        let mut copied = 0;
        let mut max_key = 0;
        let metadata_work_base = model.delegated_work;
        model.delegated_work = add(metadata_work_base, sorting_work(metadata.len(), 0)?)?;
        admitted_facts(&model, source, types.source_counts().1, limits, admit, work)?;
        work.flush()?;
        let mut iter = metadata.iter();
        work.step()?;
        work.flush()?;
        loop {
            work.flush()?;
            let next = iter.next();
            if let Some((key, value)) = next {
                known = add(known, add(key.capacity(), value.capacity())?)?;
                copied = add(copied, add(key.len(), value.len())?)?;
                max_key = max_key.max(key.len());
                string_requests(&mut model, key, value)?;
                model.delegated_work = add(
                    metadata_work_base,
                    add(sorting_work(metadata.len(), max_key)?, mul(copied, 8)?)?,
                )?;
                admit_model(&model, source, types.source_counts().1, limits, admit)?;
            }
            work.step()?;
            work.flush()?;
            let Some((_key, _value)) = next else { break };
            admitted_facts(&model, source, types.source_counts().1, limits, admit, work)?;
            work.step()?;
        }
        max_schema_floor = max_schema_floor.max(known);
        // Multiple entries may borrow the same Schema, Fields or field-ID
        // allocation. This is a necessary maximum lower floor, not a sum that
        // invents distinct ownership. The complete union is caller-invoiced.
        resources::floor(
            source,
            source_slice.max(types_floor).max(max_schema_floor),
            work,
        )?;
        admitted_facts(&model, source, types.source_counts().1, limits, admit, work)?;
        for (field, id) in fields.iter().zip(entry.field_ids) {
            let original = types.field_observed(*id, work)?;
            work.step()?;
            let same_source = original.is_some_and(|original| Arc::ptr_eq(field, original));
            shape_equal(
                same_source,
                work,
                "Schema field is not the original Field namespace source",
            )?;
        }
    }
    admitted_facts(&model, source, types.source_counts().1, limits, admit, work)
}
fn btree_lookup(n: usize) -> Result<usize, Error> {
    crate::btree_resources_v2::lookup_work_typed(n).map_err(|e| match e {
        crate::btree_resources_v2::BTreeResourceError::Arithmetic(_) => {
            CompileControlError::ResourceExhausted.into()
        }
        crate::btree_resources_v2::BTreeResourceError::SourceModel(s) => Error::SourceModel(s),
    })
}
fn decode_preflight(
    definitions: &[wire::SchemaDefinition],
    types: &DecodedTypeTable,
    source: usize,
    limits: NodeProjectionLimits,
    admit: &mut Admission<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<NodeProjectionFacts, Error> {
    let mut model = Model {
        items: definitions.len(),
        ..Model::default()
    };
    model.request::<usize>(definitions.len(), 1)?;
    model.request::<(u32, Schema)>(definitions.len(), 2)?;
    model.delegated_work = prepare_work_upper_bound(definitions.len())?;
    admitted_facts(&model, source, types.field_count(), limits, admit, work)?;
    let mut known = add(
        bytes::<wire::SchemaDefinition>(definitions.len())?,
        types.necessary_fields_retained_floor()?,
    )?;
    resources::floor(source, known, work)?;
    for entry in definitions {
        model.refs = add(model.refs, entry.field_ids.len())?;
        model.items = add(
            model.items,
            add(entry.field_ids.len(), entry.metadata.len())?,
        )?;
        model.request::<Arc<Field>>(entry.field_ids.len(), 1)?;
        model.layout_request(fields_layout(entry.field_ids.len())?, 1)?;
        let table = maps::fresh_table_layout::<String, String>(entry.metadata.len())?;
        if let Some(layout) = table.layout {
            model.layout_request(layout, 1)?;
        }
        known = add(
            known,
            add(
                bytes::<u32>(entry.field_ids.capacity())?,
                bytes::<ArrowFieldMetadataEntry>(entry.metadata.capacity())?,
            )?,
        )?;
        model.delegated_work = add(
            model.delegated_work,
            mul(
                mul(entry.field_ids.len(), btree_lookup(types.field_count())?)?,
                2,
            )?,
        )?;
        let metadata_work_base = model.delegated_work;
        model.delegated_work = add(
            metadata_work_base,
            add(
                maps::fresh_string_table_work_upper_bound(entry.metadata.len(), 0, 0)?,
                entry.metadata.len(),
            )?,
        )?;
        admitted_facts(&model, source, types.field_count(), limits, admit, work)?;
        let mut key_bytes = 0;
        let mut copied = 0;
        let mut max_key = 0;
        for metadata in &entry.metadata {
            known = add(
                known,
                add(metadata.key.capacity(), metadata.value.capacity())?,
            )?;
            string_requests(&mut model, &metadata.key, &metadata.value)?;
            key_bytes = add(key_bytes, metadata.key.len())?;
            copied = add(copied, add(metadata.key.len(), metadata.value.len())?)?;
            max_key = max_key.max(metadata.key.len());
            // Replace this entry's prefix contribution, never charge it twice.
            model.delegated_work = add(
                metadata_work_base,
                add(
                    maps::fresh_string_table_work_upper_bound(
                        entry.metadata.len(),
                        key_bytes,
                        max_key,
                    )?,
                    add(
                        mul(entry.metadata.len(), add(max_key, 1)?)?,
                        mul(copied, 8)?,
                    )?,
                )?,
            )?;
            admitted_facts(&model, source, types.field_count(), limits, admit, work)?;
            work.step()?;
        }
        resources::floor(source, known, work)?;
        admitted_facts(&model, source, types.field_count(), limits, admit, work)?;
        let mut previous = None;
        for metadata in &entry.metadata {
            let sorted = ordered_key(previous, &metadata.key, work)?;
            shape_equal(
                sorted,
                work,
                "Schema metadata keys are duplicated or not strictly ordered",
            )?;
            previous = Some(metadata.key.as_str());
        }
        for id in &entry.field_ids {
            let found = types.field_observed(*id, work)?.is_some();
            shape_equal(found, work, "Schema Field reference is unknown")?;
        }
    }
    admitted_facts(&model, source, types.field_count(), limits, admit, work)
}
fn encode_emit(
    sources: &[SchemaSource<'_>],
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<wire::SchemaDefinition>, Error> {
    let mut definitions = resources::reserve(sources.len(), work)?;
    for entry in sources {
        let mut fields = resources::reserve(entry.field_ids.len(), work)?;
        for id in entry.field_ids {
            fields.push(*id);
            work.step()?;
        }
        let metadata = encode_metadata(entry.source.metadata(), work)?;
        definitions.push(wire::SchemaDefinition {
            id: entry.id,
            field_ids: fields,
            metadata,
        });
        work.step()?;
    }
    Ok(definitions)
}
fn decode_emit(
    definitions: &[wire::SchemaDefinition],
    types: &DecodedTypeTable,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(OwnedSchemaDefinitions, usize), Error> {
    let mut output = resources::reserve(definitions.len(), work)?;
    let mut retained = bytes::<(u32, Schema)>(definitions.len())?;
    for entry in definitions {
        let mut fields = resources::reserve(entry.field_ids.len(), work)?;
        for id in &entry.field_ids {
            let original = types.field_observed(*id, work)?;
            work.step()?;
            let original =
                original.ok_or_else(|| invalid("prepared Schema Field reference changed"))?;
            work.flush()?;
            fields.push(Arc::clone(original));
            work.step()?;
            work.flush()?;
        }
        retained = add(retained, fields_layout(fields.len())?.size())?;
        retained = add(retained, map_lower_floor(entry.metadata.len())?)?;
        work.flush()?;
        let fields = Fields::from(fields);
        work.step()?;
        work.flush()?;
        work.flush()?;
        let mut metadata = HashMap::new();
        work.step()?;
        work.flush()?;
        let reserved = metadata.try_reserve(entry.metadata.len());
        if reserved.is_ok() {
            work.step()?;
        }
        reserve_exit::<Error>(reserved, work)?;
        for value in &entry.metadata {
            let key = copy_string(&value.key, work)?;
            let value = copy_string(&value.value, work)?;
            retained = add(retained, add(key.capacity(), value.capacity())?)?;
            work.step()?;
            work.flush()?;
            let old = metadata.insert(key, value);
            work.step()?;
            work.flush()?;
            shape_equal(
                old.is_none(),
                work,
                "prepared Schema metadata identity changed",
            )?;
        }
        let schema = Schema::new_with_metadata(fields, metadata);
        output.push((entry.id, schema));
        work.step()?;
    }
    Ok((resources::boxed(output, work)?, retained))
}
/// Retains the actual borrowed source schema/Field/control owners.
pub struct PreparedSchemasEncode<'loan, 'source, 'control> {
    sources: &'loan [SchemaSource<'source>],
    types: &'loan EncodedTypeTable<'source>,
    control: &'control dyn PureCompileControl,
    source_bytes: usize,
    index: BindingIndex,
    facts: NodeProjectionFacts,
}
pub struct EncodedSchemas<'loan, 'source, 'control> {
    definitions: Vec<wire::SchemaDefinition>,
    sources: &'loan [SchemaSource<'source>],
    types: &'loan EncodedTypeTable<'source>,
    control: &'control dyn PureCompileControl,
    source_bytes: usize,
    index: BindingIndex,
    facts: NodeProjectionFacts,
}
impl EncodedSchemas<'_, '_, '_> {
    pub fn as_wire(&self) -> &[wire::SchemaDefinition] {
        &self.definitions
    }
    pub fn facts(&self) -> &NodeProjectionFacts {
        &self.facts
    }
    pub fn original_control(&self) -> &dyn PureCompileControl {
        self.control
    }
    pub fn types(&self) -> &EncodedTypeTable<'_> {
        self.types
    }
    pub fn source_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&Schema>, Error> {
        shape_equal(
            std::ptr::eq(work.control(), self.control),
            work,
            "Schema lookup has a different original control",
        )?;
        let at = self.index.find(id, |at| self.sources[at].id, work)?;
        Ok(at.map(|at| self.sources[at].source))
    }
    pub fn into_wire(self) -> Vec<wire::SchemaDefinition> {
        self.definitions
    }
    pub fn retained_invoice_floor(&self) -> Result<usize, Error> {
        add(
            self.source_bytes,
            add(
                size_of::<Self>(),
                add(
                    self.index.backing_bytes()?,
                    bytes::<wire::SchemaDefinition>(self.definitions.capacity())?,
                )?,
            )?,
        )
    }
}
impl<'loan, 'source, 'control> PreparedSchemasEncode<'loan, 'source, 'control> {
    pub fn facts(&self) -> &NodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<EncodedSchemas<'loan, 'source, 'control>, Error> {
        let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Encode)?;
        let result = self.emit_observed_in(&mut |_| Ok(()), &mut work);
        finish(work, result)
    }
    /// Uses the caller's original scope. The caller owns the ordinary footer.
    pub fn emit_observed_in(
        self,
        admit: &mut Admission<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<EncodedSchemas<'loan, 'source, 'control>, Error> {
        if !std::ptr::eq(work.control(), self.control) {
            return Err(invalid("Schema emission has a different original control"));
        }
        admit(&self.facts)?;
        let definitions = encode_emit(self.sources, work)?;
        Ok(EncodedSchemas {
            definitions,
            sources: self.sources,
            types: self.types,
            control: self.control,
            source_bytes: self.source_bytes,
            index: self.index,
            facts: self.facts,
        })
    }
}
pub fn prepare_schemas_encode<'loan, 'source, 'control>(
    sources: &'loan [SchemaSource<'source>],
    types: &'loan EncodedTypeTable<'source>,
    source_retained_bytes: usize,
    limits: NodeProjectionLimits,
    control: &'control dyn PureCompileControl,
) -> Result<PreparedSchemasEncode<'loan, 'source, 'control>, Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = prepare_schemas_encode_observed_in(
        sources,
        types,
        source_retained_bytes,
        limits,
        &mut |_| Ok(()),
        &mut work,
    );
    finish(work, result)
}
/// Prepares the original schema body in an existing caller-owned scope. The
/// synchronous parent sees cumulative contributions, with source B once.
pub fn prepare_schemas_encode_observed_in<'loan, 'source, 'control>(
    sources: &'loan [SchemaSource<'source>],
    types: &'loan EncodedTypeTable<'source>,
    source_retained_bytes: usize,
    limits: NodeProjectionLimits,
    admit: &mut Admission<'_>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedSchemasEncode<'loan, 'source, 'control>, Error> {
    let facts = encode_preflight(sources, types, source_retained_bytes, limits, admit, work)?;
    let index = BindingIndex::prepare(sources.len(), |at| sources[at].id, work)?;
    Ok(PreparedSchemasEncode {
        sources,
        types,
        control: work.control(),
        source_bytes: source_retained_bytes,
        index,
        facts,
    })
}
pub struct PreparedSchemasDecode<'loan, 'control> {
    definitions: &'loan [wire::SchemaDefinition],
    types: &'loan DecodedTypeTable,
    control: &'control dyn PureCompileControl,
    source_bytes: usize,
    index: BindingIndex,
    facts: NodeProjectionFacts,
}
pub struct DecodedSchemas<'loan, 'control> {
    definitions: OwnedSchemaDefinitions,
    retained_bytes: usize,
    types: &'loan DecodedTypeTable,
    control: &'control dyn PureCompileControl,
    source_bytes: usize,
    index: BindingIndex,
    facts: NodeProjectionFacts,
}
impl DecodedSchemas<'_, '_> {
    pub fn definitions(&self) -> &[(u32, Schema)] {
        &self.definitions
    }
    pub fn facts(&self) -> &NodeProjectionFacts {
        &self.facts
    }
    pub fn original_control(&self) -> &dyn PureCompileControl {
        self.control
    }
    pub fn types(&self) -> &DecodedTypeTable {
        self.types
    }
    pub fn schema_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&Schema>, Error> {
        shape_equal(
            std::ptr::eq(work.control(), self.control),
            work,
            "Schema lookup has a different original control",
        )?;
        let at = self.index.find(id, |at| self.definitions[at].0, work)?;
        Ok(at.map(|at| &self.definitions[at].1))
    }
    pub fn retained_output_floor(&self) -> Result<usize, Error> {
        add(
            size_of::<Self>(),
            add(self.index.backing_bytes()?, self.retained_bytes)?,
        )
    }
    pub fn retained_invoice_floor(&self) -> Result<usize, Error> {
        add(self.source_bytes, self.retained_output_floor()?)
    }
    pub fn into_definitions(self) -> OwnedSchemaDefinitions {
        self.definitions
    }
}
impl<'loan, 'control> PreparedSchemasDecode<'loan, 'control> {
    pub fn facts(&self) -> &NodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<DecodedSchemas<'loan, 'control>, Error> {
        let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Decode)?;
        let result = self.emit_observed_in(&mut |_| Ok(()), &mut work);
        finish(work, result)
    }
    /// Materializes in the same caller scope; no entry, reset or footer.
    pub fn emit_observed_in(
        self,
        admit: &mut Admission<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<DecodedSchemas<'loan, 'control>, Error> {
        if !std::ptr::eq(work.control(), self.control) {
            return Err(invalid("Schema emission has a different original control"));
        }
        admit(&self.facts)?;
        let (definitions, retained_bytes) = decode_emit(self.definitions, self.types, work)?;
        Ok(DecodedSchemas {
            definitions,
            retained_bytes,
            types: self.types,
            control: self.control,
            source_bytes: self.source_bytes,
            index: self.index,
            facts: self.facts,
        })
    }
}
pub fn prepare_schemas_decode<'loan, 'control>(
    definitions: &'loan [wire::SchemaDefinition],
    types: &'loan DecodedTypeTable,
    source_retained_bytes: usize,
    limits: NodeProjectionLimits,
    control: &'control dyn PureCompileControl,
) -> Result<PreparedSchemasDecode<'loan, 'control>, Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = prepare_schemas_decode_observed_in(
        definitions,
        types,
        source_retained_bytes,
        limits,
        &mut |_| Ok(()),
        &mut work,
    );
    finish(work, result)
}
/// Retains the exact raw definitions and decoded Field namespace loans.
pub fn prepare_schemas_decode_observed_in<'loan, 'control>(
    definitions: &'loan [wire::SchemaDefinition],
    types: &'loan DecodedTypeTable,
    source_retained_bytes: usize,
    limits: NodeProjectionLimits,
    admit: &mut Admission<'_>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedSchemasDecode<'loan, 'control>, Error> {
    let facts = decode_preflight(
        definitions,
        types,
        source_retained_bytes,
        limits,
        admit,
        work,
    )?;
    let index = BindingIndex::prepare(definitions.len(), |at| definitions[at].id, work)?;
    Ok(PreparedSchemasDecode {
        definitions,
        types,
        control: work.control(),
        source_bytes: source_retained_bytes,
        index,
        facts,
    })
}
#[cfg(test)]
#[path = "physical_schema_v2/tests.rs"]
mod tests;
