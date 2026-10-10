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

//! Positive metadata-clone facts from the original insertion-only constructor.
//! An arbitrary HashMap capacity, Field size, or equal metadata is not an origin.
//! This owns no allocator, MEM grant, account, observation, or execution policy.
use super::hashmap::{FreshTableFacts, HashMapResourceError, fresh_table_layout};
use arrow_schema::{Field, FieldRef, Fields, Schema, SchemaRef};
use std::{
    collections::{HashMap, TryReserveError},
    sync::{Arc, Weak},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataMaterializationError {
    Model(HashMapResourceError),
    ValueType(crate::ValueTypeError),
    InsertCountExceededOriginalReservation,
    MissingOriginalFieldOrigin,
    Arithmetic,
}

impl From<crate::ValueTypeError> for MetadataMaterializationError {
    fn from(error: crate::ValueTypeError) -> Self {
        Self::ValueType(error)
    }
}

/// ONE original Field clone's requests. Shared child Field/Fields/UnionFields
/// and timezone Arc payloads make no new requests. The original Dictionary
/// boxes recurse through the existing borrowed source grammar. The table is
/// paired with this actual Field owner, never inferred from map capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OriginalFieldCloneRequest {
    pub name_bytes: usize,
    pub metadata: MetadataTableCloneRequest,
    pub dictionary_box_requests: usize,
    pub dictionary_box_bytes: usize,
    pub request_bytes: usize,
}
fn original_field_clone_request<
    'a,
    E: From<MetadataMaterializationError> + From<crate::ValueTypeError>,
>(
    field: &'a Field,
    origin: MetadataCloneOrigin,
    scratch: &mut crate::owned_resources::type_validation::TypeValidationScratch<'a>,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<OriginalFieldCloneRequest, E> {
    let metadata = original_map_clone_request(origin, field.metadata(), observe)?;
    let mut dictionary_box_requests = 0_usize;
    let mut dictionary_box_bytes = 0_usize;
    crate::visit_original_data_type_clone_with_scratch_observed(
        field.data_type(),
        scratch,
        |visit| {
            if let crate::ValueTypeVisit::TypeNode(arrow_schema::DataType::Dictionary(_, _)) = visit
            {
                dictionary_box_requests = dictionary_box_requests
                    .checked_add(2)
                    .ok_or_else(|| E::from(MetadataMaterializationError::Arithmetic))?;
                dictionary_box_bytes = dictionary_box_bytes
                    .checked_add(2 * std::mem::size_of::<arrow_schema::DataType>())
                    .ok_or_else(|| E::from(MetadataMaterializationError::Arithmetic))?;
            }
            observe()
        },
    )?;
    let request_bytes = field
        .name()
        .len()
        .checked_add(metadata.request_bytes().map_err(E::from)?)
        .and_then(|n| n.checked_add(dictionary_box_bytes))
        .ok_or_else(|| E::from(MetadataMaterializationError::Arithmetic))?;
    Ok(OriginalFieldCloneRequest {
        name_bytes: field.name().len(),
        metadata,
        dictionary_box_requests,
        dictionary_box_bytes,
        request_bytes,
    })
}

/// Allocation requests of ONE original immutable map clone. These are a
/// numerical caller contribution, not an operation grant or complete Field
/// clone extent: the latter also owns its name and DataType clone occurrences.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataTableCloneRequest {
    pub table_backing: Option<std::alloc::Layout>,
    pub text_requests: usize,
    pub text_bytes: usize,
}
impl MetadataTableCloneRequest {
    pub fn request_bytes(self) -> Result<usize, MetadataMaterializationError> {
        self.table_backing
            .map_or(0, |layout| layout.size())
            .checked_add(self.text_bytes)
            .ok_or(MetadataMaterializationError::Arithmetic)
    }
}

// Only an already-paired owner calls this private arithmetic. An arbitrary
// HashMap plus a copied numerical origin cannot mint a source pairing.
fn original_map_clone_request<E: From<MetadataMaterializationError>>(
    metadata: MetadataCloneOrigin,
    values: &HashMap<String, String>,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<MetadataTableCloneRequest, E> {
    let table_backing = metadata.table_request().map_err(E::from)?.layout;
    let mut text_requests = 0_usize;
    let mut text_bytes = 0_usize;
    for (key, value) in values {
        // String::clone requests the original length, independently for both
        // strings. An empty string contributes no physical allocation request.
        for text in [key, value] {
            if !text.is_empty() {
                text_requests = text_requests
                    .checked_add(1)
                    .ok_or_else(|| E::from(MetadataMaterializationError::Arithmetic))?;
                text_bytes = text_bytes
                    .checked_add(text.len())
                    .ok_or_else(|| E::from(MetadataMaterializationError::Arithmetic))?;
            }
        }
        observe()?;
    }
    Ok(MetadataTableCloneRequest {
        table_backing,
        text_requests,
        text_bytes,
    })
}

/// The original HashMap::with_capacity and insertion occurrences live here.
/// No removal, mutable map escape, second reservation or table compaction can
/// invalidate its construction receipt. A failed numerical source projection
/// stays separate from the original successful value operation.
#[derive(Debug)]
pub struct MaterializedMetadataMap {
    values: HashMap<String, String>,
    original_reservation: usize,
    insertions: usize,
    reserved_once: bool,
    facts: Result<FreshTableFacts, MetadataMaterializationError>,
}
impl MaterializedMetadataMap {
    pub fn with_capacity(original_reservation: usize) -> Self {
        // Keep the original allocation and panic before numerical observation.
        let values = HashMap::with_capacity(original_reservation);
        let facts = fresh_table_layout::<String, String>(original_reservation)
            .map_err(MetadataMaterializationError::Model);
        Self {
            values,
            original_reservation,
            insertions: 0,
            reserved_once: true,
            facts,
        }
    }
    /// Preserve the original empty-map plus ONE fallible reservation author.
    /// A second reservation remains an explicitly unavailable resource fact;
    /// it never changes the original map's value or error operation.
    pub fn new_for_original_reserve() -> Self {
        Self {
            values: HashMap::new(),
            original_reservation: 0,
            insertions: 0,
            facts: fresh_table_layout::<String, String>(0)
                .map_err(MetadataMaterializationError::Model),
            reserved_once: false,
        }
    }
    pub fn try_reserve_original(&mut self, additional: usize) -> Result<(), TryReserveError> {
        let result = self.values.try_reserve(additional);
        if result.is_ok() {
            if self.reserved_once || self.insertions != 0 {
                self.facts =
                    Err(MetadataMaterializationError::InsertCountExceededOriginalReservation);
            } else {
                self.original_reservation = additional;
                self.facts = fresh_table_layout::<String, String>(additional)
                    .map_err(MetadataMaterializationError::Model);
            }
            self.reserved_once = true;
        }
        result
    }
    pub fn insert(&mut self, key: String, value: String) -> Option<String> {
        // Exactly the original map insertion, replacement result and Drop
        // timing; receiving a fact never chooses value semantics or capacity.
        let previous = self.values.insert(key, value);
        match self.insertions.checked_add(1) {
            Some(insertions) => {
                self.insertions = insertions;
                if insertions > self.original_reservation {
                    self.facts =
                        Err(MetadataMaterializationError::InsertCountExceededOriginalReservation);
                }
            }
            None => self.facts = Err(MetadataMaterializationError::Arithmetic),
        }
        previous
    }
    pub fn into_schema(self, fields: impl Into<Fields>) -> MaterializedSchema {
        MaterializedSchema {
            schema: Schema::new_with_metadata(fields, self.values),
            metadata: MetadataCloneOrigin { facts: self.facts },
        }
    }
    pub fn into_field(self, field: Field) -> MaterializedField {
        MaterializedField {
            field: field.with_metadata(self.values),
            metadata: MetadataCloneOrigin { facts: self.facts },
        }
    }
}

/// No public constructor accepts an arbitrary map or a fabricated Layout.
/// The source map is kept immutable inside its paired original Field owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataCloneOrigin {
    facts: Result<FreshTableFacts, MetadataMaterializationError>,
}
impl MetadataCloneOrigin {
    /// RawTable::clone allocates its original bucket count; this is not a
    /// capacity-to-buckets inversion over a possibly erased foreign table.
    pub fn table_request(self) -> Result<FreshTableFacts, MetadataMaterializationError> {
        self.facts
    }
}

#[derive(Debug)]
pub struct MaterializedField {
    field: Field,
    metadata: MetadataCloneOrigin,
}
impl MaterializedField {
    pub fn field(&self) -> &Field {
        &self.field
    }
    pub fn metadata_origin(&self) -> MetadataCloneOrigin {
        self.metadata
    }
    pub fn original_metadata_clone_request<E: From<MetadataMaterializationError>>(
        &self,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<MetadataTableCloneRequest, E> {
        original_map_clone_request(self.metadata, self.field.metadata(), observe)
    }
    pub fn original_field_clone_request<
        'a,
        E: From<MetadataMaterializationError> + From<crate::ValueTypeError>,
    >(
        &'a self,
        scratch: &mut crate::owned_resources::type_validation::TypeValidationScratch<'a>,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<OriginalFieldCloneRequest, E> {
        original_field_clone_request(&self.field, self.metadata, scratch, observe)
    }
    pub fn clone_original(&self) -> Self {
        Self {
            field: self.field.clone(),
            metadata: self.metadata,
        }
    }
    /// Preserve the original Arc constructor at its existing publication site.
    pub fn into_shared(self) -> SharedMaterializedField {
        SharedMaterializedField {
            field: Arc::new(self.field),
            metadata: self.metadata,
        }
    }
    /// Explicitly leaving the resource-receipt chain grants nothing. Original
    /// value-only callers may retain their existing plain Field API.
    pub fn into_original_field(self) -> Field {
        self.field
    }
    pub fn with_nullable_original(&self, nullable: bool) -> Self {
        Self {
            field: self.field.clone().with_nullable(nullable),
            metadata: self.metadata,
        }
    }
    pub fn rebuild_original(&self, data_type: arrow_schema::DataType, nullable: bool) -> Self {
        Self {
            field: rebuild_original_field(&self.field, data_type, nullable),
            metadata: self.metadata,
        }
    }
}
#[derive(Clone, Debug)]
pub struct SharedMaterializedField {
    field: FieldRef,
    metadata: MetadataCloneOrigin,
}
impl SharedMaterializedField {
    pub fn field(&self) -> &FieldRef {
        &self.field
    }
    pub fn metadata_origin(&self) -> MetadataCloneOrigin {
        self.metadata
    }
    pub fn loan(&self) -> MetadataFieldLoan {
        MetadataFieldLoan {
            field: Arc::downgrade(&self.field),
            metadata: self.metadata,
        }
    }
    pub fn into_original_field_ref(self) -> FieldRef {
        self.field
    }
    pub fn lends(&self, actual: &FieldRef) -> bool {
        Arc::ptr_eq(&self.field, actual)
    }
    pub fn clone_original(&self) -> MaterializedField {
        MaterializedField {
            field: self.field.as_ref().clone(),
            metadata: self.metadata,
        }
    }
}

/// The original source's schema metadata is paired before it can escape. This
/// does not certify the schema fields' independent construction origins.
#[derive(Debug)]
pub struct MaterializedSchema {
    schema: Schema,
    metadata: MetadataCloneOrigin,
}
impl MaterializedSchema {
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
    pub fn metadata_origin(&self) -> MetadataCloneOrigin {
        self.metadata
    }
    pub fn into_shared(self) -> SharedMaterializedSchema {
        SharedMaterializedSchema {
            schema: Arc::new(self.schema),
            metadata: self.metadata,
        }
    }
    pub fn into_original_schema(self) -> Schema {
        self.schema
    }
}
#[derive(Clone, Debug)]
pub struct SharedMaterializedSchema {
    schema: SchemaRef,
    metadata: MetadataCloneOrigin,
}
impl SharedMaterializedSchema {
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    pub fn metadata_origin(&self) -> MetadataCloneOrigin {
        self.metadata
    }
    pub fn into_original_schema_ref(self) -> SchemaRef {
        self.schema
    }
    pub fn clone_original(&self) -> MaterializedSchema {
        MaterializedSchema {
            schema: self.schema.as_ref().clone(),
            metadata: self.metadata,
        }
    }
    /// Clone the original table before its caller's existing slot/index loop.
    /// The numerical receipt follows the clone without reconstructing a map.
    pub fn original_metadata_clone_request<E: From<MetadataMaterializationError>>(
        &self,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<MetadataTableCloneRequest, E> {
        original_map_clone_request(self.metadata, self.schema.metadata(), observe)
    }
    pub fn clone_metadata_original(&self) -> OriginalMetadataClone {
        OriginalMetadataClone {
            values: self.schema.metadata().clone(),
            metadata: self.metadata,
        }
    }
    pub fn lends(&self, actual: &SchemaRef) -> bool {
        Arc::ptr_eq(&self.schema, actual)
    }
}

/// Call the ONE original FVT materializer, then record its explicit root-map
/// construction branch. No metadata iteration, capacity inversion, root type
/// inference or reconstructed Field substitutes for that source occurrence.
/// The root map is new/empty, or made from the original one-entry array; its
/// nested DataType FieldRefs remain the original source's independent owners.
pub fn materialize_value_field(
    value: &crate::FunctionValueType,
    name: impl Into<String>,
) -> Result<MaterializedField, crate::ValueTypeError> {
    let field = value.try_to_field(name)?;
    let original_entries = usize::from(value.logical_type.metadata_value().is_some());
    Ok(MaterializedField {
        field,
        metadata: MetadataCloneOrigin {
            facts: fresh_table_layout::<String, String>(original_entries)
                .map_err(MetadataMaterializationError::Model),
        },
    })
}

/// ONE original Chunk field reconstruction. Dictionary attributes retain its
/// original Field::new behavior. This is neither a carrier conversion nor a
/// source receipt constructor for an arbitrary foreign metadata table.
pub fn rebuild_original_field(
    expected: &Field,
    data_type: arrow_schema::DataType,
    nullable: bool,
) -> Field {
    Field::new(expected.name(), data_type, nullable).with_metadata(expected.metadata().clone())
}

/// A source-owned collection of positive construction occurrences. It claims
/// only the exact owners present in the set; absence is not an empty/fresh map
/// proof. The source visitor supplies every original nested occurrence.
#[derive(Clone, Debug)]
pub struct SchemaMetadataMaterializations {
    schema: SharedMaterializedSchema,
    fields: Arc<[MetadataFieldLoan]>,
}
impl SchemaMetadataMaterializations {
    pub fn from_materialized_owners(
        schema: SharedMaterializedSchema,
        fields: Arc<[MetadataFieldLoan]>,
    ) -> Self {
        Self { schema, fields }
    }
    pub fn schema_owner(&self) -> &SharedMaterializedSchema {
        &self.schema
    }
    pub fn fields(&self) -> &[MetadataFieldLoan] {
        &self.fields
    }
    pub fn field_namespace(&self) -> MaterializedFieldNamespace {
        MaterializedFieldNamespace {
            fields: self.fields.clone(),
        }
    }
    pub fn field_origin(&self, field: &FieldRef) -> Option<MetadataCloneOrigin> {
        self.fields
            .iter()
            .find(|entry| entry.lends(field))
            .map(|entry| entry.metadata_origin())
    }
    /// Borrow only the exact retained owner. Every inspected namespace entry
    /// belongs to the caller's current operation; a refusal has no footer.
    pub fn field_loan_observed<E>(
        &self,
        field: &FieldRef,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<Option<&MetadataFieldLoan>, E> {
        for entry in self.fields.iter() {
            let same = entry.lends(field);
            observe()?;
            if same {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }
    pub fn field_origin_observed<E>(
        &self,
        field: &FieldRef,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<Option<MetadataCloneOrigin>, E> {
        self.field_loan_observed(field, observe)
            .map(|loan| loan.map(MetadataFieldLoan::metadata_origin))
    }
    /// A real original Schema clone shares its original FieldRefs and clones
    /// exactly the paired root table. No metadata iteration rematerializes it.
    pub fn original_borrowed_field_clone_request_observed<
        'a,
        E: From<MetadataMaterializationError> + From<crate::ValueTypeError>,
    >(
        &self,
        actual: &'a Field,
        scratch: &mut crate::owned_resources::type_validation::TypeValidationScratch<'a>,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<Option<OriginalFieldCloneRequest>, E> {
        for loan in self.fields.iter() {
            let same = loan.lends_borrowed_field(actual);
            observe()?;
            if same {
                return loan
                    .original_borrowed_field_clone_request(actual, scratch, observe)
                    .expect("exact borrowed original Field loan")
                    .map(Some);
            }
        }
        Ok(None)
    }
    pub fn clone_original_schema(&self) -> Self {
        Self {
            schema: self.schema.clone_original().into_shared(),
            fields: self.fields.clone(),
        }
    }
}

/// Exact source identity without retaining a removed projection's Field
/// payload. Its original Arc allocation header stays pinned until the last
/// weak loan drops; this header is a real retained resource, not free stock.
#[derive(Clone, Debug)]
pub struct MetadataFieldLoan {
    field: Weak<Field>,
    metadata: MetadataCloneOrigin,
}
impl MetadataFieldLoan {
    pub fn lends_borrowed_field(&self, actual: &Field) -> bool {
        self.field.as_ptr() == actual as *const Field
    }
    pub fn original_borrowed_field_clone_request<
        'a,
        E: From<MetadataMaterializationError> + From<crate::ValueTypeError>,
    >(
        &self,
        actual: &'a Field,
        scratch: &mut crate::owned_resources::type_validation::TypeValidationScratch<'a>,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Option<Result<OriginalFieldCloneRequest, E>> {
        self.lends_borrowed_field(actual)
            .then(|| original_field_clone_request(actual, self.metadata, scratch, observe))
    }
    pub fn lends(&self, actual: &FieldRef) -> bool {
        // The retained Weak pins this exact allocation header against reuse.
        // Compare only addresses; no payload dereference or temporary Weak.
        self.field.as_ptr() == Arc::as_ptr(actual)
    }
    pub fn metadata_origin(&self) -> MetadataCloneOrigin {
        self.metadata
    }
    pub fn original_metadata_clone_request<E: From<MetadataMaterializationError>>(
        &self,
        actual: &FieldRef,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Option<Result<MetadataTableCloneRequest, E>> {
        self.lends(actual)
            .then(|| original_map_clone_request(self.metadata, actual.metadata(), observe))
    }
    pub fn original_field_clone_request<
        'a,
        E: From<MetadataMaterializationError> + From<crate::ValueTypeError>,
    >(
        &self,
        actual: &'a FieldRef,
        scratch: &mut crate::owned_resources::type_validation::TypeValidationScratch<'a>,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Option<Result<OriginalFieldCloneRequest, E>> {
        self.lends(actual)
            .then(|| original_field_clone_request(actual, self.metadata, scratch, observe))
    }
    pub fn clone_original(&self, actual: &FieldRef) -> Option<MaterializedField> {
        self.lends(actual).then(|| MaterializedField {
            field: actual.as_ref().clone(),
            metadata: self.metadata,
        })
    }
}

/// The original projection supplies one already-cloned Field occurrence.
/// A foreign value remains an explicit absence of a clone source receipt.
#[derive(Debug)]
pub enum ProjectedMaterializedField {
    Original(MaterializedField),
    Foreign(Field),
}
impl SchemaMetadataMaterializations {
    pub fn clone_field_original_observed<E>(
        &self,
        field: &FieldRef,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<ProjectedMaterializedField, E> {
        Ok(match self.field_loan_observed(field, observe)? {
            Some(loan) => ProjectedMaterializedField::Original(
                loan.clone_original(field)
                    .expect("exact original Field loan"),
            ),
            None => ProjectedMaterializedField::Foreign(field.as_ref().clone()),
        })
    }
    pub fn clone_field_original(&self, field: &FieldRef) -> ProjectedMaterializedField {
        match self.fields.iter().find(|entry| entry.lends(field)) {
            Some(entry) => ProjectedMaterializedField::Original(
                entry
                    .clone_original(field)
                    .expect("exact original Field loan"),
            ),
            None => ProjectedMaterializedField::Foreign(field.as_ref().clone()),
        }
    }
    /// ONE original Schema projection constructor. The source owns its exact
    /// root map; child references are the original Field-clone products. No
    /// metadata iteration, compaction or capacity inversion happens here.
    /// The caller must admit both source receipt-vector copies and new root
    /// loans before calling this allocating materialization operation.
    pub fn project_original_schema(&self, projected: Vec<ProjectedMaterializedField>) -> Self {
        let mut fields = Vec::with_capacity(projected.len());
        let mut origins = Vec::with_capacity(projected.len());
        for field in projected {
            match field {
                ProjectedMaterializedField::Original(field) => {
                    origins.push(Some(field.metadata));
                    fields.push(field.field);
                }
                ProjectedMaterializedField::Foreign(field) => {
                    origins.push(None);
                    fields.push(field);
                }
            }
        }
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            self.schema.schema.metadata().clone(),
        ));
        let mut loans = Vec::with_capacity(self.fields.len() + origins.len());
        loans.extend(self.fields.iter().cloned());
        for (field, origin) in schema.fields().iter().zip(origins) {
            if let Some(metadata) = origin {
                loans.push(MetadataFieldLoan {
                    field: Arc::downgrade(field),
                    metadata,
                });
            }
        }
        Self {
            schema: SharedMaterializedSchema {
                schema,
                metadata: self.schema.metadata,
            },
            fields: loans.into(),
        }
    }
}

/// A table cloned from its paired immutable Schema. Only the original owner
/// can construct this object; no metadata iterator or capacity inference is
/// accepted. The caller still owes admission before the original clone.
#[derive(Debug)]
pub struct OriginalMetadataClone {
    values: HashMap<String, String>,
    metadata: MetadataCloneOrigin,
}
impl OriginalMetadataClone {
    pub fn into_schema(self, fields: impl Into<Fields>) -> MaterializedSchema {
        MaterializedSchema {
            schema: Schema::new_with_metadata(fields, self.values),
            metadata: self.metadata,
        }
    }
}

/// Optional positive root provenance travels with the actual owned Field.
/// Plain callers retain the original Field behavior and expose no fact.
pub enum OriginalFieldMaterialization {
    Plain(Field),
    Materialized(MaterializedField),
}
impl std::fmt::Debug for OriginalFieldMaterialization {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self.field(), out)
    }
}
impl PartialEq for OriginalFieldMaterialization {
    fn eq(&self, other: &Self) -> bool {
        self.field() == other.field()
    }
}
impl Eq for OriginalFieldMaterialization {}
impl Clone for OriginalFieldMaterialization {
    fn clone(&self) -> Self {
        match self {
            Self::Plain(field) => Self::Plain(field.clone()),
            Self::Materialized(field) => Self::Materialized(field.clone_original()),
        }
    }
}
impl OriginalFieldMaterialization {
    pub fn field(&self) -> &Field {
        match self {
            Self::Plain(field) => field,
            Self::Materialized(field) => field.field(),
        }
    }
    pub fn metadata_origin(&self) -> Option<MetadataCloneOrigin> {
        match self {
            Self::Plain(_) => None,
            Self::Materialized(field) => Some(field.metadata_origin()),
        }
    }
    pub fn original_field_clone_request<
        'a,
        E: From<MetadataMaterializationError> + From<crate::ValueTypeError>,
    >(
        &'a self,
        scratch: &mut crate::owned_resources::type_validation::TypeValidationScratch<'a>,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Option<Result<OriginalFieldCloneRequest, E>> {
        match self {
            Self::Plain(_) => None,
            Self::Materialized(field) => Some(field.original_field_clone_request(scratch, observe)),
        }
    }
    pub fn with_nullable_original(&self, nullable: bool) -> Self {
        match self {
            Self::Plain(field) => Self::Plain(field.clone().with_nullable(nullable)),
            Self::Materialized(field) => Self::Materialized(field.with_nullable_original(nullable)),
        }
    }
    pub fn rebuild_original(&self, data_type: arrow_schema::DataType, nullable: bool) -> Self {
        match self {
            Self::Plain(field) => Self::Plain(rebuild_original_field(field, data_type, nullable)),
            Self::Materialized(field) => {
                Self::Materialized(field.rebuild_original(data_type, nullable))
            }
        }
    }
    pub fn with_nullable_owned(self, nullable: bool) -> Self {
        match self {
            Self::Plain(field) => Self::Plain(field.with_nullable(nullable)),
            Self::Materialized(field) => Self::Materialized(MaterializedField {
                field: field.field.with_nullable(nullable),
                metadata: field.metadata,
            }),
        }
    }
    /// Move through the original owned Field::with_name call, with no clone.
    pub fn with_name_owned(self, name: impl Into<String>) -> Self {
        match self {
            Self::Plain(field) => Self::Plain(field.with_name(name)),
            Self::Materialized(field) => Self::Materialized(MaterializedField {
                field: field.field.with_name(name),
                metadata: field.metadata,
            }),
        }
    }
    pub fn into_original_field(self) -> Field {
        match self {
            Self::Plain(field) => field,
            Self::Materialized(field) => field.into_original_field(),
        }
    }
    pub fn into_shared(self) -> (FieldRef, Option<MetadataFieldLoan>) {
        match self {
            Self::Plain(field) => (Arc::new(field), None),
            Self::Materialized(field) => {
                let shared = field.into_shared();
                let loan = shared.loan();
                (shared.into_original_field_ref(), Some(loan))
            }
        }
    }
    pub fn clone_from_source(
        field: &FieldRef,
        source: Option<&SchemaMetadataMaterializations>,
    ) -> Self {
        match source.map(|source| source.clone_field_original(field)) {
            Some(ProjectedMaterializedField::Original(field)) => Self::Materialized(field),
            Some(ProjectedMaterializedField::Foreign(field)) => Self::Plain(field),
            None => Self::Plain(field.as_ref().clone()),
        }
    }
}

impl SchemaMetadataMaterializations {
    /// Rebuild one exact borrowed source occurrence through the original
    /// reconstruction body. This never traverses or reinterprets DataType.
    pub fn rebuild_field_original(
        &self,
        field: &FieldRef,
        data_type: arrow_schema::DataType,
        nullable: bool,
    ) -> OriginalFieldMaterialization {
        match self.fields.iter().find(|entry| entry.lends(field)) {
            Some(entry) => OriginalFieldMaterialization::Materialized(MaterializedField {
                field: rebuild_original_field(field, data_type, nullable),
                metadata: entry.metadata,
            }),
            None => OriginalFieldMaterialization::Plain(rebuild_original_field(
                field, data_type, nullable,
            )),
        }
    }
    pub fn with_additional_original_loans(&self, additional: Vec<MetadataFieldLoan>) -> Self {
        let mut fields = Vec::with_capacity(self.fields.len() + additional.len());
        fields.extend(self.fields.iter().cloned());
        fields.extend(additional);
        Self {
            schema: self.schema.clone(),
            fields: fields.into(),
        }
    }
}

/// Additional backing of the original source-sidecar publication. The actual
/// count is authored before the original Vec/Box/Arc operations. Keeping all
/// request contributions live is a conservative coexistence envelope, including
/// the old namespace, roots, Vec trim and the final Arc slice simultaneously.
/// This covers sidecar backing only, not Field/DataType/map clone payloads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataSidecarRequest {
    pub inherited_loans: usize,
    pub original_roots: usize,
    pub loans_upper_bound: usize,
    pub temporary_requests_bytes: usize,
    pub result_request_bytes: usize,
    pub coexistence_request_bytes: usize,
}
impl MetadataSidecarRequest {
    pub fn original_publication(
        inherited_loans: usize,
        original_roots: usize,
    ) -> Result<Self, MetadataMaterializationError> {
        let loans_upper_bound = inherited_loans
            .checked_add(original_roots)
            .ok_or(MetadataMaterializationError::Arithmetic)?;
        let loans = std::alloc::Layout::array::<MetadataFieldLoan>(loans_upper_bound)
            .map_err(|_| MetadataMaterializationError::Arithmetic)?;
        let roots = std::alloc::Layout::array::<FieldRef>(original_roots)
            .map_err(|_| MetadataMaterializationError::Arithmetic)?;
        let sidecar_arc = super::layout::arc_layout(loans)
            .map_err(|_| MetadataMaterializationError::Arithmetic)?;
        let fields_arc = super::layout::arc_layout(roots)
            .map_err(|_| MetadataMaterializationError::Arithmetic)?;
        // Vec::with_capacity requests the original count. Vec -> Box may trim
        // unused foreign-root slots; the full original count bounds that trim.
        // Positive typed/inherited roots occupy their actual owned carrier,
        // not the smaller plain Field representation. Origins are retained
        // until publication; the separate FieldRef vector cannot reuse this
        // carrier allocation through in-place iterator specialization.
        let input_fields =
            std::alloc::Layout::array::<OriginalFieldMaterialization>(original_roots)
                .map_err(|_| MetadataMaterializationError::Arithmetic)?;
        let origins = std::alloc::Layout::array::<Option<MetadataCloneOrigin>>(original_roots)
            .map_err(|_| MetadataMaterializationError::Arithmetic)?;
        let temporary_requests_bytes = loans
            .size()
            .checked_mul(2)
            .and_then(|n| n.checked_add(roots.size()))
            .and_then(|n| n.checked_add(input_fields.size()))
            .and_then(|n| n.checked_add(origins.size()))
            .ok_or(MetadataMaterializationError::Arithmetic)?;
        let root_arcs = super::layout::arc_layout(std::alloc::Layout::new::<Field>())
            .map_err(|_| MetadataMaterializationError::Arithmetic)?
            .size()
            .checked_mul(original_roots)
            .ok_or(MetadataMaterializationError::Arithmetic)?;
        let schema_arc = super::layout::arc_layout(std::alloc::Layout::new::<Schema>())
            .map_err(|_| MetadataMaterializationError::Arithmetic)?
            .size();
        let result_request_bytes = sidecar_arc
            .size()
            .checked_add(fields_arc.size())
            .and_then(|n| n.checked_add(root_arcs))
            .and_then(|n| n.checked_add(schema_arc))
            .ok_or(MetadataMaterializationError::Arithmetic)?;
        let coexistence_request_bytes = temporary_requests_bytes
            .checked_add(result_request_bytes)
            .ok_or(MetadataMaterializationError::Arithmetic)?;
        Ok(Self {
            inherited_loans,
            original_roots,
            loans_upper_bound,
            temporary_requests_bytes,
            result_request_bytes,
            coexistence_request_bytes,
        })
    }
}

/// The decoder's same original sparse Field owners. This is a numerical
/// provenance loan, not a wire fact, host grant or function/runtime binding.
#[derive(Clone, Debug)]
pub struct MaterializedFieldNamespace {
    fields: Arc<[MetadataFieldLoan]>,
}
impl MaterializedFieldNamespace {
    /// Concatenate these two actual source namespaces, never electing a
    /// source by field equality. The existing caller's preparation scope owns
    /// this additional table; this method does not claim a MEM grant.
    pub fn join_original_observed(
        &self,
        other: &Self,
        work: &mut crate::CompileCheckpoints<'_>,
    ) -> Result<Self, crate::CompileControlError> {
        let count = self
            .fields
            .len()
            .checked_add(other.fields.len())
            .ok_or(crate::CompileControlError::ResourceExhausted)?;
        work.flush()?;
        let mut fields = Vec::new();
        let reserved = fields.try_reserve_exact(count);
        if reserved.is_ok() {
            work.step()?;
        }
        crate::owned_resources::copy::reserve_exit::<crate::CompileControlError>(reserved, work)?;
        for source in [self, other] {
            for field in source.fields.iter() {
                fields.push(field.clone());
                work.step()?;
            }
        }
        work.flush()?;
        let result = Self::from_original_loans(fields.into());
        work.flush()?;
        Ok(result)
    }
    pub fn from_original_loans(fields: Arc<[MetadataFieldLoan]>) -> Self {
        Self { fields }
    }
    pub fn fields(&self) -> &[MetadataFieldLoan] {
        &self.fields
    }
    pub fn original_publication_request(
        &self,
        roots: usize,
    ) -> Result<MetadataSidecarRequest, MetadataMaterializationError> {
        MetadataSidecarRequest::original_publication(self.fields.len(), roots)
    }
    pub fn original_field_clone_request_observed<
        'a,
        E: From<MetadataMaterializationError> + From<crate::ValueTypeError>,
    >(
        &self,
        actual: &'a FieldRef,
        scratch: &mut crate::owned_resources::type_validation::TypeValidationScratch<'a>,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<Option<OriginalFieldCloneRequest>, E> {
        for loan in self.fields.iter() {
            let same = loan.lends(actual);
            observe()?;
            if same {
                return loan
                    .original_field_clone_request(actual, scratch, observe)
                    .expect("exact original Field loan")
                    .map(Some);
            }
        }
        Ok(None)
    }
}

/// Materialize the original typed root Fields once, retaining their paired
/// metadata construction occurrence until the original Schema conversion.
/// All vectors/Arc metadata are real additional retained preparation backing;
/// the caller owes its admission before using this optional positive path.
pub struct TypedSchemaMaterializations {
    fields: OriginalSchemaFields,
    namespace: MaterializedFieldNamespace,
}
enum OriginalSchemaFields {
    Typed(Vec<MaterializedField>),
    Inherited(Vec<OriginalFieldMaterialization>),
}
impl TypedSchemaMaterializations {
    pub fn new(fields: Vec<MaterializedField>, namespace: MaterializedFieldNamespace) -> Self {
        Self {
            fields: OriginalSchemaFields::Typed(fields),
            namespace,
        }
    }
    pub fn from_original_fields(
        fields: Vec<OriginalFieldMaterialization>,
        namespace: MaterializedFieldNamespace,
    ) -> Self {
        Self {
            fields: OriginalSchemaFields::Inherited(fields),
            namespace,
        }
    }
    pub fn original_publication_request(
        &self,
    ) -> Result<MetadataSidecarRequest, MetadataMaterializationError> {
        let roots = match &self.fields {
            OriginalSchemaFields::Typed(fields) => fields.len(),
            OriginalSchemaFields::Inherited(fields) => fields.len(),
        };
        self.namespace.original_publication_request(roots)
    }
    pub fn into_original_schema_in<E: From<MetadataMaterializationError>>(
        self,
        admit: &mut impl FnMut(&MetadataSidecarRequest) -> Result<(), E>,
    ) -> Result<SchemaMetadataMaterializations, E> {
        let request = self.original_publication_request().map_err(E::from)?;
        admit(&request)?;
        Ok(self.into_original_schema())
    }
    pub fn into_original_schema(self) -> SchemaMetadataMaterializations {
        let roots = match &self.fields {
            OriginalSchemaFields::Typed(fields) => fields.len(),
            OriginalSchemaFields::Inherited(fields) => fields.len(),
        };
        let mut loans = Vec::with_capacity(self.namespace.fields.len() + roots);
        loans.extend(self.namespace.fields.iter().cloned());
        // Schema::new creates its original empty RandomState map before
        // converting Field values into Fields. Keep that original seed order;
        // this is not a claim that OS seed acquisition is allocator/CPU-bounded.
        let metadata = MaterializedMetadataMap::new_for_original_reserve();
        // The two typed input containers share ONE original Field -> Arc ->
        // Schema conversion. Plain foreign fields retain no fabricated origin.
        fn publish(
            fields: impl Iterator<Item = OriginalFieldMaterialization>,
            roots: usize,
            loans: &mut Vec<MetadataFieldLoan>,
        ) -> Vec<FieldRef> {
            // Keep the real root buffer explicit: in-place Map collection from
            // a larger owned Field carrier could reuse its larger capacity.
            let mut published = Vec::with_capacity(roots);
            for field in fields {
                let (field, loan) = field.into_shared();
                if let Some(loan) = loan {
                    loans.push(loan);
                }
                published.push(field);
            }
            published
        }
        let fields = match self.fields {
            OriginalSchemaFields::Typed(fields) => publish(
                fields
                    .into_iter()
                    .map(OriginalFieldMaterialization::Materialized),
                roots,
                &mut loans,
            ),
            OriginalSchemaFields::Inherited(fields) => {
                publish(fields.into_iter(), roots, &mut loans)
            }
        };
        let schema = metadata.into_schema(fields).into_shared();
        SchemaMetadataMaterializations::from_materialized_owners(schema, loans.into())
    }
}
