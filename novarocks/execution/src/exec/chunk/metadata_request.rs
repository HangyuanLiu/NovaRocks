// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//! Borrowed requests of the original Chunk nullable-reconciliation and schema
//! publication. Cumulative requests bound coexistence; they are not source
//! stock, a grant, a post-operation capacity estimate or a result-drop receipt.
//! Legacy constructors do not call this optional positive-source entrypoint.
use super::*;
use novarocks_type_contract::{ControlResourceError, ValueTypeError, ValueTypeVisit};
use novarocks_type_contract::owned_resources::{
    hashmap::fresh_table_layout,
    layout::arc_layout,
    metadata_materialization::{MetadataMaterializationError, MetadataSidecarRequest},
    type_validation::TypeValidationScratch,
    vec::original_fresh_push_requests,
};
use std::alloc::Layout;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ChunkMetadataRequest {
    pub field_clone_requests_bytes: usize,
    pub field_arc_requests_bytes: usize,
    pub semantic_clone_requests_bytes: usize,
    pub struct_publication_requests_bytes: usize,
    pub descriptor_requests_bytes: usize,
    pub sidecar_requests_bytes: usize,
    pub cumulative_requests_bytes: usize,
    pub nested_field_occurrences: usize,
}
fn add<E: From<MetadataMaterializationError>>(a: usize, b: usize) -> Result<usize, E> {
    a.checked_add(b)
        .ok_or_else(|| E::from(MetadataMaterializationError::Arithmetic))
}
fn times<E: From<MetadataMaterializationError>>(a: usize, b: usize) -> Result<usize, E> {
    a.checked_mul(b)
        .ok_or_else(|| E::from(MetadataMaterializationError::Arithmetic))
}
fn array<T, E: From<MetadataMaterializationError>>(count: usize) -> Result<usize, E> {
    Layout::array::<T>(count)
        .map(|layout| layout.size())
        .map_err(|_| E::from(MetadataMaterializationError::Arithmetic))
}
fn arc<T, E: From<MetadataMaterializationError>>() -> Result<usize, E> {
    arc_layout(Layout::new::<T>())
        .map(|layout| layout.size())
        .map_err(|_| E::from(MetadataMaterializationError::Arithmetic))
}

impl ChunkSchema {
    /// Pre-operation positive source contribution for ONE original alignment.
    /// The three root clone occurrences are reconcile's root construction,
    /// the owned slot clone after its temporary Arc, and final Schema's clone.
    /// Two root Arc requests are temporary and final publication. Nested
    /// fields are conservatively allowed the same requests even on branches
    /// sharing their Arc. Structs include the original Result-collect Vec's
    /// fresh push growth and Fields Arc. All are actual source geometry.
    ///
    /// The original compatibility checker/formatter and Arrow validation are
    /// separate operation contributions; no value/error operation runs here.
    /// A missing positive source returns None at the root or an explicit
    /// MissingOriginalFieldOrigin for a foreign nested owner, never a grant.
    pub(crate) fn original_reconcile_metadata_request_observed<
        'a,
        E: From<MetadataMaterializationError> + From<ValueTypeError> + From<ControlResourceError>,
    >(
        &'a self,
        traversal: &mut TypeValidationScratch<'a>,
        clone_scratch: &mut TypeValidationScratch<'a>,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<Option<ChunkMetadataRequest>, E> {
        let Some(source) = self.metadata_materializations() else {
            return Ok(None);
        };
        let mut result = ChunkMetadataRequest::default();
        for slot in self.slots() {
            let root = slot
                .field
                .original_field_clone_request(clone_scratch, observe)
                .ok_or_else(|| {
                    E::from(MetadataMaterializationError::MissingOriginalFieldOrigin)
                })??;
            result.field_clone_requests_bytes = add::<E>(
                result.field_clone_requests_bytes,
                times::<E>(root.request_bytes, 3)?,
            )?;
            result.field_arc_requests_bytes = add::<E>(
                result.field_arc_requests_bytes,
                times::<E>(arc::<Field, E>()?, 2)?,
            )?;
            result.semantic_clone_requests_bytes = add::<E>(
                result.semantic_clone_requests_bytes,
                slot.field_schema
                    .original_clone_request_observed(observe)?
                    .request_bytes,
            )?;
            novarocks_type_contract::validate_value_type_structure_with_scratch_observed(
                slot.data_type(),
                traversal,
                |visit| {
                    match visit {
                        ValueTypeVisit::Field(field) => {
                            let request = source
                                .original_borrowed_field_clone_request_observed(
                                    field,
                                    clone_scratch,
                                    observe,
                                )?
                                .ok_or_else(|| {
                                    E::from(
                                        MetadataMaterializationError::MissingOriginalFieldOrigin,
                                    )
                                })?;
                            result.nested_field_occurrences =
                                add::<E>(result.nested_field_occurrences, 1)?;
                            result.field_clone_requests_bytes = add::<E>(
                                result.field_clone_requests_bytes,
                                times::<E>(request.request_bytes, 3)?,
                            )?;
                            result.field_arc_requests_bytes = add::<E>(
                                result.field_arc_requests_bytes,
                                times::<E>(arc::<Field, E>()?, 2)?,
                            )?;
                        }
                        ValueTypeVisit::TypeNode(DataType::Struct(fields)) => {
                            // Result iteration may have lower hint zero. ONE
                            // original fresh Vec growth author covers it and
                            // any Vec->Box trim before the original Arc slice.
                            let pushed = original_fresh_push_requests::<Arc<Field>, E>(
                                fields.len(),
                                observe,
                            )?;
                            let boxed = array::<Arc<Field>, E>(fields.len())?;
                            let shared =
                                arc_layout(Layout::array::<Arc<Field>>(fields.len()).map_err(
                                    |_| E::from(MetadataMaterializationError::Arithmetic),
                                )?)
                                .map_err(|_| E::from(MetadataMaterializationError::Arithmetic))?
                                .size();
                            result.struct_publication_requests_bytes = add::<E>(
                                result.struct_publication_requests_bytes,
                                add::<E>(add::<E>(pushed, boxed)?, shared)?,
                            )?;
                        }
                        _ => {}
                    }
                    observe()
                },
            )?;
        }
        let roots = self.slots().len();
        let inherited = add::<E>(source.fields().len(), result.nested_field_occurrences)?;
        // Sources::finish followed by final ChunkSchema publication are two
        // distinct sidecar tables. This full publication bound includes both;
        // it intentionally does not subtract their shared Schema payload.
        let sidecar =
            MetadataSidecarRequest::original_publication(inherited, roots).map_err(E::from)?;
        result.sidecar_requests_bytes = times::<E>(sidecar.coexistence_request_bytes, 2)?;
        result.sidecar_requests_bytes = add::<E>(
            result.sidecar_requests_bytes,
            original_fresh_push_requests::<MetadataFieldLoan, E>(
                result.nested_field_occurrences,
                observe,
            )?,
        )?;
        let table = fresh_table_layout::<SlotId, usize>(roots)
            .map_err(|cause| E::from(MetadataMaterializationError::Model(cause)))?;
        result.descriptor_requests_bytes = table.request_bytes_upper_bound;
        for bytes in [
            array::<ChunkSlotSchema, E>(roots)?,
            array::<SlotId, E>(roots)?,
            array::<Arc<Field>, E>(roots)?,
            arc::<ChunkSchema, E>()?,
        ] {
            result.descriptor_requests_bytes = add::<E>(result.descriptor_requests_bytes, bytes)?;
        }
        result.cumulative_requests_bytes = result.field_clone_requests_bytes;
        for bytes in [
            result.field_arc_requests_bytes,
            result.semantic_clone_requests_bytes,
            result.struct_publication_requests_bytes,
            result.descriptor_requests_bytes,
            result.sidecar_requests_bytes,
        ] {
            result.cumulative_requests_bytes = add::<E>(result.cumulative_requests_bytes, bytes)?;
        }
        Ok(Some(result))
    }
}
