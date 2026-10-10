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

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorRowConversionFootprint,
    ConnectorWriteFieldBinding, ConnectorWriteInputShape,
};
use std::mem::size_of;
fn exhausted() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "COW target input clone footprint overflow",
    )
}
fn add(a: u64, b: u64) -> Result<u64, ConnectorError> {
    a.checked_add(b).ok_or_else(exhausted)
}
fn fields(part: &[ConnectorWriteFieldBinding]) -> Result<u64, ConnectorError> {
    if part.is_empty() {
        return Ok(0);
    } // No backing is cloned for an absent arm.
    let slots = (part.len() as u64)
        .checked_mul(size_of::<ConnectorWriteFieldBinding>() as u64)
        .ok_or_else(exhausted)?;
    // schema_bytes also charges conservative Schema/Field/DataType roots and
    // shared nested Arc backing. Repeated inline/shared charge is an upper,
    // not an exact unique-byte claim. The iterator is actually ExactSize.
    let schema = ConnectorRowConversionFootprint::for_fields(part.iter().map(|b| b.field()))?;
    add(
        slots,
        u64::try_from(schema.schema_bytes).map_err(|_| exhausted())?,
    )
}
/// The enum root/Vec headers are in the CowTargetWritePlan exact vector slots.
/// This excludes that root to avoid claiming it as a second physical block.
/// General input public API semantics and errors are not changed here.
pub(crate) fn new_input_clone_heap_upper(
    input: &ConnectorWriteInputShape,
) -> Result<u64, ConnectorError> {
    let (first, second): (&[ConnectorWriteFieldBinding], &[ConnectorWriteFieldBinding]) =
        match input {
            ConnectorWriteInputShape::Data { fields: f } => (f, &[]),
            ConnectorWriteInputShape::RowLineage {
                data_fields,
                row_identity_fields,
            } => (data_fields, row_identity_fields),
            ConnectorWriteInputShape::PositionDelete {
                identity_fields,
                partition_source_fields,
            }
            | ConnectorWriteInputShape::DeletionVector {
                identity_fields,
                partition_source_fields,
            } => (identity_fields, partition_source_fields),
            ConnectorWriteInputShape::EqualityDelete { equality_fields } => (equality_fields, &[]),
        };
    // Do not chain the slices: Chain is not an ExactSizeIterator contract.
    add(fields(first)?, fields(second)?)
}
