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

//! Plain representation of a required decoded leaf under a retained typed
//! carrier. This is not an independent semantic projection of a raw child.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, LargeStringArray, StringArray};
use arrow_schema::DataType;
use novarocks_type_contract::{ValueKind, ValueTypeRef};

use super::{
    ArrayNormalizationStep, ArrayPathStep, ArrayProjectionFailure, ArrayProjectionOperation,
    ArrayProjectionViolation,
};

pub fn normalize_inactive_nulls<E, F>(
    root: &ValueTypeRef,
    source: &ArrayRef,
    path: &[ArrayPathStep],
    decoded: ArrayRef,
    observe: &mut F,
) -> Result<ArrayRef, E>
where
    E: From<ArrayProjectionFailure>,
    F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
{
    if decoded.null_count() == 0 {
        return Ok(decoded);
    }
    let mut value = root;
    for step in path {
        observe(ArrayNormalizationStep::InspectConstructionWork { units: 1 })?;
        value = match (step, value.kind()) {
            (ArrayPathStep::StructField(index), ValueKind::Struct(fields)) => fields
                .get(*index)
                .map(|field| field.value_type())
                .ok_or_else(|| {
                    E::from(ArrayProjectionFailure::Contract(
                        ArrayProjectionViolation::StructOccurrence,
                    ))
                })?,
            (ArrayPathStep::ListItem, ValueKind::List(item))
            | (ArrayPathStep::ListItem, ValueKind::FixedSizeList { item, .. }) => item,
            (ArrayPathStep::MapKey, ValueKind::Map { key, .. }) => key,
            (ArrayPathStep::MapValue, ValueKind::Map { value, .. }) => value,
            _ => {
                return Err(E::from(ArrayProjectionFailure::Contract(
                    ArrayProjectionViolation::HydrationPath,
                )));
            }
        };
    }
    if value.nullable() {
        return Ok(decoded);
    }
    let DataType::Dictionary(key, source_values) = source.data_type() else {
        return Err(E::from(ArrayProjectionFailure::Contract(
            ArrayProjectionViolation::MissingDictionaryCarrier,
        )));
    };
    if key.as_ref() != &DataType::Int32 || source_values.as_ref() != decoded.data_type() {
        return Err(E::from(ArrayProjectionFailure::Contract(
            ArrayProjectionViolation::UnsupportedEncodingTransition,
        )));
    }
    macro_rules! normalize {
        ($array:ty) => {{
            let array = decoded.as_any().downcast_ref::<$array>().ok_or_else(|| {
                E::from(ArrayProjectionFailure::Contract(
                    ArrayProjectionViolation::PhysicalWrapper,
                ))
            })?;
            // The locked Int32 String dictionary cast uses take_bytes, which
            // gives every decoded NULL an empty offset range. Verify that
            // condition at this occurrence rather than assuming arbitrary
            // inactive payload can be made non-NULL without changing values.
            for start in (0..array.len()).step_by(1024) {
                let end = array.len().min(start.saturating_add(1024));
                observe(ArrayNormalizationStep::InspectConstructionWork { units: end - start })?;
                for row in start..end {
                    if array.is_null(row)
                        && array.value_offsets()[row] != array.value_offsets()[row + 1]
                    {
                        return Err(E::from(ArrayProjectionFailure::Contract(
                            ArrayProjectionViolation::NonemptyNullString,
                        )));
                    }
                }
            }
            observe(ArrayNormalizationStep::ValidateStringWrapper {
                path,
                source: &decoded,
            })?;
            // Arrow has no safe existing-array NULL replacement API. The safe
            // constructor rechecks UTF-8 and offset boundaries under the
            // caller's observed locked-operation work bound.
            let normalized =
                <$array>::try_new(array.offsets().clone(), array.values().clone(), None).map_err(
                    |error| {
                        E::from(ArrayProjectionFailure::Arrow {
                            operation: ArrayProjectionOperation::NormalizeRequiredString,
                            source: error,
                        })
                    },
                )?;
            observe(ArrayNormalizationStep::OpaqueOperationFinished)?;
            Ok(Arc::new(normalized) as ArrayRef)
        }};
    }
    match decoded.data_type() {
        DataType::Utf8 => normalize!(StringArray),
        DataType::LargeUtf8 => normalize!(LargeStringArray),
        _ => Err(E::from(ArrayProjectionFailure::Contract(
            ArrayProjectionViolation::UnsupportedNullNormalization,
        ))),
    }
}
