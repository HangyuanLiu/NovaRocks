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

//! Value-layer events retain the existing construction observer's ownership.
use super::{ArrayNormalizationStep, ArrayPathStep, ArrayProjectionFailure};
use arrow_array::ArrayRef;
use novarocks_type_contract::{TypedValueBinding, ValueTypeStorageLifetime};
use novarocks_value_arrays::{
    ValueFailure,
    decode::ValueConversionContext,
    hydrate::{HydrateEvent, HydrateOptions, HydratePathStep, HydratePlan},
};

pub fn value_failure<E: From<ArrayProjectionFailure>>(error: ValueFailure<E>) -> E {
    match error {
        ValueFailure::Caller(error) => error,
        ValueFailure::Value(source) => E::from(ArrayProjectionFailure::Value(source)),
    }
}

/// Convert only structure facts. Long value work goes directly to the actual
/// caller control checkpoint, not the metadata construction profile.
pub fn observe_value_hydration<E>(
    event: HydrateEvent<'_>,
    observe: &mut impl for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
) -> Result<(), E> {
    let mut path_storage = [ArrayPathStep::ListItem; novarocks_type_contract::MAX_VALUE_TYPE_DEPTH];
    let convert_path = |path: &[HydratePathStep], storage: &mut [ArrayPathStep]| {
        for (input, output) in path.iter().zip(storage) {
            *output = match input {
                HydratePathStep::ListItem => ArrayPathStep::ListItem,
                HydratePathStep::UnionField(id) => ArrayPathStep::UnionField(*id),
                HydratePathStep::StructField(i) => ArrayPathStep::StructField(*i),
                HydratePathStep::MapKey => ArrayPathStep::MapKey,
                HydratePathStep::MapValue => ArrayPathStep::MapValue,
            };
        }
    };
    match event {
        HydrateEvent::InspectDictionaryRows {
            source,
            row_start,
            row_count,
        } => observe(ArrayNormalizationStep::InspectDictionaryRows {
            source,
            row_start,
            row_count,
        }),
        HydrateEvent::InspectType(ty) => observe(ArrayNormalizationStep::InspectType(ty)),
        HydrateEvent::InspectField(field) => observe(ArrayNormalizationStep::InspectField {
            source: field,
            name: field.name(),
        }),
        HydrateEvent::SliceArray {
            source,
            offset,
            length,
        } => observe(ArrayNormalizationStep::SliceArray {
            source,
            offset,
            length,
        }),
        HydrateEvent::RebuildArray {
            path,
            source,
            target,
        } => {
            convert_path(path, &mut path_storage);
            observe(ArrayNormalizationStep::RebuildArray {
                path: &path_storage[..path.len()],
                source,
                target,
            })
        }
        HydrateEvent::RebuildType { source, .. } => {
            observe(ArrayNormalizationStep::RebuildType { source })
        }
        HydrateEvent::RebuildField { source, target, .. } => {
            observe(ArrayNormalizationStep::RebuildField {
                source,
                target,
                name: source.name(),
            })
        }
        // The parent Map array observation already covers its entries wrapper.
        HydrateEvent::MapEntries { .. } => Ok(()),
        HydrateEvent::ValueStructure { path, event } => {
            convert_path(path, &mut path_storage);
            observe(ArrayNormalizationStep::ValueStructure {
                path: &path_storage[..path.len()],
                event,
            })
        }
        HydrateEvent::OpaqueOperationFinished => {
            observe(ArrayNormalizationStep::OpaqueOperationFinished)
        }
    }
}

/// The caller supplies its operation context and real creation/source custody.
/// Growth remains the caller's existing capacity policy; it is never translated
/// to one metadata request per payload buffer.
pub fn normalize_typed_array_plain_observed<E, F>(
    source: &ArrayRef,
    binding: &TypedValueBinding,
    context: &mut ValueConversionContext,
    storage: &ValueTypeStorageLifetime,
    growth: &mut novarocks_value_arrays::control::Growth<'_, E>,
    work: &mut novarocks_value_arrays::control::Work<'_, E>,
    observe: &mut F,
) -> Result<ArrayRef, E>
where
    E: From<ArrayProjectionFailure>,
    F: for<'a> FnMut(ArrayNormalizationStep<'a>) -> Result<(), E>,
{
    let mut observer = |event: HydrateEvent<'_>| observe_value_hydration(event, observe);
    let plan = HydratePlan::plan_with_options_and_storage_observed(
        source,
        binding,
        HydrateOptions::default(),
        context,
        storage,
        &mut |_, _| false,
        growth,
        work,
        &mut observer,
    )
    .map_err(value_failure)?;
    plan.require_complete()
        .map_err(|source| E::from(ArrayProjectionFailure::Value(source)))?;
    plan.execute_observed(context, growth, work, &mut observer)
        .map_err(value_failure)
}
