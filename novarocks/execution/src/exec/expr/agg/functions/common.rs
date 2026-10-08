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
use super::super::*;
use arrow::array::{
    BinaryArray, BooleanArray, BooleanBuilder, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryArray, ListArray, MapArray, StringArray, StringBuilder, StructArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow_buffer::i256;
use std::cmp::Ordering;

use crate::exec::expr::agg::{AggregateAllocator, AggregateVec, aggregate_bytes};
use novarocks_types::largeint;

pub(in crate::exec::expr::agg) fn build_bool_array(
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, String> {
    let mut builder = BooleanBuilder::new();
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const BoolState) };
        if state.has_value {
            builder.append_value(state.value);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[derive(Debug)]
pub(super) struct TrackedUtf8State {
    pub(super) value: Option<AggregateVec<u8>>,
    pub(super) allocator: AggregateAllocator,
}

impl TrackedUtf8State {
    pub(super) fn new(allocator: AggregateAllocator) -> Self {
        Self {
            value: None,
            allocator,
        }
    }

    pub(super) fn replace(&mut self, value: &str) -> Result<(), String> {
        let replacement = aggregate_bytes(self.allocator.clone(), value.as_bytes())?;
        self.value = Some(replacement);
        Ok(())
    }
}

pub(in crate::exec::expr::agg) fn build_utf8_array(
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, String> {
    let mut builder = StringBuilder::new();
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const TrackedUtf8State) };
        match &state.value {
            Some(value) => {
                builder.append_value(std::str::from_utf8(value).map_err(|error| error.to_string())?)
            }
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

pub(in crate::exec::expr::agg) fn build_date32_array(
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, String> {
    let mut values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const I32State) };
        values.push(state.has_value.then_some(state.value));
    }
    Ok(Arc::new(Date32Array::from(values)))
}

pub(in crate::exec::expr::agg) fn build_timestamp_array(
    offset: usize,
    group_states: &[AggStatePtr],
    output_type: &DataType,
) -> Result<ArrayRef, String> {
    let (unit, tz) = match output_type {
        DataType::Timestamp(unit, tz) => (*unit, tz.as_deref().map(|s| s.to_string())),
        other => return Err(format!("timestamp output type mismatch: {:?}", other)),
    };
    let mut values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const I64State) };
        values.push(state.has_value.then_some(state.value));
    }
    let array: ArrayRef = match unit {
        TimeUnit::Second => {
            let array = TimestampSecondArray::from(values);
            if let Some(tz) = tz {
                Arc::new(array.with_timezone(tz))
            } else {
                Arc::new(array)
            }
        }
        TimeUnit::Millisecond => {
            let array = TimestampMillisecondArray::from(values);
            if let Some(tz) = tz {
                Arc::new(array.with_timezone(tz))
            } else {
                Arc::new(array)
            }
        }
        TimeUnit::Microsecond => {
            let array = TimestampMicrosecondArray::from(values);
            if let Some(tz) = tz {
                Arc::new(array.with_timezone(tz))
            } else {
                Arc::new(array)
            }
        }
        TimeUnit::Nanosecond => {
            let array = TimestampNanosecondArray::from(values);
            if let Some(tz) = tz {
                Arc::new(array.with_timezone(tz))
            } else {
                Arc::new(array)
            }
        }
    };
    Ok(array)
}

pub(in crate::exec::expr::agg) fn build_decimal128_array(
    offset: usize,
    group_states: &[AggStatePtr],
    output_type: &DataType,
) -> Result<ArrayRef, String> {
    let (precision, scale) = match output_type {
        DataType::Decimal128(precision, scale) => (*precision, *scale),
        other => return Err(format!("decimal output type mismatch: {:?}", other)),
    };
    let mut values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const I128State) };
        values.push(state.has_value.then_some(state.value));
    }
    let array = Decimal128Array::from(values)
        .with_precision_and_scale(precision, scale)
        .map_err(|e| e.to_string())?;
    Ok(Arc::new(array))
}

pub(in crate::exec::expr::agg) fn build_decimal256_array(
    offset: usize,
    group_states: &[AggStatePtr],
    output_type: &DataType,
) -> Result<ArrayRef, String> {
    let (precision, scale) = match output_type {
        DataType::Decimal256(precision, scale) => (*precision, *scale),
        other => return Err(format!("decimal256 output type mismatch: {:?}", other)),
    };
    let mut values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const I256State) };
        values.push(state.has_value.then_some(state.value));
    }
    let array = Decimal256Array::from(values)
        .with_precision_and_scale(precision, scale)
        .map_err(|e| e.to_string())?;
    Ok(Arc::new(array))
}

pub(in crate::exec::expr::agg) fn build_largeint_array(
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, String> {
    let mut values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const I128State) };
        values.push(state.has_value.then_some(state.value));
    }
    largeint::array_from_i128(&values)
}

pub use novarocks_functions::aggregate_scalar::AggScalarValue;
pub(super) type TrackedAggScalarValue =
    novarocks_functions::aggregate_scalar::TrackedAggScalarValue<AggregateAllocator>;

pub(super) fn aggregate_vec_with_capacity<T>(
    allocator: &AggregateAllocator,
    capacity: usize,
    operation: &str,
) -> Result<AggregateVec<T>, String> {
    let mut values = AggregateVec::new_in(allocator.clone());
    values
        .try_reserve_exact(capacity)
        .map_err(|_| allocator.allocation_error(operation))?;
    Ok(values)
}

pub(super) fn tracked_scalar_from_array(
    array: &ArrayRef,
    row: usize,
    allocator: &AggregateAllocator,
) -> Result<Option<TrackedAggScalarValue>, String> {
    novarocks_functions::aggregate_scalar::tracked_scalar_from_array(
        array,
        row,
        allocator,
        &mut novarocks_functions::aggregate_scalar::ScalarWork::new(None),
    )
    .map_err(|error| error.to_string())
}

#[cfg(test)]
pub(super) fn tracked_scalar_from_value(
    value: AggScalarValue,
    allocator: &AggregateAllocator,
) -> Result<TrackedAggScalarValue, String> {
    Ok(match value {
        AggScalarValue::Bool(value) => TrackedAggScalarValue::Bool(value),
        AggScalarValue::Int64(value) => TrackedAggScalarValue::Int64(value),
        AggScalarValue::Float64(value) => TrackedAggScalarValue::Float64(value),
        AggScalarValue::Utf8(value) => {
            TrackedAggScalarValue::Utf8(aggregate_bytes(allocator.clone(), value.as_bytes())?)
        }
        AggScalarValue::Date32(value) => TrackedAggScalarValue::Date32(value),
        AggScalarValue::Timestamp(value) => TrackedAggScalarValue::Timestamp(value),
        AggScalarValue::Decimal128(value) => TrackedAggScalarValue::Decimal128(value),
        AggScalarValue::Decimal256(value) => TrackedAggScalarValue::Decimal256(value),
        AggScalarValue::Binary(value) => {
            TrackedAggScalarValue::Binary(aggregate_bytes(allocator.clone(), &value)?)
        }
        AggScalarValue::Struct(values) => TrackedAggScalarValue::Struct(
            tracked_optional_values_from_values(values, allocator, "struct")?,
        ),
        AggScalarValue::List(values) => TrackedAggScalarValue::List(
            tracked_optional_values_from_values(values, allocator, "list")?,
        ),
        AggScalarValue::Map(entries) => {
            let mut tracked = aggregate_vec_with_capacity(
                allocator,
                entries.len(),
                "reserve aggregate map scalar",
            )?;
            for (key, value) in entries {
                tracked.push((
                    key.map(|value| tracked_scalar_from_value(value, allocator))
                        .transpose()?,
                    value
                        .map(|value| tracked_scalar_from_value(value, allocator))
                        .transpose()?,
                ));
            }
            TrackedAggScalarValue::Map(tracked)
        }
    })
}

#[cfg(test)]
fn tracked_optional_values_from_values(
    values: Vec<Option<AggScalarValue>>,
    allocator: &AggregateAllocator,
    kind: &str,
) -> Result<AggregateVec<Option<TrackedAggScalarValue>>, String> {
    let operation = match kind {
        "struct" => "reserve aggregate struct scalar",
        "list" => "reserve aggregate list scalar",
        _ => "reserve aggregate nested scalar",
    };
    let mut tracked = aggregate_vec_with_capacity(allocator, values.len(), operation)?;
    for value in values {
        tracked.push(
            value
                .map(|value| tracked_scalar_from_value(value, allocator))
                .transpose()?,
        );
    }
    Ok(tracked)
}

pub(super) fn tracked_scalar_to_output(
    value: &TrackedAggScalarValue,
) -> Result<AggScalarValue, String> {
    novarocks_functions::aggregate_scalar::tracked_scalar_to_output(
        value,
        &mut novarocks_functions::aggregate_scalar::ScalarWork::new(None),
    )
    .map_err(|error| error.to_string())
}

pub(super) fn compare_tracked_scalar_values(
    left: &TrackedAggScalarValue,
    right: &TrackedAggScalarValue,
) -> Result<Ordering, String> {
    novarocks_functions::aggregate_scalar::compare_tracked_scalar_values(
        left,
        right,
        &mut novarocks_functions::aggregate_scalar::ScalarWork::new(None),
    )
    .map_err(|error| error.to_string())
}

pub(super) fn tracked_key_fingerprint(
    key: &TrackedAggScalarValue,
    allocator: &AggregateAllocator,
) -> Result<AggregateVec<u8>, String> {
    novarocks_functions::aggregate_scalar_fingerprint::tracked_key_fingerprint(
        key,
        allocator,
        &mut novarocks_functions::aggregate_scalar::ScalarWork::new(None),
    )
    .map_err(|error| error.to_string())
}
pub(super) fn tracked_optional_key_fingerprint(
    value: &Option<TrackedAggScalarValue>,
    allocator: &AggregateAllocator,
) -> Result<AggregateVec<u8>, String> {
    novarocks_functions::aggregate_scalar_fingerprint::tracked_optional_key_fingerprint(
        value,
        allocator,
        &mut novarocks_functions::aggregate_scalar::ScalarWork::new(None),
    )
    .map_err(|error| error.to_string())
}

/// Heap bytes owned by a scalar value, excluding the inline enum body.
///
/// This walks a newly materialized value once. Aggregate states that can hold
/// an unbounded number of values cache the resulting sum so their
/// `retained_bytes` implementation remains O(1).
#[cfg(test)]
pub(super) fn scalar_heap_bytes(value: &AggScalarValue) -> usize {
    match value {
        AggScalarValue::Utf8(value) => value.capacity(),
        AggScalarValue::Binary(value) => value.capacity(),
        AggScalarValue::Struct(values) | AggScalarValue::List(values) => values
            .capacity()
            .saturating_mul(std::mem::size_of::<Option<AggScalarValue>>())
            .saturating_add(
                values
                    .iter()
                    .flatten()
                    .map(scalar_heap_bytes)
                    .sum::<usize>(),
            ),
        AggScalarValue::Map(entries) => entries
            .capacity()
            .saturating_mul(std::mem::size_of::<(
                Option<AggScalarValue>,
                Option<AggScalarValue>,
            )>())
            .saturating_add(
                entries
                    .iter()
                    .flat_map(|(key, value)| [key.as_ref(), value.as_ref()])
                    .flatten()
                    .map(scalar_heap_bytes)
                    .sum::<usize>(),
            ),
        _ => 0,
    }
}

pub fn scalar_from_array(array: &ArrayRef, row: usize) -> Result<Option<AggScalarValue>, String> {
    novarocks_functions::aggregate_scalar::scalar_from_array(
        array,
        row,
        &mut novarocks_functions::aggregate_scalar::ScalarWork::new(None),
    )
    .map_err(|error| error.to_string())
}

pub(in crate::exec::expr::agg) fn scalar_to_string(
    value: &AggScalarValue,
    data_type: &DataType,
) -> Result<String, String> {
    novarocks_functions::aggregate_format::scalar_to_string(
        value,
        data_type,
        &mut novarocks_functions::aggregate_scalar::ScalarWork::new(None),
    )
    .map_err(|error| error.to_string())
}

pub fn compare_scalar_values(
    left: &AggScalarValue,
    right: &AggScalarValue,
) -> Result<Ordering, String> {
    novarocks_functions::aggregate_scalar_fingerprint::compare_scalar_values(
        left,
        right,
        &mut novarocks_functions::aggregate_scalar::ScalarWork::new(None),
    )
    .map_err(|error| error.to_string())
}
pub(in crate::exec::expr::agg) fn key_fingerprint(key: &AggScalarValue) -> Vec<u8> {
    novarocks_functions::aggregate_scalar_fingerprint::key_fingerprint(
        key,
        &mut novarocks_functions::aggregate_scalar::ScalarWork::new(None),
    )
    .expect("legacy fingerprint observer cannot fail")
}

pub fn build_scalar_array(
    output_type: &DataType,
    values: Vec<Option<AggScalarValue>>,
) -> Result<ArrayRef, String> {
    novarocks_functions::aggregate_scalar::build_scalar_array(
        output_type,
        values,
        &mut novarocks_functions::aggregate_scalar::ScalarWork::new(None),
    )
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod retained_bytes_tests {
    use super::*;
    use crate::runtime::mem_tracker::MemTracker;
    use arrow::datatypes::{Field, Fields};

    #[test]
    fn scalar_heap_bytes_counts_nested_capacities_once() {
        let mut text = String::with_capacity(64);
        text.push_str("value");
        let text_capacity = text.capacity();
        let mut binary = Vec::with_capacity(32);
        binary.extend_from_slice(b"bytes");
        let binary_capacity = binary.capacity();
        let mut items = Vec::with_capacity(8);
        items.push(Some(AggScalarValue::Utf8(text)));
        items.push(Some(AggScalarValue::Binary(binary)));
        let items_capacity = items.capacity();
        let value = AggScalarValue::List(items);

        assert_eq!(
            scalar_heap_bytes(&value),
            items_capacity * std::mem::size_of::<Option<AggScalarValue>>()
                + text_capacity
                + binary_capacity
        );
    }

    #[test]
    fn tracked_scalar_owns_nested_string_binary_list_struct_and_map_allocations() {
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let map_entry_type = DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Binary, true),
        ]));
        let map_type = DataType::Map(
            Arc::new(Field::new("entries", map_entry_type, false)),
            false,
        );
        let struct_type = DataType::Struct(Fields::from(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("bytes", DataType::Binary, true),
            Field::new("items", list_type.clone(), true),
            Field::new("attributes", map_type.clone(), true),
        ]));
        let input_value = AggScalarValue::Struct(vec![
            Some(AggScalarValue::Utf8("root".to_string())),
            Some(AggScalarValue::Binary(vec![1, 2, 3])),
            Some(AggScalarValue::List(vec![
                Some(AggScalarValue::Utf8("first".to_string())),
                None,
            ])),
            Some(AggScalarValue::Map(vec![(
                Some(AggScalarValue::Utf8("key".to_string())),
                Some(AggScalarValue::Binary(vec![4, 5])),
            )])),
        ]);
        let array = build_scalar_array(&struct_type, vec![Some(input_value)]).unwrap();
        let tracker = MemTracker::new_root("nested-tracked-scalar-test");
        let allocator = AggregateAllocator::new(Arc::clone(&tracker));

        let tracked = tracked_scalar_from_array(&array, 0, &allocator)
            .unwrap()
            .unwrap();
        assert!(tracker.current() > 0);
        let output = tracked_scalar_to_output(&tracked).unwrap();
        let AggScalarValue::Struct(fields) = output else {
            panic!("expected struct output");
        };
        assert!(matches!(
            fields[0].as_ref(),
            Some(AggScalarValue::Utf8(value)) if value == "root"
        ));
        assert!(matches!(
            fields[1].as_ref(),
            Some(AggScalarValue::Binary(value)) if value == &[1, 2, 3]
        ));
        assert!(matches!(fields[2], Some(AggScalarValue::List(_))));
        assert!(matches!(fields[3], Some(AggScalarValue::Map(_))));

        drop(tracked);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn tracked_nested_scalar_oom_releases_partial_recursive_allocations() {
        let struct_type =
            DataType::Struct(Fields::from(vec![Field::new("text", DataType::Utf8, true)]));
        let array = build_scalar_array(
            &struct_type,
            vec![Some(AggScalarValue::Struct(vec![Some(
                AggScalarValue::Utf8("too-large".to_string()),
            )]))],
        )
        .unwrap();
        let tracker = MemTracker::new_root("nested-tracked-scalar-oom-test");
        tracker.install_limit_once(1).unwrap();
        let allocator = AggregateAllocator::new(Arc::clone(&tracker));

        let error = tracked_scalar_from_array(&array, 0, &allocator).unwrap_err();
        assert!(error.contains("ResourceExhausted"));
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn tracked_fingerprint_is_exactly_reserved_and_released() {
        let value = AggScalarValue::Struct(vec![
            Some(AggScalarValue::Int64(7)),
            Some(AggScalarValue::Utf8("tracked".to_string())),
        ]);
        let data_type = DataType::Struct(Fields::from(vec![
            Field::new("number", DataType::Int64, false),
            Field::new("text", DataType::Utf8, false),
        ]));
        let array = build_scalar_array(&data_type, vec![Some(value.clone())]).unwrap();
        let tracker = MemTracker::new_root("tracked-fingerprint-test");
        let allocator = AggregateAllocator::new(Arc::clone(&tracker));
        let tracked = tracked_scalar_from_array(&array, 0, &allocator)
            .unwrap()
            .unwrap();
        let state_bytes = tracker.current();

        let fingerprint = tracked_key_fingerprint(&tracked, &allocator).unwrap();
        assert_eq!(fingerprint.as_slice(), key_fingerprint(&value));
        assert_eq!(fingerprint.len(), fingerprint.capacity());
        assert!(tracker.current() > state_bytes);

        drop(fingerprint);
        drop(tracked);
        assert_eq!(tracker.current(), 0);
    }
}
