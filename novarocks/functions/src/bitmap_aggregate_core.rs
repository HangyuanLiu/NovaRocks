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
//! ONE original bitmap aggregate state/update/merge math, independent of Execution pointers/tracker.
use crate::aggregate_scalar::{ScalarStateAllocator, ScalarStateError};
use crate::bitmap_value::{BitmapDecodePort, LegacyBitmapPort};
use arrow_array::*;
use arrow_schema::DataType;
use hashbrown::hash_map::DefaultHashBuilder;
use std::collections::BTreeSet;
type BitmapValues = BTreeSet<u64>;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BitmapOperation {
    Count,
    BitmapProjection,
}
pub struct BitmapState<A: ScalarStateAllocator> {
    allocator: A,
    values: hashbrown::HashSet<u64, DefaultHashBuilder, A>,
    /// Whether this state has observed at least one non-null input row.
    /// SQL aggregate semantics: a group whose inputs are all NULL must
    /// finalize to NULL, not to the per-element identity (empty bitmap / 0).
    /// Tracked separately from `values` because a non-null empty BITMAP input
    /// (e.g. `bitmap_empty()`) still marks the group as non-NULL.
    pub has_value: bool,
}

impl<A: ScalarStateAllocator> BitmapState<A> {
    pub fn new(allocator: A) -> Self {
        Self {
            values: hashbrown::HashSet::with_hasher_in(
                DefaultHashBuilder::default(),
                allocator.clone(),
            ),
            allocator,
            has_value: false,
        }
    }
}

impl<A: ScalarStateAllocator> BitmapState<A> {
    pub fn insert(&mut self, value: u64) -> Result<(), ScalarStateError> {
        if self.values.contains(&value) {
            return Ok(());
        }
        self.values.try_reserve(1).map_err(|_| {
            self.allocator
                .scalar_allocation_error("reserve bitmap hash set")
        })?;
        self.values.insert(value);
        Ok(())
    }

    pub(crate) fn value_count(&self) -> usize {
        self.values.len()
    }
    pub fn encoded_values(&self) -> BitmapValues {
        self.values.iter().copied().collect()
    }
}

impl<A: ScalarStateAllocator> BitmapState<A> {
    pub fn observe(&mut self) {
        self.has_value = true;
    }
    pub fn cardinality(&self) -> Result<Option<i64>, CardinalityFailure> {
        if !self.has_value {
            return Ok(None);
        }
        i64::try_from(self.values.len())
            .map(Some)
            .map_err(|_| CardinalityFailure(self.values.len()))
    }
}
#[derive(Clone, Copy, Debug)]
pub struct CardinalityFailure(pub usize);
impl std::fmt::Display for CardinalityFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bitmap_union_int cardinality overflow: {}", self.0)
    }
}

#[derive(Clone, Copy)]
pub enum InputFailure<'a> {
    Downcast(&'static str),
    Unsupported(&'a DataType),
}
impl std::fmt::Display for InputFailure<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Downcast(message) => f.write_str(message),
            Self::Unsupported(ty) => write!(
                f,
                "bitmap aggregate expects BOOLEAN/INTEGER/VARCHAR/BINARY input, got {:?}",
                ty
            ),
        }
    }
}
/// Typed original state mutation seam, not a decoder or evaluator callback.
pub trait BitmapAggregatePort: BitmapDecodePort {
    fn input_failure(&mut self, failure: InputFailure<'_>) -> Self::Error;
    fn observe(&mut self, ordinal: usize) -> Result<(), Self::Error>;
    fn insert(&mut self, ordinal: usize, value: u64) -> Result<(), Self::Error>;
}
pub fn update<P: BitmapAggregatePort, I: Iterator<Item = usize> + Clone>(
    operation: BitmapOperation,
    array: &ArrayRef,
    rows: I,
    port: &mut P,
) -> Result<(), P::Error> {
    let include_negative = matches!(operation, BitmapOperation::Count);

    macro_rules! update_signed {
        ($arr_ty:ty) => {{
            let arr = array.as_any().downcast_ref::<$arr_ty>().ok_or_else(|| {
                port.input_failure(InputFailure::Downcast(
                    "failed to downcast signed integer array",
                ))
            })?;
            for (ordinal, row) in rows.clone().enumerate() {
                port.step()?;
                if arr.is_null(row) {
                    continue;
                }
                let raw = i64::from(arr.value(row));
                if !include_negative && raw < 0 {
                    continue;
                }
                port.observe(ordinal)?;
                port.insert(ordinal, raw as u64)?;
            }
            Ok(())
        }};
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    port.input_failure(InputFailure::Downcast("failed to downcast to BooleanArray"))
                })?;
            for (ordinal, row) in rows.clone().enumerate() {
                port.step()?;
                if arr.is_null(row) {
                    continue;
                }
                port.observe(ordinal)?;
                port.insert(ordinal, if arr.value(row) { 1 } else { 0 })?;
            }
            Ok(())
        }
        DataType::Int8 => update_signed!(Int8Array),
        DataType::Int16 => update_signed!(Int16Array),
        DataType::Int32 => update_signed!(Int32Array),
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                port.input_failure(InputFailure::Downcast("failed to downcast to Int64Array"))
            })?;
            for (ordinal, row) in rows.clone().enumerate() {
                port.step()?;
                if arr.is_null(row) {
                    continue;
                }
                let raw = arr.value(row);
                if !include_negative && raw < 0 {
                    continue;
                }
                port.observe(ordinal)?;
                port.insert(ordinal, raw as u64)?;
            }
            Ok(())
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().ok_or_else(|| {
                port.input_failure(InputFailure::Downcast("failed to downcast to UInt8Array"))
            })?;
            for (ordinal, row) in rows.clone().enumerate() {
                port.step()?;
                if arr.is_null(row) {
                    continue;
                }
                port.observe(ordinal)?;
                port.insert(ordinal, u64::from(arr.value(row)))?;
            }
            Ok(())
        }
        DataType::UInt16 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| {
                    port.input_failure(InputFailure::Downcast("failed to downcast to UInt16Array"))
                })?;
            for (ordinal, row) in rows.clone().enumerate() {
                port.step()?;
                if arr.is_null(row) {
                    continue;
                }
                port.observe(ordinal)?;
                port.insert(ordinal, u64::from(arr.value(row)))?;
            }
            Ok(())
        }
        DataType::UInt32 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| {
                    port.input_failure(InputFailure::Downcast("failed to downcast to UInt32Array"))
                })?;
            for (ordinal, row) in rows.clone().enumerate() {
                port.step()?;
                if arr.is_null(row) {
                    continue;
                }
                port.observe(ordinal)?;
                port.insert(ordinal, u64::from(arr.value(row)))?;
            }
            Ok(())
        }
        DataType::UInt64 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| {
                    port.input_failure(InputFailure::Downcast("failed to downcast to UInt64Array"))
                })?;
            for (ordinal, row) in rows.clone().enumerate() {
                port.step()?;
                if arr.is_null(row) {
                    continue;
                }
                port.observe(ordinal)?;
                port.insert(ordinal, arr.value(row))?;
            }
            Ok(())
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    port.input_failure(InputFailure::Downcast("failed to downcast to StringArray"))
                })?;
            for (ordinal, row) in rows.clone().enumerate() {
                port.step()?;
                if arr.is_null(row) {
                    continue;
                }
                let Ok(value) = arr.value(row).trim().parse::<i128>() else {
                    continue;
                };
                if !include_negative && value < 0 {
                    continue;
                }
                if value < i64::MIN as i128 || value > u64::MAX as i128 {
                    continue;
                }
                port.observe(ordinal)?;
                if value < 0 {
                    port.insert(ordinal, (value as i64) as u64)?;
                } else {
                    port.insert(ordinal, value as u64)?;
                }
            }
            Ok(())
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    port.input_failure(InputFailure::Downcast(
                        "failed to downcast to LargeStringArray",
                    ))
                })?;
            for (ordinal, row) in rows.clone().enumerate() {
                port.step()?;
                if arr.is_null(row) {
                    continue;
                }
                let Ok(value) = arr.value(row).trim().parse::<i128>() else {
                    continue;
                };
                if !include_negative && value < 0 {
                    continue;
                }
                if value < i64::MIN as i128 || value > u64::MAX as i128 {
                    continue;
                }
                port.observe(ordinal)?;
                if value < 0 {
                    port.insert(ordinal, (value as i64) as u64)?;
                } else {
                    port.insert(ordinal, value as u64)?;
                }
            }
            Ok(())
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    port.input_failure(InputFailure::Downcast("failed to downcast to BinaryArray"))
                })?;
            for (ordinal, row) in rows.clone().enumerate() {
                port.step()?;
                if arr.is_null(row) {
                    continue;
                }
                match crate::bitmap_value::decode_bitmap_with_port(arr.value(row), port) {
                    Ok(decoded) => {
                        port.observe(ordinal)?;
                        for value in decoded {
                            port.step()?;
                            port.insert(ordinal, value)?;
                        }
                        continue;
                    }
                    Err(error) if P::is_data(&error) => {}
                    Err(error) => return Err(error),
                }
                let Ok(text) = std::str::from_utf8(arr.value(row)) else {
                    continue;
                };
                let Ok(value) = text.trim().parse::<i128>() else {
                    continue;
                };
                if !include_negative && value < 0 {
                    continue;
                }
                if value < i64::MIN as i128 || value > u64::MAX as i128 {
                    continue;
                }
                port.observe(ordinal)?;
                if value < 0 {
                    port.insert(ordinal, (value as i64) as u64)?;
                } else {
                    port.insert(ordinal, value as u64)?;
                }
            }
            Ok(())
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    port.input_failure(InputFailure::Downcast(
                        "failed to downcast to LargeBinaryArray",
                    ))
                })?;
            for (ordinal, row) in rows.clone().enumerate() {
                port.step()?;
                if arr.is_null(row) {
                    continue;
                }
                match crate::bitmap_value::decode_bitmap_with_port(arr.value(row), port) {
                    Ok(decoded) => {
                        port.observe(ordinal)?;
                        for value in decoded {
                            port.step()?;
                            port.insert(ordinal, value)?;
                        }
                        continue;
                    }
                    Err(error) if P::is_data(&error) => {}
                    Err(error) => return Err(error),
                }
                let Ok(text) = std::str::from_utf8(arr.value(row)) else {
                    continue;
                };
                let Ok(value) = text.trim().parse::<i128>() else {
                    continue;
                };
                if !include_negative && value < 0 {
                    continue;
                }
                if value < i64::MIN as i128 || value > u64::MAX as i128 {
                    continue;
                }
                port.observe(ordinal)?;
                if value < 0 {
                    port.insert(ordinal, (value as i64) as u64)?;
                } else {
                    port.insert(ordinal, value as u64)?;
                }
            }
            Ok(())
        }
        other => Err(port.input_failure(InputFailure::Unsupported(other))),
    }
}
pub fn merge<P: BitmapAggregatePort, I: Iterator<Item = usize>>(
    array: &BinaryArray,
    rows: I,
    port: &mut P,
) -> Result<(), P::Error> {
    for (ordinal, row) in rows.enumerate() {
        port.step()?;
        if array.is_null(row) {
            continue;
        }
        let decoded = crate::bitmap_value::decode_bitmap_with_port(array.value(row), port)?;
        port.observe(ordinal)?;
        for value in decoded {
            port.step()?;
            port.insert(ordinal, value)?;
        }
    }
    Ok(())
}
#[derive(Clone, Copy)]
pub enum StateMutation {
    Observe,
    Insert(u64),
}
pub struct LegacyRows<W> {
    writer: W,
    decoder: LegacyBitmapPort,
}
impl<W: FnMut(usize, StateMutation) -> Result<(), String>> LegacyRows<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            decoder: LegacyBitmapPort,
        }
    }
}
impl<W: FnMut(usize, StateMutation) -> Result<(), String>> BitmapDecodePort for LegacyRows<W> {
    type Error = String;
    fn data(&mut self, m: std::fmt::Arguments<'_>) -> String {
        self.decoder.data(m)
    }
    fn is_data(error: &String) -> bool {
        LegacyBitmapPort::is_data(error)
    }
    fn step(&mut self) -> Result<(), String> {
        self.decoder.step()
    }
    fn boundary(&mut self) -> Result<(), String> {
        self.decoder.boundary()
    }
    fn before_render(&mut self, n: usize) -> Result<(), String> {
        self.decoder.before_render(n)
    }
    fn before_tree_insert(&mut self, n: usize) -> Result<(), String> {
        self.decoder.before_tree_insert(n)
    }
    fn before_tree_collection(&mut self, n: usize) -> Result<(), String> {
        self.decoder.before_tree_collection(n)
    }
    fn before_roaring(&mut self, n: usize) -> Result<(), String> {
        self.decoder.before_roaring(n)
    }
    fn before_u32_collection(&mut self, n: u64) -> Result<(), String> {
        self.decoder.before_u32_collection(n)
    }
}
impl<W: FnMut(usize, StateMutation) -> Result<(), String>> BitmapAggregatePort for LegacyRows<W> {
    fn input_failure(&mut self, f: InputFailure<'_>) -> String {
        f.to_string()
    }
    fn observe(&mut self, n: usize) -> Result<(), String> {
        (self.writer)(n, StateMutation::Observe)
    }
    fn insert(&mut self, n: usize, v: u64) -> Result<(), String> {
        (self.writer)(n, StateMutation::Insert(v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    #[test]
    fn bitmap_aggregate_original_core_signed_policy_and_actual_address_order() {
        let a = Arc::new(Int32Array::from(vec![Some(-1), Some(7), None])) as ArrayRef;
        for operation in [BitmapOperation::Count, BitmapOperation::BitmapProjection] {
            let mut mutations = Vec::new();
            let mut port = LegacyRows::new(|ordinal, mutation| {
                mutations.push((
                    ordinal,
                    match mutation {
                        StateMutation::Observe => None,
                        StateMutation::Insert(value) => Some(value),
                    },
                ));
                Ok(())
            });
            update(operation, &a, [1usize, 0, 2].into_iter(), &mut port).unwrap();
            let expected = if operation == BitmapOperation::Count {
                vec![(0, None), (0, Some(7)), (1, None), (1, Some(u64::MAX))]
            } else {
                vec![(0, None), (0, Some(7))]
            };
            assert_eq!(mutations, expected);
        }
    }
    #[test]
    fn bitmap_aggregate_original_core_binary_update_skip_vs_merge_full_data() {
        let a = Arc::new(BinaryArray::from(vec![
            Some(b"bad".as_slice()),
            Some([0u8].as_slice()),
            Some(b"-1".as_slice()),
        ])) as ArrayRef;
        let mut mutations = Vec::new();
        let mut port = LegacyRows::new(|ordinal, mutation| {
            mutations.push((
                ordinal,
                match mutation {
                    StateMutation::Observe => None,
                    StateMutation::Insert(value) => Some(value),
                },
            ));
            Ok(())
        });
        update(BitmapOperation::Count, &a, 0..3, &mut port).unwrap();
        assert_eq!(mutations, vec![(1, None), (2, None), (2, Some(u64::MAX))]);
        let a = a.as_any().downcast_ref::<BinaryArray>().unwrap();
        let expected = crate::bitmap_value::decode_bitmap(b"bad").unwrap_err();
        let mut port = LegacyRows::new(|_, _| Ok(()));
        assert_eq!(merge(a, 0..3, &mut port).unwrap_err(), expected);
    }
    #[test]
    fn bitmap_aggregate_original_core_unsupported_empty_type_is_not_null_success() {
        let a = Arc::new(NullArray::new(0)) as ArrayRef;
        let mut port = LegacyRows::new(|_, _| panic!("unsupported carrier cannot mutate state"));
        assert_eq!(
            update(BitmapOperation::Count, &a, 0..0, &mut port).unwrap_err(),
            "bitmap aggregate expects BOOLEAN/INTEGER/VARCHAR/BINARY input, got Null"
        );
    }
}
