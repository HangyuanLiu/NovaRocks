// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Exact visible-tuple content encoding. This never uses SQL key normalization.

use crate::exec::expr::agg::AggregateVec;
use arrow::array::*;
use arrow::datatypes::*;
use novarocks_type_contract::{NATIVE_RESULT_CONTENT_V1_CANONICAL_BYTES, ResultContentEquivalence};
use std::sync::Arc;

/// Immutable encoder for one frozen visible output schema. A key contains the
/// schema interpretation followed by logical values, never offsets or dictionary IDs.
#[derive(Clone, Debug)]
pub struct ContentKeyEncoder {
    storage: Arc<ContentKeyStorage>,
}
#[derive(Debug)]
enum ContentKeyStorage {
    Standard {
        types: Vec<DataType>,
        prefix: Vec<u8>,
    },
    Tracked {
        types: AggregateVec<DataType>,
        prefix: AggregateVec<u8>,
    },
}
impl ContentKeyEncoder {
    pub fn try_new(schema: &Schema) -> Result<Self, String> {
        Self::try_new_types(
            schema
                .fields()
                .iter()
                .map(|field| field.data_type().clone())
                .collect(),
        )
    }
    pub fn try_new_types(types: Vec<DataType>) -> Result<Self, String> {
        let length = prefix_len(&types)?;
        let mut prefix = Vec::new();
        prefix
            .try_reserve_exact(length)
            .map_err(|_| "result content schema allocation was refused".to_string())?;
        write_prefix(&types, &mut |part| {
            prefix.extend_from_slice(part);
            Ok(())
        })?;
        Ok(Self {
            storage: Arc::new(ContentKeyStorage::Standard { types, prefix }),
        })
    }
    pub(crate) fn try_new_types_tracked(types: AggregateVec<DataType>) -> Result<Self, String> {
        let allocator = types.allocator().clone();
        let length = prefix_len(&types)?;
        let mut prefix = AggregateVec::new_in(allocator.clone());
        prefix
            .try_reserve_exact(length)
            .map_err(|_| allocator.allocation_error("result content schema prefix"))?;
        write_prefix(&types, &mut |part| {
            prefix.extend_from_slice(part);
            Ok(())
        })?;
        Ok(Self {
            storage: Arc::new(ContentKeyStorage::Tracked { types, prefix }),
        })
    }
    fn types(&self) -> &[DataType] {
        match self.storage.as_ref() {
            ContentKeyStorage::Standard { types, .. } => types,
            ContentKeyStorage::Tracked { types, .. } => types,
        }
    }
    fn prefix(&self) -> &[u8] {
        match self.storage.as_ref() {
            ContentKeyStorage::Standard { prefix, .. } => prefix,
            ContentKeyStorage::Tracked { prefix, .. } => prefix,
        }
    }
    pub fn encoded_prefix_len(&self) -> usize {
        self.prefix().len()
    }
    /// Counts the exact encoded bytes without allocating a tuple buffer.
    pub fn encoded_row_len(&self, columns: &[ArrayRef], row: usize) -> Result<usize, String> {
        let mut size = self.prefix().len();
        self.visit_row(columns, row, &mut |bytes| {
            size = size
                .checked_add(bytes.len())
                .ok_or_else(|| "result content key size overflow".to_string())?;
            Ok(())
        })?;
        Ok(size)
    }

    pub fn encode_row(&self, columns: &[ArrayRef], row: usize) -> Result<Vec<u8>, String> {
        let mut key = Vec::new();
        self.encode_row_into(columns, row, &mut key)?;
        Ok(key)
    }

    /// Reuses caller-owned scratch storage, which holds only one tuple's key.
    pub fn encode_row_into(
        &self,
        columns: &[ArrayRef],
        row: usize,
        key: &mut Vec<u8>,
    ) -> Result<(), String> {
        key.clear();
        if columns.len() != self.types().len() {
            return Err("result content columns do not match the frozen schema".to_owned());
        }
        key.extend_from_slice(self.prefix());
        self.visit_row(columns, row, &mut |bytes| {
            key.extend_from_slice(bytes);
            Ok(())
        })?;
        Ok(())
    }
    /// Writes canonical bytes without allocating a temporary tuple buffer.
    pub fn encode_row_to(
        &self,
        columns: &[ArrayRef],
        row: usize,
        write: &mut impl FnMut(&[u8]) -> Result<(), String>,
    ) -> Result<(), String> {
        write(self.prefix())?;
        self.visit_row(columns, row, write)
    }
    fn visit_row(
        &self,
        columns: &[ArrayRef],
        row: usize,
        write: &mut impl FnMut(&[u8]) -> Result<(), String>,
    ) -> Result<(), String> {
        if columns.len() != self.types().len() {
            return Err("result content columns do not match the frozen schema".into());
        }
        for (index, array) in columns.iter().enumerate() {
            if !same_content_type(array.data_type(), &self.types()[index]) {
                return Err(format!(
                    "result content column {index} has a different frozen type"
                ));
            }
            encode_value(array.as_ref(), row, write)?;
        }
        Ok(())
    }
}

fn same_content_type(a: &DataType, b: &DataType) -> bool {
    if let DataType::Dictionary(_, value) = a {
        return same_content_type(value, b);
    }
    if let DataType::Dictionary(_, value) = b {
        return same_content_type(a, value);
    }
    match (a, b) {
        (
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
        ) => true,
        (DataType::Binary | DataType::BinaryView, DataType::Binary | DataType::BinaryView) => true,
        (
            DataType::List(a) | DataType::LargeList(a) | DataType::FixedSizeList(a, _),
            DataType::List(b) | DataType::LargeList(b) | DataType::FixedSizeList(b, _),
        ) => same_content_type(a.data_type(), b.data_type()),
        (DataType::Struct(a), DataType::Struct(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b)
                    .all(|(a, b)| same_content_type(a.data_type(), b.data_type()))
        }
        (DataType::Map(a, _), DataType::Map(b, _)) => {
            same_content_type(a.data_type(), b.data_type())
        }
        _ => a == b,
    }
}

fn prefix_len(types: &[DataType]) -> Result<usize, String> {
    let mut size = 0usize;
    write_prefix(types, &mut |part| {
        size = size
            .checked_add(part.len())
            .ok_or_else(|| "result content schema prefix size overflow".to_string())?;
        Ok(())
    })?;
    Ok(size)
}
fn write_prefix(
    types: &[DataType],
    write: &mut impl FnMut(&[u8]) -> Result<(), String>,
) -> Result<(), String> {
    write(NATIVE_RESULT_CONTENT_V1_CANONICAL_BYTES)?;
    write(&(types.len() as u64).to_le_bytes())?;
    for ty in types {
        if !ResultContentEquivalence::NativeResultContentV1.supports(ty) {
            return Err(format!("result content encoding does not support {ty}"));
        }
        encode_type(ty, write)?;
    }
    Ok(())
}
fn unit_tag(unit: &TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    }
}

// These tags are the NativeResultContentV1 byte vocabulary. Container carrier
// widths and dictionary keys are omitted because they do not change content.
fn encode_type(
    ty: &DataType,
    key: &mut impl FnMut(&[u8]) -> Result<(), String>,
) -> Result<(), String> {
    let tag = match ty {
        DataType::Dictionary(_, value) => return encode_type(value, key),
        DataType::Null => 0,
        DataType::Boolean => 1,
        DataType::Int8 => 2,
        DataType::Int16 => 3,
        DataType::Int32 => 4,
        DataType::Int64 => 5,
        DataType::UInt8 => 6,
        DataType::UInt16 => 7,
        DataType::UInt32 => 8,
        DataType::UInt64 => 9,
        DataType::Float32 => 10,
        DataType::Float64 => 11,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => 12,
        DataType::Binary | DataType::BinaryView => 13,
        DataType::LargeBinary => 27,
        DataType::FixedSizeBinary(_) => 14,
        DataType::Date32 => 15,
        DataType::Date64 => 16,
        DataType::Timestamp(..) => 17,
        DataType::Time32(_) => 18,
        DataType::Time64(_) => 19,
        DataType::Decimal32(..) => 20,
        DataType::Decimal64(..) => 21,
        DataType::Decimal128(..) => 22,
        DataType::Decimal256(..) => 23,
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..) => 24,
        DataType::Struct(_) => 25,
        DataType::Map(..) => 26,
        _ => return Err(format!("result content encoding does not support {ty}")),
    };
    key(&[tag])?;
    match ty {
        DataType::FixedSizeBinary(width) => key(&width.to_le_bytes())?,
        DataType::Timestamp(unit, tz) => {
            key(&[unit_tag(unit)])?;
            key(&[u8::from(tz.is_some())])?;
            if let Some(tz) = tz {
                emit_bytes(key, tz.as_bytes())?;
            }
        }
        DataType::Time32(unit) | DataType::Time64(unit) => key(&[unit_tag(unit)])?,
        DataType::Decimal32(p, s)
        | DataType::Decimal64(p, s)
        | DataType::Decimal128(p, s)
        | DataType::Decimal256(p, s) => key(&[*p, *s as u8])?,
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            encode_type(field.data_type(), key)?
        }
        DataType::Struct(fields) => {
            key(&(fields.len() as u64).to_le_bytes())?;
            for field in fields {
                encode_type(field.data_type(), key)?;
            }
        }
        DataType::Map(entries, _) => encode_type(entries.data_type(), key)?,
        _ => {}
    }
    Ok(())
}

fn downcast<T: 'static>(array: &dyn Array) -> Result<&T, String> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        format!(
            "result content array disagrees with type {}",
            array.data_type()
        )
    })
}

fn emit_bytes(
    write: &mut impl FnMut(&[u8]) -> Result<(), String>,
    bytes: &[u8],
) -> Result<(), String> {
    write(&(bytes.len() as u64).to_le_bytes())?;
    write(bytes)
}
fn encode_value(
    array: &dyn Array,
    row: usize,
    key: &mut impl FnMut(&[u8]) -> Result<(), String>,
) -> Result<(), String> {
    if row >= array.len() {
        return Err("result content row is out of bounds".to_owned());
    }
    if let DataType::Dictionary(key_type, _) = array.data_type() {
        macro_rules! dict {
            ($ty:ty) => {{
                let dict = downcast::<DictionaryArray<$ty>>(array)?;
                if let Some(index) = dict.key(row) {
                    return encode_value(dict.values().as_ref(), index, key);
                }
                key(&[0])?;
                return Ok(());
            }};
        }
        match key_type.as_ref() {
            DataType::Int8 => dict!(Int8Type),
            DataType::Int16 => dict!(Int16Type),
            DataType::Int32 => dict!(Int32Type),
            DataType::Int64 => dict!(Int64Type),
            DataType::UInt8 => dict!(UInt8Type),
            DataType::UInt16 => dict!(UInt16Type),
            DataType::UInt32 => dict!(UInt32Type),
            DataType::UInt64 => dict!(UInt64Type),
            _ => return Err("result content dictionary has an unsupported key type".to_owned()),
        }
    }
    if array.is_null(row) {
        key(&[0])?;
        return Ok(());
    }
    key(&[1])?;
    macro_rules! primitive {
        ($ty:ty) => {
            key(&downcast::<$ty>(array)?.value(row).to_le_bytes())?
        };
    }
    macro_rules! bytes {
        ($ty:ty) => {
            emit_bytes(key, downcast::<$ty>(array)?.value(row).as_ref())?
        };
    }
    match array.data_type() {
        DataType::Null => {}
        DataType::Boolean => key(&[u8::from(downcast::<BooleanArray>(array)?.value(row))])?,
        DataType::Int8 => primitive!(Int8Array),
        DataType::Int16 => primitive!(Int16Array),
        DataType::Int32 => primitive!(Int32Array),
        DataType::Int64 => primitive!(Int64Array),
        DataType::UInt8 => primitive!(UInt8Array),
        DataType::UInt16 => primitive!(UInt16Array),
        DataType::UInt32 => primitive!(UInt32Array),
        DataType::UInt64 => primitive!(UInt64Array),
        DataType::Float32 => key(&downcast::<Float32Array>(array)?
            .value(row)
            .to_bits()
            .to_le_bytes())?,
        DataType::Float64 => key(&downcast::<Float64Array>(array)?
            .value(row)
            .to_bits()
            .to_le_bytes())?,
        DataType::Date32 => primitive!(Date32Array),
        DataType::Date64 => primitive!(Date64Array),
        DataType::Decimal32(..) => primitive!(Decimal32Array),
        DataType::Decimal64(..) => primitive!(Decimal64Array),
        DataType::Decimal128(..) => primitive!(Decimal128Array),
        DataType::Decimal256(..) => primitive!(Decimal256Array),
        DataType::Time32(TimeUnit::Second) => primitive!(Time32SecondArray),
        DataType::Time32(TimeUnit::Millisecond) => primitive!(Time32MillisecondArray),
        DataType::Time64(TimeUnit::Microsecond) => primitive!(Time64MicrosecondArray),
        DataType::Time64(TimeUnit::Nanosecond) => primitive!(Time64NanosecondArray),
        DataType::Timestamp(TimeUnit::Second, _) => primitive!(TimestampSecondArray),
        DataType::Timestamp(TimeUnit::Millisecond, _) => primitive!(TimestampMillisecondArray),
        DataType::Timestamp(TimeUnit::Microsecond, _) => primitive!(TimestampMicrosecondArray),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => primitive!(TimestampNanosecondArray),
        DataType::Utf8 => bytes!(StringArray),
        DataType::LargeUtf8 => bytes!(LargeStringArray),
        DataType::Utf8View => bytes!(StringViewArray),
        DataType::Binary => bytes!(BinaryArray),
        DataType::LargeBinary => {
            // The frozen native type mapping reserves LargeBinary for VARIANT.
            novarocks_types::value::variant::visit_serialized_result_content(
                downcast::<LargeBinaryArray>(array)?.value(row),
                key,
            )?;
        }
        DataType::BinaryView => bytes!(BinaryViewArray),
        DataType::FixedSizeBinary(_) => key(downcast::<FixedSizeBinaryArray>(array)?.value(row))?,
        DataType::List(_) => {
            encode_sequence(downcast::<ListArray>(array)?.value(row).as_ref(), key)?
        }
        DataType::LargeList(_) => {
            encode_sequence(downcast::<LargeListArray>(array)?.value(row).as_ref(), key)?
        }
        DataType::FixedSizeList(..) => encode_sequence(
            downcast::<FixedSizeListArray>(array)?.value(row).as_ref(),
            key,
        )?,
        DataType::Struct(_) => {
            for column in downcast::<StructArray>(array)?.columns() {
                encode_value(column.as_ref(), row, key)?;
            }
        }
        DataType::Map(..) => encode_sequence(&downcast::<MapArray>(array)?.value(row), key)?,
        other => return Err(format!("result content encoding does not support {other}")),
    }
    Ok(())
}

fn encode_sequence(
    array: &dyn Array,
    key: &mut impl FnMut(&[u8]) -> Result<(), String>,
) -> Result<(), String> {
    key(&(array.len() as u64).to_le_bytes())?;
    for row in 0..array.len() {
        encode_value(array, row, key)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    fn keys(array: ArrayRef) -> Vec<Vec<u8>> {
        let encoder = ContentKeyEncoder::try_new_types(vec![array.data_type().clone()]).unwrap();
        (0..array.len())
            .map(|row| {
                let key = encoder.encode_row(&[Arc::clone(&array)], row).unwrap();
                assert_eq!(
                    encoder.encoded_row_len(&[array.clone()], row).unwrap(),
                    key.len()
                );
                key
            })
            .collect()
    }

    #[test]
    fn content_key_all_native_primitive_carriers_preserve_null_and_value() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(BooleanArray::from(vec![None, Some(false), Some(true)])),
            Arc::new(Int8Array::from(vec![None, Some(i8::MIN), Some(i8::MAX)])),
            Arc::new(Int16Array::from(vec![None, Some(i16::MIN), Some(i16::MAX)])),
            Arc::new(Int32Array::from(vec![None, Some(i32::MIN), Some(i32::MAX)])),
            Arc::new(Int64Array::from(vec![None, Some(i64::MIN), Some(i64::MAX)])),
            Arc::new(UInt8Array::from(vec![None, Some(0), Some(u8::MAX)])),
            Arc::new(UInt16Array::from(vec![None, Some(0), Some(u16::MAX)])),
            Arc::new(UInt32Array::from(vec![None, Some(0), Some(u32::MAX)])),
            Arc::new(UInt64Array::from(vec![None, Some(0), Some(u64::MAX)])),
            Arc::new(Date32Array::from(vec![None, Some(-1), Some(1)])),
            Arc::new(Date64Array::from(vec![
                None,
                Some(-86400000),
                Some(86400000),
            ])),
            Arc::new(Time32SecondArray::from(vec![None, Some(0), Some(1)])),
            Arc::new(Time32MillisecondArray::from(vec![None, Some(0), Some(1)])),
            Arc::new(Time64MicrosecondArray::from(vec![None, Some(0), Some(1)])),
            Arc::new(Time64NanosecondArray::from(vec![None, Some(0), Some(1)])),
            Arc::new(TimestampSecondArray::from(vec![None, Some(-1), Some(1)])),
            Arc::new(TimestampMillisecondArray::from(vec![
                None,
                Some(-1),
                Some(1),
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                None,
                Some(-1),
                Some(1),
            ])),
            Arc::new(TimestampNanosecondArray::from(vec![
                None,
                Some(-1),
                Some(1),
            ])),
            Arc::new(
                Decimal32Array::from(vec![None, Some(-1), Some(1)])
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
            ),
            Arc::new(
                Decimal64Array::from(vec![None, Some(-1), Some(1)])
                    .with_precision_and_scale(18, 2)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![None, Some(-1), Some(1)])
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ),
            Arc::new(
                Decimal256Array::from(vec![
                    None,
                    Some(i256::from_i128(-1)),
                    Some(i256::from_i128(1)),
                ])
                .with_precision_and_scale(76, 2)
                .unwrap(),
            ),
            Arc::new(BinaryArray::from(vec![
                None,
                Some(b"".as_slice()),
                Some(b"a".as_slice()),
            ])),
            Arc::new(StringArray::from(vec![None, Some(""), Some("a")])),
        ];
        for array in arrays {
            let ty = array.data_type().clone();
            let keys = keys(array);
            assert_eq!(
                keys.iter().collect::<std::collections::BTreeSet<_>>().len(),
                3,
                "{ty}"
            );
        }
        let plain: ArrayRef = Arc::new(StringArray::from(vec!["abc", "def"]));
        for other in [
            Arc::new(LargeStringArray::from(vec!["abc", "def"])) as ArrayRef,
            Arc::new(StringViewArray::from(vec!["abc", "def"])) as ArrayRef,
        ] {
            assert_eq!(keys(plain.clone()), keys(other));
        }
    }

    #[test]
    fn content_key_variant_dictionary_offsets_and_float_bits_are_exact() {
        use novarocks_types::value::variant::{VariantMetadata, VariantValue};
        let first = VariantValue::create(
            &[1, 2, 0, 1, 2, b'a', b'b'],
            &[2, 2, 0, 1, 0, 2, 4, 12, 1, 12, 2],
        )
        .unwrap()
        .serialize();
        let second = VariantValue::create(
            &[1, 2, 0, 1, 2, b'b', b'a'],
            &[2, 2, 1, 0, 0, 2, 4, 12, 1, 12, 2],
        )
        .unwrap()
        .serialize();
        let changed = VariantValue::create(
            &[1, 2, 0, 1, 2, b'a', b'b'],
            &[2, 2, 0, 1, 0, 2, 4, 12, 1, 12, 3],
        )
        .unwrap()
        .serialize();
        let mut float_a = vec![28];
        float_a.extend_from_slice(&0x7ff8_0000_0000_0001u64.to_le_bytes());
        let mut float_b = vec![28];
        float_b.extend_from_slice(&0x7ff8_0000_0000_0002u64.to_le_bytes());
        let float_a = VariantValue::create(VariantMetadata::empty().raw(), &float_a)
            .unwrap()
            .serialize();
        let float_b = VariantValue::create(VariantMetadata::empty().raw(), &float_b)
            .unwrap()
            .serialize();
        let array = Arc::new(LargeBinaryArray::from(vec![
            first.as_slice(),
            second.as_slice(),
            changed.as_slice(),
            float_a.as_slice(),
            float_b.as_slice(),
        ])) as ArrayRef;
        let keys = keys(array);
        assert_eq!(keys[0], keys[1]);
        assert_ne!(keys[0], keys[2]);
        assert_ne!(keys[3], keys[4]);
    }

    #[test]
    fn content_key_counting_covers_repeated_variant_dictionary_expansion() {
        use novarocks_types::value::variant::VariantValue;
        let key = vec![b'x'; 300];
        let mut metadata = vec![65, 1, 0, 0, 0];
        metadata.extend_from_slice(&300u16.to_le_bytes());
        metadata.extend_from_slice(&key);
        let object = [2, 1, 0, 0, 2, 12, 1];
        let mut value = vec![7, 100];
        for index in 0..=100u16 {
            value.extend_from_slice(&(index * 7).to_le_bytes());
        }
        for _ in 0..100 {
            value.extend_from_slice(&object);
        }
        let serialized = VariantValue::create(&metadata, &value).unwrap().serialize();
        let array = Arc::new(LargeBinaryArray::from(vec![serialized.as_slice()])) as ArrayRef;
        let encoder = ContentKeyEncoder::try_new_types(vec![DataType::LargeBinary]).unwrap();
        let counted = encoder.encoded_row_len(&[array.clone()], 0).unwrap();
        assert!(counted > serialized.len() * 2);
        assert_eq!(counted, encoder.encode_row(&[array], 0).unwrap().len());
    }

    #[test]
    fn content_key_preserves_signed_zero_and_every_nan_bit() {
        let bits = [
            0,
            1 << 63,
            0x7ff8_0000_0000_0001,
            0x7ff8_0000_0000_0002,
            0xfff8_0000_0000_0001,
            0x7ff0_0000_0000_0001,
        ];
        let keys = keys(Arc::new(Float64Array::from(
            bits.map(f64::from_bits).to_vec(),
        )));
        assert_eq!(
            keys.iter().collect::<std::collections::BTreeSet<_>>().len(),
            bits.len()
        );
        let keys = self::keys(Arc::new(Float32Array::from(vec![
            0.,
            -0.,
            f32::from_bits(0x7fc0_0001),
            f32::from_bits(0x7fc0_0002),
        ])));
        assert_eq!(
            keys.iter().collect::<std::collections::BTreeSet<_>>().len(),
            4
        );
    }

    #[test]
    fn content_key_null_lengths_dictionary_ids_and_offsets_are_logical() {
        let plain: ArrayRef = Arc::new(StringArray::from(vec![
            Some("a"),
            Some("b"),
            None,
            Some(""),
        ]));
        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        builder.append("b").unwrap();
        builder.append("a").unwrap();
        builder.append("b").unwrap();
        builder.append_null();
        builder.append("").unwrap();
        let dict: ArrayRef = Arc::new(builder.finish().slice(1, 4));
        let encoder = ContentKeyEncoder::try_new_types(vec![DataType::Utf8]).unwrap();
        for row in 0..4 {
            assert_eq!(
                encoder.encode_row(&[Arc::clone(&plain)], row).unwrap(),
                encoder.encode_row(&[Arc::clone(&dict)], row).unwrap()
            );
        }
        assert_ne!(keys(plain)[2], keys(dict)[3]);
        let left = keys(Arc::new(StringArray::from(vec!["ab", "a"])));
        assert_ne!(left[0], left[1]);
    }

    #[test]
    fn content_key_interpretation_pins_decimal_scale_timestamp_unit_and_timezone() {
        let decimal = |scale| {
            Arc::new(
                Decimal128Array::from(vec![100])
                    .with_precision_and_scale(10, scale)
                    .unwrap(),
            ) as ArrayRef
        };
        assert_ne!(keys(decimal(1)), keys(decimal(2)));
        let a: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![100]).with_timezone("UTC"));
        let b: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(vec![100]).with_timezone("Asia/Shanghai"));
        assert_ne!(keys(a), keys(b));
        assert_ne!(
            keys(Arc::new(TimestampMicrosecondArray::from(vec![100]))),
            keys(Arc::new(TimestampNanosecondArray::from(vec![100])))
        );
    }

    #[test]
    fn content_key_recursive_list_struct_map_and_largeint_keep_order_and_null() {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), None]),
            Some(vec![None, Some(1)]),
            None,
        ]);
        let list_keys = keys(Arc::new(list));
        assert_eq!(
            list_keys
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            3
        );
        let fields = vec![Arc::new(Field::new("x", DataType::Float64, true))];
        let array = StructArray::new(
            fields.into(),
            vec![Arc::new(Float64Array::from(vec![0., -0.]))],
            None,
        );
        assert_ne!(
            keys(Arc::new(array))[0],
            keys(Arc::new(StructArray::from(vec![(
                Arc::new(Field::new("x", DataType::Float64, true)),
                Arc::new(Float64Array::from(vec![0., -0.])) as ArrayRef
            )])))[1]
        );
        let mut map = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        map.keys().append_value("a");
        map.values().append_value(1);
        map.keys().append_value("b");
        map.values().append_null();
        map.append(true).unwrap();
        map.keys().append_value("b");
        map.values().append_null();
        map.keys().append_value("a");
        map.values().append_value(1);
        map.append(true).unwrap();
        let keys = keys(Arc::new(map.finish()));
        assert_ne!(keys[0], keys[1]);
        let mut binary = FixedSizeBinaryBuilder::new(16);
        binary.append_value(1i128.to_le_bytes()).unwrap();
        binary.append_value((-1i128).to_le_bytes()).unwrap();
        let keys = self::keys(Arc::new(binary.finish()));
        assert_ne!(keys[0], keys[1]);
    }

    #[test]
    fn content_key_full_bytes_remain_distinct_under_a_hash_collision() {
        let keys = keys(Arc::new(Int64Array::from(vec![1, 2])));
        // A candidate hash is only an index. Equality always checks the complete key.
        let collision_hash = |_key: &[u8]| 0_u64;
        assert_eq!(collision_hash(&keys[0]), collision_hash(&keys[1]));
        assert_ne!(keys[0], keys[1]);
    }
}
