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

//! Leaf-only ScalarValueV1 Native source bridge. The immutable source stays
//! owned across turns; each turn borrows its selected cell anew. No lifetime
//! extension, hydration, dictionary expansion, or variable-value copy occurs.
//!
//! The caller prepays the cursor box and one-column RecordBatch clone BEFORE
//! construction, proves the complete original Chunk backing separately, and
//! retains its original RootInputPermit until this cursor and its input exit.
//! These checks neither fund source growth nor admit a root/session assignment.
//! Empty input, cumulative row admission, NoRows at sealed End, and nested
//! containers remain the root owner's responsibilities. This module does not
//! install a producer or enable the Host's ScalarValueV1 gate.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    DictionaryArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, NullArray, RecordBatch,
    StringArray, Time64MicrosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Int32Type};
use novarocks_execution::exec::chunk::Chunk;
use novarocks_result_contract::{
    BorrowedScalarLeaf, ScalarField, ScalarLeafCursor, ScalarLeafError, ScalarOpaqueType,
    ScalarSchema, ScalarTimestampUnit, ScalarValueType,
};
use novarocks_result_render::{RenderTurn, RenderTurnStatus};
use novarocks_type_contract::result_scalar_type::scalar_field_matches_storage;
use novarocks_types::logical::{LogicalType, NR_LOGICAL_TYPE_KEY};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeScalarLeafError {
    Shape,
    EmptyInput,
    Slot,
    Type,
    LogicalMetadata,
    UnsupportedContainer,
    ScratchLimit,
    Leaf(ScalarLeafError),
    ChangedInput,
    Failed,
}
impl std::fmt::Display for NativeScalarLeafError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Shape => "scalar leaf input must contain exactly one column and at most one row",
            Self::EmptyInput => "empty scalar input does not encode a value",
            Self::Slot => "scalar leaf input differs from its exact Native source slot",
            Self::Type => "scalar leaf input differs from its exact physical carrier",
            Self::LogicalMetadata => "scalar leaf logical metadata differs from its frozen facts",
            Self::UnsupportedContainer => "Native scalar container cursor is not installed",
            Self::ScratchLimit => "scalar leaf cursor exceeds its prepaid scratch capacity",
            Self::Leaf(error) => return std::fmt::Display::fmt(error, f),
            Self::ChangedInput => "scalar leaf input changed during emission",
            Self::Failed => "Native scalar leaf cursor is failed",
        })
    }
}
impl std::error::Error for NativeScalarLeafError {}
impl From<ScalarLeafError> for NativeScalarLeafError {
    fn from(value: ScalarLeafError) -> Self {
        Self::Leaf(value)
    }
}

/// No extra cursor heap storage exists besides the separately prepaid Box and
/// RecordBatch columns Vec. The schema Arc retains an existing frozen owner.
pub struct NativeScalarLeafEncoder {
    batch: RecordBatch,
    schema: Arc<ScalarSchema>,
    encoded_len: usize,
    offset: usize,
    failed: bool,
}
impl NativeScalarLeafEncoder {
    pub const fn inline_capacity_bytes() -> usize {
        size_of::<Self>()
    }
    /// Actual one-column Vec clone Layout plus the caller's cursor Box Layout.
    /// No schema copy, variable payload, or source backing is included here.
    pub const fn scratch_capacity_bytes() -> usize {
        size_of::<Self>() + size_of::<ArrayRef>()
    }
    /// Borrowed validation only. Shape/slot checks precede metadata, physical
    /// cell access and all cursor/RecordBatch cloning. The caller still proves
    /// complete original ownership and atomically admits cumulative 0/1 rows.
    pub fn validate_input(
        chunk: &Chunk,
        schema: &ScalarSchema,
    ) -> Result<(), NativeScalarLeafError> {
        let batch = &chunk.batch;
        if batch.num_columns() != 1
            || batch.num_rows() > 1
            || chunk.chunk_schema().slots().len() != 1
        {
            return Err(NativeScalarLeafError::Shape);
        }
        if batch.num_rows() == 0 {
            return Err(NativeScalarLeafError::EmptyInput);
        }
        let slot = &chunk.chunk_schema().slots()[0];
        if schema.source_slot() != Some(slot.slot_id().as_u32()) {
            return Err(NativeScalarLeafError::Slot);
        }
        reject_container(schema.field())?;
        let logical = expected_logical(&schema.field().value_type);
        if !slot.field_schema().children().is_empty()
            || slot.field_schema().logical_type() != logical
        {
            return Err(NativeScalarLeafError::LogicalMetadata);
        }
        validate_field(schema.field(), slot.field())?;
        validate_field(schema.field(), &batch.schema_ref().fields()[0])?;
        let leaf = borrow_leaf(schema.field(), batch.column(0).as_ref())?;
        ScalarLeafCursor::try_new(schema, leaf)?;
        Ok(())
    }
    pub fn try_new(
        chunk: &Chunk,
        schema: Arc<ScalarSchema>,
        prepaid_scratch_capacity: usize,
    ) -> Result<Self, NativeScalarLeafError> {
        Self::validate_input(chunk, &schema)?;
        if Self::scratch_capacity_bytes() > prepaid_scratch_capacity {
            return Err(NativeScalarLeafError::ScratchLimit);
        }
        let encoded_len = ScalarLeafCursor::try_new(
            &schema,
            borrow_validated_leaf(schema.field(), chunk.batch.column(0).as_ref())?,
        )?
        .encoded_len();
        Ok(Self {
            batch: chunk.batch.clone(),
            schema,
            encoded_len,
            offset: 0,
            failed: false,
        })
    }
    pub const fn encoded_len(&self) -> usize {
        self.encoded_len
    }
    pub fn step(&mut self, output: &mut [u8]) -> Result<RenderTurn, NativeScalarLeafError> {
        if self.failed {
            return Err(NativeScalarLeafError::Failed);
        }
        let result = self.step_inner(output);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn step_inner(&mut self, output: &mut [u8]) -> Result<RenderTurn, NativeScalarLeafError> {
        if self.offset == self.encoded_len {
            return Ok(RenderTurn {
                emitted_bytes: 0,
                examined_bytes: 0,
                visited_cells: 0,
                completed_rows: 0,
                status: RenderTurnStatus::InputComplete,
            });
        }
        let cursor = ScalarLeafCursor::try_new(
            &self.schema,
            borrow_validated_leaf(self.schema.field(), self.batch.column(0).as_ref())?,
        )?;
        if cursor.encoded_len() != self.encoded_len {
            return Err(NativeScalarLeafError::ChangedInput);
        }
        let turn = cursor.copy_range(self.offset, output)?;
        self.offset = self
            .offset
            .checked_add(turn.emitted_bytes)
            .ok_or(NativeScalarLeafError::ChangedInput)?;
        Ok(RenderTurn {
            emitted_bytes: turn.emitted_bytes,
            examined_bytes: 0,
            // At most 304 coefficient limbs plus constant leaf selection. This
            // conservative work charge remains below the 1024-cell quantum.
            visited_cells: 512,
            completed_rows: u64::from(turn.complete),
            status: if turn.complete {
                RenderTurnStatus::InputComplete
            } else {
                RenderTurnStatus::NeedsOutput
            },
        })
    }
}

fn reject_container(field: &ScalarField) -> Result<(), NativeScalarLeafError> {
    if matches!(
        field.value_type,
        ScalarValueType::List(_) | ScalarValueType::Map { .. } | ScalarValueType::Struct(_)
    ) {
        return Err(NativeScalarLeafError::UnsupportedContainer);
    }
    Ok(())
}
fn expected_logical(value: &ScalarValueType) -> Option<LogicalType> {
    match value {
        ScalarValueType::Json => Some(LogicalType::Json),
        ScalarValueType::Opaque(ScalarOpaqueType::Hll) => Some(LogicalType::Hll),
        ScalarValueType::Opaque(ScalarOpaqueType::Bitmap) => Some(LogicalType::Bitmap),
        ScalarValueType::Opaque(ScalarOpaqueType::Object) => Some(LogicalType::Object),
        ScalarValueType::Opaque(ScalarOpaqueType::Percentile) => Some(LogicalType::Percentile),
        _ => None,
    }
}
fn validate_field(expected: &ScalarField, actual: &Field) -> Result<(), NativeScalarLeafError> {
    if !carrier_matches(expected, actual.data_type(), actual.is_nullable()) {
        return Err(NativeScalarLeafError::Type);
    }
    // Borrow the canonical fact; normalizing with logical_type_of_field would
    // allocate and could conceal a stale/forged cached logical classification.
    let logical = actual
        .metadata()
        .get(NR_LOGICAL_TYPE_KEY)
        .map(String::as_str);
    if logical != expected_logical(&expected.value_type).map(LogicalType::metadata_value) {
        return Err(NativeScalarLeafError::LogicalMetadata);
    }
    Ok(())
}
fn dictionary_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Dictionary(key, value)
        if key.as_ref() == &DataType::Int32 && matches!(value.as_ref(), DataType::Utf8 | DataType::LargeUtf8))
}
fn carrier_matches(expected: &ScalarField, actual: &DataType, nullable: bool) -> bool {
    if dictionary_type(actual) {
        expected.nullable == nullable
            && matches!(
                expected.value_type,
                ScalarValueType::String | ScalarValueType::Json
            )
    } else {
        scalar_field_matches_storage(expected, actual, nullable)
    }
}
fn exact<T: Array + 'static>(array: &dyn Array) -> Result<&T, NativeScalarLeafError> {
    let value = array
        .as_any()
        .downcast_ref::<T>()
        .ok_or(NativeScalarLeafError::Type)?;
    if std::ptr::from_ref(array).cast::<()>() != std::ptr::from_ref(value).cast::<()>()
        || size_of_val(array) != size_of::<T>()
        || align_of_val(array) != align_of::<T>()
    {
        return Err(NativeScalarLeafError::Type);
    }
    Ok(value)
}
fn borrow_leaf<'a>(
    expected: &ScalarField,
    array: &'a dyn Array,
) -> Result<BorrowedScalarLeaf<'a>, NativeScalarLeafError> {
    if array.len() != 1 || !carrier_matches(expected, array.data_type(), expected.nullable) {
        return Err(NativeScalarLeafError::Type);
    }
    borrow_validated_leaf(expected, array)
}
// Only the constructor's fully validated immutable concrete arrays reach this
// path. In particular a turn never rescans the retained timezone string.
fn borrow_validated_leaf<'a>(
    expected: &ScalarField,
    array: &'a dyn Array,
) -> Result<BorrowedScalarLeaf<'a>, NativeScalarLeafError> {
    use BorrowedScalarLeaf as V;
    use ScalarValueType as S;
    if array.len() != 1 {
        return Err(NativeScalarLeafError::Type);
    }
    if dictionary_type(array.data_type()) {
        let dictionary = exact::<DictionaryArray<Int32Type>>(array)?;
        if dictionary.keys().is_null(0) {
            return Ok(V::Null);
        }
        let key =
            usize::try_from(dictionary.keys().value(0)).map_err(|_| NativeScalarLeafError::Type)?;
        let values = dictionary.values();
        if key >= values.len() {
            return Err(NativeScalarLeafError::Type);
        }
        let text = match values.data_type() {
            DataType::Utf8 => {
                let values = exact::<StringArray>(values.as_ref())?;
                if values.is_null(key) {
                    return Ok(V::Null);
                }
                values.value(key)
            }
            DataType::LargeUtf8 => {
                let values = exact::<LargeStringArray>(values.as_ref())?;
                if values.is_null(key) {
                    return Ok(V::Null);
                }
                values.value(key)
            }
            _ => return Err(NativeScalarLeafError::Type),
        };
        return Ok(if matches!(expected.value_type, S::Json) {
            V::Json(text)
        } else {
            V::String(text)
        });
    }
    macro_rules! selected {
        ($ty:ty, $value:ident, $expression:expr) => {{
            let $value = exact::<$ty>(array)?;
            if $value.is_null(0) {
                V::Null
            } else {
                $expression
            }
        }};
    }
    Ok(match &expected.value_type {
        S::Null => {
            exact::<NullArray>(array)?;
            V::Null
        }
        S::Boolean => selected!(BooleanArray, a, V::Boolean(a.value(0))),
        S::SignedInteger(8) => selected!(
            Int8Array,
            a,
            V::SignedInteger {
                bits: 8,
                value: i64::from(a.value(0))
            }
        ),
        S::SignedInteger(16) => selected!(
            Int16Array,
            a,
            V::SignedInteger {
                bits: 16,
                value: i64::from(a.value(0))
            }
        ),
        S::SignedInteger(32) => selected!(
            Int32Array,
            a,
            V::SignedInteger {
                bits: 32,
                value: i64::from(a.value(0))
            }
        ),
        S::SignedInteger(64) => selected!(
            Int64Array,
            a,
            V::SignedInteger {
                bits: 64,
                value: a.value(0)
            }
        ),
        S::LargeInt => selected!(
            FixedSizeBinaryArray,
            a,
            V::LargeInt(i128::from_le_bytes(
                a.value(0)
                    .try_into()
                    .map_err(|_| NativeScalarLeafError::Type)?
            ))
        ),
        S::Float32 => selected!(Float32Array, a, V::Float32(a.value(0).to_bits())),
        S::Float64 => selected!(Float64Array, a, V::Float64(a.value(0).to_bits())),
        S::Decimal {
            bits: 128,
            precision,
            scale,
        } => selected!(
            Decimal128Array,
            a,
            V::Decimal128 {
                coefficient: a.value(0),
                precision: *precision,
                scale: *scale
            }
        ),
        S::Decimal {
            bits: 256,
            precision,
            scale,
        } => selected!(
            Decimal256Array,
            a,
            V::Decimal256 {
                coefficient_le: a.value(0).to_le_bytes(),
                precision: *precision,
                scale: *scale
            }
        ),
        S::String => selected!(StringArray, a, V::String(a.value(0))),
        S::Binary => selected!(BinaryArray, a, V::Binary(a.value(0))),
        S::Json => selected!(StringArray, a, V::Json(a.value(0))),
        S::Variant => selected!(LargeBinaryArray, a, V::Variant(a.value(0))),
        S::Opaque(kind) => selected!(
            BinaryArray,
            a,
            V::Opaque {
                kind: *kind,
                bytes: a.value(0)
            }
        ),
        S::Date => selected!(Date32Array, a, V::Date(a.value(0))),
        S::TimeMicros => selected!(Time64MicrosecondArray, a, V::TimeMicros(a.value(0))),
        S::Timestamp {
            unit: ScalarTimestampUnit::Microsecond,
            ..
        } => selected!(
            TimestampMicrosecondArray,
            a,
            V::Timestamp {
                ticks: a.value(0),
                unit: ScalarTimestampUnit::Microsecond
            }
        ),
        S::Timestamp {
            unit: ScalarTimestampUnit::Nanosecond,
            ..
        } => selected!(
            TimestampNanosecondArray,
            a,
            V::Timestamp {
                ticks: a.value(0),
                unit: ScalarTimestampUnit::Nanosecond
            }
        ),
        S::List(_) | S::Map { .. } | S::Struct(_) => {
            return Err(NativeScalarLeafError::UnsupportedContainer);
        }
        _ => return Err(NativeScalarLeafError::Type),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::i256;
    use novarocks_execution::exec::chunk::{ChunkFieldSchema, ChunkSchema, ChunkSlotSchema};
    use novarocks_result_contract::SCALAR_LEAF_HEADER_BYTES;
    use novarocks_types::SlotId;

    fn schema(value_type: ScalarValueType, nullable: bool) -> Arc<ScalarSchema> {
        Arc::new(
            ScalarSchema::try_new(ScalarField {
                nullable,
                value_type,
            })
            .unwrap()
            .bind_native_slots(&[7])
            .unwrap(),
        )
    }
    // These source fixtures are not original funding/provenance proofs. The
    // leaf bridge is tested separately from the caller's complete Chunk proof.
    fn chunk(array: ArrayRef, logical: Option<&str>, cached: Option<ChunkFieldSchema>) -> Chunk {
        let mut field = Field::new("value", array.data_type().clone(), true);
        if let Some(logical) = logical {
            field =
                field.with_metadata([(NR_LOGICAL_TYPE_KEY.to_owned(), logical.to_owned())].into());
        }
        let slot = ChunkSlotSchema::try_new_with_field(SlotId(7), field, cached, None).unwrap();
        let schema = Arc::new(ChunkSchema::try_new(vec![slot]).unwrap());
        let batch = RecordBatch::try_new(schema.arrow_schema_ref(), vec![array]).unwrap();
        Chunk::try_new_with_chunk_schema(batch, schema).unwrap()
    }
    fn encode(input: &Chunk, expected: Arc<ScalarSchema>, quantum: usize) -> Vec<u8> {
        let mut cursor = NativeScalarLeafEncoder::try_new(
            input,
            expected,
            NativeScalarLeafEncoder::scratch_capacity_bytes(),
        )
        .unwrap();
        let mut bytes = Vec::new();
        let mut rows = 0;
        for _ in 0..100_000 {
            let mut output = vec![0; quantum];
            let turn = cursor.step(&mut output).unwrap();
            assert!(turn.visited_cells <= 1024);
            assert!(turn.emitted_bytes <= 64 * 1024);
            rows += turn.completed_rows;
            bytes.extend_from_slice(&output[..turn.emitted_bytes]);
            if turn.status == RenderTurnStatus::InputComplete {
                assert_eq!(rows, 1);
                assert_eq!(cursor.step(&mut output).unwrap().completed_rows, 0);
                return bytes;
            }
        }
        panic!("bounded scalar cursor failed to complete");
    }
    #[test]
    fn shape_and_empty_refuse_before_semantic_metadata() {
        let expected = schema(ScalarValueType::Json, true);
        let empty = chunk(
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Some("forged"),
            None,
        );
        assert_eq!(
            NativeScalarLeafEncoder::validate_input(&empty, &expected),
            Err(NativeScalarLeafError::EmptyInput)
        );
        let rows = chunk(
            Arc::new(StringArray::from(vec!["a", "b"])),
            Some("forged"),
            None,
        );
        assert_eq!(
            NativeScalarLeafEncoder::validate_input(&rows, &expected),
            Err(NativeScalarLeafError::Shape)
        );
    }
    #[test]
    fn canonical_metadata_and_cached_facts_must_both_match() {
        let expected = schema(ScalarValueType::Json, true);
        let plain = chunk(Arc::new(StringArray::from(vec!["{}"])), None, None);
        assert_eq!(
            NativeScalarLeafEncoder::validate_input(&plain, &expected),
            Err(NativeScalarLeafError::LogicalMetadata)
        );
        let fake = ChunkFieldSchema::from_field(
            &Field::new("fake", DataType::Utf8, true)
                .with_metadata([(NR_LOGICAL_TYPE_KEY.to_owned(), "json".to_owned())].into()),
        )
        .unwrap();
        let forged_cache = chunk(Arc::new(StringArray::from(vec!["{}"])), None, Some(fake));
        assert_eq!(
            NativeScalarLeafEncoder::validate_input(&forged_cache, &expected),
            Err(NativeScalarLeafError::LogicalMetadata)
        );
        let normalized = chunk(
            Arc::new(StringArray::from(vec!["{}"])),
            Some(" JSON "),
            None,
        );
        assert_eq!(
            NativeScalarLeafEncoder::validate_input(&normalized, &expected),
            Err(NativeScalarLeafError::LogicalMetadata)
        );
        let exact = chunk(Arc::new(StringArray::from(vec!["{}"])), Some("json"), None);
        assert!(NativeScalarLeafEncoder::validate_input(&exact, &expected).is_ok());
        assert_eq!(
            NativeScalarLeafEncoder::validate_input(&exact, &schema(ScalarValueType::String, true)),
            Err(NativeScalarLeafError::LogicalMetadata)
        );
    }
    #[test]
    fn cached_logical_fact_cannot_override_or_erase_exact_field_metadata() {
        let canonical_json_with_empty_cache = chunk(
            Arc::new(StringArray::from(vec!["{}"])),
            Some("json"),
            Some(ChunkFieldSchema::empty()),
        );
        assert_eq!(
            NativeScalarLeafEncoder::validate_input(
                &canonical_json_with_empty_cache,
                &schema(ScalarValueType::Json, true),
            ),
            Err(NativeScalarLeafError::LogicalMetadata),
        );
        let cached_json = ChunkFieldSchema::from_field(
            &Field::new("cache", DataType::Utf8, true)
                .with_metadata([(NR_LOGICAL_TYPE_KEY.to_owned(), "json".to_owned())].into()),
        )
        .unwrap();
        let plain_string_with_json_cache = chunk(
            Arc::new(StringArray::from(vec!["plain"])),
            None,
            Some(cached_json),
        );
        assert_eq!(
            NativeScalarLeafEncoder::validate_input(
                &plain_string_with_json_cache,
                &schema(ScalarValueType::String, true),
            ),
            Err(NativeScalarLeafError::LogicalMetadata),
        );
    }
    #[test]
    fn exact_64k_payload_forms_one_record_in_two_bounded_turns() {
        let payload = "x".repeat(64 * 1024);
        let input = chunk(
            Arc::new(StringArray::from(vec![payload.as_str()])),
            None,
            None,
        );
        let expected = schema(ScalarValueType::String, true);
        let mut cursor = NativeScalarLeafEncoder::try_new(
            &input,
            Arc::clone(&expected),
            NativeScalarLeafEncoder::scratch_capacity_bytes(),
        )
        .unwrap();
        assert_eq!(cursor.encoded_len(), 65_560);
        let mut output = vec![0xa5; 64 * 1024 + 17];
        let first = cursor.step(&mut output).unwrap();
        assert_eq!(first.emitted_bytes, 65_536);
        assert_eq!(first.completed_rows, 0);
        assert_eq!(first.status, RenderTurnStatus::NeedsOutput);
        assert!(
            output[first.emitted_bytes..]
                .iter()
                .all(|byte| *byte == 0xa5)
        );
        let mut record = output[..first.emitted_bytes].to_vec();
        output.fill(0xa5);
        let second = cursor.step(&mut output).unwrap();
        assert_eq!(second.emitted_bytes, 24);
        assert_eq!(second.completed_rows, 1);
        assert_eq!(second.status, RenderTurnStatus::InputComplete);
        assert!(
            output[second.emitted_bytes..]
                .iter()
                .all(|byte| *byte == 0xa5)
        );
        record.extend_from_slice(&output[..second.emitted_bytes]);
        assert_eq!(record.len(), 65_560);
        assert_eq!(u32::from_le_bytes(record[4..8].try_into().unwrap()), 65_560);
        assert_eq!(
            u32::from_le_bytes(record[8..12].try_into().unwrap()),
            65_536
        );
        assert_eq!(&record[SCALAR_LEAF_HEADER_BYTES..], payload.as_bytes());
        assert_eq!(
            BorrowedScalarLeaf::decode(&expected, &record).unwrap(),
            BorrowedScalarLeaf::String(&payload),
        );
        let complete = cursor.step(&mut output).unwrap();
        assert_eq!(complete.emitted_bytes, 0);
        assert_eq!(complete.completed_rows, 0);
    }
    #[test]
    fn dictionary_key_and_selected_value_null_are_distinct_physical_cases() {
        for keys in [
            Int32Array::from(vec![None]),
            Int32Array::from(vec![Some(0)]),
        ] {
            let dictionary = DictionaryArray::<Int32Type>::try_new(
                keys,
                Arc::new(StringArray::from(vec![None::<&str>, Some("unused")])),
            )
            .unwrap();
            let input = chunk(Arc::new(dictionary), None, None);
            let expected = schema(ScalarValueType::String, true);
            let wire = encode(&input, Arc::clone(&expected), 1);
            assert_eq!(
                BorrowedScalarLeaf::decode(&expected, &wire).unwrap(),
                BorrowedScalarLeaf::Null
            );
        }
    }
    #[test]
    fn only_selected_dictionary_value_is_encoded_without_expansion() {
        let large = "u".repeat(128 * 1024);
        let values = Arc::new(LargeStringArray::from(vec!["selected", large.as_str()]));
        let dictionary =
            DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0]), values).unwrap();
        let input = chunk(Arc::new(dictionary), None, None);
        let expected = schema(ScalarValueType::String, true);
        let wire = encode(&input, Arc::clone(&expected), 1);
        assert_eq!(&wire[SCALAR_LEAF_HEADER_BYTES..], b"selected");
        assert_eq!(
            BorrowedScalarLeaf::decode(&expected, &wire).unwrap(),
            BorrowedScalarLeaf::String("selected")
        );
        // This result does not approve ignoring the large unselected backing:
        // the caller's complete original proof still counts all of it.
    }
    #[test]
    fn raw_binary_and_opaque_bytes_survive_one_byte_output_turns() {
        for (value_type, logical) in [
            (ScalarValueType::Binary, None),
            (ScalarValueType::Opaque(ScalarOpaqueType::Hll), Some("hll")),
        ] {
            let input = chunk(
                Arc::new(BinaryArray::from(vec![&[0xff, 0x00, 0x80][..]])),
                logical,
                None,
            );
            let expected = schema(value_type, true);
            let wire = encode(&input, Arc::clone(&expected), 1);
            assert_eq!(&wire[SCALAR_LEAF_HEADER_BYTES..], &[0xff, 0x00, 0x80]);
            assert!(BorrowedScalarLeaf::decode(&expected, &wire).is_ok());
        }
    }
    #[test]
    fn raw_nanosecond_ticks_and_timezone_remain_exact() {
        let array = TimestampNanosecondArray::from(vec![-1_234_567_891]).with_timezone("UTC");
        let input = chunk(Arc::new(array), None, None);
        let expected = schema(
            ScalarValueType::Timestamp {
                unit: ScalarTimestampUnit::Nanosecond,
                timezone: Some("UTC".to_owned()),
            },
            true,
        );
        let wire = encode(&input, Arc::clone(&expected), 1);
        assert_eq!(
            &wire[SCALAR_LEAF_HEADER_BYTES..],
            &(-1_234_567_891_i64).to_le_bytes()
        );
        assert_eq!(
            BorrowedScalarLeaf::decode(&expected, &wire).unwrap(),
            BorrowedScalarLeaf::Timestamp {
                ticks: -1_234_567_891,
                unit: ScalarTimestampUnit::Nanosecond
            }
        );
        let wrong_zone = schema(
            ScalarValueType::Timestamp {
                unit: ScalarTimestampUnit::Nanosecond,
                timezone: Some("+00:00".to_owned()),
            },
            true,
        );
        assert_eq!(
            NativeScalarLeafEncoder::validate_input(&input, &wrong_zone),
            Err(NativeScalarLeafError::Type)
        );
    }
    #[test]
    fn decimal256_preserves_unscaled_coefficient_and_declared_scale() {
        let coefficient = i256::from_i128(-123_456_789_123_456_789_123_456_789);
        let input = chunk(
            Arc::new(
                Decimal256Array::from(vec![coefficient])
                    .with_precision_and_scale(40, 9)
                    .unwrap(),
            ),
            None,
            None,
        );
        let expected = schema(
            ScalarValueType::Decimal {
                bits: 256,
                precision: 40,
                scale: 9,
            },
            true,
        );
        let wire = encode(&input, Arc::clone(&expected), 1);
        assert_eq!(
            &wire[SCALAR_LEAF_HEADER_BYTES..],
            &coefficient.to_le_bytes()
        );
        assert_eq!(
            BorrowedScalarLeaf::decode(&expected, &wire).unwrap(),
            BorrowedScalarLeaf::Decimal256 {
                coefficient_le: coefficient.to_le_bytes(),
                precision: 40,
                scale: 9
            }
        );
    }
    #[test]
    fn fixed_leaf_payloads_preserve_integer_float_and_temporal_bits() {
        let large = i128::MIN;
        let large_array =
            FixedSizeBinaryArray::try_from_iter([large.to_le_bytes()].into_iter()).unwrap();
        let cases: Vec<(ArrayRef, ScalarValueType, Vec<u8>)> = vec![
            (
                Arc::new(BooleanArray::from(vec![true])),
                ScalarValueType::Boolean,
                vec![1],
            ),
            (
                Arc::new(Int8Array::from(vec![i8::MIN])),
                ScalarValueType::SignedInteger(8),
                i8::MIN.to_le_bytes().to_vec(),
            ),
            (
                Arc::new(Int16Array::from(vec![i16::MIN])),
                ScalarValueType::SignedInteger(16),
                i16::MIN.to_le_bytes().to_vec(),
            ),
            (
                Arc::new(Int32Array::from(vec![i32::MIN])),
                ScalarValueType::SignedInteger(32),
                i32::MIN.to_le_bytes().to_vec(),
            ),
            (
                Arc::new(Int64Array::from(vec![i64::MIN])),
                ScalarValueType::SignedInteger(64),
                i64::MIN.to_le_bytes().to_vec(),
            ),
            (
                Arc::new(large_array),
                ScalarValueType::LargeInt,
                large.to_le_bytes().to_vec(),
            ),
            (
                Arc::new(Float32Array::from(vec![-0.0])),
                ScalarValueType::Float32,
                (-0.0_f32).to_bits().to_le_bytes().to_vec(),
            ),
            (
                Arc::new(Float64Array::from(vec![-0.0])),
                ScalarValueType::Float64,
                (-0.0_f64).to_bits().to_le_bytes().to_vec(),
            ),
            (
                Arc::new(Date32Array::from(vec![-719_560])),
                ScalarValueType::Date,
                (-719_560_i32).to_le_bytes().to_vec(),
            ),
            (
                Arc::new(Time64MicrosecondArray::from(vec![-123_456_789])),
                ScalarValueType::TimeMicros,
                (-123_456_789_i64).to_le_bytes().to_vec(),
            ),
            (
                Arc::new(TimestampMicrosecondArray::from(vec![-1_234_567])),
                ScalarValueType::Timestamp {
                    unit: ScalarTimestampUnit::Microsecond,
                    timezone: None,
                },
                (-1_234_567_i64).to_le_bytes().to_vec(),
            ),
            (
                Arc::new(
                    Decimal128Array::from(vec![-123_456_789_123_456_789_123_i128])
                        .with_precision_and_scale(30, 3)
                        .unwrap(),
                ),
                ScalarValueType::Decimal {
                    bits: 128,
                    precision: 30,
                    scale: 3,
                },
                (-123_456_789_123_456_789_123_i128).to_le_bytes().to_vec(),
            ),
        ];
        for (array, semantic, payload) in cases {
            let input = chunk(array, None, None);
            let expected = schema(semantic, true);
            let wire = encode(&input, Arc::clone(&expected), 1);
            assert_eq!(&wire[SCALAR_LEAF_HEADER_BYTES..], payload.as_slice());
            assert!(BorrowedScalarLeaf::decode(&expected, &wire).is_ok());
        }
    }
    #[test]
    fn variant_and_json_preserve_the_declared_domain_without_formatting() {
        let variant = chunk(
            Arc::new(LargeBinaryArray::from(vec![&[0xff, 0x00][..]])),
            None,
            None,
        );
        let expected = schema(ScalarValueType::Variant, true);
        let wire = encode(&variant, Arc::clone(&expected), 1);
        assert_eq!(
            BorrowedScalarLeaf::decode(&expected, &wire).unwrap(),
            BorrowedScalarLeaf::Variant(&[0xff, 0x00])
        );
        let json = chunk(
            Arc::new(StringArray::from(vec![" {\"key\": 1} "])),
            Some("json"),
            None,
        );
        let expected = schema(ScalarValueType::Json, true);
        let wire = encode(&json, Arc::clone(&expected), 1);
        assert_eq!(&wire[SCALAR_LEAF_HEADER_BYTES..], b" {\"key\": 1} ");
        assert_eq!(
            BorrowedScalarLeaf::decode(&expected, &wire).unwrap(),
            BorrowedScalarLeaf::Json(" {\"key\": 1} ")
        );
    }
    #[test]
    fn immutable_batch_alias_remains_until_cursor_exits() {
        let array: ArrayRef = Arc::new(BinaryArray::from(vec![&[0xff, 0x00][..]]));
        let weak = Arc::downgrade(&array);
        let input = chunk(array, None, None);
        let expected = schema(ScalarValueType::Binary, true);
        let mut cursor = NativeScalarLeafEncoder::try_new(
            &input,
            Arc::clone(&expected),
            NativeScalarLeafEncoder::scratch_capacity_bytes(),
        )
        .unwrap();
        drop(input);
        assert!(weak.upgrade().is_some());
        let mut one = [0; 1];
        assert_eq!(cursor.step(&mut one).unwrap().emitted_bytes, 1);
        drop(cursor);
        assert!(weak.upgrade().is_none());
        // Physical source alias evidence only; the caller's permit/funding
        // ownership is intentionally not replaced by this Weak observation.
    }
    #[test]
    fn unsupported_dictionary_key_and_slice_selection_are_explicit() {
        use arrow::datatypes::Int8Type;
        let wrong = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0]),
            Arc::new(StringArray::from(vec!["value"])),
        )
        .unwrap();
        let input = chunk(Arc::new(wrong), None, None);
        let expected = schema(ScalarValueType::String, true);
        assert_eq!(
            NativeScalarLeafEncoder::validate_input(&input, &expected),
            Err(NativeScalarLeafError::Type)
        );
        let input = chunk(Arc::new(Int64Array::from(vec![11, -22, 33])), None, None).slice(1, 1);
        let expected = schema(ScalarValueType::SignedInteger(64), true);
        let wire = encode(&input, Arc::clone(&expected), 1);
        assert_eq!(&wire[SCALAR_LEAF_HEADER_BYTES..], &(-22_i64).to_le_bytes());
    }
    #[test]
    fn scratch_and_value_limits_are_checked_before_record_batch_clone() {
        let input = chunk(Arc::new(StringArray::from(vec!["value"])), None, None);
        let expected = schema(ScalarValueType::String, true);
        assert!(matches!(
            NativeScalarLeafEncoder::try_new(
                &input,
                Arc::clone(&expected),
                NativeScalarLeafEncoder::scratch_capacity_bytes() - 1
            ),
            Err(NativeScalarLeafError::ScratchLimit)
        ));
        let large = "v".repeat(64 * 1024 + 1);
        let oversized = chunk(
            Arc::new(StringArray::from(vec![large.as_str()])),
            None,
            None,
        );
        assert_eq!(
            NativeScalarLeafEncoder::validate_input(&oversized, &expected),
            Err(NativeScalarLeafError::Leaf(ScalarLeafError::ValueLimit))
        );
    }
    #[test]
    fn exact_slot_and_leaf_only_domain_cannot_fallback() {
        let input = chunk(Arc::new(Int32Array::from(vec![7])), None, None);
        let wrong = Arc::new(
            ScalarSchema::try_new(ScalarField {
                nullable: true,
                value_type: ScalarValueType::SignedInteger(32),
            })
            .unwrap()
            .bind_native_slots(&[8])
            .unwrap(),
        );
        assert_eq!(
            NativeScalarLeafEncoder::validate_input(&input, &wrong),
            Err(NativeScalarLeafError::Slot)
        );
        let container = schema(
            ScalarValueType::List(Box::new(ScalarField {
                nullable: true,
                value_type: ScalarValueType::SignedInteger(32),
            })),
            true,
        );
        assert_eq!(
            NativeScalarLeafEncoder::validate_input(&input, &container),
            Err(NativeScalarLeafError::UnsupportedContainer)
        );
    }
}
