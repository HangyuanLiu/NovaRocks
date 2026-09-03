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

//! Theta sketch wrapper for Iceberg Puffin NDV statistics.
//!
//! Wraps `datasketches::theta::ThetaSketch` and adds:
//! - Serialization to Apache DataSketches compact binary format
//! - Deserialization from compact binary format
//! - Set union across multiple sketches
//!
//! The compact binary format is compatible with Java/Spark/Trino DataSketches
//! implementations, enabling full interoperability via the standard
//! `apache-datasketches-theta-v1` Puffin blob type.

// NOTE: Most of the public surface is only consumed by callers that will be
// wired in by follow-up agents (StatsAssembler and StatsLoader). Suppress
// dead-code warnings until those land.
#![allow(dead_code)]

use std::hash::Hash;

use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::datatypes::DataType;
use datasketches::hash::value::canonical_float;
use datasketches::theta::{CompactThetaSketch, ThetaSketch, ThetaSketchBuilder, ThetaUnionBuilder};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

const DEFAULT_LG_K: u8 = 12;

/// Provider-local lifecycle adapter around the standard DataSketches Theta
/// mutable and compact states.
pub struct ThetaSketchHandle {
    state: ThetaSketchState,
    // Standard compact sketches do not encode nominal k. This value is only
    // retained for constructing a new mutable sketch or union.
    lg_k: u8,
}

enum ThetaSketchState {
    Mutable(ThetaSketch),
    Compact(CompactThetaSketch),
}

impl ThetaSketchHandle {
    /// Create a new empty sketch with the given `lg_k` (log2 of nominal size k).
    pub fn new(lg_k: u8) -> Result<Self, String> {
        let sketch = ThetaSketchBuilder::default()
            .lg_k(lg_k)
            .build()
            .map_err(|error| format!("create theta sketch: {error}"))?;
        Ok(Self {
            state: ThetaSketchState::Mutable(sketch),
            lg_k,
        })
    }

    /// Update the sketch with a hashable value.
    pub fn update<T: Hash>(&mut self, value: T) -> Result<(), String> {
        match &mut self.state {
            ThetaSketchState::Mutable(sketch) => {
                sketch.update(value);
                Ok(())
            }
            ThetaSketchState::Compact(_) => {
                Err("cannot update an immutable compact theta sketch".to_string())
            }
        }
    }

    /// Update with the same Java-compatible canonical floating-point mapping
    /// used by the former DataSketches 0.2 `update_f64` API.
    pub fn update_f64(&mut self, value: f64) -> Result<(), String> {
        self.update(canonical_float::from_f64(value))
    }

    /// Return the cardinality estimate.
    pub fn estimate(&self) -> f64 {
        match &self.state {
            ThetaSketchState::Mutable(sketch) => sketch.estimate(),
            ThetaSketchState::Compact(sketch) => sketch.estimate(),
        }
    }

    /// Serialize the sketch to Apache DataSketches compact binary format.
    ///
    /// The output is compatible with the Java DataSketches library and can be
    /// embedded directly as an `apache-datasketches-theta-v1` Puffin blob.
    pub fn serialize(&self) -> Vec<u8> {
        self.compact().serialize()
    }

    /// Deserialize a sketch from Apache DataSketches compact binary format.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, String> {
        Self::deserialize_with_lg_k(bytes, DEFAULT_LG_K)
    }

    /// Deserialize a standard compact body while retaining the private
    /// carrier's nominal-k configuration for a later union.
    pub(crate) fn deserialize_with_lg_k(bytes: &[u8], lg_k: u8) -> Result<Self, String> {
        ThetaSketchBuilder::default()
            .lg_k(lg_k)
            .build()
            .map_err(|error| format!("validate theta lg_k: {error}"))?;
        let sketch = CompactThetaSketch::deserialize(bytes)
            .map_err(|error| format!("deserialize compact theta sketch: {error}"))?;
        Ok(Self {
            state: ThetaSketchState::Compact(sketch),
            lg_k,
        })
    }

    pub(crate) fn is_ordered(&self) -> bool {
        match &self.state {
            ThetaSketchState::Mutable(_) => true,
            ThetaSketchState::Compact(sketch) => sketch.is_ordered(),
        }
    }

    /// Union multiple sketches into a single result sketch.
    ///
    pub fn union(sketches: &[&Self]) -> Result<Self, String> {
        let lg_k = sketches.first().map_or(DEFAULT_LG_K, |sketch| sketch.lg_k);
        if sketches.iter().any(|sketch| sketch.lg_k != lg_k) {
            return Err("cannot union theta sketches with different lg_k values".to_string());
        }
        let mut union = ThetaUnionBuilder::default()
            .lg_k(lg_k)
            .build()
            .map_err(|error| format!("create theta union: {error}"))?;
        for sketch in sketches {
            union
                .update(sketch.as_view())
                .map_err(|error| format!("update theta union: {error}"))?;
        }
        Ok(Self {
            state: ThetaSketchState::Compact(union.to_sketch(true)),
            lg_k,
        })
    }

    /// Deserialize multiple compact binary blobs and union them.
    pub fn union_bytes(serialized: &[&[u8]]) -> Result<Self, String> {
        let deserialized: Vec<Self> = serialized
            .iter()
            .map(|b| Self::deserialize(b))
            .collect::<Result<Vec<_>, _>>()?;
        let refs: Vec<&Self> = deserialized.iter().collect();
        Self::union(&refs)
    }

    fn compact(&self) -> CompactThetaSketch {
        match &self.state {
            ThetaSketchState::Mutable(sketch) => sketch.compact(true),
            ThetaSketchState::Compact(sketch) => sketch.clone(),
        }
    }

    fn as_view(&self) -> datasketches::theta::ThetaSketchView<'_> {
        match &self.state {
            ThetaSketchState::Mutable(sketch) => sketch.as_view(),
            ThetaSketchState::Compact(sketch) => sketch.as_view(),
        }
    }

    pub(crate) fn num_retained(&self) -> usize {
        match &self.state {
            ThetaSketchState::Mutable(sketch) => sketch.num_retained(),
            ThetaSketchState::Compact(sketch) => sketch.num_retained(),
        }
    }
}

/// Compute one Theta sketch for every primitive Arrow column carrying an
/// Iceberg parquet field id.
///
/// This preserves the legacy NCP-8 migration input exactly: integers, dates,
/// timestamps, decimals, booleans, and strings use the same Rust `Hash`
/// inputs as before, while floats remain bit inputs with canonical NaN bits.
/// It is not the future Iceberg logical-value canonicalization contract.
pub fn compute_theta_sketches_for_batch(
    batch: &RecordBatch,
) -> Result<Option<std::collections::HashMap<i32, ThetaSketchHandle>>, String> {
    collect_theta_sketches(batch)
}

/// Build per-field Theta sketches from a batch that has no parquet field-id
/// metadata, using a lower-cased column-name to field-id map.
pub fn collect_theta_sketches_by_name(
    batch: &RecordBatch,
    name_to_field_id: &std::collections::HashMap<String, i32>,
) -> Result<std::collections::HashMap<i32, ThetaSketchHandle>, String> {
    const LG_K: u8 = 12;
    let schema = batch.schema();
    let mut sketches = std::collections::HashMap::new();
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let Some(&field_id) = name_to_field_id.get(&field.name().to_lowercase()) else {
            continue;
        };
        let mut sketch = ThetaSketchHandle::new(LG_K)?;
        if feed_array_into_sketch(&mut sketch, field.data_type(), batch.column(col_idx))? {
            sketches.insert(field_id, sketch);
        }
    }
    Ok(sketches)
}

fn collect_theta_sketches(
    batch: &RecordBatch,
) -> Result<Option<std::collections::HashMap<i32, ThetaSketchHandle>>, String> {
    // Apache DataSketches Java/Spark default lg_k = 12 (k = 4096, ~1.5% error).
    const LG_K: u8 = 12;

    let schema = batch.schema();
    let mut sketches = std::collections::HashMap::new();
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let Some(field_id_str) = field.metadata().get(PARQUET_FIELD_ID_META_KEY) else {
            continue;
        };
        let Ok(field_id) = field_id_str.parse::<i32>() else {
            continue;
        };
        let mut sketch = ThetaSketchHandle::new(LG_K)?;
        if feed_array_into_sketch(&mut sketch, field.data_type(), batch.column(col_idx))? {
            sketches.insert(field_id, sketch);
        }
    }
    Ok((!sketches.is_empty()).then_some(sketches))
}

fn feed_array_into_sketch(
    sketch: &mut ThetaSketchHandle,
    data_type: &DataType,
    array: &ArrayRef,
) -> Result<bool, String> {
    use arrow::array::{
        BooleanArray, Date32Array, Date64Array, Decimal128Array, Float32Array, Float64Array,
        Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    use arrow::datatypes::TimeUnit;

    let mut updated = false;
    macro_rules! feed_int {
        ($ty:ty) => {{
            if let Some(values) = array.as_any().downcast_ref::<$ty>() {
                for row in 0..values.len() {
                    if !values.is_null(row) {
                        sketch.update(values.value(row))?;
                        updated = true;
                    }
                }
            }
        }};
    }

    match data_type {
        DataType::Boolean => {
            if let Some(values) = array.as_any().downcast_ref::<BooleanArray>() {
                for row in 0..values.len() {
                    if !values.is_null(row) {
                        sketch.update(u8::from(values.value(row)))?;
                        updated = true;
                    }
                }
            }
        }
        DataType::Int8 => feed_int!(Int8Array),
        DataType::Int16 => feed_int!(Int16Array),
        DataType::Int32 => feed_int!(Int32Array),
        DataType::Int64 => feed_int!(Int64Array),
        DataType::Date32 => feed_int!(Date32Array),
        DataType::Date64 => feed_int!(Date64Array),
        DataType::Float32 => {
            if let Some(values) = array.as_any().downcast_ref::<Float32Array>() {
                for row in 0..values.len() {
                    if !values.is_null(row) {
                        let value = values.value(row);
                        sketch.update(if value.is_nan() {
                            f32::NAN.to_bits()
                        } else {
                            value.to_bits()
                        })?;
                        updated = true;
                    }
                }
            }
        }
        DataType::Float64 => {
            if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
                for row in 0..values.len() {
                    if !values.is_null(row) {
                        let value = values.value(row);
                        sketch.update(if value.is_nan() {
                            f64::NAN.to_bits()
                        } else {
                            value.to_bits()
                        })?;
                        updated = true;
                    }
                }
            }
        }
        DataType::Decimal128(_, _) => {
            if let Some(values) = array.as_any().downcast_ref::<Decimal128Array>() {
                for row in 0..values.len() {
                    if !values.is_null(row) {
                        sketch.update(values.value(row))?;
                        updated = true;
                    }
                }
            }
        }
        DataType::Utf8 => {
            if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
                for row in 0..values.len() {
                    if !values.is_null(row) {
                        sketch.update(values.value(row))?;
                        updated = true;
                    }
                }
            }
        }
        DataType::LargeUtf8 => {
            if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
                for row in 0..values.len() {
                    if !values.is_null(row) {
                        sketch.update(values.value(row))?;
                        updated = true;
                    }
                }
            }
        }
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => feed_int!(TimestampSecondArray),
            TimeUnit::Millisecond => feed_int!(TimestampMillisecondArray),
            TimeUnit::Microsecond => feed_int!(TimestampMicrosecondArray),
            TimeUnit::Nanosecond => feed_int!(TimestampNanosecondArray),
        },
        _ => {}
    }
    Ok(updated)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use arrow::array::{ArrayRef, BinaryArray, Float64Array, Int32Array};
    use arrow::datatypes::{Field, Schema};

    fn sketch(lg_k: u8) -> ThetaSketchHandle {
        ThetaSketchHandle::new(lg_k).expect("theta sketch")
    }

    fn update_range(sketch: &mut ThetaSketchHandle, values: impl Iterator<Item = i32>) {
        for value in values {
            sketch.update(value).expect("theta update");
        }
    }

    #[test]
    fn mutable_exact_and_estimation_states_use_standard_compact_codec() {
        let empty = sketch(12);
        assert_eq!(empty.estimate(), 0.0);

        let mut exact = sketch(12);
        update_range(&mut exact, 0..100);
        let exact_compact = CompactThetaSketch::deserialize(&exact.serialize()).expect("exact");
        assert_eq!(exact_compact.estimate(), 100.0);
        assert!(exact_compact.is_ordered());

        let mut estimated = sketch(10);
        update_range(&mut estimated, 0..50_000);
        let estimated_compact =
            CompactThetaSketch::deserialize(&estimated.serialize()).expect("estimated");
        assert!(estimated_compact.is_estimation_mode());
        assert!((estimated_compact.estimate() - estimated.estimate()).abs() < 1.0);
    }

    #[test]
    fn batch_collection_uses_field_ids_and_canonicalizes_nan() {
        let mut id_metadata = HashMap::new();
        id_metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), "7".to_string());
        let mut invalid_metadata = HashMap::new();
        invalid_metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), "invalid".to_string());
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("measure", DataType::Float64, true).with_metadata(id_metadata),
            Field::new("ignored_binary", DataType::Binary, true),
            Field::new("ignored_bad_id", DataType::Int32, true).with_metadata(invalid_metadata),
        ]));
        let first_nan = f64::from_bits(0x7ff8_0000_0000_0001);
        let second_nan = f64::from_bits(0x7fff_ffff_ffff_ffff);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(Float64Array::from(vec![
                    Some(first_nan),
                    Some(second_nan),
                    None,
                    Some(1.0),
                ])) as ArrayRef,
                std::sync::Arc::new(BinaryArray::from(vec![Some(b"ignored".as_slice()); 4]))
                    as ArrayRef,
                std::sync::Arc::new(Int32Array::from(vec![Some(1); 4])) as ArrayRef,
            ],
        )
        .expect("batch");

        let sketches = compute_theta_sketches_for_batch(&batch)
            .expect("collect theta sketches")
            .expect("field-id sketch");
        assert_eq!(sketches.len(), 1);
        let mut expected = sketch(12);
        expected.update(f64::NAN.to_bits()).expect("canonical NaN");
        expected.update(f64::NAN.to_bits()).expect("canonical NaN");
        expected.update(1.0_f64.to_bits()).expect("float bits");
        assert_eq!(
            sketches.get(&7).expect("id 7").serialize(),
            expected.serialize()
        );
    }

    #[test]
    fn batch_collection_by_name_lowercases_names() {
        let batch = RecordBatch::try_new(
            std::sync::Arc::new(Schema::new(vec![Field::new(
                "MiXeD",
                DataType::Int32,
                true,
            )])),
            vec![std::sync::Arc::new(Int32Array::from(vec![
                Some(1),
                None,
                Some(2),
            ]))],
        )
        .expect("batch");
        let sketches =
            collect_theta_sketches_by_name(&batch, &HashMap::from([("mixed".to_string(), 9)]))
                .expect("collect by name");
        assert_eq!(sketches.len(), 1);
        assert!(sketches.contains_key(&9));
    }

    #[test]
    fn standard_union_handles_mutable_and_compact_states() {
        let empty = sketch(12);
        let mut a = sketch(12);
        update_range(&mut a, 0..5_000);
        let mut b = sketch(12);
        update_range(&mut b, 5_000..10_000);
        let a_bytes = a.serialize();
        let b_bytes = b.serialize();
        let compact_a = ThetaSketchHandle::deserialize(&a_bytes).expect("compact a");
        let combined = ThetaSketchHandle::union(&[&empty, &compact_a, &b]).expect("union");
        let est = combined.estimate();
        assert!(
            (9_000.0..11_000.0).contains(&est),
            "union estimate {est} out of expected range"
        );
        let from_bytes =
            ThetaSketchHandle::union_bytes(&[&a_bytes, &b_bytes]).expect("union bytes");
        assert!((from_bytes.estimate() - combined.estimate()).abs() < 1.0);
    }

    #[test]
    fn standard_decoder_rejects_malformed_and_non_default_seed() {
        assert!(ThetaSketchHandle::deserialize(&[0; 4]).is_err());
        assert!(
            ThetaSketchHandle::deserialize(include_bytes!(
                "../../../../tests/datasketches-tck/fixtures/theta/java62_quickselect_n1000_custom_seed_ordered_v3.sk"
            ))
            .is_err()
        );
    }

    #[test]
    fn compact_state_rejects_updates() {
        let compact = sketch(12).serialize();
        let mut restored = ThetaSketchHandle::deserialize(&compact).expect("compact");
        assert!(restored.update(1_u64).is_err());
        assert!(restored.update_f64(1.0).is_err());
    }

    #[test]
    fn invalid_build_and_union_configuration_return_errors() {
        assert!(ThetaSketchHandle::new(4).is_err());
        let body = sketch(12).serialize();
        let left = ThetaSketchHandle::deserialize_with_lg_k(&body, 11).expect("left");
        let right = ThetaSketchHandle::deserialize_with_lg_k(&body, 12).expect("right");
        assert!(ThetaSketchHandle::union(&[&left, &right]).is_err());
    }

    #[test]
    fn update_f64_preserves_the_java_compatible_0_2_mapping() {
        let mut adapter = sketch(12);
        adapter.update_f64(-0.0).expect("negative zero");
        adapter
            .update_f64(f64::from_bits(0x7fff_ffff_ffff_ffff))
            .expect("NaN");

        let mut standard = ThetaSketchBuilder::default().build().expect("standard");
        // DataSketches 0.2 `update_f64` fed these canonical i64 bit
        // patterns into the normal update path.
        standard.update(0_i64);
        standard.update(0x7ff8_0000_0000_0000_i64);
        assert_eq!(adapter.serialize(), standard.compact(true).serialize());
    }

    #[test]
    fn generic_string_update_preserves_the_existing_hash_domain() {
        let mut adapter = sketch(12);
        adapter.update("alpha").expect("adapter string");
        let mut standard = ThetaSketchBuilder::default().build().expect("standard");
        standard.update("alpha");
        assert_eq!(adapter.serialize(), standard.compact(true).serialize());
    }

    #[test]
    fn java_and_cpp_compact_fixtures_deserialize_and_union() {
        let java = ThetaSketchHandle::deserialize(include_bytes!(
            "../../../../tests/datasketches-tck/fixtures/theta/java62_quickselect_n100000_ordered_v3.sk"
        ))
        .expect("Java compact");
        let cpp = ThetaSketchHandle::deserialize(include_bytes!(
            "../../../../tests/datasketches-tck/fixtures/theta/theta_n100000_cpp.sk"
        ))
        .expect("C++ compact");
        assert!(java.estimate().is_finite());
        assert!(cpp.estimate().is_finite());
        let union = ThetaSketchHandle::union(&[&java, &cpp]).expect("cross-language union");
        assert!(union.estimate().is_finite());
        CompactThetaSketch::deserialize(&union.serialize()).expect("union compact");
    }
}
