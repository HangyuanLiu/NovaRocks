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
use super::*;
use arrow_array::{
    Array,
    builder::{BinaryBuilder, StringBuilder},
};

fn state(a: usize, b: usize, sorted: bool, initialized: bool) -> MannStateExtents {
    MannStateExtents {
        sample_lengths: [a, b],
        sample_capacities: [a, b],
        sorted,
        initialized,
    }
}

#[test]
fn mann_whitney_resources_zero_rows_keep_real_default_builder_payload() {
    let source = MannSourceParams::try_new().unwrap();
    for kind in [FlatByteKind::Binary, FlatByteKind::Utf8] {
        let facts = MannEmissionResources::new(kind, 0).finish(&source).unwrap();
        let array = match kind {
            FlatByteKind::Binary => {
                std::sync::Arc::new(BinaryBuilder::new().finish()) as arrow_array::ArrayRef
            }
            FlatByteKind::Utf8 => {
                std::sync::Arc::new(StringBuilder::new().finish()) as arrow_array::ArrayRef
            }
        };
        assert_eq!(array.len(), 0);
        let actual = array.to_data();
        assert_eq!(
            actual
                .buffers()
                .iter()
                .map(|buffer| buffer.capacity())
                .sum::<usize>(),
            5124
        );
        assert!(actual.nulls().is_none());
        assert_eq!(
            facts.payload,
            ComponentFacts {
                requests: RequestUpper {
                    bytes: 5124,
                    requests: 2
                },
                peak: 5124,
                retained: 5124
            }
        );
        assert_eq!(facts.row_requests, ZERO_REQUESTS);
        assert_eq!(
            facts.global_output_retained,
            5124 + facts.finish_and_custody.global_retained_metadata_upper
        );
        assert_eq!(
            facts.typed_output_retained,
            facts.finish_and_custody.custody_typed.bytes
        );
        assert_eq!(
            facts.body_checkpoint_clone,
            RequestUpper {
                bytes: 512,
                requests: 1
            }
        );
        assert_eq!(facts.external_failure_stock_upper, 512);
    }
}

#[test]
fn mann_whitney_resources_codec_matches_original_append_capacity_chain() {
    for count in [0, 1, 2, 3, 7, 29, 63, 127, 220, 513, 1024] {
        let (facts, output) = codec_facts(count).unwrap();
        let mut actual = Vec::<u8>::new();
        let mut cumulative = 0;
        let mut allocations = 0;
        let mut peak = 0;
        let mut append = |part: &[u8]| {
            let before = actual.capacity();
            actual.extend_from_slice(part);
            if actual.capacity() != before {
                cumulative += actual.capacity();
                allocations += 1;
                peak = peak.max(before + actual.capacity());
            }
        };
        append(&[1]);
        append(&1i64.to_le_bytes());
        append(&(count as u32).to_le_bytes());
        append(&0u32.to_le_bytes());
        for _ in 0..count {
            append(&1f64.to_le_bytes());
        }
        assert_eq!(
            facts.requests,
            RequestUpper {
                bytes: cumulative,
                requests: allocations
            }
        );
        assert_eq!(facts.peak, peak);
        assert_eq!(facts.retained, actual.capacity());
        assert_eq!(output, actual.len());
    }
    assert_eq!(
        codec_facts(usize::MAX),
        Err(ScalarResourceError::Arithmetic)
    );
}

#[test]
fn mann_whitney_resources_renderer_bounds_actual_original_float_authors() {
    let source = MannSourceParams::try_new().unwrap();
    for sd in [
        0.0,
        -0.0,
        f64::from_bits(1),
        -f64::from_bits(1),
        f64::MIN_POSITIVE,
        f64::MAX,
        -f64::MAX,
        f64::INFINITY,
        f64::NAN,
    ] {
        let text = format!(r#"{{"Logical Error": "sd({}) is not a valid value."}}"#, sd);
        assert!(text.len() <= 374, "{} bytes", text.len());
        assert!(text.capacity() <= source.renderer.retained);
    }
    for value in [
        0.0,
        -0.0,
        f64::from_bits(1),
        f64::MAX,
        f64::INFINITY,
        f64::NAN,
    ] {
        let number = serde_json::Number::from_f64(value)
            .map(|number| number.to_string())
            .unwrap_or_else(|| "null".to_string());
        assert!(number.len() <= 24);
        let text = format!("[{}, {}]", number, number);
        assert!(text.len() <= 52);
        assert!(text.capacity() <= source.renderer.retained);
    }
}

#[test]
fn mann_whitney_resources_orders_scratch_and_carries_combined_through_renderer() {
    let source = MannSourceParams::try_new().unwrap();
    let unsorted = state(300, 400, false, true);
    let sorted = state(300, 400, true, true);
    let sort = sample_sorts(unsorted).unwrap();
    assert_eq!(sort.requests.bytes, 700 * std::mem::size_of::<f64>());
    assert_eq!(sort.peak, 400 * std::mem::size_of::<f64>());
    assert_eq!(sort.retained, 0);
    let with_sort = row_facts(FlatByteKind::Utf8, unsorted, &source).unwrap();
    let no_sort = row_facts(FlatByteKind::Utf8, sorted, &source).unwrap();
    assert_eq!(
        with_sort.requests.bytes - no_sort.requests.bytes,
        sort.requests.bytes
    );
    assert_eq!(with_sort.requests.requests - no_sort.requests.requests, 2);
    let combined = 700 * std::mem::size_of::<(f64, usize)>();
    assert!(no_sort.peak >= combined + source.renderer.peak);
    assert!(no_sort.peak >= 2 * combined);
    assert_eq!(no_sort.returned, source.renderer.retained);
    let mut one = MannEmissionResources::new(FlatByteKind::Utf8, 1);
    one.add_state(sorted, &source).unwrap();
    let one = one.finish(&source).unwrap();
    let mut two = MannEmissionResources::new(FlatByteKind::Utf8, 2);
    two.add_state(sorted, &source).unwrap();
    two.add_state(sorted, &source).unwrap();
    let two = two.finish(&source).unwrap();
    assert_eq!(two.row_requests.bytes, 2 * one.row_requests.bytes);
    assert_eq!(two.row_requests.requests, 2 * one.row_requests.requests);
    assert_eq!(two.row_peak, one.row_peak);
    assert_eq!(two.row_returned_stock, one.row_returned_stock);
    assert!(two.operation_global_peak >= two.payload.peak + two.row_peak + 1024);
}

#[test]
fn mann_whitney_resources_null_groups_preserve_offsets_and_validity_growth() {
    let source = MannSourceParams::try_new().unwrap();
    let rows = 2050;
    let mut model = MannEmissionResources::new(FlatByteKind::Utf8, rows);
    let mut builder = StringBuilder::new();
    for _ in 0..rows {
        model.add_state(state(0, 0, true, false), &source).unwrap();
        builder.append_null();
    }
    let facts = model.finish(&source).unwrap();
    let array = builder.finish();
    assert_eq!(array.null_count(), rows);
    let actual = array.to_data();
    let stock = actual
        .buffers()
        .iter()
        .map(|buffer| buffer.capacity())
        .sum::<usize>()
        + actual.nulls().unwrap().buffer().capacity();
    assert!(facts.payload.retained >= stock);
    assert_eq!(facts.row_requests, ZERO_REQUESTS);
    // Intermediate states still serialize their original header.
    assert_ne!(
        row_facts(FlatByteKind::Binary, state(0, 0, true, false), &source)
            .unwrap()
            .requests,
        ZERO_REQUESTS
    );
}

#[test]
fn mann_whitney_resources_reject_missing_states_and_unrepresentable_extents() {
    let source = MannSourceParams::try_new().unwrap();
    assert!(matches!(
        MannEmissionResources::new(FlatByteKind::Binary, 1).finish(&source),
        Err(ScalarResourceError::Invariant(_))
    ));
    let mut full = MannEmissionResources::new(FlatByteKind::Binary, 0);
    assert!(matches!(
        full.add_state(state(0, 0, true, false), &source),
        Err(ScalarResourceError::Invariant(_))
    ));
    assert_eq!(
        default_byte_payload(usize::MAX, 0, true, &source),
        Err(ScalarResourceError::Arithmetic)
    );
    assert_eq!(
        default_byte_payload(0, usize::MAX, false, &source),
        Err(ScalarResourceError::Arithmetic)
    );
    assert_eq!(
        row_facts(
            FlatByteKind::Utf8,
            state(usize::MAX, 1, false, true),
            &source
        ),
        Err(ScalarResourceError::Arithmetic)
    );
    assert!(vector::<(f64, usize)>(0, usize::MAX / 32, 4).is_err());
}

#[test]
fn mann_whitney_resources_guard_rejects_a_second_number_author() {
    let lock = include_str!("../../../../../Cargo.lock");
    require_number_sources(lock).unwrap();
    for name in ["serde_json", "zmij"] {
        let additional = format!("{lock}\n[[package]]\nname = \"{name}\"\nversion = \"0.0.0\"\n");
        assert!(matches!(
            require_number_sources(&additional),
            Err(ScalarResourceError::SourceModel(_))
        ));
        assert!(
            require_number_sources(
                &lock.replace(&format!("name = \"{name}\""), "name = \"unrelated\"")
            )
            .is_err()
        );
    }
    assert!(require_number_sources(&lock.replace("1.0.150", "1.0.151")).is_err());
}
