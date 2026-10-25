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

use super::*;
use crate::{
    ByteLayout,
    access::ByteSource,
    decode::{DecodedBytes, ValueConversionContext},
    reachability::{ReachableRows, SelectionSet},
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, DictionaryArray, Int32Array, LargeBinaryArray,
    LargeStringArray, StringArray, StringViewArray, types::Int32Type,
};
use std::{cell::Cell, convert::Infallible, sync::Arc};

fn decode(input: &ArrayRef, layout: ByteLayout) -> DecodedBytes {
    let dictionary_layout =
        matches!(input.data_type(), arrow_schema::DataType::Dictionary(_, _)).then_some(layout);
    let source = ByteSource::parse(input, layout, dictionary_layout).unwrap();
    ValueConversionContext::default()
        .decode(&source, &ReachableRows::all(input.len()))
        .unwrap()
}
fn all(decoded: &DecodedBytes) -> SelectionSet {
    ReachableRows::all(decoded.len())
        .materialize(&mut |_| Ok::<_, Infallible>(()))
        .unwrap()
}
fn uniform<H: ByteHashKernel>(
    input: &PreparedBytes<'_>,
    kernel: &H,
    seed: H::Seed,
    nulls: NullHash,
) -> Vec<u64> {
    let mut output = vec![0; input.len()];
    hash_into(
        input,
        kernel,
        HashOptions {
            seeds: HashSeed::Uniform(seed),
            nulls,
        },
        &mut output,
        &mut HashScratch::new(),
        &mut |_| Ok::<_, Infallible>(()),
        &mut |_| Ok::<_, Infallible>(()),
    )
    .unwrap();
    output
}
#[test]
fn independent_golden_hashes_agree_across_string_and_binary_carriers() {
    let text = [Some(""), Some("hello"), None, Some("123456789")];
    let bytes: Vec<_> = text.iter().map(|s| s.map(str::as_bytes)).collect();
    let inputs: Vec<(ArrayRef, ByteLayout)> = vec![
        (Arc::new(StringArray::from(text.to_vec())), ByteLayout::Utf8),
        (
            Arc::new(LargeStringArray::from(text.to_vec())),
            ByteLayout::LargeUtf8,
        ),
        (
            Arc::new(StringViewArray::from(text.to_vec())),
            ByteLayout::Utf8View,
        ),
        (
            Arc::new(BinaryArray::from(bytes.clone())),
            ByteLayout::Binary,
        ),
        (
            Arc::new(LargeBinaryArray::from(bytes.clone())),
            ByteLayout::LargeBinary,
        ),
        (
            Arc::new(BinaryViewArray::from(bytes)),
            ByteLayout::BinaryView,
        ),
    ];
    for (input, layout) in inputs {
        let decoded = decode(&input, layout);
        let rows = all(&decoded);
        let prepared = PreparedBytes::new(&decoded, &rows).unwrap();
        let actual = uniform(
            &prepared,
            &Fnv1a64,
            14695981039346656037,
            NullHash::Value(99),
        );
        assert_eq!(
            actual,
            [
                14695981039346656037,
                0xa430d84680aabd0b,
                99,
                0x06d5573923c6cdfc
            ]
        );
        let actual = uniform(&prepared, &Crc32, 0, NullHash::Value(99));
        assert_eq!(actual, [0, 0x3610a686, 99, 0xcbf43926]);
    }
}

struct Counted<'a>(&'a Cell<usize>);
impl ByteHashKernel for Counted<'_> {
    type Seed = u64;
    type State = (u64, usize);
    fn init(&self, seed: u64) -> Self::State {
        self.0.set(self.0.get() + 1);
        (seed, 0)
    }
    fn update(&self, state: &mut Self::State, bytes: &[u8]) {
        Fnv1a64.update(&mut state.0, bytes);
        state.1 += bytes.len();
    }
    fn finish(&self, state: Self::State) -> u64 {
        state.0
    }
}
#[test]
fn dictionary_bottom_reuse_is_per_call_and_null_keys_do_not_read_their_integer() {
    let values: ArrayRef = Arc::new(StringViewArray::from(vec![
        Some("hello"),
        None,
        Some("ignored"),
    ]));
    let input: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), Some(0), Some(1), None, Some(0)]),
            values,
        )
        .unwrap(),
    );
    let decoded = decode(&input, ByteLayout::Utf8View);
    let rows = all(&decoded);
    let prepared = PreparedBytes::new(&decoded, &rows).unwrap();
    let calls = Cell::new(0);
    let kernel = Counted(&calls);
    let mut output = vec![0; 5];
    let mut scratch = HashScratch::new();
    for seed in [17, 29] {
        hash_into(
            &prepared,
            &kernel,
            HashOptions {
                seeds: HashSeed::Uniform(seed),
                nulls: NullHash::PreserveSeed,
            },
            &mut output,
            &mut scratch,
            &mut |_| Ok::<_, Infallible>(()),
            &mut |_| Ok::<_, Infallible>(()),
        )
        .unwrap();
        let expected = [Some("hello"), Some("hello"), None, None, Some("hello")];
        for (actual, value) in output.iter().zip(expected) {
            let oracle = value.map_or(seed, |v| {
                let mut state = seed;
                Fnv1a64.update(&mut state, v.as_bytes());
                state
            });
            assert_eq!(*actual, oracle);
        }
    }
    assert_eq!(
        calls.get(),
        2,
        "one reachable non-NULL bottom value per call"
    );
}
#[test]
fn opaque_per_row_states_keep_prefixes_and_independent_null_preserved_values() {
    let values: ArrayRef = Arc::new(StringViewArray::from(vec![Some("suffix"), None]));
    let input: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), Some(0), Some(1), None]),
            values,
        )
        .unwrap(),
    );
    let decoded = decode(&input, ByteLayout::Utf8View);
    let rows = all(&decoded);
    let prepared = PreparedBytes::new(&decoded, &rows).unwrap();
    let calls = Cell::new(0);
    let kernel = Counted(&calls);
    let states = [(13, 91), (27, 92), (39, 93), (51, 94)].map(|(seed, preserved_seed)| HashState {
        state: (seed, 100),
        preserved_seed,
    });
    let mut output = [0; 4];
    hash_into(
        &prepared,
        &kernel,
        HashOptions {
            seeds: HashSeed::PerRow(&states),
            nulls: NullHash::PreserveSeed,
        },
        &mut output,
        &mut HashScratch::new(),
        &mut |_| Ok::<_, Infallible>(()),
        &mut |_| Ok::<_, Infallible>(()),
    )
    .unwrap();
    let mut first = 13;
    Fnv1a64.update(&mut first, b"suffix");
    let mut second = 27;
    Fnv1a64.update(&mut second, b"suffix");
    assert_eq!(output, [first, second, 93, 94]);
    assert_eq!(
        calls.get(),
        0,
        "existing running states are not reinitialized"
    );
}
#[test]
fn sparse_dictionary_scratch_scales_with_selected_rows_and_slice_keeps_coordinates() {
    let values: ArrayRef = Arc::new(StringViewArray::from(vec!["x"; 100_000]));
    let input: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(4), Some(99_999), Some(3)]),
            values,
        )
        .unwrap(),
    );
    let decoded = decode(&input, ByteLayout::Utf8View);
    let mut rows = SelectionSet::try_new(3).unwrap();
    rows.insert(1).unwrap();
    let prepared = PreparedBytes::new(&decoded, &rows).unwrap();
    let mut growth_bytes = 0;
    let mut output = [0];
    hash_into(
        &prepared,
        &Fnv1a64,
        HashOptions {
            seeds: HashSeed::Uniform(17),
            nulls: NullHash::Value(8),
        },
        &mut output,
        &mut HashScratch::new(),
        &mut |layout| {
            growth_bytes += layout.size();
            Ok::<_, Infallible>(())
        },
        &mut |_| Ok::<_, Infallible>(()),
    )
    .unwrap();
    assert!(growth_bytes < 1024);
    let input: ArrayRef = Arc::new(
        StringArray::from(vec![Some("before"), Some("hello"), None, Some("after")]).slice(1, 2),
    );
    let decoded = decode(&input, ByteLayout::Utf8);
    let rows = all(&decoded);
    let prepared = PreparedBytes::new(&decoded, &rows).unwrap();
    assert_eq!(
        uniform(&prepared, &Crc32, 7, NullHash::PreserveSeed),
        [0x84307a96, 7]
    );
}
#[test]
fn crc_running_state_composes_across_chunks_without_using_finalized_hash_as_state() {
    let text = "123456789".repeat(20_000);
    let input: ArrayRef = Arc::new(StringArray::from(vec![text.as_str()]));
    let decoded = decode(&input, ByteLayout::Utf8);
    let rows = all(&decoded);
    let prepared = PreparedBytes::new(&decoded, &rows).unwrap();
    let mut state = Crc32.init(19);
    Crc32.update(&mut state, text.as_bytes());
    assert_eq!(
        uniform(&prepared, &Crc32, 19, NullHash::PreserveSeed),
        [Crc32.finish(state)]
    );
}
#[test]
fn long_value_and_dictionary_growth_preserve_original_caller_failure() {
    let text = "x".repeat(BYTE_BLOCK * 3);
    let input: ArrayRef = Arc::new(StringArray::from(vec![text.as_str()]));
    let decoded = decode(&input, ByteLayout::Utf8);
    let rows = all(&decoded);
    let prepared = PreparedBytes::new(&decoded, &rows).unwrap();
    let mut output = [0];
    let mut checked = 0;
    let error = hash_into(
        &prepared,
        &Fnv1a64,
        HashOptions {
            seeds: HashSeed::Uniform(17),
            nulls: NullHash::Value(0),
        },
        &mut output,
        &mut HashScratch::new(),
        &mut |_| Ok::<_, &'static str>(()),
        &mut |n| {
            checked += n;
            if checked >= BYTE_BLOCK * 2 {
                Err("long-value-stop")
            } else {
                Ok(())
            }
        },
    )
    .unwrap_err();
    assert!(matches!(error, ValueFailure::Caller("long-value-stop")));
    let value: ArrayRef = Arc::new(StringArray::from(vec!["x"]));
    let source = ByteSource::constant(&value, ByteLayout::Utf8, 5).unwrap();
    let decoded = ValueConversionContext::default()
        .decode(&source, &ReachableRows::all(5))
        .unwrap();
    let rows = all(&decoded);
    let prepared = PreparedBytes::new(&decoded, &rows).unwrap();
    let mut output = [0; 5];
    let error = hash_into(
        &prepared,
        &Fnv1a64,
        HashOptions {
            seeds: HashSeed::Uniform(17),
            nulls: NullHash::Value(0),
        },
        &mut output,
        &mut HashScratch::new(),
        &mut |_| Err("growth-stop"),
        &mut |_| Ok(()),
    )
    .unwrap_err();
    assert!(matches!(error, ValueFailure::Caller("growth-stop")));
}
#[test]
fn all_null_and_empty_values_keep_bounded_row_control_without_per_row_callbacks() {
    let input: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>; 5000]));
    let decoded = decode(&input, ByteLayout::Utf8);
    let rows = all(&decoded);
    let prepared = PreparedBytes::new(&decoded, &rows).unwrap();
    let mut output = vec![0; 5000];
    let mut calls = 0;
    hash_into(
        &prepared,
        &Fnv1a64,
        HashOptions {
            seeds: HashSeed::Uniform(17),
            nulls: NullHash::PreserveSeed,
        },
        &mut output,
        &mut HashScratch::new(),
        &mut |_| Ok::<_, Infallible>(()),
        &mut |_| {
            calls += 1;
            Ok::<_, Infallible>(())
        },
    )
    .unwrap();
    assert_eq!(output, vec![17; 5000]);
    assert!(
        calls < prepared.len() / 2,
        "control is per block, not per row: {calls}"
    );
}
