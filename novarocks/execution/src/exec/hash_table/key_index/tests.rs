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
    exec::{
        hash_table::allocation::AggregateAllocator,
        pipeline::operator::{ConstructionFailure, InputConstructionOwner},
    },
    runtime::{
        fragment::{ExecutionFailure, ExecutionResult},
        mem_tracker::MemTracker,
    },
};
use arrow::{
    array::*,
    datatypes::{DataType, Int32Type},
};
use novarocks_type_contract::FunctionValueType;
use novarocks_value_arrays::{
    batch::hash::{ByteHashKernel, HashScratch},
    decode::ValueConversionContext,
    reachability::{ReachableRows, SelectionSet},
};
use std::{alloc::Layout, cell::Cell, sync::Arc};

fn owner() -> Box<dyn InputConstructionOwner> {
    owner_with_tracker().0
}
fn owner_with_tracker() -> (Box<dyn InputConstructionOwner>, Arc<MemTracker>) {
    let tracker = MemTracker::new_root("local key test construction");
    let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        tracker.clone(),
        Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
    ).unwrap();
    let declaration = crate::exec::chunk::typed_fixture::layout(
        Arc::new(arrow::datatypes::Schema::empty()),
        &[],
        &[],
    );
    let schema = factory.create_compiled_chunk_schema(&declaration).unwrap();
    (factory.begin_typed_construction(&schema).unwrap(), tracker)
}
fn readable(len: usize) -> SelectionSet {
    ReachableRows::all(len)
        .materialize(&mut |_| Ok::<_, ExecutionFailure>(()))
        .unwrap()
}
fn input(
    arrays: &[ArrayRef],
    types: &[FunctionValueType],
    rows: &[usize],
    owner: &dyn InputConstructionOwner,
) -> KeyInput {
    KeyInput::new(
        arrays,
        types,
        &readable(arrays[0].len()),
        rows,
        &mut ValueConversionContext::default(),
        owner,
        &|| Ok(()),
    )
    .unwrap()
}
fn hash(
    input: &KeyInput,
    spec: &KeySpec,
    policy: &LocalHashPolicy,
    owner: &dyn InputConstructionOwner,
) -> LocalHashBatch {
    let prepared = PreparedKeyBatch::new(
        spec,
        input,
        AggregateAllocator::new(MemTracker::new_root("local key test")),
        owner,
        &|| Ok(()),
    )
    .unwrap();
    hash_batch(
        &prepared,
        policy,
        &mut HashScratch::new(),
        owner,
        &|| Ok(()),
    )
    .unwrap()
}
#[test]
fn streaming_xxh3_matches_independent_one_shot_across_block_boundaries() {
    let data: Vec<u8> = (0..131_113).map(|n| (n % 251) as u8).collect();
    for width in [1, 17, 239, 1024, 65_536] {
        let kernel = super::local_hash::Xxh3Kernel;
        let mut state = kernel.init(59);
        for block in data.chunks(width) {
            kernel.update(&mut state, block);
        }
        assert_eq!(
            kernel.finish(state),
            xxhash_rust::xxh3::xxh3_64_with_seed(&data, 59)
        );
    }
}
#[test]
fn byte_carriers_share_strategy_hash_and_original_occurrence_order() {
    let owner = owner();
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let policy = LocalHashPolicy::with_seed(&spec, 31, owner.as_ref()).unwrap();
    let values = [
        Some("long shared prefix A"),
        None,
        Some("long shared prefix B"),
    ];
    let offsets: ArrayRef = Arc::new(StringArray::from(values.to_vec()));
    let view: ArrayRef = Arc::new(StringViewArray::from(values.to_vec()));
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![1, 0, 2]),
            Arc::new(StringArray::from(vec![None, values[0], values[2]])),
        )
        .unwrap(),
    );
    let mut outputs = Vec::new();
    for array in [offsets, view, dictionary] {
        let binding = if matches!(array.data_type(), DataType::Utf8View) {
            FunctionValueType::new(DataType::Utf8View, true)
        } else {
            ty.clone()
        };
        let page = input(&[array], &[binding], &[2, 0, 2, 1], owner.as_ref());
        assert_eq!(spec.strategy(), KeyStrategy::Bytes);
        let result = hash(&page, &spec, &policy, owner.as_ref());
        let actual: Vec<_> = (0..4).map(|i| result.hash(i).unwrap()).collect();
        assert_eq!(actual[0], actual[2]);
        assert!((0..4).all(|i| result.participates(i).unwrap()));
        assert_eq!(
            (0..4).map(|i| page.row(i).unwrap()).collect::<Vec<_>>(),
            vec![2, 0, 2, 1]
        );
        outputs.push(actual);
    }
    assert!(outputs.windows(2).all(|pair| pair[0] == pair[1]));
}
#[test]
fn dictionary_hashes_only_selected_distinct_nonnull_bottom_values() {
    let owner = owner();
    let ty = FunctionValueType::new(DataType::Utf8, false);
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Unmatchable],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let policy = LocalHashPolicy::with_seed(&spec, 7, owner.as_ref()).unwrap();
    let array: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(1), None, Some(0), Some(1), Some(2)]),
            Arc::new(StringArray::from(vec![Some("x"), Some("y"), None])),
        )
        .unwrap(),
    );
    let page = input(&[array], &[ty], &[3, 0, 2, 3], owner.as_ref());
    super::local_hash::BYTE_HASH_INITS.with(|count| count.set(0));
    let result = hash(&page, &spec, &policy, owner.as_ref());
    assert_eq!(super::local_hash::BYTE_HASH_INITS.with(Cell::get), 2);
    assert_eq!(result.hash(0).unwrap(), result.hash(1).unwrap());
    assert_eq!(result.hash(0).unwrap(), result.hash(3).unwrap());
}
#[test]
fn required_null_and_undefined_occurrences_are_rejected_before_hashing() {
    let owner = owner();
    let array: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None]));
    let ty = FunctionValueType::new(DataType::Int32, false);
    let result = KeyInput::new(
        &[array.clone()],
        &[ty.clone()],
        &readable(2),
        &[1],
        &mut ValueConversionContext::default(),
        owner.as_ref(),
        &|| Ok(()),
    );
    assert!(result.is_err());
    let selected = ReachableRows::indices(2, &[0])
        .materialize(&mut |_| Ok::<_, ExecutionFailure>(()))
        .unwrap();
    let result = KeyInput::new(
        &[array],
        &[ty],
        &selected,
        &[1],
        &mut ValueConversionContext::default(),
        owner.as_ref(),
        &|| Ok(()),
    );
    assert!(result.is_err());
}
#[test]
fn mixed_null_policies_keep_null_class_separate_from_nonparticipation() {
    let owner = owner();
    let types = vec![FunctionValueType::new(DataType::Int32, true); 2];
    let spec = KeySpec::new(
        &types,
        &[NullPolicy::Equal, NullPolicy::Unmatchable],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let policy = LocalHashPolicy::with_seed(&spec, 19, owner.as_ref()).unwrap();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![None, None, Some(1)])),
        Arc::new(Int32Array::from(vec![Some(2), None, Some(2)])),
    ];
    let page = input(&arrays, &types, &[0, 1, 2, 0], owner.as_ref());
    let result = hash(&page, &spec, &policy, owner.as_ref());
    assert_eq!(
        (0..4)
            .map(|i| result.participates(i).unwrap())
            .collect::<Vec<_>>(),
        vec![true, false, true, true]
    );
    assert_eq!(result.hash(0).unwrap(), result.hash(3).unwrap());
}
#[test]
fn float_canonical_author_unifies_signed_zero_and_nan_payloads() {
    let owner = owner();
    let ty = FunctionValueType::new(DataType::Float64, false);
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let policy = LocalHashPolicy::with_seed(&spec, 71, owner.as_ref()).unwrap();
    let a: ArrayRef = Arc::new(Float64Array::from(vec![
        0.0,
        -0.0,
        f64::from_bits(0x7ff8000000000001),
        f64::from_bits(0xfff8000000000002),
    ]));
    let page = input(&[a], &[ty], &[0, 1, 2, 3], owner.as_ref());
    let result = hash(&page, &spec, &policy, owner.as_ref());
    assert_eq!(result.hash(0).unwrap(), result.hash(1).unwrap());
    assert_eq!(result.hash(2).unwrap(), result.hash(3).unwrap());
}
#[test]
fn full_decimal_domain_is_not_reconstructed_from_equal_carrier_width() {
    let owner = owner();
    let expected = FunctionValueType::new(DataType::Decimal128(10, 2), false);
    let actual = FunctionValueType::new(DataType::Decimal128(10, 3), false);
    let spec = KeySpec::new(
        &[expected],
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let array: ArrayRef = Arc::new(
        Decimal128Array::from(vec![123])
            .with_precision_and_scale(10, 3)
            .unwrap(),
    );
    let page = input(&[array], &[actual], &[0], owner.as_ref());
    assert!(
        PreparedKeyBatch::new(
            &spec,
            &page,
            AggregateAllocator::new(MemTracker::new_root("domain refusal")),
            owner.as_ref(),
            &|| Ok(())
        )
        .is_err()
    );
}
#[test]
fn saved_hashes_require_the_original_input_and_live_policy_even_with_equal_seed() {
    let owner = owner();
    let ty = FunctionValueType::new(DataType::Int32, false);
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let policy = LocalHashPolicy::with_seed(&spec, 1, owner.as_ref()).unwrap();
    let other_policy = LocalHashPolicy::with_seed(&spec, 1, owner.as_ref()).unwrap();
    let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    let first = input(
        std::slice::from_ref(&array),
        std::slice::from_ref(&ty),
        &[0, 1],
        owner.as_ref(),
    );
    let second = input(&[array], &[ty], &[0, 1], owner.as_ref());
    let hashes = hash(&first, &spec, &policy, owner.as_ref());
    hashes.validate(&first.clone(), &policy.clone()).unwrap();
    assert!(hashes.validate(&second, &policy).is_err());
    assert!(hashes.validate(&first, &other_policy).is_err());
}
#[test]
fn long_value_control_failure_is_preserved_without_publishing_a_hash_batch() {
    let owner = owner();
    let ty = FunctionValueType::new(DataType::Utf8, false);
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let policy = LocalHashPolicy::with_seed(&spec, 8, owner.as_ref()).unwrap();
    let array: ArrayRef = Arc::new(StringArray::from(vec!["x".repeat(512 * 1024)]));
    let page = input(&[array], &[ty], &[0], owner.as_ref());
    let prepared = PreparedKeyBatch::new(
        &spec,
        &page,
        AggregateAllocator::new(MemTracker::new_root("long bytes")),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let visits = Cell::new(0);
    let cause: ExecutionFailure = ConstructionFailure::Control(
        novarocks_type_contract::CompileControlError::DeadlineExceeded,
    )
    .into();
    let result = hash_batch(
        &prepared,
        &policy,
        &mut HashScratch::new(),
        owner.as_ref(),
        &|| {
            visits.set(visits.get() + 1);
            if visits.get() == 8 {
                Err(cause.clone())
            } else {
                Ok(())
            }
        },
    );
    match result {
        Err(error) => assert_eq!(error, cause),
        Ok(_) => panic!("incomplete hash must not be published"),
    }
}
struct RefusingOwner<'a>(&'a dyn InputConstructionOwner, ExecutionFailure);
impl InputConstructionOwner for RefusingOwner<'_> {
    fn storage_lifetime(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
        self.0.storage_lifetime()
    }
    fn request(&self, _: Layout) -> Result<(), ConstructionFailure> {
        Err(ConstructionFailure::from(self.1.clone()))
    }
    fn observe_opaque_upper_bound(&self, n: usize) -> Result<(), ConstructionFailure> {
        self.0.observe_opaque_upper_bound(n)
    }
    fn observe(
        &self,
        n: usize,
        path: &[crate::exec::chunk::ArrayPathStep],
        step: crate::exec::chunk::ArrayNormalizationStep<'_>,
    ) -> Result<(), ConstructionFailure> {
        self.0.observe(n, path, step)
    }
}
#[test]
fn original_growth_refusal_wins_before_prepared_storage_allocation() {
    let owner = owner();
    let cause: ExecutionFailure = ConstructionFailure::Control(
        novarocks_type_contract::CompileControlError::ResourceExhausted,
    )
    .into();
    let refusing = RefusingOwner(owner.as_ref(), cause.clone());
    let ty = FunctionValueType::new(DataType::Int32, false);
    let result = KeySpec::new(&[ty], &[NullPolicy::Equal], &refusing, &|| Ok(()));
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("refused allocation cannot construct a spec"),
    };
    // The original caller fact remains nested intact through construction.
    assert_eq!(
        novarocks_failure_contract::FailureProjection::failure_fact(&error),
        novarocks_failure_contract::FailureProjection::failure_fact(&cause)
    );
}

fn pairs(
    left: &KeyInput,
    left_rows: &[usize],
    right: &KeyInput,
    right_rows: &[usize],
    spec: &KeySpec,
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<Vec<u8>> {
    let allocator = AggregateAllocator::new(MemTracker::new_root("local key comparison"));
    let left = PreparedKeyBatch::new(spec, left, allocator.clone(), owner, &|| Ok(()))?;
    let right = PreparedKeyBatch::new(spec, right, allocator, owner, &|| Ok(()))?;
    let mut output = vec![0; left_rows.len()];
    super::pairs::compare_prepared_pairs(
        &left,
        left_rows,
        &right,
        right_rows,
        &mut output,
        owner,
        checkpoint,
    )?;
    Ok(output)
}

// These are literal equality-class oracles for K's collision path, independent
// of the randomized hash and table implementation. Unit scope is sufficient:
// no fixture, global state or native transport participates in comparisons.
#[test]
fn prepared_collision_pairs_keep_mixed_null_policy_dictionary_values_and_order() {
    let owner = owner();
    let types = [
        FunctionValueType::new(DataType::Int32, true),
        FunctionValueType::new(DataType::Utf8, true),
    ];
    let left = input(
        &[
            Arc::new(Int32Array::from(vec![Some(7), Some(7), None, Some(8)])),
            Arc::new(StringArray::from(vec![
                Some("same prefix left"),
                None,
                Some("same prefix right"),
                Some("same prefix left"),
            ])),
        ],
        &types,
        &[3, 0, 2, 1, 0],
        owner.as_ref(),
    );
    let right = input(
        &[
            Arc::new(Int32Array::from(vec![Some(7), Some(7), None, Some(8)])),
            Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![Some(1), None, Some(0), Some(1)]),
                    Arc::new(StringArray::from(vec![
                        "same prefix right",
                        "same prefix left",
                    ])),
                )
                .unwrap(),
            ),
        ],
        &types,
        &[2, 0, 1, 3],
        owner.as_ref(),
    );
    for (policies, expected) in [
        (
            [NullPolicy::Equal, NullPolicy::Equal],
            vec![1, 1, 0, 1, 0, 1],
        ),
        (
            [NullPolicy::Unmatchable, NullPolicy::Equal],
            vec![1, 0, 0, 1, 0, 1],
        ),
        (
            [NullPolicy::Equal, NullPolicy::Unmatchable],
            vec![1, 1, 0, 0, 0, 1],
        ),
    ] {
        let spec = KeySpec::new(&types, &policies, owner.as_ref(), &|| Ok(())).unwrap();
        assert_eq!(
            pairs(
                &left,
                &[0, 2, 0, 1, 3, 0],
                &right,
                &[0, 2, 2, 1, 0, 0],
                &spec,
                owner.as_ref(),
                &|| Ok(())
            )
            .unwrap(),
            expected
        );
    }
}

#[test]
fn fixed_collision_pairs_use_canonical_nan_and_zero_with_exact_decimal_values() {
    let owner = owner();
    let types = [
        FunctionValueType::new(DataType::Float64, false),
        FunctionValueType::new(DataType::Decimal128(18, 3), false),
    ];
    let left = input(
        &[
            Arc::new(Float64Array::from(vec![
                0.0,
                f64::from_bits(0x7ff8_0000_0000_0001),
                1.0,
            ])),
            Arc::new(
                Decimal128Array::from(vec![1230, 1230, 1230])
                    .with_precision_and_scale(18, 3)
                    .unwrap(),
            ),
        ],
        &types,
        &[0, 1, 2],
        owner.as_ref(),
    );
    let right = input(
        &[
            Arc::new(Float64Array::from(vec![
                -0.0,
                f64::from_bits(0x7ff8_0000_0000_0042),
                1.0,
            ])),
            Arc::new(
                Decimal128Array::from(vec![1230, 1230, 1231])
                    .with_precision_and_scale(18, 3)
                    .unwrap(),
            ),
        ],
        &types,
        &[0, 1, 2],
        owner.as_ref(),
    );
    let spec = KeySpec::new(&types, &[NullPolicy::Equal; 2], owner.as_ref(), &|| Ok(())).unwrap();
    assert_eq!(
        pairs(
            &left,
            &[1, 0, 2, 1],
            &right,
            &[1, 0, 2, 0],
            &spec,
            owner.as_ref(),
            &|| Ok(())
        )
        .unwrap(),
        vec![1, 1, 0, 0]
    );
}

#[test]
fn complex_collision_pairs_reuse_original_nested_framing_and_sql_bytes() {
    let owner = owner();
    let mut lists = ListBuilder::new(Int32Builder::new());
    for values in [Some(&[1, 2][..]), Some(&[1, 3][..]), None, Some(&[][..])] {
        if let Some(values) = values {
            for &v in values {
                lists.values().append_value(v);
            }
        }
        lists.append(values.is_some());
    }
    let lists: ArrayRef = Arc::new(lists.finish());
    let types = [
        FunctionValueType::new(lists.data_type().clone(), true),
        FunctionValueType::new(DataType::Utf8, false),
    ];
    let keys = input(
        &[lists, Arc::new(StringArray::from(vec!["same prefix"; 4]))],
        &types,
        &[3, 2, 0, 1],
        owner.as_ref(),
    );
    let spec = KeySpec::new(&types, &[NullPolicy::Equal; 2], owner.as_ref(), &|| Ok(())).unwrap();
    assert_eq!(
        pairs(
            &keys,
            &[0, 0, 2, 3, 2, 1],
            &keys,
            &[0, 1, 2, 3, 3, 1],
            &spec,
            owner.as_ref(),
            &|| Ok(())
        )
        .unwrap(),
        vec![1, 0, 1, 1, 0, 1]
    );
}

#[test]
fn long_collision_comparison_preserves_caller_stop_and_unselected_rows_are_rejected() {
    let owner = owner();
    let text = "same prefix complete bytes".repeat(30_000);
    let types = [FunctionValueType::new(DataType::Utf8, false)];
    let keys = input(
        &[Arc::new(StringArray::from(vec![text.as_str(), "hidden"]))],
        &types,
        &[0],
        owner.as_ref(),
    );
    let spec = KeySpec::new(&types, &[NullPolicy::Equal], owner.as_ref(), &|| Ok(())).unwrap();
    assert_eq!(
        pairs(
            &keys,
            &[0, 0],
            &keys,
            &[0, 0],
            &spec,
            owner.as_ref(),
            &|| Ok(())
        )
        .unwrap(),
        vec![1, 1]
    );
    let cause: ExecutionFailure = ConstructionFailure::Control(
        novarocks_type_contract::CompileControlError::DeadlineExceeded,
    )
    .into();
    let checks = Cell::new(0usize);
    let error = pairs(&keys, &[0], &keys, &[0], &spec, owner.as_ref(), &|| {
        checks.set(checks.get() + 1);
        if checks.get() == 8 {
            Err(cause.clone())
        } else {
            Ok(())
        }
    })
    .unwrap_err();
    assert_eq!(error, cause);
    assert!(pairs(&keys, &[1], &keys, &[0], &spec, owner.as_ref(), &|| Ok(())).is_err());
}

#[test]
fn owned_byte_representatives_keep_order_and_survive_the_actual_source_payload_exit() {
    use arrow_buffer::{Buffer, OffsetBuffer, ScalarBuffer};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Source {
        payload: Vec<u8>,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for Source {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (owner, tracker) = owner_with_tracker();
    let drops = Arc::new(AtomicUsize::new(0));
    let wanted = "same prefix selected representative";
    let hidden = "unused backing".repeat(40_000);
    let payload = [hidden.as_bytes(), wanted.as_bytes()].concat();
    let source = Arc::new(Source {
        payload,
        drops: drops.clone(),
    });
    let pointer = std::ptr::NonNull::new(source.payload.as_ptr().cast_mut()).unwrap();
    let length = source.payload.len();
    // SAFETY: Source keeps this unchanged Vec allocation alive for every
    // Buffer alias. The only mutable pointer is never written through.
    let payload = unsafe { Buffer::from_custom_allocation(pointer, length, source) };
    let array: ArrayRef = Arc::new(StringArray::new(
        OffsetBuffer::new(ScalarBuffer::from(vec![
            0i32,
            hidden.len() as i32,
            length as i32,
        ])),
        payload,
        None,
    ));
    let ty = FunctionValueType::new(DataType::Utf8, false);
    let keys = input(&[array], std::slice::from_ref(&ty), &[1], owner.as_ref());
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let page = {
        let prepared = PreparedKeyBatch::new(
            &spec,
            &keys,
            AggregateAllocator::new(tracker.clone()),
            owner.as_ref(),
            &|| Ok(()),
        )
        .unwrap();
        let mut pages = super::store::OwnedByteColumn::copy_pages(
            &prepared,
            0,
            &[1, 1],
            owner.as_ref(),
            crate::runtime::tracked_state_allocator::expression_allocation_host(tracker.clone()),
            &CopyControl,
        )
        .unwrap();
        assert_eq!(pages.len(), 1);
        let page = pages.pop().unwrap();
        assert_eq!(page.len(), 2);
        let mut matches = [None; 3];
        page.compare_pairs(&prepared, 0, &[1, 1, 1], &[1, 0, 1], &mut matches, &|| {
            Ok(())
        })
        .unwrap();
        assert_eq!(matches, [Some(true); 3]);
        page
    };
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(keys);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let array = page
        .array()
        .as_any()
        .downcast_ref::<StringViewArray>()
        .unwrap();
    assert_eq!(array.value(0), wanted);
    assert_eq!(array.value(1), wanted);
    assert_eq!(page.binding().data_type(), &DataType::Utf8View);
    assert!(page.binding().same_value_domain(&ty));
    assert!(!page.storage().is_pure());
}

#[test]
fn owned_byte_page_target_splits_work_without_rejecting_a_larger_single_value() {
    let (owner, tracker) = owner_with_tracker();
    let long = "x".repeat(150_000);
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let keys = input(
        &[Arc::new(StringArray::from(vec![
            Some(long.as_str()),
            None,
            Some("tail"),
        ]))],
        std::slice::from_ref(&ty),
        &[0, 1, 2],
        owner.as_ref(),
    );
    let spec = KeySpec::new(&[ty], &[NullPolicy::Equal], owner.as_ref(), &|| Ok(())).unwrap();
    let prepared = PreparedKeyBatch::new(
        &spec,
        &keys,
        AggregateAllocator::new(tracker.clone()),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let pages = super::store::OwnedByteColumn::copy_pages(
        &prepared,
        0,
        &[0, 1, 2],
        owner.as_ref(),
        crate::runtime::tracked_state_allocator::expression_allocation_host(tracker.clone()),
        &CopyControl,
    )
    .unwrap();
    // The page target is private tuning. Preserve values across any valid
    // page boundaries instead of freezing a production page size in a test.
    let actual = pages
        .iter()
        .flat_map(|page| {
            page.array()
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap()
                .iter()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, vec![Some(long.as_str()), None, Some("tail")]);
    let pages = super::store::OwnedByteColumn::copy_pages(
        &prepared,
        0,
        &[2, 1, 2],
        owner.as_ref(),
        crate::runtime::tracked_state_allocator::expression_allocation_host(tracker.clone()),
        &CopyControl,
    )
    .unwrap();
    let actual = pages
        .iter()
        .flat_map(|page| {
            page.array()
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap()
                .iter()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, vec![Some("tail"), None, Some("tail")]);
}

#[test]
fn owned_binary_pages_preserve_non_utf8_empty_null_and_cross_carrier_pairs() {
    let (owner, tracker) = owner_with_tracker();
    let raw = [0xff, 0, 1];
    let ty = FunctionValueType::new(DataType::Binary, true);
    let keys = input(
        &[Arc::new(BinaryArray::from(vec![
            Some(raw.as_slice()),
            Some(b"".as_slice()),
            None,
        ]))],
        std::slice::from_ref(&ty),
        &[0, 1, 2],
        owner.as_ref(),
    );
    let spec = KeySpec::new(&[ty], &[NullPolicy::Equal], owner.as_ref(), &|| Ok(())).unwrap();
    let prepared = PreparedKeyBatch::new(
        &spec,
        &keys,
        AggregateAllocator::new(tracker.clone()),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let pages = super::store::OwnedByteColumn::copy_pages(
        &prepared,
        0,
        &[2, 0, 1, 0],
        owner.as_ref(),
        crate::runtime::tracked_state_allocator::expression_allocation_host(tracker.clone()),
        &CopyControl,
    )
    .unwrap();
    let actual = pages
        .iter()
        .flat_map(|page| {
            page.array()
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .unwrap()
                .iter()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            None,
            Some(raw.as_slice()),
            Some(b"".as_slice()),
            Some(raw.as_slice())
        ]
    );
    let locations = pages
        .iter()
        .enumerate()
        .flat_map(|(page, values)| (0..values.len()).map(move |row| (page, row)))
        .collect::<Vec<_>>();
    let probe = input(
        &[Arc::new(BinaryViewArray::from(vec![
            Some(raw.as_slice()),
            None,
            Some(b"".as_slice()),
        ]))],
        &[FunctionValueType::new(DataType::BinaryView, true)],
        &[0, 1, 2],
        owner.as_ref(),
    );
    let probe = PreparedKeyBatch::new(
        &spec,
        &probe,
        AggregateAllocator::new(tracker),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let pairs = [(0, 1, true), (1, 0, true), (2, 2, true), (0, 2, false)];
    for (page_index, page) in pages.iter().enumerate() {
        let selected = pairs
            .iter()
            .filter(|(_, stored, _)| locations[*stored].0 == page_index)
            .collect::<Vec<_>>();
        let input_rows = selected.iter().map(|(row, _, _)| *row).collect::<Vec<_>>();
        let page_rows = selected
            .iter()
            .map(|(_, stored, _)| locations[*stored].1)
            .collect::<Vec<_>>();
        let mut out = vec![None; selected.len()];
        page.compare_pairs(&probe, 0, &input_rows, &page_rows, &mut out, &|| Ok(()))
            .unwrap();
        assert_eq!(
            out,
            selected
                .iter()
                .map(|(_, _, expected)| Some(*expected))
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn owned_byte_publication_preserves_the_original_construction_refusal() {
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Binary, false);
    let keys = input(
        &[Arc::new(BinaryArray::from(vec![
            b"binary value".as_slice(),
        ]))],
        std::slice::from_ref(&ty),
        &[0],
        owner.as_ref(),
    );
    let spec = KeySpec::new(&[ty], &[NullPolicy::Equal], owner.as_ref(), &|| Ok(())).unwrap();
    let prepared = PreparedKeyBatch::new(
        &spec,
        &keys,
        AggregateAllocator::new(tracker.clone()),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let cause: ExecutionFailure = ConstructionFailure::Control(
        novarocks_type_contract::CompileControlError::ResourceExhausted,
    )
    .into();
    let refusing = RefusingOwner(owner.as_ref(), cause.clone());
    let result = super::store::OwnedByteColumn::copy_pages(
        &prepared,
        0,
        &[0],
        &refusing,
        crate::runtime::tracked_state_allocator::expression_allocation_host(tracker.clone()),
        &CopyControl,
    );
    let error = match result {
        Err(cause) => cause,
        Ok(_) => panic!("refused key page cannot be published"),
    };
    assert_eq!(
        novarocks_failure_contract::FailureProjection::failure_fact(&error),
        novarocks_failure_contract::FailureProjection::failure_fact(&cause)
    );
}

#[test]
fn complex_neighbor_does_not_rehash_repeated_long_dictionary_components() {
    let owner = owner();
    let mut lists = ListBuilder::new(Int32Builder::new());
    for values in [&[1, 2][..], &[1, 2][..], &[3][..]] {
        for &value in values {
            lists.values().append_value(value);
        }
        lists.append(true);
    }
    let list: ArrayRef = Arc::new(lists.finish());
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0, 0, 0]),
            Arc::new(StringArray::from(vec!["shared prefix".repeat(16_384)])),
        )
        .unwrap(),
    );
    let types = vec![
        FunctionValueType::new(list.data_type().clone(), false),
        FunctionValueType::new(DataType::Utf8, false),
    ];
    let spec = KeySpec::new(
        &types,
        &[NullPolicy::Equal, NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    assert_eq!(spec.strategy(), KeyStrategy::Canonical);
    let policy = LocalHashPolicy::with_seed(&spec, 67, owner.as_ref()).unwrap();
    let page = input(&[list, dictionary], &types, &[2, 0, 1, 0], owner.as_ref());
    super::local_hash::BYTE_HASH_INITS.with(|count| count.set(0));
    let result = hash(&page, &spec, &policy, owner.as_ref());
    assert_eq!(super::local_hash::BYTE_HASH_INITS.with(Cell::get), 1);
    assert_eq!(result.hash(1).unwrap(), result.hash(2).unwrap());
    assert_eq!(result.hash(1).unwrap(), result.hash(3).unwrap());
}

#[test]
fn sliced_dictionary_keeps_logical_null_and_repeated_occurrence_coordinates() {
    let owner = owner();
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Unmatchable],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let policy = LocalHashPolicy::with_seed(&spec, 43, owner.as_ref()).unwrap();
    let array: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![1, 1, 0, 1, 1]),
            Arc::new(StringArray::from(vec![
                None,
                Some("the same long backing value"),
            ])),
        )
        .unwrap(),
    );
    let page = input(&[array.slice(1, 3)], &[ty], &[2, 0, 1, 2], owner.as_ref());
    super::local_hash::BYTE_HASH_INITS.with(|count| count.set(0));
    let result = hash(&page, &spec, &policy, owner.as_ref());
    assert_eq!(super::local_hash::BYTE_HASH_INITS.with(Cell::get), 1);
    assert_eq!(
        (0..4)
            .map(|i| result.participates(i).unwrap())
            .collect::<Vec<_>>(),
        vec![true, true, false, true]
    );
    assert_eq!(result.hash(0).unwrap(), result.hash(1).unwrap());
    assert_eq!(result.hash(0).unwrap(), result.hash(3).unwrap());
}
#[test]
fn nominal_json_and_string_with_the_same_arrow_carrier_are_distinct_domains() {
    let owner = owner();
    let string = FunctionValueType::new(DataType::Utf8, false);
    let json = FunctionValueType::try_with_logical_type(
        DataType::Utf8,
        false,
        novarocks_type_contract::ValueLogicalType::Json,
    )
    .unwrap();
    let spec = KeySpec::new(&[string], &[NullPolicy::Equal], owner.as_ref(), &|| Ok(())).unwrap();
    let array: ArrayRef = Arc::new(StringArray::from(vec!["{}"]));
    let page = input(&[array], &[json], &[0], owner.as_ref());
    assert!(
        PreparedKeyBatch::new(
            &spec,
            &page,
            AggregateAllocator::new(MemTracker::new_root("nominal key domain")),
            owner.as_ref(),
            &|| Ok(())
        )
        .is_err()
    );
}

struct CopyControl;
impl novarocks_functions::KernelEvaluationControl for CopyControl {
    fn checkpoint(&self, _: u32) -> Result<(), novarocks_functions::KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), novarocks_functions::KernelFailure> {
        panic!("byte copy never waits")
    }
}

#[test]
fn owned_complex_components_keep_original_framing_nullable_classes_and_literal_list_values() {
    let (owner, tracker) = owner_with_tracker();
    let mut lists = ListBuilder::new(Int32Builder::new());
    for values in [
        Some(vec![Some(1), None, Some(2)]),
        None,
        Some(vec![]),
        Some(vec![Some(1), None, Some(3)]),
    ] {
        if let Some(values) = &values {
            for value in values {
                lists.values().append_option(*value);
            }
        }
        lists.append(values.is_some());
    }
    let array: ArrayRef = Arc::new(lists.finish());
    let ty = FunctionValueType::new(array.data_type().clone(), true);
    let keys = input(
        &[array],
        std::slice::from_ref(&ty),
        &[3, 2, 0, 1, 0],
        owner.as_ref(),
    );
    for (policy, expected) in [
        (NullPolicy::Equal, [1, 0, 1, 1, 0]),
        (NullPolicy::Unmatchable, [1, 0, 0, 1, 0]),
    ] {
        let spec = KeySpec::new(
            std::slice::from_ref(&ty),
            &[policy],
            owner.as_ref(),
            &|| Ok(()),
        )
        .unwrap();
        let prepared = PreparedKeyBatch::new(
            &spec,
            &keys,
            AggregateAllocator::new(tracker.clone()),
            owner.as_ref(),
            &|| Ok(()),
        )
        .unwrap();
        let stored = super::canonical_store::OwnedCanonicalColumn::copy_selected(
            &prepared,
            0,
            &[0, 1, 2],
            owner.as_ref(),
            crate::runtime::tracked_state_allocator::expression_allocation_host(tracker.clone()),
            &CopyControl,
        )
        .unwrap();
        assert_eq!(stored.len(), 3);
        let mut out = [0; 5];
        stored
            .compare_pairs(
                &prepared,
                0,
                &[0, 3, 1, 2, 0],
                &[0, 0, 1, 2, 2],
                &mut out,
                &|| Ok(()),
            )
            .unwrap();
        assert_eq!(out, expected);
        let read = stored.read(0).unwrap();
        assert!(read.binding.same_value_domain(&ty));
        // The original decoder is only an independent test observer here;
        // production typed materialization still needs its governed creator.
        let decoded = super::super::key_builder::decode_group_key_rows(
            ty.data_type(),
            &[
                read.payload,
                stored.read(1).unwrap().payload,
                stored.read(2).unwrap().payload,
            ],
        )
        .unwrap();
        let list = decoded.as_any().downcast_ref::<ListArray>().unwrap();
        let first = list.value(0);
        assert_eq!(
            first
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1), None, Some(2)]
        );
        assert!(list.is_null(1));
        assert_eq!(list.value_length(2), 0);
    }
}

#[test]
fn empty_complex_value_does_not_pin_unreferenced_view_payload_or_invent_an_arrow_binding() {
    use arrow_buffer::{Buffer, OffsetBuffer, ScalarBuffer};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Payload {
        bytes: Vec<u8>,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (owner, tracker) = owner_with_tracker();
    let drops = Arc::new(AtomicUsize::new(0));
    let source = Arc::new(Payload {
        bytes: "unreferenced view backing".repeat(8192).into_bytes(),
        drops: drops.clone(),
    });
    let length = source.bytes.len();
    let pointer = std::ptr::NonNull::new(source.bytes.as_ptr().cast_mut()).unwrap();
    // SAFETY: the immutable Vec and its complete extent live in Payload until
    // every Buffer alias exits. No write is performed through this pointer.
    let buffer = unsafe { Buffer::from_custom_allocation(pointer, length, source) };
    let offsets: ArrayRef = Arc::new(StringArray::new(
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, length as i32])),
        buffer,
        None,
    ));
    let mut view = novarocks_value_arrays::build::ValueBuilder::new(
        novarocks_value_arrays::ByteLayout::Utf8View,
    );
    view.append_array_value(
        offsets.as_ref(),
        0,
        &mut novarocks_value_arrays::control::global_growth,
        &mut novarocks_value_arrays::control::uncontrolled,
    )
    .unwrap();
    let view = view.finish().unwrap().array;
    drop(offsets);
    let field = Arc::new(arrow::datatypes::Field::new(
        "item",
        DataType::Utf8View,
        true,
    ));
    let list: ArrayRef = Arc::new(ListArray::new(
        field.clone(),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 0])),
        view,
        None,
    ));
    let ty = FunctionValueType::new(list.data_type().clone(), true);
    let keys = input(&[list], std::slice::from_ref(&ty), &[0], owner.as_ref());
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let stored = {
        let prepared = PreparedKeyBatch::new(
            &spec,
            &keys,
            AggregateAllocator::new(tracker.clone()),
            owner.as_ref(),
            &|| Ok(()),
        )
        .unwrap();
        super::canonical_store::OwnedCanonicalColumn::copy_selected(
            &prepared,
            0,
            &[0],
            owner.as_ref(),
            crate::runtime::tracked_state_allocator::expression_allocation_host(tracker.clone()),
            &CopyControl,
        )
        .unwrap()
    };
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(keys);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let read = stored.read(0).unwrap();
    assert!(read.payload.is_some());
    assert!(read.binding.same_value_domain(&ty));
    let independent_empty: ArrayRef = Arc::new(ListArray::new(
        field,
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 0])),
        new_empty_array(&DataType::Utf8View),
        None,
    ));
    let probe = input(&[independent_empty], &[ty], &[0], owner.as_ref());
    let probe = PreparedKeyBatch::new(
        &spec,
        &probe,
        AggregateAllocator::new(tracker),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let mut out = [0];
    stored
        .compare_pairs(&probe, 0, &[0], &[0], &mut out, &|| Ok(()))
        .unwrap();
    assert_eq!(out, [1]);
}

#[test]
fn complex_owned_serialization_keeps_the_original_stop_inside_a_long_value() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Stop {
        visits: AtomicUsize,
    }
    impl novarocks_functions::KernelEvaluationControl for Stop {
        fn checkpoint(&self, _: u32) -> Result<(), novarocks_functions::KernelFailure> {
            if self.visits.fetch_add(1, Ordering::SeqCst) + 1 == 12 {
                Err(novarocks_functions::KernelFailure::DeadlineExceeded)
            } else {
                Ok(())
            }
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), novarocks_functions::KernelFailure> {
            panic!("copy never waits")
        }
    }
    let (owner, tracker) = owner_with_tracker();
    let mut lists = ListBuilder::new(StringBuilder::new());
    lists.values().append_value("x".repeat(700_000));
    lists.append(true);
    let array: ArrayRef = Arc::new(lists.finish());
    let ty = FunctionValueType::new(array.data_type().clone(), true);
    let keys = input(&[array], std::slice::from_ref(&ty), &[0], owner.as_ref());
    let spec = KeySpec::new(&[ty], &[NullPolicy::Equal], owner.as_ref(), &|| Ok(())).unwrap();
    let prepared = PreparedKeyBatch::new(
        &spec,
        &keys,
        AggregateAllocator::new(tracker.clone()),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let cause = ExecutionFailure::from(novarocks_functions::KernelFailure::DeadlineExceeded);
    let control = Stop {
        visits: AtomicUsize::new(0),
    };
    let error = super::canonical_store::OwnedCanonicalColumn::copy_selected(
        &prepared,
        0,
        &[0],
        owner.as_ref(),
        crate::runtime::tracked_state_allocator::expression_allocation_host(tracker),
        &control,
    )
    .err()
    .unwrap();
    assert_eq!(error, cause);
    assert_eq!(control.visits.load(Ordering::SeqCst), 12);
}

#[test]
fn canonical_owned_locations_and_reads_preserve_literals_across_actual_page_boundaries() {
    let (owner, tracker) = owner_with_tracker();
    let alpha = "alpha".repeat(40);
    let beta = "beta".repeat(40);
    let mut lists = ListBuilder::new(StringBuilder::new());
    for value in [Some(alpha.as_str()), Some(beta.as_str()), None, Some("")] {
        if let Some(value) = value {
            lists.values().append_value(value);
        }
        lists.append(value.is_some());
    }
    let array: ArrayRef = Arc::new(lists.finish());
    let ty = FunctionValueType::new(array.data_type().clone(), true);
    let keys = input(
        &[array],
        std::slice::from_ref(&ty),
        &[3, 2, 1, 0],
        owner.as_ref(),
    );
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let prepared = PreparedKeyBatch::new(
        &spec,
        &keys,
        AggregateAllocator::new(tracker.clone()),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    // A unit-only soft target exercises the real publication loop without
    // freezing production page size or introducing a product size limit.
    let stored = super::canonical_store::OwnedCanonicalColumn::copy_selected_for_target(
        &prepared,
        0,
        &[1, 2, 0, 3, 1],
        owner.as_ref(),
        crate::runtime::tracked_state_allocator::expression_allocation_host(tracker),
        &CopyControl,
        128,
    )
    .unwrap();
    assert!(stored.page_count() > 1);
    let payloads = (0..stored.len())
        .map(|row| stored.read(row).unwrap().payload)
        .collect::<Vec<_>>();
    let decoded =
        super::super::key_builder::decode_group_key_rows(ty.data_type(), &payloads).unwrap();
    let lists = decoded.as_any().downcast_ref::<ListArray>().unwrap();
    for (row, expected) in [
        Some(beta.as_str()),
        None,
        Some(alpha.as_str()),
        Some(""),
        Some(beta.as_str()),
    ]
    .into_iter()
    .enumerate()
    {
        match expected {
            None => assert!(lists.is_null(row)),
            Some(expected) => {
                let child = lists.value(row);
                assert_eq!(
                    child
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![Some(expected)]
                );
            }
        }
    }
    let mut out = [0; 6];
    stored
        .compare_pairs(
            &prepared,
            0,
            &[1, 1, 0, 3, 2, 0],
            &[0, 4, 2, 3, 1, 0],
            &mut out,
            &|| Ok(()),
        )
        .unwrap();
    assert_eq!(out, [1, 1, 1, 1, 1, 0]);
}

#[test]
fn owned_fixed_decimal_rows_are_selected_copied_and_keep_exact_signed_parameters() {
    use arrow_buffer::{Buffer, NullBuffer, ScalarBuffer};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Source {
        values: Vec<i128>,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for Source {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (owner, tracker) = owner_with_tracker();
    let drops = Arc::new(AtomicUsize::new(0));
    let source = Arc::new(Source {
        values: vec![0, 1230, 4560],
        drops: drops.clone(),
    });
    let ptr = std::ptr::NonNull::new(source.values.as_ptr().cast_mut().cast::<u8>()).unwrap();
    let bytes = source.values.len() * std::mem::size_of::<i128>();
    // SAFETY: the immutable Vec<i128> provides correct alignment and extent;
    // the custom owner keeps it alive for every Buffer clone and slice.
    let payload = unsafe { Buffer::from_custom_allocation(ptr, bytes, source) };
    let values: ArrayRef = Arc::new(
        Decimal128Array::new(
            ScalarBuffer::new(payload, 0, 3),
            Some(NullBuffer::from(vec![false, true, true])),
        )
        .with_precision_and_scale(18, 3)
        .unwrap(),
    );
    let ty = FunctionValueType::new(DataType::Decimal128(18, 3), true);
    let keys = input(
        &[values],
        std::slice::from_ref(&ty),
        &[2, 0, 1],
        owner.as_ref(),
    );
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let stored = {
        let prepared = PreparedKeyBatch::new(
            &spec,
            &keys,
            AggregateAllocator::new(tracker.clone()),
            owner.as_ref(),
            &|| Ok(()),
        )
        .unwrap();
        let stored = super::fixed_store::OwnedFixedColumn::copy_selected(
            &prepared,
            0,
            &[1, 0, 1],
            owner.as_ref(),
            crate::runtime::tracked_state_allocator::expression_allocation_host(tracker),
            &CopyControl,
        )
        .unwrap();
        let mut out = [0; 3];
        stored
            .compare_pairs(&prepared, 0, &[0, 2, 1], &[1, 0, 2], &mut out, &|| Ok(()))
            .unwrap();
        assert_eq!(out, [1, 0, 1]);
        stored
    };
    drop(keys);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(
        stored
            .binding()
            .exactly_equals_observed::<novarocks_type_contract::ValueTypeError>(&ty, || Ok(()))
            .unwrap()
    );
    let values = stored
        .array()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(values.value(0), 1230);
    assert!(values.is_null(1));
    assert_eq!(values.value(2), 1230);
    assert!(!stored.storage().is_pure());
}

#[test]
fn owned_fixed_float_and_non_uuid_fixed_binary_keep_values_and_canonical_equality() {
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Float64, false);
    let keys = input(
        &[Arc::new(Float64Array::from(vec![
            -0.0,
            0.0,
            f64::from_bits(0x7ff8_0000_0000_0011),
            f64::from_bits(0x7ff8_0000_0000_0022),
        ]))],
        std::slice::from_ref(&ty),
        &[0, 1, 2, 3],
        owner.as_ref(),
    );
    let spec = KeySpec::new(&[ty], &[NullPolicy::Equal], owner.as_ref(), &|| Ok(())).unwrap();
    let prepared = PreparedKeyBatch::new(
        &spec,
        &keys,
        AggregateAllocator::new(tracker.clone()),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let stored = super::fixed_store::OwnedFixedColumn::copy_selected(
        &prepared,
        0,
        &[0, 2],
        owner.as_ref(),
        crate::runtime::tracked_state_allocator::expression_allocation_host(tracker.clone()),
        &CopyControl,
    )
    .unwrap();
    let mut out = [0; 3];
    stored
        .compare_pairs(&prepared, 0, &[1, 3, 1], &[0, 1, 1], &mut out, &|| Ok(()))
        .unwrap();
    assert_eq!(out, [1, 1, 0]);
    let actual = stored
        .array()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(actual.value(0).to_bits(), (-0.0f64).to_bits());
    assert_eq!(actual.value(1).to_bits(), 0x7ff8_0000_0000_0011);
    let a = [1u8; 32];
    let mut b = a;
    b[31] = 2;
    let array: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_from_iter([a.as_slice(), b.as_slice()].into_iter()).unwrap(),
    );
    let ty = FunctionValueType::new(DataType::FixedSizeBinary(32), false);
    let keys = input(&[array], std::slice::from_ref(&ty), &[0, 1], owner.as_ref());
    let spec = KeySpec::new(&[ty], &[NullPolicy::Equal], owner.as_ref(), &|| Ok(())).unwrap();
    let prepared = PreparedKeyBatch::new(
        &spec,
        &keys,
        AggregateAllocator::new(tracker.clone()),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let stored = super::fixed_store::OwnedFixedColumn::copy_selected(
        &prepared,
        0,
        &[1],
        owner.as_ref(),
        crate::runtime::tracked_state_allocator::expression_allocation_host(tracker),
        &CopyControl,
    )
    .unwrap();
    assert_eq!(stored.binding().data_type(), &DataType::FixedSizeBinary(32));
    assert_eq!(
        stored
            .array()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
        b
    );
    let mut out = [0; 2];
    stored
        .compare_pairs(&prepared, 0, &[0, 1], &[0, 0], &mut out, &|| Ok(()))
        .unwrap();
    assert_eq!(out, [0, 1]);
}

#[test]
fn owned_fixed_copy_preserves_original_control_and_construction_refusals() {
    struct Stop;
    impl novarocks_functions::KernelEvaluationControl for Stop {
        fn checkpoint(&self, _: u32) -> Result<(), novarocks_functions::KernelFailure> {
            Err(novarocks_functions::KernelFailure::DeadlineExceeded)
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), novarocks_functions::KernelFailure> {
            panic!("copy never waits")
        }
    }
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::UInt64, false);
    let keys = input(
        &[Arc::new(UInt64Array::from(vec![u64::MAX, 7]))],
        std::slice::from_ref(&ty),
        &[0, 1],
        owner.as_ref(),
    );
    let spec = KeySpec::new(&[ty], &[NullPolicy::Equal], owner.as_ref(), &|| Ok(())).unwrap();
    let prepared = PreparedKeyBatch::new(
        &spec,
        &keys,
        AggregateAllocator::new(tracker.clone()),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let error = super::fixed_store::OwnedFixedColumn::copy_selected(
        &prepared,
        0,
        &[0],
        owner.as_ref(),
        crate::runtime::tracked_state_allocator::expression_allocation_host(tracker.clone()),
        &Stop,
    )
    .err()
    .unwrap();
    assert_eq!(
        error,
        ExecutionFailure::from(novarocks_functions::KernelFailure::DeadlineExceeded)
    );
    let cause: ExecutionFailure = ConstructionFailure::Control(
        novarocks_type_contract::CompileControlError::ResourceExhausted,
    )
    .into();
    let refusing = RefusingOwner(owner.as_ref(), cause.clone());
    let error = super::fixed_store::OwnedFixedColumn::copy_selected(
        &prepared,
        0,
        &[0],
        &refusing,
        crate::runtime::tracked_state_allocator::expression_allocation_host(tracker),
        &CopyControl,
    )
    .err()
    .unwrap();
    assert_eq!(
        novarocks_failure_contract::FailureProjection::failure_fact(&error),
        novarocks_failure_contract::FailureProjection::failure_fact(&cause)
    );
}

struct TaggedCreator<'a> {
    original: &'a dyn InputConstructionOwner,
    storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl InputConstructionOwner for TaggedCreator<'_> {
    fn storage_lifetime(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
        self.storage.clone()
    }
    fn request(&self, layout: Layout) -> Result<(), ConstructionFailure> {
        self.original.request(layout)
    }
    fn observe_opaque_upper_bound(&self, bytes: usize) -> Result<(), ConstructionFailure> {
        self.original.observe_opaque_upper_bound(bytes)
    }
    fn observe(
        &self,
        n: usize,
        path: &[crate::exec::chunk::ArrayPathStep],
        step: crate::exec::chunk::ArrayNormalizationStep<'_>,
    ) -> Result<(), ConstructionFailure> {
        self.original.observe(n, path, step)
    }
}

#[test]
fn owned_byte_creator_stays_with_the_last_arrow_buffer_alias_after_page_drop() {
    use novarocks_type_contract::{ValueTypeStorageHold, ValueTypeStorageLifetime};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Tag(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Tag {}
    impl Drop for Tag {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Utf8, false);
    let keys = input(
        &[Arc::new(StringArray::from(vec![
            "long owned payload retained by a sliced Arrow buffer",
        ]))],
        std::slice::from_ref(&ty),
        &[0],
        owner.as_ref(),
    );
    let spec = KeySpec::new(&[ty], &[NullPolicy::Equal], owner.as_ref(), &|| Ok(())).unwrap();
    let prepared = PreparedKeyBatch::new(
        &spec,
        &keys,
        AggregateAllocator::new(tracker.clone()),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let tagged = TaggedCreator {
        original: owner.as_ref(),
        storage: owner
            .storage_lifetime()
            .retaining_observed::<ConstructionFailure>(
                &ValueTypeStorageLifetime::held(Arc::new(Tag(drops.clone()))),
                |layout| owner.request(layout),
            )
            .unwrap(),
    };
    let pages = super::store::OwnedByteColumn::copy_pages(
        &prepared,
        0,
        &[0],
        &tagged,
        crate::runtime::tracked_state_allocator::expression_allocation_host(tracker),
        &CopyControl,
    )
    .unwrap();
    let alias = pages[0].array().slice(0, 1);
    drop(pages);
    drop(tagged);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(
        alias
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .value(0),
        "long owned payload retained by a sliced Arrow buffer"
    );
    drop(alias);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn owned_byte_page_directory_grows_with_published_pages_instead_of_representative_rows() {
    let (owner, tracker) = owner_with_tracker();
    let count = 8192;
    let values = (0..count)
        .map(|i| format!("key-{i:08}-{}", "payload".repeat(12)))
        .collect::<Vec<_>>();
    let rows = (0..count).collect::<Vec<_>>();
    let ty = FunctionValueType::new(DataType::Utf8, false);
    let keys = input(
        &[Arc::new(StringArray::from(values))],
        std::slice::from_ref(&ty),
        &rows,
        owner.as_ref(),
    );
    let spec = KeySpec::new(&[ty], &[NullPolicy::Equal], owner.as_ref(), &|| Ok(())).unwrap();
    let prepared = PreparedKeyBatch::new(
        &spec,
        &keys,
        AggregateAllocator::new(tracker.clone()),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let pages = super::store::OwnedByteColumn::copy_pages(
        &prepared,
        0,
        &rows,
        owner.as_ref(),
        crate::runtime::tracked_state_allocator::expression_allocation_host(tracker),
        &CopyControl,
    )
    .unwrap();
    assert_eq!(
        pages
            .iter()
            .map(super::store::OwnedByteColumn::len)
            .sum::<usize>(),
        count
    );
    // The original Vec author may keep amortized spare slots; capacity scales
    // with real page count, never one page header for every short input key.
    assert!(pages.capacity() <= pages.len().saturating_mul(2).max(4));
    assert!(pages.capacity() < count / 2);
    let mut ordinal = 0;
    for page in &pages {
        let values = page
            .array()
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        for row in 0..values.len() {
            assert_eq!(
                values.value(row),
                format!("key-{ordinal:08}-{}", "payload".repeat(12))
            );
            ordinal += 1;
        }
    }
}

#[test]
fn owned_byte_page_retains_actual_nullable_binding_and_rejects_another_nominal_domain() {
    let (owner, tracker) = owner_with_tracker();
    let required = FunctionValueType::new(DataType::Utf8, false);
    let nullable = FunctionValueType::new(DataType::Utf8, true);
    let keys = input(
        &[Arc::new(StringArray::from(vec![None, Some("{}")]))],
        &[nullable],
        &[0, 1],
        owner.as_ref(),
    );
    let spec = KeySpec::new(
        &[required],
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let prepared = PreparedKeyBatch::new(
        &spec,
        &keys,
        AggregateAllocator::new(tracker.clone()),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let pages = super::store::OwnedByteColumn::copy_pages(
        &prepared,
        0,
        &[0, 1],
        owner.as_ref(),
        crate::runtime::tracked_state_allocator::expression_allocation_host(tracker.clone()),
        &CopyControl,
    )
    .unwrap();
    assert!(pages[0].binding().nullable());
    assert!(pages[0].array().is_null(0));
    let json = FunctionValueType::try_with_logical_type(
        DataType::Utf8,
        false,
        novarocks_type_contract::ValueLogicalType::Json,
    )
    .unwrap();
    let other = input(
        &[Arc::new(StringArray::from(vec!["{}", "{}"]))],
        std::slice::from_ref(&json),
        &[0, 1],
        owner.as_ref(),
    );
    let json_spec =
        KeySpec::new(&[json], &[NullPolicy::Equal], owner.as_ref(), &|| Ok(())).unwrap();
    let other = PreparedKeyBatch::new(
        &json_spec,
        &other,
        AggregateAllocator::new(tracker),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let mut out = [None];
    assert!(
        pages[0]
            .compare_pairs(&other, 0, &[1], &[1], &mut out, &|| Ok(()))
            .is_err()
    );
    assert_eq!(out, [None]);
}
