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
    let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        MemTracker::new_root("local key test construction"),
        Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
    ).unwrap();
    let declaration = crate::exec::chunk::typed_fixture::layout(
        Arc::new(arrow::datatypes::Schema::empty()),
        &[],
        &[],
    );
    let schema = factory.create_compiled_chunk_schema(&declaration).unwrap();
    factory.begin_typed_construction(&schema).unwrap()
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
