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

fn insert(
    index: &mut KeyIndex,
    keys: &KeyInput,
    tracker: &Arc<MemTracker>,
    owner: &dyn InputConstructionOwner,
) {
    let spec = index.spec().clone();
    let prepared = PreparedKeyBatch::new(
        &spec,
        keys,
        AggregateAllocator::new(tracker.clone()),
        owner,
        &|| Ok(()),
    )
    .unwrap();
    let hashes = index_hash(keys, index, tracker, owner).force_hash(0);
    index
        .intern_batch(&prepared, &hashes, owner, &CopyControl)
        .unwrap();
}

#[test]
fn compact_preserves_collision_classes_requested_values_and_future_interns() {
    let (owner, tracker) = owner_with_tracker();
    let types = [
        FunctionValueType::new(DataType::Int32, false),
        FunctionValueType::new(DataType::Utf8, true),
        FunctionValueType::new(
            DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Int32,
                true,
            ))),
            true,
        ),
    ];
    let spec = KeySpec::new(&types, &[NullPolicy::Equal; 3], owner.as_ref(), &|| Ok(())).unwrap();
    let mut index =
        KeyIndex::new(spec, IndexPurpose::KeyIds, tracker.clone(), owner.as_ref()).unwrap();
    for batch in 0..3 {
        let mut lists = ListBuilder::new(Int32Builder::new());
        lists.values().append_value(batch);
        lists.values().append_null();
        lists.append(true);
        let keys = input(
            &[
                Arc::new(Int32Array::from(vec![batch])),
                Arc::new(StringArray::from(vec![if batch == 1 {
                    None
                } else {
                    Some("same long prefix with payload")
                }])),
                Arc::new(lists.finish()),
            ],
            &types,
            &[0],
            owner.as_ref(),
        );
        insert(&mut index, &keys, &tracker, owner.as_ref());
    }
    let ids = [KeyId(2), KeyId(0), KeyId(2), KeyId(1)];
    let before = index.read_keys(&ids, owner.as_ref(), &CopyControl).unwrap();
    let old_page_count = before.columns()[1].pages().len();
    let old_frames: Vec<Vec<u8>> = before.columns()[2]
        .pages()
        .iter()
        .flat_map(|page| {
            let KeyReadValues::Canonical { encoded, .. } = page.values() else {
                panic!("encoded column")
            };
            page.rows()
                .iter()
                .map(|row| encoded.value(row.row).to_vec())
                .collect::<Vec<_>>()
        })
        .collect();
    drop(before);
    index.compact(owner.as_ref(), &CopyControl).unwrap();
    let after = index.read_keys(&ids, owner.as_ref(), &CopyControl).unwrap();
    assert!(after.columns()[1].pages().len() < old_page_count);
    let mut new_frames = vec![Vec::new(); ids.len()];
    for page in after.columns()[2].pages() {
        let KeyReadValues::Canonical { encoded, .. } = page.values() else {
            panic!("encoded column")
        };
        for row in page.rows() {
            new_frames[row.ordinal] = encoded.value(row.row).to_vec();
        }
    }
    let mut sorted_before = old_frames;
    sorted_before.sort();
    let mut sorted_after = new_frames.clone();
    sorted_after.sort();
    assert_eq!(sorted_after, sorted_before);
    assert_eq!(new_frames[0], new_frames[2]);
    drop(after);
    let exported = index
        .export_keys(&ids, &types, owner.as_ref(), &CopyControl)
        .unwrap();
    assert_eq!(
        exported.arrays()[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap(),
        &Int32Array::from(vec![2, 0, 2, 1])
    );
    assert_eq!(
        exported.arrays()[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap(),
        &StringArray::from(vec![
            Some("same long prefix with payload"),
            Some("same long prefix with payload"),
            Some("same long prefix with payload"),
            None
        ])
    );
    let list = exported.arrays()[2]
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    for (ordinal, value) in [2, 0, 2, 1].into_iter().enumerate() {
        assert_eq!(
            list.value(ordinal)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap(),
            &Int32Array::from(vec![Some(value), None])
        );
    }
    // Continue through the same frozen hash identity and private collision chain.
    let mut lists = ListBuilder::new(Int32Builder::new());
    for value in [2, 3, 0] {
        lists.values().append_value(value);
        lists.values().append_null();
        lists.append(true);
    }
    let keys = input(
        &[
            Arc::new(Int32Array::from(vec![2, 3, 0])),
            Arc::new(StringArray::from(vec!["same long prefix with payload"; 3])),
            Arc::new(lists.finish()),
        ],
        &types,
        &[0, 1, 2, 1],
        owner.as_ref(),
    );
    insert(&mut index, &keys, &tracker, owner.as_ref());
    assert_eq!(index.len(), 4);
    let hashes = index_hash(&keys, &index, &tracker, owner.as_ref()).force_hash(0);
    let prepared = PreparedKeyBatch::new(
        index.spec(),
        &keys,
        AggregateAllocator::new(tracker),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    assert_eq!(
        index
            .probe_batch(&prepared, &hashes, owner.as_ref(), &CopyControl)
            .unwrap()
            .matches(),
        &[
            KeyMatch::Found(KeyId(2)),
            KeyMatch::Found(KeyId(3)),
            KeyMatch::Found(KeyId(0)),
            KeyMatch::Found(KeyId(3))
        ]
    );
}

#[test]
fn compact_hash_presence_keeps_membership_without_exposing_ids_and_accepts_empty() {
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let mut index = KeyIndex::new(
        spec,
        IndexPurpose::PresenceOnly,
        tracker.clone(),
        owner.as_ref(),
    )
    .unwrap();
    index.compact(owner.as_ref(), &CopyControl).unwrap();
    for values in [vec![Some("alpha"), None], vec![Some("beta"), Some("alpha")]] {
        let keys = input(
            &[Arc::new(StringArray::from(values))],
            std::slice::from_ref(&ty),
            &[0, 1],
            owner.as_ref(),
        );
        let spec = index.spec().clone();
        let prepared = PreparedKeyBatch::new(
            &spec,
            &keys,
            AggregateAllocator::new(tracker.clone()),
            owner.as_ref(),
            &|| Ok(()),
        )
        .unwrap();
        let hashes = index_hash(&keys, &index, &tracker, owner.as_ref()).force_hash(0);
        index
            .include_presence(&prepared, &hashes, owner.as_ref(), &CopyControl)
            .unwrap();
    }
    index.compact(owner.as_ref(), &CopyControl).unwrap();
    let query = input(
        &[Arc::new(StringArray::from(vec![
            Some("alpha"),
            Some("gamma"),
            None,
            Some("beta"),
        ]))],
        std::slice::from_ref(&ty),
        &[0, 1, 2, 3],
        owner.as_ref(),
    );
    let hashes = index_hash(&query, &index, &tracker, owner.as_ref()).force_hash(0);
    let prepared = PreparedKeyBatch::new(
        index.spec(),
        &query,
        AggregateAllocator::new(tracker),
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    assert_eq!(
        index
            .contains_batch(&prepared, &hashes, owner.as_ref(), &CopyControl)
            .unwrap()
            .present(),
        &[1, 0, 1, 1]
    );
    assert!(index.read_keys(&[], owner.as_ref(), &CopyControl).is_err());
    assert!(
        index
            .export_keys(&[], &[ty], owner.as_ref(), &CopyControl)
            .is_err()
    );
}

#[test]
fn compact_refusal_during_long_payload_growth_latches_original_cause() {
    struct DenyLarge<'a>(&'a dyn InputConstructionOwner, ExecutionFailure);
    impl InputConstructionOwner for DenyLarge<'_> {
        fn storage_lifetime(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
            self.0.storage_lifetime()
        }
        fn request(&self, layout: Layout) -> Result<(), ConstructionFailure> {
            if layout.size() > 100_000 {
                Err(ConstructionFailure::from(self.1.clone()))
            } else {
                self.0.request(layout)
            }
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
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Utf8, false);
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let mut index =
        KeyIndex::new(spec, IndexPurpose::KeyIds, tracker.clone(), owner.as_ref()).unwrap();
    let payload = "x".repeat(200_000);
    let keys = input(
        &[Arc::new(StringArray::from(vec![payload.as_str()]))],
        std::slice::from_ref(&ty),
        &[0],
        owner.as_ref(),
    );
    insert(&mut index, &keys, &tracker, owner.as_ref());
    let cause: ExecutionFailure = ConstructionFailure::Control(
        novarocks_type_contract::CompileControlError::ResourceExhausted,
    )
    .into();
    let refusal = DenyLarge(owner.as_ref(), cause.clone());
    let error = index.compact(&refusal, &CopyControl).unwrap_err();
    assert_eq!(
        novarocks_failure_contract::FailureProjection::failure_fact(&error),
        novarocks_failure_contract::FailureProjection::failure_fact(&cause)
    );
    let again = index.compact(owner.as_ref(), &CopyControl).unwrap_err();
    assert_eq!(error, again);
    assert_eq!(
        index
            .export_keys(&[KeyId(0)], &[ty], owner.as_ref(), &CopyControl)
            .err()
            .unwrap(),
        error
    );
}

#[test]
fn compact_releases_previous_creator_only_after_last_escaped_buffer_alias() {
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
    let spec = KeySpec::new(
        std::slice::from_ref(&ty),
        &[NullPolicy::Equal],
        owner.as_ref(),
        &|| Ok(()),
    )
    .unwrap();
    let mut index =
        KeyIndex::new(spec, IndexPurpose::KeyIds, tracker.clone(), owner.as_ref()).unwrap();
    let keys = input(
        &[Arc::new(StringArray::from(vec![
            "old creator retained by escaped immutable key buffer",
        ]))],
        std::slice::from_ref(&ty),
        &[0],
        owner.as_ref(),
    );
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
    insert(&mut index, &keys, &tracker, &tagged);
    let read = index
        .read_keys(&[KeyId(0)], owner.as_ref(), &CopyControl)
        .unwrap();
    let KeyReadValues::Bytes { array, .. } = read.columns()[0].pages()[0].values() else {
        panic!("byte key")
    };
    let alias = Arc::clone(array);
    let buffer = alias.to_data().buffers()[0].clone();
    drop(read);
    drop(tagged);
    index.compact(owner.as_ref(), &CopyControl).unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(
        alias
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .value(0),
        "old creator retained by escaped immutable key buffer"
    );
    drop(alias);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(buffer);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let exported = index
        .export_keys(&[KeyId(0)], &[ty], owner.as_ref(), &CopyControl)
        .unwrap();
    assert_eq!(
        exported.arrays()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "old creator retained by escaped immutable key buffer"
    );
}

#[test]
fn compact_final_control_refusal_never_exposes_a_committed_prefix_as_success() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Control {
        calls: AtomicUsize,
        stop: usize,
    }
    impl novarocks_functions::KernelEvaluationControl for Control {
        fn checkpoint(&self, _: u32) -> Result<(), novarocks_functions::KernelFailure> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call == self.stop {
                Err(novarocks_functions::KernelFailure::DeadlineExceeded)
            } else {
                Ok(())
            }
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), novarocks_functions::KernelFailure> {
            panic!("compaction never waits")
        }
    }
    let (owner, tracker) = owner_with_tracker();
    let ty = FunctionValueType::new(DataType::Int64, false);
    let keys = input(
        &[Arc::new(Int64Array::from_iter_values(0..513))],
        std::slice::from_ref(&ty),
        &(0..513).collect::<Vec<_>>(),
        owner.as_ref(),
    );
    let make = || {
        let spec = KeySpec::new(
            std::slice::from_ref(&ty),
            &[NullPolicy::Equal],
            owner.as_ref(),
            &|| Ok(()),
        )
        .unwrap();
        let mut index =
            KeyIndex::new(spec, IndexPurpose::KeyIds, tracker.clone(), owner.as_ref()).unwrap();
        insert(&mut index, &keys, &tracker, owner.as_ref());
        index
    };
    let mut baseline = make();
    let count = Control {
        calls: AtomicUsize::new(0),
        stop: usize::MAX,
    };
    baseline.compact(owner.as_ref(), &count).unwrap();
    let calls = count.calls.load(Ordering::SeqCst);
    assert!(calls > 1);
    let mut index = make();
    let final_stop = Control {
        calls: AtomicUsize::new(0),
        stop: calls,
    };
    let expected = ExecutionFailure::from(novarocks_functions::KernelFailure::DeadlineExceeded);
    assert_eq!(
        index.compact(owner.as_ref(), &final_stop).unwrap_err(),
        expected
    );
    assert_eq!(final_stop.calls.load(Ordering::SeqCst), calls);
    assert_eq!(
        index.compact(owner.as_ref(), &CopyControl).unwrap_err(),
        expected
    );
    assert_eq!(
        index
            .read_keys(&[KeyId(0)], owner.as_ref(), &CopyControl)
            .err()
            .unwrap(),
        expected
    );
}

#[test]
fn compact_preserves_actual_nullable_page_declarations_when_spec_root_is_required() {
    let (owner, tracker) = owner_with_tracker();
    let mut lists = ListBuilder::new(Int32Builder::new());
    lists.append(false);
    lists.values().append_value(7);
    lists.append(true);
    let sources: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![None, Some("kept")])),
        Arc::new(Int32Array::from(vec![None, Some(7)])),
        Arc::new(lists.finish()),
    ];
    for source in sources {
        let declared = FunctionValueType::new(source.data_type().clone(), false);
        let actual = FunctionValueType::new(source.data_type().clone(), true);
        let spec = KeySpec::new(
            std::slice::from_ref(&declared),
            &[NullPolicy::Equal],
            owner.as_ref(),
            &|| Ok(()),
        )
        .unwrap();
        let mut index =
            KeyIndex::new(spec, IndexPurpose::KeyIds, tracker.clone(), owner.as_ref()).unwrap();
        let keys = input(
            &[source],
            std::slice::from_ref(&actual),
            &[0, 1],
            owner.as_ref(),
        );
        insert(&mut index, &keys, &tracker, owner.as_ref());
        index.compact(owner.as_ref(), &CopyControl).unwrap();
        let read = index
            .read_keys(&[KeyId(0), KeyId(1)], owner.as_ref(), &CopyControl)
            .unwrap();
        for page in read.columns()[0].pages() {
            let binding = match page.values() {
                KeyReadValues::Fixed { binding, .. } | KeyReadValues::Bytes { binding, .. } => {
                    *binding
                }
                KeyReadValues::Canonical {
                    logical_binding, ..
                } => *logical_binding,
            };
            assert!(binding.nullable());
        }
        drop(read);
        let nullable = index
            .export_keys(&[KeyId(0)], &[actual], owner.as_ref(), &CopyControl)
            .unwrap();
        assert_eq!(nullable.arrays()[0].null_count(), 1);
        let error = index
            .export_keys(&[KeyId(0)], &[declared], owner.as_ref(), &CopyControl)
            .err()
            .unwrap();
        assert_value_failure(&error, novarocks_value_arrays::ValueViolation::RequiredNull);
    }
}
