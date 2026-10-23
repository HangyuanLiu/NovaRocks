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
use crate::exec::pipeline::test_driver::TestDriver;
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

fn mode() -> AssertNumRowsMode {
    AssertNumRowsMode::PerKeyAtMostOne {
        key_slots: vec![SlotId(7)],
        key_labels: vec!["target".into()],
        message_prefix: "selected match".into(),
    }
}
fn chunk(key: Option<ArrayRef>, applies: Option<Vec<Option<bool>>>) -> Chunk {
    let mut fields = Vec::new();
    let mut arrays = Vec::new();
    let mut slots = Vec::new();
    if let Some(key) = key {
        fields.push(Field::new("target", key.data_type().clone(), true));
        arrays.push(key);
        slots.push(SlotId(7));
    }
    if let Some(applies) = applies {
        fields.push(Field::new("applies", DataType::Boolean, true));
        arrays.push(Arc::new(BooleanArray::from(applies)) as ArrayRef);
        slots.push(SlotId(8));
    }
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    let shape = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
        batch.schema().as_ref(),
        &slots,
    )
    .unwrap();
    Chunk::try_new_with_chunk_schema(batch, shape).unwrap()
}
fn bound(
    parent: Arc<MemTracker>,
    applies: Option<SlotId>,
    limit: usize,
) -> AssertNumRowsProcessorFactory {
    AssertNumRowsProcessorFactory::new_bound(19, mode(), applies, parent, limit).unwrap()
}

#[test]
fn applicability_is_evaluated_before_target_access_and_null_validation() {
    let root = MemTracker::new_root("selected assertion");
    let factory = bound(root.clone(), Some(SlotId(8)), 16 * 1024);
    let mut operator = factory.create(1, 0);
    let state = RuntimeState::default();
    // There is deliberately no target column. NoAction rows never inspect it.
    TestDriver::new(operator.as_mut(), &state)
        .input_and_collect(chunk(None, Some(vec![Some(false), None])))
        .unwrap();
    assert_eq!(root.current(), 0);
    let keys = Arc::new(Int32Array::from(vec![Some(7), Some(7), None])) as ArrayRef;
    TestDriver::new(operator.as_mut(), &state)
        .input_and_collect(chunk(Some(keys), Some(vec![Some(true), Some(false), None])))
        .unwrap();
    let keys = Arc::new(Int32Array::from(vec![Some(7)])) as ArrayRef;
    let error = TestDriver::new(operator.as_mut(), &state)
        .input_and_collect(chunk(Some(keys), Some(vec![Some(true)])))
        .unwrap_err();
    assert_eq!(error.failure_fact().class(), FailureClass::Data);
    assert!(error.detail().contains("duplicate target=7"));
    operator.cancel();
    assert_eq!(root.current(), 0);
}

#[test]
fn a_selected_action_requires_a_real_nonnull_target() {
    let root = MemTracker::new_root("selected target");
    let mut operator = bound(root.clone(), Some(SlotId(8)), 4096).create(1, 0);
    let input = chunk(
        Some(Arc::new(Int32Array::from(vec![None]))),
        Some(vec![Some(true)]),
    );
    let error = TestDriver::new(operator.as_mut(), &RuntimeState::default())
        .input_and_collect(input)
        .unwrap_err();
    assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    assert!(error.detail().contains("no target key"));
    operator.cancel();
    assert_eq!(root.current(), 0);
}

#[test]
fn canonical_float_equality_rejects_signed_zero_and_nan_duplicates() {
    for values in [
        vec![0.0, -0.0],
        vec![f64::NAN, f64::from_bits(0x7ff8_0000_0000_0001)],
    ] {
        let root = MemTracker::new_root("canonical float");
        let mut operator = bound(root.clone(), None, 4096).create(1, 0);
        let input = chunk(Some(Arc::new(Float64Array::from(values))), None);
        let error = TestDriver::new(operator.as_mut(), &RuntimeState::default())
            .input_and_collect(input)
            .unwrap_err();
        assert_eq!(error.failure_fact().class(), FailureClass::Data);
        operator.cancel();
        assert_eq!(root.current(), 0);
    }
}

#[test]
fn immutable_type_authority_rejects_cross_batch_domain_drift() {
    let root = MemTracker::new_root("type drift");
    let mut operator = bound(root.clone(), None, 4096).create(1, 0);
    let state = RuntimeState::default();
    TestDriver::new(operator.as_mut(), &state)
        .input_and_collect(chunk(Some(Arc::new(Int32Array::from(vec![7]))), None))
        .unwrap();
    let error = TestDriver::new(operator.as_mut(), &state)
        .input_and_collect(chunk(Some(Arc::new(StringArray::from(vec!["7"]))), None))
        .unwrap_err();
    assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    assert!(error.detail().contains("key type changed"));
    operator.cancel();
    assert_eq!(root.current(), 0);
}

#[test]
fn all_drivers_share_one_state_limit_and_failed_growth_returns_its_charge() {
    let root = MemTracker::new_root("shared assertion");
    let factory = bound(root.clone(), None, 4096);
    let mut drivers = [factory.create(2, 0), factory.create(2, 1)];
    let state = RuntimeState::default();
    let mut rejected = false;
    for value in 0..64 {
        let text = format!("{value:04}{}", "x".repeat(256));
        let input = chunk(Some(Arc::new(StringArray::from(vec![text]))), None);
        let before = root.current();
        let result = TestDriver::new(drivers[value % 2].as_mut(), &state).input_and_collect(input);
        if let Err(error) = result {
            assert_eq!(error.failure_fact().class(), FailureClass::Resource);
            assert_eq!(
                root.current(),
                before,
                "failed candidate/growth must return every unpublished charge"
            );
            assert!(root.current() <= 4096);
            rejected = true;
            break;
        }
    }
    assert!(
        rejected,
        "two drivers cannot independently spend the same 4 KiB limit"
    );
    for driver in &mut drivers {
        driver.cancel();
    }
    assert_eq!(root.current(), 0);
}
