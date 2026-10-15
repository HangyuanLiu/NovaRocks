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
use crate::exec::chunk::{ArrayNormalizationStep, ChunkSlotSchema};
use crate::exec::pipeline::operator::input_construction::{
    ConstructionFailure, try_runtime_input_construction_factory,
};
use crate::runtime::{mem_tracker::MemTracker, runtime_state::RuntimeErrorState};
use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_type_contract::{
    TypedValueBinding, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
};
use std::sync::Arc;

#[test]
fn canonical_typed_actual_schema_requires_the_original_arc_not_equal_foreign_schema() {
    let pure = ValueTypeStorageLifetime::pure();
    let binding = TypedValueBinding::try_new(
        ValueTypeRef::try_new(ValueKind::Scalar(DataType::Int32), false, pure.clone()).unwrap(),
        ValueLayout::try_new(&DataType::Int32, pure.clone()).unwrap(),
    )
    .unwrap();
    let slot = ChunkSlotSchema::try_new_typed(
        novarocks_types::SlotId::new(1),
        Arc::new(Field::new("value", DataType::Int32, false)),
        binding,
        None,
        pure.clone(),
    )
    .unwrap();
    let schema = Arc::new(ChunkSchema::try_new_typed(vec![slot], pure).unwrap());
    let tracker = MemTracker::new_root("canonical schema root proof");
    let factory = try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        tracker.clone(),
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let owner = factory.begin_typed_construction(&schema).unwrap();
    owner
        .observe(
            0,
            &[],
            ArrayNormalizationStep::CreateChunkColumns { channels: 1 },
        )
        .unwrap();
    // Input data is an explicit pure fixture; BE wrapper construction is under
    // the real host, with no claim that this scope funded source buffer bytes.
    let values: ArrayRef = Arc::new(Int32Array::from(vec![1]));
    let mut chunk = Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
        schema,
        vec![values],
        1,
        owner.storage_lifetime(),
        |step| owner.observe(0, &[], step),
    )
    .unwrap();
    let limits = RootArrayStorageLimits {
        bytes: 96 << 20,
        nodes: 8192,
        depth: 64,
    };
    assert!(borrowed_root_chunk_storage(&chunk, limits).is_ok());
    let foreign = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));
    assert_eq!(foreign.as_ref(), chunk.record_batch().schema_ref().as_ref());
    let replacement =
        RecordBatch::try_new(foreign, chunk.record_batch().columns().to_vec()).unwrap();
    chunk.replace_record_batch_for_test(replacement);
    assert_eq!(
        borrowed_root_chunk_storage(&chunk, limits),
        Err(RootArrayStorageError::UnknownMetadataOwner)
    );
    drop(chunk);
    drop(owner);
    drop(factory);
    assert_eq!(tracker.current(), 0);
}
