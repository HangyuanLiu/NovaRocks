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

//! Original array_append raw contracts, no pure implementation involved.
use super::{ExprArena, ExprId, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, ListArray, NullArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_buffer::{NullBuffer, OffsetBuffer};
use novarocks_types::SlotId;
use std::{collections::HashMap, sync::Arc};
fn list(
    field: Arc<Field>,
    values: ArrayRef,
    offsets: Vec<i32>,
    valid: Option<Vec<bool>>,
) -> ArrayRef {
    Arc::new(ListArray::new(
        field,
        OffsetBuffer::new(offsets.into()),
        values,
        valid.map(NullBuffer::from),
    ))
}
fn raw(
    columns: Vec<ArrayRef>,
    output: Option<DataType>,
) -> Result<ArrayRef, crate::runtime::fragment::ExecutionFailure> {
    let slots = (0..columns.len())
        .map(|i| SlotId::new(i as u32 + 1))
        .collect::<Vec<_>>();
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("v{i}"), a.data_type().clone(), true))
        .collect::<Vec<_>>();
    let mut arena = ExprArena::default();
    let ids = columns
        .iter()
        .enumerate()
        .map(|(i, a)| arena.push_typed(ExprNode::SlotId(slots[i]), a.data_type().clone()))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let id = match output {
        Some(t) => arena.push_typed(ExprNode::SlotId(SlotId::new(999)), t),
        None => ExprId(usize::MAX),
    };
    super::function::array::eval_array_append(&arena, id, &ids, &chunk)
}
#[test]
fn original_array_append_null_parent_skips_target_but_null_target_is_appended() {
    let f = Arc::new(Field::new("item", DataType::Int32, true));
    let a = list(
        f,
        Arc::new(Int32Array::from(vec![1, 2, 3])),
        vec![0, 2, 3, 3],
        Some(vec![true, false, true]),
    );
    let t: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(8), Some(9)]));
    let out = raw(vec![a, t], None).unwrap();
    let l = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(l.value_offsets(), &[0, 3, 3, 4]);
    assert!(l.is_null(1));
    assert_eq!(
        l.values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(1), Some(2), None, Some(9)]
    );
}
#[test]
fn array_append_keeps_planned_field_metadata_and_rejects_a_nonlist_target() {
    let f = Arc::new(
        Field::new("source", DataType::Int32, true)
            .with_metadata(HashMap::from([("source-tag".into(), "kept".into())])),
    );
    let a = list(
        f.clone(),
        Arc::new(Int32Array::from(vec![1])),
        vec![0, 1],
        None,
    );
    let target: ArrayRef = Arc::new(Int32Array::from(vec![2]));
    let out = raw(vec![a.clone(), target.clone()], None).unwrap();
    assert_eq!(out.data_type(), &DataType::List(f.clone()));
    let refusal = raw(vec![a.clone(), target.clone()], Some(DataType::Int64)).unwrap_err();
    assert_frozen_target(&refusal);
    let planned = Arc::new(
        Field::new("planned", DataType::Int64, true)
            .with_metadata(HashMap::from([("planned-tag".into(), "kept".into())])),
    );
    let out = raw(vec![a, target], Some(DataType::List(planned.clone()))).unwrap();
    let l = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(out.data_type(), &DataType::List(planned));
    assert_eq!(
        l.values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(1), Some(2)]
    );
}
#[test]
fn original_array_append_sliced_offsets_empty_list_and_null_child_domain() {
    let f = Arc::new(Field::new("item", DataType::Null, true));
    let a = list(f, Arc::new(NullArray::new(2)), vec![0, 2, 2], None);
    let t: ArrayRef = Arc::new(NullArray::new(2));
    let out = raw(vec![a.slice(1, 1), t.slice(1, 1)], None).unwrap();
    let l = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(l.value_offsets(), &[0, 1]);
    assert_eq!(l.values().data_type(), &DataType::Null);
    assert_eq!(l.values().len(), 1);
    assert_eq!(l.null_count(), 0);
}
#[test]
fn original_array_append_full_default_arrow_cast_failure_and_list_admission() {
    let a: ArrayRef = Arc::new(Int32Array::from(vec![1]));
    let t: ArrayRef = Arc::new(StringArray::from(vec!["x"]));
    assert_eq!(
        (raw(vec![a, t], None).unwrap_err()).to_string(),
        "array_append expects ListArray, got Int32"
    );
    let field = Arc::new(Field::new("item", DataType::Int32, true));
    let a = list(field, Arc::new(Int32Array::from(vec![1])), vec![0, 1], None);
    let target: ArrayRef = list(
        Arc::new(Field::new("item", DataType::Int32, true)),
        Arc::new(Int32Array::from(vec![2])),
        vec![0, 1],
        None,
    );
    let cause = arrow::compute::cast(&target, &DataType::Int32).unwrap_err();
    let refusal = raw(vec![a, target], None).unwrap_err();
    assert_original_arrow(&refusal, &cause);
}

pub(super) fn assert_original_arrow(
    error: &crate::runtime::fragment::ExecutionFailure,
    expected: &arrow::error::ArrowError,
) {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    let mut source: &(dyn std::error::Error + 'static) = error;
    loop {
        if let Some(original) = source.downcast_ref::<arrow::error::ArrowError>() {
            assert_eq!(original.to_string(), expected.to_string());
            break;
        }
        source = source
            .source()
            .expect("original collection cast refusal must survive its envelope");
    }
}
pub(super) fn assert_frozen_target(error: &crate::runtime::fragment::ExecutionFailure) {
    use novarocks_failure_contract::{FailureClass, FailureFact, FailureProjection};
    assert_eq!(
        error.failure_fact(),
        FailureFact::new(
            FailureClass::Invariant,
            crate::runtime::fragment::ExecutionReason::ExpressionContract.reason()
        )
    );
}
