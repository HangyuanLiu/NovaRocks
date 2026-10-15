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

//! Plain representation of a required decoded leaf under a retained typed
//! carrier. This is not an independent semantic projection of a raw child.

#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use arrow::array::Array;
#[cfg(test)]
use arrow::array::ArrayRef;
#[cfg(test)]
use arrow::array::LargeStringArray;
#[cfg(test)]
use arrow::array::StringArray;
#[cfg(test)]
use arrow::datatypes::DataType;
#[cfg(test)]
use novarocks_type_contract::ValueKind;
#[cfg(test)]
use novarocks_type_contract::ValueTypeRef;

#[cfg(test)]
use super::ArrayNormalizationStep;
#[cfg(test)]
use super::ArrayPathStep;

pub(super) use novarocks_functions::array_projection::normalize_inactive_nulls;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::chunk::hydrate::hydrate_dictionary_columns_owned_observed;
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
    use crate::exec::pipeline::operator::input_construction::{
        ConstructionFailure, InputConstructionFactory, try_runtime_input_construction_factory,
    };
    use crate::runtime::fragment::{ExecutionFailure, PipelineOperation};
    use crate::runtime::mem_tracker::MemTracker;
    use crate::runtime::runtime_state::RuntimeErrorState;
    use arrow::array::{ArrayData, DictionaryArray, Int32Array, ListArray};
    use arrow::buffer::{Buffer, NullBuffer};
    use arrow::datatypes::{Field, Int32Type};
    use novarocks_type_contract::{TypedValueBinding, ValueLayout, ValueTypeStorageLifetime};
    use novarocks_types::SlotId;

    fn typed_source(array: ArrayRef, item_nullable: bool) -> Chunk {
        let storage = ValueTypeStorageLifetime::pure();
        let value = ValueTypeRef::try_new(
            ValueKind::List(
                ValueTypeRef::try_new(ValueKind::String, item_nullable, storage.clone()).unwrap(),
            ),
            true,
            storage.clone(),
        )
        .unwrap();
        let binding = TypedValueBinding::try_new(
            value,
            ValueLayout::try_new(array.data_type(), storage.clone()).unwrap(),
        )
        .unwrap();
        let slot = ChunkSlotSchema::try_new_typed(
            SlotId::new(1),
            Arc::new(Field::new("input", array.data_type().clone(), true)),
            binding,
            None,
            storage.clone(),
        )
        .unwrap();
        Chunk::try_new_typed_with_columns_owned_observed::<String, _>(
            Arc::new(ChunkSchema::try_new_typed(vec![slot], storage.clone()).unwrap()),
            vec![array],
            2,
            storage,
            |_| Ok(()),
        )
        .unwrap()
    }

    fn encoded_source(large: bool, item_nullable: bool) -> Chunk {
        let values = ArrayData::builder(DataType::Utf8)
            .len(2)
            .buffers(vec![
                Buffer::from_slice_ref([0i32, 4, 10]),
                Buffer::from(b"keptshadow".as_slice()),
            ])
            .nulls(Some(NullBuffer::from(vec![true, false])))
            .build()
            .unwrap();
        let mut values: ArrayRef = Arc::new(StringArray::from(values));
        if large {
            values = arrow::compute::cast(values.as_ref(), &DataType::LargeUtf8).unwrap();
        }
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![1, 0, 1]), values).unwrap(),
        );
        let data = ArrayData::builder(DataType::List(Arc::new(Field::new(
            "item",
            dictionary.data_type().clone(),
            item_nullable,
        ))))
        .len(3)
        .buffers(vec![Buffer::from_slice_ref([0i32, 1, 2, 3])])
        .child_data(vec![dictionary.to_data()])
        .nulls(Some(NullBuffer::from(vec![false, true, true])))
        .build()
        .unwrap();
        data.validate_full().unwrap();
        let array: ArrayRef = Arc::new(ListArray::from(data));
        typed_source(array.slice(0, 2), item_nullable)
    }

    fn factory(tracker: &Arc<MemTracker>) -> Arc<dyn InputConstructionFactory> {
        try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker.clone(),
            Arc::new(RuntimeErrorState::default()),
        )
        .unwrap()
    }

    #[test]
    fn required_hydrated_strings_share_payload_and_offsets_and_retain_real_creator() {
        for large in [false, true] {
            let source = encoded_source(large, false);
            let tracker = MemTracker::new_root("inactive required strings");
            let factory = factory(&tracker);
            let scope = factory.begin_input_construction(&source).unwrap();
            let mut pointers = None;
            let output = hydrate_dictionary_columns_owned_observed(
                &source,
                scope.storage_lifetime(),
                |_, _, _, _| false,
                |occurrence, path, step| {
                    if let ArrayNormalizationStep::ValidateStringWrapper { source, .. } = step {
                        assert_eq!(path, &[ArrayPathStep::ListItem]);
                        assert_eq!(source.null_count(), 2);
                        pointers = Some(match source.data_type() {
                            DataType::Utf8 => {
                                let a = source.as_any().downcast_ref::<StringArray>().unwrap();
                                (a.values().as_ptr(), a.offsets().as_ptr() as *const u8)
                            }
                            DataType::LargeUtf8 => {
                                let a = source.as_any().downcast_ref::<LargeStringArray>().unwrap();
                                (a.values().as_ptr(), a.offsets().as_ptr() as *const u8)
                            }
                            _ => unreachable!(),
                        });
                    }
                    scope.observe(occurrence, path, step)
                },
                |layout| scope.request(layout),
            )
            .unwrap();
            let list = output.columns()[0]
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            assert_eq!(list.nulls(), source.columns()[0].nulls());
            assert_eq!(list.value_offsets(), &[0, 1, 2]);
            assert_eq!(list.values().null_count(), 0);
            let actual = if large {
                let a = list
                    .values()
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .unwrap();
                assert_eq!(a.value(0), "");
                assert_eq!(a.value(1), "kept");
                assert_eq!(a.value(2), "");
                (a.values().as_ptr(), a.offsets().as_ptr() as *const u8)
            } else {
                let a = list
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert_eq!(a.value(0), "");
                assert_eq!(a.value(1), "kept");
                assert_eq!(a.value(2), "");
                (a.values().as_ptr(), a.offsets().as_ptr() as *const u8)
            };
            assert_eq!(Some(actual), pointers);
            assert_eq!(
                output.chunk_schema().slots()[0]
                    .typed_binding()
                    .value_type(),
                source.chunk_schema().slots()[0]
                    .typed_binding()
                    .value_type()
            );
            drop(source);
            drop(scope);
            drop(factory);
            assert!(tracker.current() > 0);
            drop(output);
            assert_eq!(tracker.current(), 0);
        }
    }

    #[test]
    fn nullable_decoded_leaf_keeps_null_mask_without_required_wrapper_event() {
        let source = encoded_source(false, true);
        let tracker = MemTracker::new_root("nullable hydrated strings");
        let factory = factory(&tracker);
        let scope = factory.begin_input_construction(&source).unwrap();
        let output = hydrate_dictionary_columns_owned_observed(
            &source,
            scope.storage_lifetime(),
            |_, _, _, _| false,
            |occurrence, path, step| {
                assert!(!matches!(
                    step,
                    ArrayNormalizationStep::ValidateStringWrapper { .. }
                ));
                scope.observe(occurrence, path, step)
            },
            |layout| scope.request(layout),
        )
        .unwrap();
        let list = output.columns()[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(list.values().null_count(), 2);
        let DataType::List(item) = list.data_type() else {
            unreachable!()
        };
        assert!(item.is_nullable());
        drop(output);
        drop(scope);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn required_wrapper_refusal_preserves_exact_original_cause_and_source() {
        let source = encoded_source(false, false);
        let tracker = MemTracker::new_root("required wrapper refusal");
        let factory = factory(&tracker);
        let initial = tracker.current();
        let scope = factory.begin_input_construction(&source).unwrap();
        let original = ExecutionFailure::from(novarocks_functions::KernelFailure::DeadlineExceeded)
            .at_operator(19, PipelineOperation::Admission);
        let failure = hydrate_dictionary_columns_owned_observed(
            &source,
            scope.storage_lifetime(),
            |_, _, _, _| false,
            |occurrence, path, step| {
                if matches!(step, ArrayNormalizationStep::ValidateStringWrapper { .. }) {
                    return Err(ConstructionFailure::Caller(Box::new(original.clone())));
                }
                scope.observe(occurrence, path, step)
            },
            |layout| scope.request(layout),
        )
        .unwrap_err();
        assert_eq!(ExecutionFailure::from(failure), original);
        assert!(matches!(
            source.columns()[0]
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .values()
                .data_type(),
            DataType::Dictionary(_, _)
        ));
        drop(scope);
        assert_eq!(tracker.current(), initial);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }

    fn masked_assertion(chunk: &Chunk) -> Result<ArrayRef, String> {
        use crate::exec::expr::function::FunctionKind;
        use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
        use novarocks_functions::FunctionValueType;
        let mut arena = ExprArena::default();
        let slot = arena.push_value_typed(
            ExprNode::SlotId(SlotId::new(1)),
            FunctionValueType::try_from_typed_binding(
                chunk.chunk_schema().slots()[0].typed_binding().clone(),
            )
            .unwrap(),
        );
        let arg = arena.push_value_typed(
            ExprNode::SlotId(SlotId::new(77)),
            FunctionValueType::new(DataType::Utf8, false),
        );
        let expected = arena.push_value_typed(
            ExprNode::Literal(LiteralValue::Utf8("kept".into())),
            FunctionValueType::new(DataType::Utf8, false),
        );
        let eq = arena.push_value_typed(
            ExprNode::Eq(arg, expected),
            FunctionValueType::new(DataType::Boolean, false),
        );
        let assertion = arena.push_value_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::AssertTrue,
                args: vec![eq],
            },
            FunctionValueType::new(DataType::Boolean, false),
        );
        let lambda = arena.push_value_typed(
            ExprNode::LambdaFunction {
                body: assertion,
                arg_slots: vec![SlotId::new(77)],
                common_sub_exprs: vec![],
                is_nondeterministic: false,
            },
            FunctionValueType::new(DataType::Null, true),
        );
        let output = arena.push_value_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::ArrayMap,
                args: vec![lambda, slot],
            },
            FunctionValueType::new(
                DataType::List(Arc::new(Field::new("item", DataType::Boolean, false))),
                true,
            ),
        );
        arena.eval(output, chunk)
    }

    #[test]
    fn array_map_row_effect_preserves_ancestor_mask_for_plain_and_hydrated_dictionary() {
        let source = encoded_source(false, false);
        let tracker = MemTracker::new_root("masked lambda dictionary");
        let factory = factory(&tracker);
        let scope = factory.begin_input_construction(&source).unwrap();
        let hydrated = hydrate_dictionary_columns_owned_observed(
            &source,
            scope.storage_lifetime(),
            |_, _, _, _| false,
            |occurrence, path, step| scope.observe(occurrence, path, step),
            |layout| scope.request(layout),
        )
        .unwrap();
        let plain_array: ArrayRef = Arc::new(
            ListArray::try_new(
                Arc::new(Field::new("item", DataType::Utf8, false)),
                arrow::buffer::OffsetBuffer::new(vec![0i32, 1, 2].into()),
                Arc::new(StringArray::from(vec!["would fail", "kept"])),
                Some(NullBuffer::from(vec![false, true])),
            )
            .unwrap(),
        );
        let plain = typed_source(plain_array, false);
        let actual = masked_assertion(&hydrated).unwrap();
        let reference = masked_assertion(&plain).unwrap();
        assert_eq!(actual.to_data(), reference.to_data());
        assert!(actual.is_null(0));
        assert!(!actual.is_null(1));
        let unmasked: ArrayRef = Arc::new(
            ListArray::try_new(
                Arc::new(Field::new("item", DataType::Utf8, false)),
                arrow::buffer::OffsetBuffer::new(vec![0i32, 1, 2].into()),
                Arc::new(StringArray::from(vec!["would fail", "kept"])),
                None,
            )
            .unwrap(),
        );
        assert!(
            masked_assertion(&typed_source(unmasked, false))
                .unwrap_err()
                .contains("assert_true")
        );
        drop(hydrated);
        drop(scope);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }
}
