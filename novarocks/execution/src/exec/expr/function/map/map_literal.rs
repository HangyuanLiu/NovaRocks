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
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId, cast_with_special_rules, expression_contract};
use arrow::array::{ArrayRef, MapArray, StructArray, make_array, new_empty_array};
use arrow::datatypes::{DataType, Field, Fields};
use arrow_buffer::OffsetBuffer;
use arrow_data::transform::MutableArrayData;
use std::sync::Arc;

pub fn eval_map_literal<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    if !args.len().is_multiple_of(2) {
        return Err(expression_contract(format_args!(
            "map literal expects an even number of key/value arguments, got {}",
            args.len()
        )));
    }

    let num_rows = chunk.len();
    let pair_count = args.len() / 2;
    let mut raw_keys = Vec::with_capacity(pair_count);
    let mut raw_values = Vec::with_capacity(pair_count);

    for pair in args.chunks_exact(2) {
        let key = arena.eval(pair[0], chunk)?;
        validate_child_len("key", raw_keys.len(), &key, num_rows)?;
        raw_keys.push(key);

        let value = arena.eval(pair[1], chunk)?;
        validate_child_len("value", raw_values.len(), &value, num_rows)?;
        raw_values.push(value);
    }

    let key_hint = raw_keys
        .first()
        .map(|array| array.data_type().clone())
        .unwrap_or(DataType::Null);
    let value_hint = raw_values
        .first()
        .map(|array| array.data_type().clone())
        .unwrap_or(DataType::Null);
    let (map_field, ordered) =
        super::common::output_map_field(arena.data_type(expr), &key_hint, &value_hint, "map")?;
    let DataType::Struct(fields) = map_field.data_type() else {
        return Err(expression_contract(
            "map literal map entries type must be Struct",
        ));
    };
    if fields.len() != 2 {
        return Err(expression_contract(
            "map literal map entries type must have 2 fields",
        ));
    }

    let key_field = fields[0].clone();
    let value_field = fields[1].clone();
    // Validate exact product before constructing either child output.
    let total = pair_count as u128 * num_rows as u128;
    novarocks_functions::aggregate_scalar::checked_map_output_offset_u128(0, total)?;
    let keys = build_values_array(raw_keys, key_field.data_type(), num_rows, pair_count, "key")?;
    let values = build_values_array(
        raw_values,
        value_field.data_type(),
        num_rows,
        pair_count,
        "value",
    )?;

    // These keys contain exactly this invocation's rows and pairs. Check after
    // child evaluation and coercion, preserving the frozen non-null key field.
    if keys.logical_null_count() > 0 {
        return Err(super::common::null_key());
    }
    let entries_fields = entry_fields_for_arrays(fields, &values);
    let entries = StructArray::try_new(entries_fields.clone(), vec![keys, values], None).map_err(
        |source| {
            crate::exec::expr::function::arrow_operation::arrow_contract_failure(
                "map_literal StructArray",
                source,
            )
        },
    )?;
    let map_field = Arc::new(Field::new(
        map_field.name(),
        DataType::Struct(entries_fields),
        map_field.is_nullable(),
    ));

    let mut offsets = Vec::with_capacity(num_rows + 1);
    offsets.push(0_i32);
    let mut current: i64 = 0;
    for _ in 0..num_rows {
        current = novarocks_functions::aggregate_scalar::checked_map_output_offset(
            current as i32,
            pair_count,
        )? as i64;
        offsets.push(current as i32);
    }

    let map = MapArray::try_new(
        map_field,
        OffsetBuffer::new(offsets.into()),
        entries,
        None,
        ordered,
    )
    .map_err(|source| {
        crate::exec::expr::function::arrow_operation::arrow_contract_failure(
            "map_literal MapArray",
            source,
        )
    })?;
    Ok(Arc::new(map) as ArrayRef)
}

fn validate_child_len(
    role: &str,
    index: usize,
    array: &ArrayRef,
    num_rows: usize,
) -> crate::runtime::fragment::ExecutionResult<()> {
    if array.len() == num_rows || array.len() == 1 || (num_rows == 0 && array.is_empty()) {
        return Ok(());
    }
    Err(expression_contract(format_args!(
        "map literal {} argument {} length mismatch: expected {} rows, got {}",
        role,
        index,
        num_rows,
        array.len()
    )))
}

fn build_values_array(
    arrays: Vec<ArrayRef>,
    target_type: &DataType,
    num_rows: usize,
    pair_count: usize,
    role: &str,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    if pair_count == 0 {
        return Ok(new_empty_array(target_type));
    }

    let mut casted = Vec::with_capacity(arrays.len());
    for array in arrays {
        let array = if array.data_type() == target_type {
            array
        } else {
            cast_with_special_rules(&array, target_type)?
        };
        validate_child_len(role, casted.len(), &array, num_rows)?;
        casted.push(array);
    }

    let data_storage: Vec<arrow_data::ArrayData> =
        casted.iter().map(|array| array.to_data()).collect();
    let data_refs: Vec<&arrow_data::ArrayData> = data_storage.iter().collect();
    let mut mutable = MutableArrayData::new(data_refs, true, num_rows.saturating_mul(pair_count));
    for row in 0..num_rows {
        for (idx, array) in casted.iter().enumerate() {
            let source_row = super::common::row_index(row, array.len());
            mutable.extend(idx, source_row, source_row + 1);
        }
    }
    Ok(make_array(mutable.freeze()))
}

fn entry_fields_for_arrays(fields: &Fields, values: &ArrayRef) -> Fields {
    let mut adjusted = fields.iter().cloned().collect::<Vec<_>>();
    adjusted[1] = field_with_actual_nullability(&adjusted[1], values);
    Fields::from(adjusted)
}

fn field_with_actual_nullability(field: &Arc<Field>, array: &ArrayRef) -> Arc<Field> {
    let nullable = field.is_nullable() || array.null_count() > 0;
    if field.is_nullable() == nullable {
        return field.clone();
    }
    Arc::new(Field::new(
        field.name(),
        field.data_type().clone(),
        nullable,
    ))
}

#[cfg(test)]
mod tests {
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::function::FunctionKind;
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use arrow::array::{Array, Int32Array, MapArray};
    use arrow::datatypes::{DataType, Field, Fields, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_failure_contract::FailureProjection;
    use novarocks_types::SlotId;
    use std::sync::Arc;

    fn one_row_chunk() -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .unwrap();
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn map_type() -> DataType {
        let entries = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", DataType::Int32, true)),
        ]));
        DataType::Map(Arc::new(Field::new("entries", entries, false)), false)
    }

    #[test]
    fn map_literal_accepts_variadic_pairs() {
        let chunk = one_row_chunk();
        let mut arena = ExprArena::default();
        let args = [1, 10, 2, 20, 3, 30]
            .into_iter()
            .map(|v| arena.push_typed(ExprNode::Literal(LiteralValue::Int32(v)), DataType::Int32))
            .collect::<Vec<_>>();
        let expr = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Map("map"),
                args,
            },
            map_type(),
        );

        let out = arena.eval(expr, &chunk).expect("map literal");
        let map = out.as_any().downcast_ref::<MapArray>().expect("map");
        assert_eq!(map.len(), 1);
        assert_eq!(map.value_length(0), 3);
    }

    #[test]
    fn map_literal_rejects_null_key_slots_without_widening_the_output() {
        let chunk = one_row_chunk();
        let mut arena = ExprArena::default();
        let null_key = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Int32);
        let null_value = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Int32);
        let expr = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Map("map"),
                args: vec![null_key, null_value],
            },
            map_type(),
        );

        let error = arena
            .eval(expr, &chunk)
            .expect_err("map key must be present");
        assert_eq!(
            error.failure_fact(),
            super::super::common::null_key().failure_fact()
        );
    }
    #[test]
    fn map_literal_rejects_null_keys_produced_by_output_coercion() {
        let chunk = one_row_chunk();
        let mut arena = ExprArena::default();
        let key = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("invalid integer".to_string())),
            DataType::Utf8,
        );
        let value = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(10)), DataType::Int32);
        let expr = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Map("map"),
                args: vec![key, value],
            },
            map_type(),
        );

        assert_eq!(
            arena.eval(expr, &chunk).unwrap_err().failure_fact(),
            super::super::common::null_key().failure_fact()
        );
    }

    #[test]
    fn map_literal_preserves_null_value_slots_under_the_nonnull_key_contract() {
        let chunk = one_row_chunk();
        let mut arena = ExprArena::default();
        let key = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(1)), DataType::Int32);
        let value = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Int32);
        let expr = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Map("map"),
                args: vec![key, value],
            },
            map_type(),
        );
        let output = arena.eval(expr, &chunk).unwrap();
        let output = output.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(output.value_length(0), 1);
        assert!(!output.keys().is_null(0));
        assert!(output.values().is_null(0));
        assert_eq!(output.data_type(), &map_type());
    }
}

#[cfg(test)]
mod cast_source_properties {
    use super::*;
    use arrow::array::Int64Array;
    #[test]
    fn original_child_cast_refusal_keeps_sdk_source_and_releases_its_array() {
        let input: ArrayRef = Arc::new(StructArray::new(
            vec![Arc::new(Field::new("x", DataType::Int64, false))].into(),
            vec![Arc::new(Int64Array::from(vec![1]))],
            None,
        ));
        let expected = arrow::compute::cast(&input, &DataType::Int64)
            .unwrap_err()
            .to_string();
        let weak = Arc::downgrade(&input);
        let error = build_values_array(vec![input], &DataType::Int64, 1, 1, "key").unwrap_err();
        let mut source: &(dyn std::error::Error + 'static) = &error;
        loop {
            if let Some(arrow) = source.downcast_ref::<arrow::error::ArrowError>() {
                assert_eq!(arrow.to_string(), expected);
                break;
            }
            source = source.source().expect("actual Arrow cast error retained");
        }
        assert!(weak.upgrade().is_none());
    }
}
