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
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::ArrayRef;
use std::collections::HashMap;

#[derive(Clone, Copy)]
pub struct FunctionMeta {
    pub name: &'static str,
    pub min_args: usize,
    pub max_args: usize,
}

pub fn register(map: &mut HashMap<&'static str, crate::exec::expr::function::FunctionKind>) {
    for (name, canonical) in MV_STATE_FUNCTIONS {
        map.insert(
            *name,
            crate::exec::expr::function::FunctionKind::MvState(canonical),
        );
    }
}

pub fn metadata(name: &str) -> Option<FunctionMeta> {
    MV_STATE_METADATA.iter().find(|m| m.name == name).copied()
}

pub fn eval_mv_state_function(
    name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let canonical = MV_STATE_FUNCTIONS
        .iter()
        .find_map(|(alias, target)| (*alias == name).then_some(*target))
        .unwrap_or(name);

    match canonical {
        "count_state_union" => super::count::eval_count_state_union(arena, expr, args, chunk),
        "count_state_visible" => super::count::eval_count_state_visible(arena, expr, args, chunk),
        "state_all_zero" => super::count::eval_state_all_zero(arena, expr, args, chunk),
        "mv_group_row_id" => eval_mv_group_row_id(arena, expr, args, chunk),
        "mv_content_key" => eval_mv_content_key(arena, args, chunk),
        "mv_require_non_null" => eval_mv_require_non_null(arena, expr, args, chunk),
        "mv_entry_id" => {
            Err("mv_entry_id requires an exact task-bound top-level Project evaluation".into())
        }
        "count_distinct_state_union" => {
            super::count_distinct::eval_count_distinct_state_union(arena, expr, args, chunk)
        }
        "count_distinct_state_visible" => {
            super::count_distinct::eval_count_distinct_state_visible(arena, expr, args, chunk)
        }
        "approx_count_distinct_state_union" => {
            super::approx_count_distinct::eval_approx_count_distinct_state_union(
                arena, expr, args, chunk,
            )
        }
        "approx_count_distinct_state_visible" => {
            super::approx_count_distinct::eval_approx_count_distinct_state_visible(
                arena, expr, args, chunk,
            )
        }
        "avg_state_union" => super::avg::eval_avg_state_union(arena, expr, args, chunk),
        "avg_state_visible" => super::avg::eval_avg_state_visible(arena, expr, args, chunk),
        "sum_state_union" => super::sum::eval_sum_state_union(arena, expr, args, chunk),
        "sum_state_visible" => super::sum::eval_sum_state_visible(arena, expr, args, chunk),
        "min_state_union" => super::min_max::eval_min_state_union(arena, expr, args, chunk),
        "min_state_visible" => super::min_max::eval_min_state_visible(arena, expr, args, chunk),
        "max_state_union" => super::min_max::eval_max_state_union(arena, expr, args, chunk),
        "max_state_visible" => super::min_max::eval_max_state_visible(arena, expr, args, chunk),
        "bool_or_state_union" => {
            super::bool_or_and::eval_bool_or_state_union(arena, expr, args, chunk)
        }
        "bool_or_state_visible" => {
            super::bool_or_and::eval_bool_or_state_visible(arena, expr, args, chunk)
        }
        "bool_and_state_union" => {
            super::bool_or_and::eval_bool_and_state_union(arena, expr, args, chunk)
        }
        "bool_and_state_visible" => {
            super::bool_or_and::eval_bool_and_state_visible(arena, expr, args, chunk)
        }
        other => Err(format!("unsupported mv_state function: {}", other)),
    }
}

fn eval_mv_group_row_id(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.is_empty() {
        return Err("mv_group_row_id expects at least 1 argument, got 0".to_string());
    }
    let columns = args
        .iter()
        .map(|arg| arena.eval(*arg, chunk))
        .collect::<Result<Vec<_>, _>>()?;
    crate::exec::mv::group_row_id::aggregate_group_row_id_array(&columns)
}

fn eval_mv_require_non_null(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let [input] = args else {
        return Err("mv_require_non_null requires exactly one argument".into());
    };
    let expected = arena
        .data_type(expr)
        .ok_or_else(|| "mv_require_non_null requires an exact result type".to_string())?;
    if arena.data_type(*input) != Some(expected)
        || !novarocks_type_contract::ResultContentEquivalence::NativeResultContentV1
            .supports(expected)
    {
        return Err("mv_require_non_null requires an unchanged NativeResultContentV1 type".into());
    }
    let values = arena.eval(*input, chunk)?;
    if values.data_type() != expected {
        return Err("mv_require_non_null input array differs from its exact type".into());
    }
    let has_null = if matches!(
        values.data_type(),
        arrow::datatypes::DataType::Dictionary(..)
    ) {
        (0..values.len()).try_fold(false, |found, row| {
            Ok::<_, String>(found || mv_representative_is_null(values.as_ref(), row)?)
        })?
    } else {
        values.logical_null_count() != 0
    };
    if has_null {
        return Err("mv_require_non_null encountered NULL in a non-null MV representative".into());
    }
    Ok(values)
}

// Checking dictionary values directly avoids allocating a decoded null bitmap,
// including for nested dictionaries whose unused value domain contains NULL.
fn mv_representative_is_null(values: &dyn arrow::array::Array, row: usize) -> Result<bool, String> {
    use arrow::array::DictionaryArray;
    use arrow::datatypes::*;
    if row >= values.len() {
        return Err("mv_require_non_null dictionary key is out of range".into());
    }
    if values.is_null(row) || matches!(values.data_type(), DataType::Null) {
        return Ok(true);
    }
    let DataType::Dictionary(key, _) = values.data_type() else {
        return Ok(false);
    };
    macro_rules! check_dictionary {
        ($key:ty) => {{
            let dictionary = values
                .as_any()
                .downcast_ref::<DictionaryArray<$key>>()
                .ok_or_else(|| {
                    "mv_require_non_null dictionary key type differs from its array".to_string()
                })?;
            let index = dictionary
                .key(row)
                .ok_or_else(|| "mv_require_non_null dictionary key is absent".to_string())?;
            mv_representative_is_null(dictionary.values().as_ref(), index)
        }};
    }
    match key.as_ref() {
        DataType::Int8 => check_dictionary!(Int8Type),
        DataType::Int16 => check_dictionary!(Int16Type),
        DataType::Int32 => check_dictionary!(Int32Type),
        DataType::Int64 => check_dictionary!(Int64Type),
        DataType::UInt8 => check_dictionary!(UInt8Type),
        DataType::UInt16 => check_dictionary!(UInt16Type),
        DataType::UInt32 => check_dictionary!(UInt32Type),
        DataType::UInt64 => check_dictionary!(UInt64Type),
        _ => Err("mv_require_non_null dictionary key type is unsupported".into()),
    }
}

fn eval_mv_content_key(
    arena: &ExprArena,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.is_empty() {
        return Err("mv_content_key requires at least one argument".into());
    }
    let columns = args
        .iter()
        .map(|arg| arena.eval(*arg, chunk))
        .collect::<Result<Vec<_>, _>>()?;
    let types = args
        .iter()
        .zip(&columns)
        .map(|(arg, column)| {
            arena
                .data_type(*arg)
                .cloned()
                .unwrap_or_else(|| column.data_type().clone())
        })
        .collect();
    let encoder = crate::exec::hash_table::content_key::ContentKeyEncoder::try_new_types(types)?;
    let mut builder = arrow::array::BinaryBuilder::new();
    let mut key = Vec::new();
    for row in 0..chunk.len() {
        encoder.encode_row_into(&columns, row, &mut key)?;
        builder.append_value(&key);
    }
    Ok(std::sync::Arc::new(builder.finish()))
}

static MV_STATE_FUNCTIONS: &[(&str, &str)] = &[
    ("count_state_union", "count_state_union"),
    ("count_state_visible", "count_state_visible"),
    ("state_all_zero", "state_all_zero"),
    ("mv_group_row_id", "mv_group_row_id"),
    ("mv_content_key", "mv_content_key"),
    ("mv_entry_id", "mv_entry_id"),
    ("mv_require_non_null", "mv_require_non_null"),
    ("count_distinct_state_union", "count_distinct_state_union"),
    (
        "count_distinct_state_visible",
        "count_distinct_state_visible",
    ),
    (
        "approx_count_distinct_state_union",
        "approx_count_distinct_state_union",
    ),
    (
        "approx_count_distinct_state_visible",
        "approx_count_distinct_state_visible",
    ),
    ("avg_state_union", "avg_state_union"),
    ("avg_state_visible", "avg_state_visible"),
    ("sum_state_union", "sum_state_union"),
    ("sum_state_visible", "sum_state_visible"),
    ("min_state_union", "min_state_union"),
    ("min_state_visible", "min_state_visible"),
    ("max_state_union", "max_state_union"),
    ("max_state_visible", "max_state_visible"),
    ("bool_or_state_union", "bool_or_state_union"),
    ("bool_or_state_visible", "bool_or_state_visible"),
    ("bool_and_state_union", "bool_and_state_union"),
    ("bool_and_state_visible", "bool_and_state_visible"),
];

static MV_STATE_METADATA: &[FunctionMeta] = &[
    FunctionMeta {
        name: "mv_require_non_null",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "mv_entry_id",
        min_args: 0,
        max_args: 0,
    },
    FunctionMeta {
        name: "mv_content_key",
        min_args: 1,
        max_args: usize::MAX,
    },
    FunctionMeta {
        name: "count_state_union",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "count_state_visible",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "state_all_zero",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "mv_group_row_id",
        min_args: 1,
        max_args: usize::MAX,
    },
    FunctionMeta {
        name: "count_distinct_state_union",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "count_distinct_state_visible",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "approx_count_distinct_state_union",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "approx_count_distinct_state_visible",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "avg_state_union",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "avg_state_visible",
        min_args: 2,
        max_args: 4,
    },
    FunctionMeta {
        name: "sum_state_union",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "sum_state_visible",
        min_args: 1,
        max_args: 2,
    },
    FunctionMeta {
        name: "min_state_union",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "min_state_visible",
        min_args: 1,
        max_args: 2,
    },
    FunctionMeta {
        name: "max_state_union",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "max_state_visible",
        min_args: 1,
        max_args: 2,
    },
    FunctionMeta {
        name: "bool_or_state_union",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "bool_or_state_visible",
        min_args: 1,
        max_args: 1,
    },
    FunctionMeta {
        name: "bool_and_state_union",
        min_args: 2,
        max_args: 2,
    },
    FunctionMeta {
        name: "bool_and_state_visible",
        min_args: 1,
        max_args: 1,
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, BinaryArray, Decimal128Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::exec::chunk::Chunk;
    use crate::exec::expr::function::{FunctionKind, function_metadata, lookup_function};
    use crate::exec::expr::{ExprNode, LiteralValue};
    use novarocks_types::SlotId;

    #[test]
    fn state_all_zero_is_registered_as_mv_state_function() {
        assert_eq!(
            lookup_function("state_all_zero"),
            Some(FunctionKind::MvState("state_all_zero"))
        );

        let direct_meta = metadata("state_all_zero").unwrap();
        assert_eq!(direct_meta.min_args, 1);
        assert_eq!(direct_meta.max_args, 1);

        let registry_meta = function_metadata(FunctionKind::MvState("state_all_zero"));
        assert_eq!(registry_meta.name, "state_all_zero");
        assert_eq!(registry_meta.min_args, 1);
        assert_eq!(registry_meta.max_args, 1);
    }

    #[test]
    fn mv_group_row_id_matches_aggregate_state_physical_row_ids() {
        assert_eq!(
            lookup_function("mv_group_row_id"),
            Some(FunctionKind::MvState("mv_group_row_id"))
        );

        let mut arena = ExprArena::default();
        let k1 = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let k2 = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Utf8);
        let expr = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::MvState("mv_group_row_id"),
                args: vec![k1, k2],
            },
            DataType::Utf8,
        );
        let chunk = two_key_chunk();

        let out = arena.eval(expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        let expected =
            crate::exec::mv::group_row_id::aggregate_group_row_id_array(chunk.columns()).unwrap();
        let expected = expected.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(out.len(), expected.len());
        for row in 0..out.len() {
            assert_eq!(out.value(row), expected.value(row));
        }
    }

    #[test]
    fn avg_state_visible_four_args_keeps_decimal_scale() {
        let mut arena = ExprArena::default();
        let sum_state = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Binary);
        let count_state = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Binary);
        let scale = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(4)), DataType::Int64);
        let witness = arena.push_typed(
            ExprNode::Literal(LiteralValue::Null),
            DataType::Decimal128(38, 10),
        );
        let expr = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::MvState("avg_state_visible"),
                args: vec![sum_state, count_state, scale, witness],
            },
            DataType::Decimal128(38, 10),
        );
        let sum_state = crate::exec::mv::state_codec::encode_sum_decimal128(2, 300_000);
        let count_state = crate::exec::mv::state_codec::encode_count_state(2);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("sum_state", DataType::Binary, false),
                Field::new("count_state", DataType::Binary, false),
            ])),
            vec![
                Arc::new(BinaryArray::from(vec![Some(sum_state.as_slice())])) as ArrayRef,
                Arc::new(BinaryArray::from(vec![Some(count_state.as_slice())])) as ArrayRef,
            ],
        )
        .unwrap();
        let schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1), SlotId::new(2)],
        )
        .unwrap();

        let out = arena
            .eval(expr, &Chunk::new_with_chunk_schema(batch, schema))
            .unwrap();
        let out = out.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(out.value(0), 150_000_000_000);
    }

    fn two_key_chunk() -> Chunk {
        let k1 = Arc::new(Int64Array::from(vec![Some(10), None, Some(10)])) as ArrayRef;
        let k2 = Arc::new(StringArray::from(vec![Some("a"), Some("a"), None])) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![
            Field::new("k1", DataType::Int64, true),
            Field::new("k2", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![k1, k2]).unwrap();
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1), SlotId::new(2)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }
}

#[cfg(test)]
mod content_key_tests {
    use super::*;
    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use crate::exec::expr::ExprNode;
    use arrow::array::{Array, BinaryArray, Float64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;
    use std::sync::Arc;

    fn require_non_null_input(values: ArrayRef) -> (ExprArena, ExprId, Chunk) {
        let field = Field::new("representative", values.data_type().clone(), true);
        let slot = SlotId::new(9);
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(vec![field.clone()])), vec![values]).unwrap();
        let schema = Arc::new(
            ChunkSchema::try_new(vec![
                ChunkSlotSchema::from_field(slot, &field, None).unwrap(),
            ])
            .unwrap(),
        );
        let chunk = Chunk::try_new_with_chunk_schema(batch, schema).unwrap();
        let mut arena = ExprArena::default();
        let input = arena.push_typed(ExprNode::SlotId(slot), field.data_type().clone());
        (arena, input, chunk)
    }

    #[test]
    fn mv_require_non_null_preserves_array_buffers_and_float_bits() {
        let bits = [
            0_u64,
            (-0.0_f64).to_bits(),
            0x7ff8_0000_0000_0042,
            0xfff8_0000_0000_0043,
        ];
        let values = Arc::new(Float64Array::from(bits.map(f64::from_bits).to_vec())) as ArrayRef;
        let (arena, input, chunk) = require_non_null_input(values.clone());
        assert_eq!(metadata("mv_require_non_null").unwrap().min_args, 1);
        let output =
            eval_mv_state_function("mv_require_non_null", &arena, input, &[input], &chunk).unwrap();
        assert_eq!(output.data_type(), values.data_type());
        let result = output.as_any().downcast_ref::<Float64Array>().unwrap();
        let source = values.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(result.values().as_ptr(), source.values().as_ptr());
        for (row, expected) in bits.into_iter().enumerate() {
            assert_eq!(result.value(row).to_bits(), expected);
        }
        assert!(eval_mv_state_function("mv_require_non_null", &arena, input, &[], &chunk).is_err());
        assert!(
            eval_mv_state_function(
                "mv_require_non_null",
                &arena,
                input,
                &[input, input],
                &chunk
            )
            .is_err()
        );
    }

    #[test]
    fn mv_require_non_null_rejects_physical_and_dictionary_value_nulls() {
        use arrow::array::{DictionaryArray, Int8Array, Int64Array, StringArray};
        use arrow::datatypes::Int8Type;
        let dictionary = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0_i8, 1]),
            Arc::new(StringArray::from(vec![Some("present"), None])),
        )
        .unwrap();
        assert_eq!(dictionary.null_count(), 0);
        let inputs = [
            Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef,
            Arc::new(dictionary) as ArrayRef,
        ];
        for values in inputs {
            let (arena, input, chunk) = require_non_null_input(values);
            let error =
                eval_mv_state_function("mv_require_non_null", &arena, input, &[input], &chunk)
                    .unwrap_err();
            assert!(error.contains("encountered NULL"));
        }
        let inner = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0_i8, 1]),
            Arc::new(StringArray::from(vec![Some("present"), None])),
        )
        .unwrap();
        let outer = Arc::new(
            DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0_i8, 0]), Arc::new(inner))
                .unwrap(),
        ) as ArrayRef;
        let (arena, input, chunk) = require_non_null_input(outer.clone());
        let output =
            eval_mv_state_function("mv_require_non_null", &arena, input, &[input], &chunk).unwrap();
        let source = outer
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .unwrap();
        let result = output
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .unwrap();
        assert_eq!(
            source.keys().values().as_ptr(),
            result.keys().values().as_ptr()
        );
        assert!(Arc::ptr_eq(source.values(), result.values()));
    }

    #[test]
    fn mv_require_non_null_rejects_missing_or_changed_type() {
        let (mut arena, input, chunk) =
            require_non_null_input(Arc::new(Float64Array::from(vec![1.0])));
        let untyped = arena.push(ExprNode::SlotId(SlotId::new(9)));
        assert!(
            eval_mv_state_function("mv_require_non_null", &arena, untyped, &[input], &chunk)
                .is_err()
        );
        let changed = arena.push_typed(ExprNode::SlotId(SlotId::new(9)), DataType::Int64);
        assert!(
            eval_mv_state_function("mv_require_non_null", &arena, changed, &[input], &chunk)
                .is_err()
        );
    }

    #[test]
    fn mv_content_key_scalar_dispatch_matches_public_encoder() {
        let field = Field::new("v", DataType::Float64, true);
        let slot = SlotId::new(1);
        let values = Arc::new(Float64Array::from(vec![Some(0.), Some(-0.), None])) as ArrayRef;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field.clone()])),
            vec![values.clone()],
        )
        .unwrap();
        let schema = Arc::new(
            ChunkSchema::try_new(vec![
                ChunkSlotSchema::from_field(slot, &field, None).unwrap(),
            ])
            .unwrap(),
        );
        let chunk = Chunk::try_new_with_chunk_schema(batch, schema).unwrap();
        let mut arena = ExprArena::default();
        let input = arena.push_typed(ExprNode::SlotId(slot), DataType::Float64);
        assert!(metadata("mv_content_key").is_some());
        let output =
            eval_mv_state_function("mv_content_key", &arena, input, &[input], &chunk).unwrap();
        let output = output.as_any().downcast_ref::<BinaryArray>().unwrap();
        let encoder = crate::exec::hash_table::content_key::ContentKeyEncoder::try_new_types(vec![
            DataType::Float64,
        ])
        .unwrap();
        for row in 0..3 {
            assert!(!output.is_null(row));
            assert_eq!(
                output.value(row),
                encoder
                    .encode_row(std::slice::from_ref(&values), row)
                    .unwrap()
            );
        }
        assert_ne!(output.value(0), output.value(1));
        assert!(eval_mv_state_function("mv_content_key", &arena, input, &[], &chunk).is_err());
    }
}
