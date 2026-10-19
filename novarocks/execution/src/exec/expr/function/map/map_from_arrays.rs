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
use crate::exec::expr::cast::cast_with_special_rules;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, ListArray, MapArray, StructArray, make_array};
use arrow::datatypes::{DataType, Field};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use std::sync::Arc;

pub fn eval_map_from_arrays<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let _ = chunk;
    let key_arr = arena.eval(args[0], chunk)?;
    let value_arr = arena.eval(args[1], chunk)?;

    let key_list = key_arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            format!(
                "map_from_arrays expects ListArray keys, got {:?}",
                key_arr.data_type()
            )
        })?;
    let value_list = value_arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            format!(
                "map_from_arrays expects ListArray values, got {:?}",
                value_arr.data_type()
            )
        })?;

    if key_list.len() != value_list.len() {
        return Err((format!(
            "map_from_arrays length mismatch: keys={} values={}",
            key_list.len(),
            value_list.len()
        ))
        .into());
    }

    let key_values = key_list.values();
    let value_values = value_list.values();
    let key_offsets = key_list.value_offsets();
    let value_offsets = value_list.value_offsets();

    let key_values_data = key_values.to_data();
    let value_values_data = value_values.to_data();
    let mut key_mutable = MutableArrayData::new(vec![&key_values_data], false, key_values.len());
    let mut value_mutable =
        MutableArrayData::new(vec![&value_values_data], false, value_values.len());

    let mut offsets = Vec::with_capacity(key_list.len() + 1);
    offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(key_list.len());

    for row in 0..key_list.len() {
        if key_list.is_null(row) || value_list.is_null(row) {
            null_builder.append_null();
            offsets.push(current as i32);
            continue;
        }

        let key_start = key_offsets[row] as usize;
        let key_end = key_offsets[row + 1] as usize;
        let value_start = value_offsets[row] as usize;
        let value_end = value_offsets[row + 1] as usize;
        let key_len = key_end - key_start;
        let value_len = value_end - value_start;
        if key_len != value_len {
            return Err(("Key and value arrays must be the same length".to_string()).into());
        }

        if key_len > 0 {
            for idx in 0..key_len {
                let key_idx = key_start + idx;
                let value_idx = value_start + idx;
                key_mutable.extend(0, key_idx, key_idx + 1);
                value_mutable.extend(0, value_idx, value_idx + 1);
            }
            current += key_len as i64;
            if current > i32::MAX as i64 {
                return Err(("map_from_arrays offset overflow".to_string()).into());
            }
        }
        offsets.push(current as i32);
        null_builder.append_non_null();
    }

    let mut out_keys = make_array(key_mutable.freeze());
    let mut out_values = make_array(value_mutable.freeze());
    let (map_field, ordered) = super::common::output_map_field(
        arena.data_type(expr),
        key_values.data_type(),
        value_values.data_type(),
        "map_from_arrays",
    )?;
    let DataType::Struct(fields) = map_field.data_type() else {
        return Err(("map_from_arrays map entries type must be Struct".to_string()).into());
    };
    if fields.len() != 2 {
        return Err(("map_from_arrays map entries type must have 2 fields".to_string()).into());
    }

    let key_field = fields[0].clone();
    let value_field = fields[1].clone();
    if out_keys.data_type() != key_field.data_type() {
        out_keys = cast_with_special_rules(&out_keys, key_field.data_type()).map_err(|e| {
            format!(
                "map_from_arrays failed to cast keys to output type {:?}: {}",
                key_field.data_type(),
                e
            )
        })?;
    }
    if out_values.data_type() != value_field.data_type() {
        out_values =
            cast_with_special_rules(&out_values, value_field.data_type()).map_err(|e| {
                format!(
                    "map_from_arrays failed to cast values to output type {:?}: {}",
                    value_field.data_type(),
                    e
                )
            })?;
    }

    // Design: ADR-0183 (docs/adr/ADR-0183-map-keys-are-non-null-and-unique.md)
    // Only keys assembled for non-NULL Map rows participate. Coercion may
    // introduce NULL, but it cannot widen the frozen key field's nullability.
    if out_keys.logical_null_count() > 0 {
        return Err(super::common::MAP_KEY_NULL_MESSAGE.to_string().into());
    }
    let entries_fields = fields.clone();
    let entries_fields = if out_values.data_type() != value_field.data_type() {
        let mut adjusted = entries_fields.iter().cloned().collect::<Vec<_>>();
        adjusted[1] = Arc::new(Field::new(
            value_field.name(),
            out_values.data_type().clone(),
            value_field.is_nullable(),
        ));
        arrow::datatypes::Fields::from(adjusted)
    } else {
        entries_fields
    };
    let entries = StructArray::new(entries_fields.clone(), vec![out_keys, out_values], None);
    let map_field = Arc::new(Field::new(
        map_field.name(),
        DataType::Struct(entries_fields),
        map_field.is_nullable(),
    ));
    let map = MapArray::new(
        map_field,
        OffsetBuffer::new(offsets.into()),
        entries,
        null_builder.finish(),
        ordered,
    );

    Ok(Arc::new(map) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::eval_map_from_arrays;
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::function::FunctionKind;
    use crate::exec::expr::{ExprArena, ExprNode};
    use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, ListArray, MapArray, StringArray};
    use arrow::datatypes::{DataType, Field, Fields, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use novarocks_types::SlotId;
    use std::sync::Arc;

    fn map_type(key_type: DataType) -> DataType {
        let entries = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("key", key_type, false)),
            Arc::new(Field::new("value", DataType::Int32, true)),
        ]));
        DataType::Map(Arc::new(Field::new("entries", entries, false)), false)
    }

    fn evaluate(
        keys: ListArray,
        values: ListArray,
        output_type: DataType,
    ) -> Result<ArrayRef, String> {
        let key_type = keys.data_type().clone();
        let value_type = values.data_type().clone();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("keys", key_type.clone(), true),
                Field::new("values", value_type.clone(), true),
            ])),
            vec![Arc::new(keys), Arc::new(values)],
        )
        .unwrap();
        let key_slot = SlotId::new(1);
        let value_slot = SlotId::new(2);
        let schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[key_slot, value_slot],
        )
        .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, schema);
        let mut arena = ExprArena::default();
        let key = arena.push_typed(ExprNode::SlotId(key_slot), key_type);
        let value = arena.push_typed(ExprNode::SlotId(value_slot), value_type);
        let args = [key, value];
        let expr = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Map("map_from_arrays"),
                args: args.to_vec(),
            },
            output_type,
        );
        eval_map_from_arrays(&arena, expr, &args, &chunk)
    }

    #[test]
    fn map_from_arrays_rejects_null_key_in_a_mixed_batch() {
        let keys = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1)]),
            Some(vec![None]),
        ]);
        let values = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(10)]),
            Some(vec![Some(20)]),
        ]);

        assert_eq!(
            evaluate(keys, values, map_type(DataType::Int32)).unwrap_err(),
            "map key must not be NULL"
        );
    }

    #[test]
    fn map_from_arrays_rejects_null_key_produced_by_output_coercion() {
        let keys = ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(vec![0, 1].into()),
            Arc::new(StringArray::from(vec!["invalid integer"])),
            None,
        );
        let values = ListArray::from_iter_primitive::<Int32Type, _, _>([Some(vec![Some(10)])]);

        assert_eq!(
            evaluate(keys, values, map_type(DataType::Int32)).unwrap_err(),
            "map key must not be NULL"
        );
    }

    #[test]
    fn map_from_arrays_whole_null_lists_remain_null_maps() {
        // A NULL key-list row may retain NULL backing children. They are not
        // entries of an output Map and must not fail the constructor.
        let keys = ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(vec![0, 1, 2, 3].into()),
            Arc::new(Int32Array::from(vec![None, Some(2), Some(3)])),
            Some(NullBuffer::from(vec![false, true, true])),
        );
        let values = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(10)]),
            None,
            Some(vec![Some(30)]),
        ]);
        let output_type = map_type(DataType::Int32);
        let output = evaluate(keys, values, output_type.clone()).unwrap();
        let map = output.as_any().downcast_ref::<MapArray>().unwrap();

        assert_eq!(map.data_type(), &output_type);
        assert_eq!(map.len(), 3);
        assert!(map.is_null(0));
        assert!(map.is_null(1));
        assert!(!map.is_null(2));
        assert_eq!(map.value_offsets(), &[0, 0, 0, 1]);
        assert_eq!(
            map.keys()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[3]
        );
        assert_eq!(
            map.values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[30]
        );
    }

    #[test]
    fn map_from_arrays_preserves_legal_order_empty_maps_null_values_and_key_cast() {
        let keys = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(2), Some(1)]),
            Some(vec![]),
            Some(vec![Some(3)]),
        ]);
        let values = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(20), None]),
            Some(vec![]),
            Some(vec![Some(30)]),
        ]);
        let output_type = map_type(DataType::Int64);
        let output = evaluate(keys, values, output_type.clone()).unwrap();
        let map = output.as_any().downcast_ref::<MapArray>().unwrap();

        assert_eq!(map.data_type(), &output_type);
        assert_eq!(map.null_count(), 0);
        assert_eq!(map.value_offsets(), &[0, 2, 2, 3]);
        assert_eq!(
            map.keys()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[2, 1, 3]
        );
        assert_eq!(
            map.values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(20), None, Some(30)]
        );
    }
}
