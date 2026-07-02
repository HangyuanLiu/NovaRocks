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

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, DictionaryArray};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Int32Type};

use crate::common::ids::SlotId;
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
use crate::exec::expr::function::FunctionKind;
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};

pub(crate) fn is_supported_i32_string_dictionary(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Dictionary(key_type, value_type)
            if key_type.as_ref() == &DataType::Int32 && value_type.as_ref() == &DataType::Utf8
    )
}

pub(crate) fn referenced_slots(arena: &ExprArena, expr_id: ExprId) -> Option<BTreeSet<SlotId>> {
    let mut slots = BTreeSet::new();
    collect_referenced_slots(arena, expr_id, &mut slots).then_some(slots)
}

pub(crate) fn expr_can_peel_from_slot(arena: &ExprArena, expr_id: ExprId, slot_id: SlotId) -> bool {
    peel_source_slot(arena, expr_id) == Some(slot_id)
}

pub(crate) fn try_peel_dict_expr(
    arena: &ExprArena,
    expr_id: ExprId,
    chunk: &Chunk,
) -> Result<Option<ArrayRef>, String> {
    let Some(source_slot) = peel_source_slot(arena, expr_id) else {
        return Ok(None);
    };
    let Some(source_idx) = chunk.slot_id_to_index().get(&source_slot).copied() else {
        return Ok(None);
    };
    let Some(source) = chunk.columns().get(source_idx) else {
        return Ok(None);
    };
    if !is_supported_i32_string_dictionary(source.data_type()) {
        return Ok(None);
    }
    let dict = source
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .ok_or_else(|| {
            format!(
                "dictionary peel expected Dictionary(Int32, Utf8), got {:?}",
                source.data_type()
            )
        })?;
    if dict.values().null_count() > 0 {
        return Ok(None);
    }

    let value_field = Field::new(
        chunk
            .chunk_schema()
            .field_by_slot(source_slot)
            .map(|field| field.name().as_str())
            .unwrap_or("dict_values"),
        dict.values().data_type().clone(),
        dict.values().is_nullable(),
    );
    let value_slot = ChunkSlotSchema::new_with_field(source_slot, value_field, None, None);
    let value_schema = Arc::new(ChunkSchema::try_new(vec![value_slot])?);
    let value_chunk = Chunk::try_new_with_columns(value_schema, vec![Arc::clone(dict.values())])?;
    let new_values = arena.eval(expr_id, &value_chunk)?;

    if new_values.data_type() == &DataType::Utf8 && new_values.null_count() == 0 {
        let peeled = DictionaryArray::<Int32Type>::try_new(dict.keys().clone(), new_values)
            .map_err(|e| format!("dictionary peel rewrap failed: {e}"))?;
        return Ok(Some(Arc::new(peeled) as ArrayRef));
    }

    let expanded = take(new_values.as_ref(), dict.keys(), None)
        .map_err(|e| format!("dictionary peel take failed: {e}"))?;
    Ok(Some(expanded))
}

fn peel_source_slot(arena: &ExprArena, expr_id: ExprId) -> Option<SlotId> {
    if !is_peel_safe_expr(arena, expr_id) {
        return None;
    }
    let slots = referenced_slots(arena, expr_id)?;
    if slots.len() != 1 {
        return None;
    }
    slots.into_iter().next()
}

fn collect_referenced_slots(
    arena: &ExprArena,
    expr_id: ExprId,
    slots: &mut BTreeSet<SlotId>,
) -> bool {
    let Some(node) = arena.node(expr_id) else {
        return false;
    };
    match node {
        ExprNode::Literal(_) => true,
        ExprNode::SlotId(slot_id) => {
            slots.insert(*slot_id);
            true
        }
        ExprNode::ArrayExpr { elements } | ExprNode::StructExpr { fields: elements } => {
            collect_all_referenced_slots(arena, elements, slots)
        }
        ExprNode::LambdaFunction { .. } => false,
        ExprNode::DictDecode { child, .. }
        | ExprNode::Cast(child)
        | ExprNode::CastTime(child)
        | ExprNode::CastTimeFromDatetime(child)
        | ExprNode::Not(child)
        | ExprNode::IsNull(child)
        | ExprNode::IsNotNull(child)
        | ExprNode::Clone(child) => collect_referenced_slots(arena, *child, slots),
        ExprNode::Add(left, right)
        | ExprNode::Sub(left, right)
        | ExprNode::Mul(left, right)
        | ExprNode::Div(left, right)
        | ExprNode::Mod(left, right)
        | ExprNode::Eq(left, right)
        | ExprNode::EqForNull(left, right)
        | ExprNode::Ne(left, right)
        | ExprNode::Lt(left, right)
        | ExprNode::Le(left, right)
        | ExprNode::Gt(left, right)
        | ExprNode::Ge(left, right)
        | ExprNode::And(left, right)
        | ExprNode::Or(left, right) => {
            collect_referenced_slots(arena, *left, slots)
                && collect_referenced_slots(arena, *right, slots)
        }
        ExprNode::In { child, values, .. } => {
            collect_referenced_slots(arena, *child, slots)
                && collect_all_referenced_slots(arena, values, slots)
        }
        ExprNode::Case { children, .. } => collect_all_referenced_slots(arena, children, slots),
        ExprNode::FunctionCall { args, .. } => collect_all_referenced_slots(arena, args, slots),
    }
}

fn collect_all_referenced_slots(
    arena: &ExprArena,
    expr_ids: &[ExprId],
    slots: &mut BTreeSet<SlotId>,
) -> bool {
    expr_ids
        .iter()
        .copied()
        .all(|expr_id| collect_referenced_slots(arena, expr_id, slots))
}

fn is_peel_safe_expr(arena: &ExprArena, expr_id: ExprId) -> bool {
    let Some(node) = arena.node(expr_id) else {
        return false;
    };
    match node {
        ExprNode::SlotId(_) => true,
        ExprNode::Literal(value) => !matches!(value, LiteralValue::Null),
        ExprNode::Cast(child) | ExprNode::Clone(child) => is_peel_safe_expr(arena, *child),
        ExprNode::FunctionCall { kind, args } if is_peel_safe_function(*kind) => args
            .iter()
            .copied()
            .all(|arg| is_peel_safe_expr(arena, arg)),
        _ => false,
    }
}

fn is_peel_safe_function(kind: FunctionKind) -> bool {
    matches!(kind, FunctionKind::Upper | FunctionKind::Substring)
        || matches!(
            kind,
            FunctionKind::String(
                "lower"
                    | "lcase"
                    | "trim"
                    | "ltrim"
                    | "rtrim"
                    | "reverse"
                    | "length"
                    | "char_length"
            )
        )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, DictionaryArray, StringArray};
    use arrow::datatypes::{DataType, Field, Int32Type};

    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
    use crate::exec::expr::function::FunctionKind;
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};

    use super::{
        expr_can_peel_from_slot, is_supported_i32_string_dictionary, referenced_slots,
        try_peel_dict_expr,
    };

    fn dict_status_chunk(slot_id: SlotId) -> Chunk {
        let values: ArrayRef = Arc::new(
            vec![
                Some("PAID"),
                None,
                Some("New"),
                Some("PAID"),
                Some(" shipped "),
            ]
            .into_iter()
            .collect::<DictionaryArray<Int32Type>>(),
        );
        let slot = ChunkSlotSchema::new_with_field(
            slot_id,
            Field::new("status", values.data_type().clone(), true),
            None,
            None,
        );
        Chunk::try_new_with_columns(
            Arc::new(ChunkSchema::try_new(vec![slot]).unwrap()),
            vec![values],
        )
        .unwrap()
    }

    #[test]
    fn peel_lower_over_utf8_dictionary_reuses_keys_and_lowercases_values() {
        let slot = SlotId::new(7);
        let chunk = dict_status_chunk(slot);
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(slot), DataType::Utf8);
        let lower = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::String("lower"),
                args: vec![source],
            },
            DataType::Utf8,
        );

        assert!(expr_can_peel_from_slot(&arena, lower, slot));
        let out = try_peel_dict_expr(&arena, lower, &chunk)
            .expect("peel should not error")
            .expect("lower should peel");

        let dict = out
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("dictionary output");
        let input = chunk.columns()[0]
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("input dictionary");
        assert_eq!(dict.keys(), input.keys());
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string values");
        assert!(
            values
                .iter()
                .flatten()
                .all(|v: &str| v == v.to_lowercase().as_str())
        );
        assert!(dict.is_null(1));
    }

    #[test]
    fn unsafe_coalesce_is_not_peelable_but_still_reports_referenced_slot() {
        let slot = SlotId::new(8);
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(slot), DataType::Utf8);
        let fallback = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("missing".to_string())),
            DataType::Utf8,
        );
        let coalesce = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Coalesce,
                args: vec![source, fallback],
            },
            DataType::Utf8,
        );

        assert_eq!(
            referenced_slots(&arena, coalesce)
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
            vec![slot]
        );
        assert!(!expr_can_peel_from_slot(&arena, coalesce, slot));
    }

    #[test]
    fn non_string_peel_expands_flat_values_by_keys() {
        let slot = SlotId::new(9);
        let chunk = dict_status_chunk(slot);
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(slot), DataType::Utf8);
        let length = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::String("length"),
                args: vec![source],
            },
            DataType::Int64,
        );

        let out = try_peel_dict_expr(&arena, length, &chunk)
            .expect("peel should not error")
            .expect("length should compute over dictionary values and flatten");

        assert_eq!(out.data_type(), &DataType::Int64);
        let flat = out
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("flat length output");
        assert_eq!(flat.value(0), 4);
        assert!(flat.is_null(1));
        assert_eq!(flat.value(2), 3);
    }

    #[test]
    fn supported_dictionary_type_requires_int32_keys_and_string_values() {
        let utf8_dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let int_dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32));
        let int64_key_dict =
            DataType::Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8));

        assert!(is_supported_i32_string_dictionary(&utf8_dict));
        assert!(!is_supported_i32_string_dictionary(&int_dict));
        assert!(!is_supported_i32_string_dictionary(&int64_key_dict));
    }
}
