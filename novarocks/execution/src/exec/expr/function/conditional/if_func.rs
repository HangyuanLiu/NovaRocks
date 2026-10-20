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
use crate::runtime::fragment::ExecutionFailure;
use arrow::array::{Array, ArrayRef, BooleanArray, new_null_array};
use arrow::compute::cast;
use arrow::compute::kernels::zip::zip;
use arrow::compute::take;
use arrow::datatypes::DataType;

// IF function for Arrow arrays
pub fn eval_if<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    if_expr: ExprId,
    condition_expr: ExprId,
    then_expr: ExprId,
    else_expr: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, ExecutionFailure> {
    // The frozen root is the result authority. Read it before any child;
    // branch runtime values cannot widen, narrow or repair missing metadata.
    let target_type = arena
        .data_type(if_expr)
        .ok_or_else(|| crate::exec::expr::expression_contract("if: missing output type"))?
        .clone();
    let condition_array = arena.eval(condition_expr, chunk)?;
    let len = condition_array.len();
    super::failure::preflight::<bool>(len)?;
    let condition_mask = condition_array_to_bool_mask(&condition_array)?;

    // Avoid eagerly evaluating both branches:
    // StarRocks may rewrite `CASE WHEN cond THEN expr ELSE NULL END` into `if(cond, expr, NULL)`.
    // Evaluating both branches unconditionally can raise errors even when the branch is not taken
    // (e.g. `if(x > 0, a / x, NULL)` would divide by zero for rows where x == 0).
    let mut any_true = false;
    let mut any_false = false;
    for i in 0..len {
        if condition_mask.value(i) {
            any_true = true;
        } else {
            any_false = true;
        }
    }

    if !any_true {
        let else_array = arena.eval(else_expr, chunk)?;
        return cast_branch_to(&else_array, &target_type, len).map_err(|e| {
            super::failure::context(
                format_args!("if: failed to cast else expression to {:?}", target_type),
                e,
            )
        });
    }
    if !any_false {
        let then_array = arena.eval(then_expr, chunk)?;
        return cast_branch_to(&then_array, &target_type, len).map_err(|e| {
            super::failure::context(
                format_args!("if: failed to cast then expression to {:?}", target_type),
                e,
            )
        });
    }

    // Mixed mask: evaluate each branch only on the rows where it is taken, then "scatter" the
    // results back into full-length arrays via `take` with nullable indices.
    let (true_idx_arr, false_idx_arr) = branch_indices(&condition_mask)?;

    let then_mask = BooleanArray::from(
        (0..len)
            .map(|i| condition_mask.value(i))
            .collect::<Vec<_>>(),
    );
    let else_mask = BooleanArray::from(
        (0..len)
            .map(|i| !condition_mask.value(i))
            .collect::<Vec<_>>(),
    );

    let then_batch = arrow::compute::filter_record_batch(&chunk.record_batch(), &then_mask)
        .map_err(|source| super::failure::arrow("if: failed to filter then-rows", source))?;
    let else_batch = arrow::compute::filter_record_batch(&chunk.record_batch(), &else_mask)
        .map_err(|source| super::failure::arrow("if: failed to filter else-rows", source))?;

    let then_chunk = Chunk::new_like(then_batch, chunk);
    let else_chunk = Chunk::new_like(else_batch, chunk);

    let then_small = arena.eval(then_expr, &then_chunk)?;
    let else_small = arena.eval(else_expr, &else_chunk)?;

    let then_small = cast_branch_to(&then_small, &target_type, then_small.len()).map_err(|e| {
        super::failure::context(
            format_args!("if: failed to cast then expression to {:?}", target_type),
            e,
        )
    })?;
    let else_small = cast_branch_to(&else_small, &target_type, else_small.len()).map_err(|e| {
        super::failure::context(
            format_args!("if: failed to cast else expression to {:?}", target_type),
            e,
        )
    })?;

    let then_full = take(&then_small, true_idx_arr.as_ref(), None)
        .map_err(|source| super::failure::arrow("", source))?;
    let else_full = take(&else_small, false_idx_arr.as_ref(), None)
        .map_err(|source| super::failure::arrow("", source))?;

    zip(
        &condition_mask,
        &then_full.as_ref() as &dyn arrow::array::Datum,
        &else_full.as_ref() as &dyn arrow::array::Datum,
    )
    .map_err(|source| super::failure::arrow("", source))
}

// Common batches retain compact UInt32 indices; larger admitted row domains
// use UInt64 instead of imposing an artificial u32 result-size boundary.
fn branch_indices(mask: &BooleanArray) -> Result<(ArrayRef, ArrayRef), ExecutionFailure> {
    if mask.len() <= u32::MAX as usize {
        branch_indices_for::<arrow::array::types::UInt32Type>(mask)
    } else {
        branch_indices_for::<arrow::array::types::UInt64Type>(mask)
    }
}
fn branch_indices_for<T: arrow::array::types::ArrowPrimitiveType>(
    mask: &BooleanArray,
) -> Result<(ArrayRef, ArrayRef), ExecutionFailure>
where
    arrow::array::PrimitiveArray<T>: From<Vec<Option<T::Native>>>,
{
    use arrow_buffer::ArrowNativeType;
    super::failure::preflight::<Option<T::Native>>(mask.len())?;
    let mut yes = Vec::with_capacity(mask.len());
    let mut no = Vec::with_capacity(mask.len());
    let mut t = 0usize;
    let mut f = 0usize;
    for i in 0..mask.len() {
        if mask.value(i) {
            yes.push(Some(T::Native::from_usize(t).expect(
                "the chosen unsigned index profile covers the row domain",
            )));
            no.push(None);
            t += 1;
        } else {
            yes.push(None);
            no.push(Some(T::Native::from_usize(f).expect(
                "the chosen unsigned index profile covers the row domain",
            )));
            f += 1;
        }
    }
    Ok((
        std::sync::Arc::new(arrow::array::PrimitiveArray::<T>::from(yes)),
        std::sync::Arc::new(arrow::array::PrimitiveArray::<T>::from(no)),
    ))
}

fn cast_branch_to(
    array: &ArrayRef,
    target_type: &DataType,
    len: usize,
) -> Result<ArrayRef, ExecutionFailure> {
    if array.data_type() == target_type {
        return Ok(array.clone());
    }
    if matches!(array.data_type(), DataType::Null) {
        return Ok(new_null_array(target_type, len));
    }
    cast(array.as_ref(), target_type).map_err(|source| super::failure::arrow("", source))
}

fn condition_array_to_bool_mask(array: &ArrayRef) -> Result<BooleanArray, ExecutionFailure> {
    let len = array.len();
    let values: Vec<bool> = match array.data_type() {
        DataType::Null => (0..len).map(|_| false).collect(),
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    crate::exec::expr::expression_contract(
                        "if: failed to downcast condition to BooleanArray",
                    )
                })?;
            (0..len).map(|i| !arr.is_null(i) && arr.value(i)).collect()
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| {
                    crate::exec::expr::expression_contract(
                        "if: failed to downcast condition to Int8Array",
                    )
                })?;
            (0..len)
                .map(|i| !arr.is_null(i) && arr.value(i) != 0)
                .collect()
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .ok_or_else(|| {
                    crate::exec::expr::expression_contract(
                        "if: failed to downcast condition to Int16Array",
                    )
                })?;
            (0..len)
                .map(|i| !arr.is_null(i) && arr.value(i) != 0)
                .collect()
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| {
                    crate::exec::expr::expression_contract(
                        "if: failed to downcast condition to Int32Array",
                    )
                })?;
            (0..len)
                .map(|i| !arr.is_null(i) && arr.value(i) != 0)
                .collect()
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| {
                    crate::exec::expr::expression_contract(
                        "if: failed to downcast condition to Int64Array",
                    )
                })?;
            (0..len)
                .map(|i| !arr.is_null(i) && arr.value(i) != 0)
                .collect()
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| {
                    crate::exec::expr::expression_contract(
                        "if: failed to downcast condition to Float32Array",
                    )
                })?;
            (0..len)
                .map(|i| !arr.is_null(i) && arr.value(i) != 0.0)
                .collect()
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| {
                    crate::exec::expr::expression_contract(
                        "if: failed to downcast condition to Float64Array",
                    )
                })?;
            (0..len)
                .map(|i| !arr.is_null(i) && arr.value(i) != 0.0)
                .collect()
        }
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Decimal128Array>()
                .ok_or_else(|| {
                    crate::exec::expr::expression_contract(
                        "if: failed to downcast condition to Decimal128Array",
                    )
                })?;
            (0..len)
                .map(|i| !arr.is_null(i) && arr.value(i) != 0)
                .collect()
        }
        other => {
            return Err(crate::exec::expr::expression_contract(format_args!(
                "if: condition must be boolean or numeric, got {:?}",
                other
            )));
        }
    };

    Ok(BooleanArray::from(values))
}

#[cfg(test)]
mod source_contract_tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::{ExprNode, LiteralValue};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;
    use std::sync::Arc;
    #[test]
    fn raw_if_never_replaces_the_declared_root_type_with_a_branch_type() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "condition",
            DataType::Boolean,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(BooleanArray::from(vec![true, false]))],
        )
        .unwrap();
        let slot = SlotId::new(1);
        let schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot])
                .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, schema);
        let mut arena = ExprArena::default();
        let condition = arena.push_typed(ExprNode::SlotId(slot), DataType::Boolean);
        let yes = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let no = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(0)), DataType::Int64);
        let root = arena.push_typed(
            ExprNode::FunctionCall {
                kind: crate::exec::expr::function::FunctionKind::If,
                args: vec![condition, yes, no],
            },
            DataType::Boolean,
        );
        let result = eval_if(&arena, root, condition, yes, no, &chunk).unwrap();
        assert_eq!(result.data_type(), &DataType::Boolean);
        assert_eq!(
            result.to_data(),
            BooleanArray::from(vec![true, false]).to_data()
        );
    }
}
