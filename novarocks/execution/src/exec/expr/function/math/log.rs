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
use super::common::{NumericArrayView, value_at_f64};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{ArrayRef, Float64Array};
use std::sync::Arc;

pub fn eval_log(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() == 1 {
        return super::unary_ops::eval_unary_f64(arena, expr, args, chunk, |v| v.ln());
    }
    let left = arena.eval(args[0], chunk)?;
    let right = arena.eval(args[1], chunk)?;
    let left_view = NumericArrayView::new(&left)?;
    let right_view = NumericArrayView::new(&right)?;
    let len = chunk.len();
    let mut values = Vec::with_capacity(len);
    for row in 0..len {
        let base = value_at_f64(&left_view, row, len);
        let v = value_at_f64(&right_view, row, len);
        let out = match (base, v) {
            (Some(b), Some(x)) if b > 0.0 && b != 1.0 && x > 0.0 => Some(x.log(b)),
            _ => None,
        };
        values.push(out);
    }
    let out = Arc::new(Float64Array::from(values)) as ArrayRef;
    super::common::cast_output(out, arena.data_type(expr))
}

#[cfg(test)]
mod legacy_elementary_contract_tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::ExprNode;
    use crate::exec::expr::function::FunctionKind;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    fn evaluate(name: &'static str, inputs: Vec<ArrayRef>) -> Vec<Option<f64>> {
        let fields = inputs
            .iter()
            .enumerate()
            .map(|(i, array)| Field::new(format!("v{i}"), array.data_type().clone(), true))
            .collect::<Vec<_>>();
        let slots = (1..=inputs.len())
            .map(|i| SlotId::new(i as u32))
            .collect::<Vec<_>>();
        let types = inputs
            .iter()
            .map(|array| array.data_type().clone())
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), inputs).unwrap();
        let schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, schema);
        let mut arena = ExprArena::default();
        let args = slots
            .into_iter()
            .zip(types)
            .map(|(slot, ty)| arena.push_typed(ExprNode::SlotId(slot), ty))
            .collect::<Vec<_>>();
        let call = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Math(name),
                args,
            },
            DataType::Float64,
        );
        let frozen = arena.into_immutable().unwrap();
        let result = ExprArena::from_immutable(&frozen)
            .expect("legacy frozen expression fixture")
            .eval(call, &chunk)
            .unwrap();
        assert_eq!(result.data_type(), &DataType::Float64);
        result
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .collect()
    }
    fn floats(values: Vec<Option<f64>>) -> ArrayRef {
        Arc::new(Float64Array::from(values))
    }

    #[test]
    fn legacy_elementary_log_natural_keeps_domain_and_strict_null_results() {
        let result = evaluate(
            "log",
            vec![floats(vec![
                Some(1.0),
                Some(std::f64::consts::E),
                Some(0.0),
                Some(-0.0),
                Some(-1.0),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                Some(f64::NAN),
                None,
            ])],
        );
        assert_eq!(result[0].unwrap().to_bits(), 0);
        assert!((result[1].unwrap() - 1.0).abs() < 1e-15);
        assert_eq!(&result[2..], &[None; 7]);
    }

    #[test]
    fn legacy_elementary_log_two_arguments_are_base_then_value() {
        let result = evaluate(
            "log",
            vec![
                floats(vec![
                    Some(2.0),
                    Some(8.0),
                    Some(1.0),
                    Some(0.0),
                    Some(-2.0),
                    Some(2.0),
                    Some(2.0),
                    None,
                    Some(2.0),
                ]),
                floats(vec![
                    Some(8.0),
                    Some(2.0),
                    Some(8.0),
                    Some(8.0),
                    Some(8.0),
                    Some(0.0),
                    Some(-1.0),
                    Some(8.0),
                    None,
                ]),
            ],
        );
        assert!((result[0].unwrap() - 3.0).abs() < 1e-15);
        assert!((result[1].unwrap() - 1.0 / 3.0).abs() < 1e-15);
        assert_eq!(&result[2..], &[None; 7]);
    }

    #[test]
    fn legacy_elementary_log_infinite_base_computes_before_output_sanitization() {
        let result = evaluate(
            "log",
            vec![
                floats(vec![
                    Some(f64::INFINITY),
                    Some(f64::INFINITY),
                    Some(f64::INFINITY),
                    Some(2.0),
                    Some(0.5),
                    Some(f64::NAN),
                    Some(2.0),
                ]),
                floats(vec![
                    Some(2.0),
                    Some(0.5),
                    Some(f64::INFINITY),
                    Some(f64::INFINITY),
                    Some(1.0),
                    Some(2.0),
                    Some(f64::NAN),
                ]),
            ],
        );
        assert_eq!(result[0].unwrap().to_bits(), 0);
        assert_eq!(result[1].unwrap().to_bits(), 0x8000000000000000);
        assert_eq!(result[2], None);
        assert_eq!(result[3], None);
        assert_eq!(result[4].unwrap().to_bits(), 0x8000000000000000);
        assert_eq!(&result[5..], &[None, None]);
    }
}
