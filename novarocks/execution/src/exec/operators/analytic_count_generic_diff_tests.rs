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

//! Full original COUNT OVER frame author versus actual generic/pool-address input.
use super::original_count_window_diff_tests::{C, compare, prepare};
use super::*;
use arrow::array::Int64Array;
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::pure_differential::aggregate_count_generic_diff_tests::{arrays, pool};
use crate::exec::expr::{ExprArena, ExprNode};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use novarocks_functions as f;
#[test]
fn pure_differential_count_over_original_any_generic_physical_carriers() {
    for array in arrays() {
        for source in [array.clone(), array.slice(1, 3), array.slice(2, 0)] {
            let n = source.len();
            compare(Some(source), n);
        }
    }
}
#[test]
fn pure_differential_count_over_original_any_nonzero_constant_pool_ordinal() {
    for source in arrays() {
        for ordinal in [1, 4] {
            let value = pool(source.clone(), ordinal);
            for n in [0, 1, 5, 319] {
                let mut arena = ExprArena::default();
                let id = arena.push_typed(
                    ExprNode::Constant(value.clone()),
                    value.value_type().data_type.clone(),
                );
                let batch = RecordBatch::try_new_with_options(
                    Arc::new(arrow::datatypes::Schema::empty()),
                    vec![],
                    &RecordBatchOptions::new().with_row_count(Some(n)),
                )
                .unwrap();
                let schema =
                    ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[])
                        .unwrap();
                let chunk = Chunk::new_with_chunk_schema(batch, schema);
                let materialized = arena.eval(id, &chunk).unwrap();
                for running in [false, true] {
                    let window = running.then_some(WindowFrame {
                        start: Some(WindowBoundary::Preceding(1)),
                        end: Some(WindowBoundary::CurrentRow),
                        window_type: WindowType::Rows,
                    });
                    let partitions = if n == 0 { vec![] } else { vec![(0, n)] };
                    let ctx =
                        PartitionWindowContext::new(&partitions, &[], window.as_ref()).unwrap();
                    let old = compute_count(&[materialized.clone()], &ctx, n).unwrap();
                    let old = old.as_any().downcast_ref::<Int64Array>().unwrap();
                    let prepared = prepare(Some(&materialized), running, false);
                    if n == 0 {
                        continue;
                    }
                    let args = [f::EvaluatedArgument::Constant(&value)];
                    let peers = ctx
                        .peer_groups(0)
                        .unwrap()
                        .iter()
                        .map(|(s, e)| f::WindowRowRange { start: *s, end: *e })
                        .collect::<Vec<_>>();
                    let frames = ctx
                        .frames(0)
                        .unwrap()
                        .iter()
                        .map(|(s, e)| f::WindowRowRange { start: *s, end: *e })
                        .collect::<Vec<_>>();
                    let full = f::FullPartitionWindowInput::try_new(
                        prepared.contract(),
                        n,
                        &args,
                        &[],
                        &C,
                    )
                    .unwrap();
                    let input =
                        f::WindowPartitionInput::try_new(full, &peers, &frames, &C).unwrap();
                    let mut evaluator =
                        f::WindowEvaluationPartition::begin(prepared.clone(), input, &C).unwrap();
                    let rows = (0..n).filter(|r| r % 2 == 0).collect::<Vec<_>>();
                    let selection = f::Selection::try_sparse(n, &rows).unwrap();
                    let result = evaluator.evaluate(selection, n, &C).unwrap();
                    assert!(result.errors().is_empty());
                    let result = result
                        .values()
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    for (i, row) in rows.iter().copied().enumerate() {
                        assert_eq!(result.value(i), old.value(row));
                    }
                }
            }
        }
    }
}
