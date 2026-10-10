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

//! Actual LocalCompiler window binding, runtime allocator, and output custody.
//! The old FixedZero driver helper and its oracle remain untouched.
use super::*;
use super::super::window_fixture::window_catalog_from_actual;
use novarocks_functions::FunctionKind;
use crate::runtime::fragment::ExecutionFailureCause;
use crate::runtime::mem_tracker::MemTracker;

fn source(rows: &[Row], running: bool) -> (Arc<LocalProgram>, RecordBatch) {
    use WindowBound::{CurrentRow, UnboundedPreceding};
    let shape = Shape {
        partition: vec![],
        order: vec![key(O, true, true)],
        calls: ["max_by", "min_by"]
            .into_iter()
            .map(|name| {
                let call = Call::aggregate(name, &[Arg::Column(X), Arg::Column(O)]);
                if running {
                    call.framed(WindowFrameUnits::Rows, UnboundedPreceding, CurrentRow)
                } else {
                    call.framed(
                        WindowFrameUnits::Rows,
                        UnboundedPreceding,
                        WindowBound::UnboundedFollowing,
                    )
                }
            })
            .collect(),
    };
    let actual = novarocks_functions::builtin::catalogue::by_window_private_test_catalog();
    let catalog = window_catalog_from_actual(
        &[
            ("max_by", FunctionKind::Aggregate),
            ("min_by", FunctionKind::Aggregate),
        ],
        &actual,
    );
    let program = compile(package(rows, 3, &shape, &catalog, 1), &catalog, 1);
    let (_, schema) = analytic(&program);
    let batch = sorted_input(rows, &shape, schema);
    (program, batch)
}
fn operator(
    program: &Arc<LocalProgram>,
    tracker: Option<Arc<MemTracker>>,
) -> Box<dyn crate::exec::pipeline::operator::Operator> {
    let (node, _) = analytic(program);
    let factory = CompiledWindowProcessorFactory::try_new(
        Arc::clone(program),
        node,
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let mut operator = factory.create(1, 0);
    if let Some(tracker) = tracker {
        operator.set_mem_tracker(tracker);
    }
    operator
}
fn chunk(program: &LocalProgram, batch: RecordBatch) -> Chunk {
    let (node, _) = analytic(program);
    let ProgramNodeKind::Analytic { input, .. } = program.graph().nodes()[node.index()].kind()
    else {
        unreachable!()
    };
    let schema =
        ChunkSchema::from_compiled_layout(program.graph().nodes()[input.index()].output_layout())
            .unwrap();
    Chunk::try_new_with_chunk_schema(batch, schema).unwrap()
}
#[test]
fn by_window_compiled_actual_owner_frame_null_winner_and_last_array_drop() {
    let rows = vec![
        vec![Some(1), Some(1), Some(4)],
        vec![Some(1), Some(2), None],
        vec![Some(1), Some(3), Some(9)],
    ];
    for running in [false, true] {
        let (program, batch) = source(&rows, running);
        for step in [1, 2, 3] {
            let tracker = MemTracker::new_root("actual tracked BY window");
            tracker.install_limit_once(64 * 1024 * 1024).unwrap();
            let mut operator = operator(&program, Some(Arc::clone(&tracker)));
            let processor = operator.as_processor_mut().unwrap();
            let state = RuntimeState::default();
            for offset in (0..3).step_by(step) {
                processor
                    .push_chunk(
                        &state,
                        chunk(&program, batch.slice(offset, step.min(3 - offset))),
                    )
                    .unwrap();
            }
            processor.set_finishing(&state).unwrap();
            let mut outputs = Vec::new();
            while let Some(output) = processor.pull_chunk(&state).unwrap() {
                outputs.push(output);
            }
            assert_eq!(
                outputs.iter().map(Chunk::len).collect::<Vec<_>>(),
                (0..3)
                    .step_by(step)
                    .map(|offset| step.min(3 - offset))
                    .collect::<Vec<_>>()
            );
            let values = |column: usize| {
                outputs
                    .iter()
                    .flat_map(|output| {
                        output
                            .batch
                            .column(column)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .iter()
                    })
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                values(3),
                if running {
                    vec![Some(4), None, Some(9)]
                } else {
                    vec![Some(9); 3]
                }
            );
            assert_eq!(values(4), vec![Some(4); 3]);
            let output = outputs.pop().unwrap();
            drop(outputs);
            let data = output.batch.column(3).to_data();
            let payload = data.buffers()[0].clone();
            drop(operator);
            drop(output);
            drop(data);
            assert!(
                tracker.current() > 0,
                "actual Arrow buffer loan retains admitted payload"
            );
            drop(payload);
            assert_eq!(tracker.current(), 0);
            assert!(tracker.peak() > 0);
        }
    }
}
#[test]
fn by_window_compiled_actual_missing_and_refused_allocator_latch_no_prefix_replay() {
    let (program, batch) = source(&[vec![Some(1), Some(1), Some(4)]], false);
    for install in [false, true] {
        let tracker = MemTracker::new_root("refused actual BY window");
        tracker.install_limit_once(1).unwrap();
        let mut operator = operator(&program, install.then(|| tracker.clone()));
        let processor = operator.as_processor_mut().unwrap();
        let state = RuntimeState::default();
        // Structural input ownership now obtains its real host grant at the
        // push stage. Keep the first actual refusal wherever it is authored.
        let first = match processor.push_chunk(&state, chunk(&program, batch.clone())) {
            Err(first) => first,
            Ok(()) => processor.set_finishing(&state).unwrap_err(),
        };
        match first.cause() {
            ExecutionFailureCause::Kernel(
                novarocks_functions::KernelFailure::ResourceExhausted,
            ) if install => {}
            ExecutionFailureCause::Kernel(novarocks_functions::KernelFailure::InvalidProgram(
                _,
            )) if !install => {}
            other => panic!("exact actual host refusal: {other:?}"),
        }
        assert!(!processor.need_input());
        assert!(!processor.has_output());
        assert_eq!(processor.pull_chunk(&state).unwrap_err(), first);
        assert_eq!(processor.set_finishing(&state).unwrap_err(), first);
        assert_eq!(
            processor
                .push_chunk(&state, chunk(&program, batch.clone()))
                .unwrap_err(),
            first
        );
        drop(operator);
        assert_eq!(tracker.current(), 0);
    }
}
#[test]
fn by_window_compiled_original_output_validator_keeps_full_text_and_check_order() {
    use crate::exec::chunk::{ChunkSlotSchema, ChunkSchema};
    use crate::exec::operators::analytic_shared::validate_analytic_output_columns_typed;
    use arrow::datatypes::{Field, DataType};
    use novarocks_types::SlotId;
    let schema = Arc::new(
        ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
            SlotId::new(7),
            Field::new("original", DataType::Int64, true),
            None,
            None,
        )])
        .unwrap(),
    );
    let values: Vec<ArrayRef> = vec![Arc::new(arrow::array::Int32Array::from(vec![1, 2]))];
    let error = validate_analytic_output_columns_typed(&values, &schema, 3).unwrap_err();
    assert_eq!(error.output_ordinal(), Some(0));
    assert_eq!(
        error.to_string(),
        "analytic output length mismatch at column 0: expected_rows=3 actual=2"
    );
    let error = validate_analytic_output_columns_typed(&values, &schema, 2).unwrap_err();
    assert_eq!(
        error.to_string(),
        "analytic output type mismatch at column 0: descriptor=Int64 actual=Int32"
    );
    let error = validate_analytic_output_columns_typed(&[], &schema, 2).unwrap_err();
    assert_eq!(error.output_ordinal(), None);
    assert_eq!(
        error.to_string(),
        "analytic output column count mismatch: descriptor=1 actual=0"
    );
}

#[path = "compiled_window_source_custody_tests.rs"]
mod source_custody_tests;
