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

use super::*;
use crate::exec::pipeline::builder::{unpivot_program, unpivot_rows};
use crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory;
use crate::runtime::fragment::ExecutionFailureCause;
use arrow::array::Int64Array;

fn runtime_factory(
    tracker: &Arc<MemTracker>,
    error: &Arc<RuntimeErrorState>,
) -> Arc<dyn InputConstructionFactory> {
    try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        tracker.clone(),
        error.clone(),
    )
    .unwrap()
}

/// The fixture is authored against the real compiled input declaration. All
/// newly made BE Vec/primitive wrappers use the same real creating host.
fn input(
    program: &LocalProgram,
    factory: &dyn InputConstructionFactory,
    error: Arc<RuntimeErrorState>,
) -> Chunk {
    let schema = factory
        .create_compiled_chunk_schema(program.graph().nodes()[0].output_layout())
        .unwrap();
    let owner = factory.begin_typed_construction(&schema).unwrap();
    let control = RuntimeKernelControl::new(error);
    let mut columns = unpivot_vec::<ArrayRef>(3, owner.as_ref()).unwrap();
    for values in [[1, 2, 3], [10, 20, 30], [11, 21, 31]] {
        let mut owned = unpivot_vec::<i64>(3, owner.as_ref()).unwrap();
        owned.extend_from_slice(&values);
        primitive_adoption(owner.as_ref(), &control).unwrap();
        request_arc::<Int64Array>(owner.as_ref()).unwrap();
        columns.push(Arc::new(Int64Array::from(owned)) as ArrayRef);
    }
    Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
        schema,
        columns,
        3,
        owner.storage_lifetime(),
        |step| owner.observe(0, &[], step),
    )
    .unwrap()
}

fn prepared(
    program: Arc<LocalProgram>,
    factory: Arc<dyn InputConstructionFactory>,
    error: Arc<RuntimeErrorState>,
    tracker: Arc<MemTracker>,
    state: &RuntimeState,
) -> Box<dyn Operator> {
    let declaration =
        CompiledUnpivotProcessorFactory::try_new(program, ProgramNodeId::new(1), error).unwrap();
    let mut operator = declaration.create(1, 0);
    operator.set_mem_tracker(tracker);
    operator.set_construction_factory(factory);
    operator.bind_runtime_state(state).unwrap();
    operator.prepare().unwrap();
    operator
}

#[test]
fn compiled_unpivot_output_and_slot_retain_actual_creator_after_operator_exit() {
    let program = unpivot_program();
    let program_weak = Arc::downgrade(&program);
    let tracker = MemTracker::new_root("compiled Unpivot last output owner");
    let state = RuntimeState::new(None, None, None, None, None, None, None);
    let error = state.error_state();
    let error_weak = Arc::downgrade(&error);
    let factory = runtime_factory(&tracker, &error);
    let source = input(&program, factory.as_ref(), error.clone());
    let mut operator = prepared(
        program.clone(),
        factory.clone(),
        error.clone(),
        tracker.clone(),
        &state,
    );
    operator
        .as_processor_mut()
        .unwrap()
        .push_chunk(&state, source)
        .unwrap();
    let output = operator
        .as_processor_mut()
        .unwrap()
        .pull_chunk(&state)
        .unwrap()
        .unwrap();
    assert_eq!(output.len(), 2);
    let rows = unpivot_rows(std::slice::from_ref(&output));
    assert_eq!(
        rows[0],
        (
            1,
            Some(10),
            "first".to_owned(),
            vec![7, 8],
            vec![("a".to_owned(), "x".to_owned())]
        )
    );
    assert_eq!(rows[1].1, Some(20));
    let projection = output.batch_projection();
    let slot = output.chunk_schema().slots()[0].clone();
    drop(output);
    drop(operator);
    drop(factory);
    drop(program);
    drop(error);
    drop(state);
    assert!(
        program_weak.upgrade().is_none(),
        "output does not retain the LocalProgram"
    );
    assert!(
        error_weak.upgrade().is_none(),
        "output does not retain runtime cancellation control"
    );
    assert!(
        tracker.current() > 0,
        "the actual output wrapper responsibility remains"
    );
    drop(projection);
    assert!(
        tracker.current() > 0,
        "the independent last schema slot retains its actual creator"
    );
    drop(slot);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn compiled_unpivot_candidate_refusal_preserves_typed_limit_and_original_input() {
    let program = unpivot_program();
    let tracker = MemTracker::new_root("compiled Unpivot candidate refusal");
    let state = RuntimeState::new(None, None, None, None, None, None, None);
    let error = state.error_state();
    let factory = runtime_factory(&tracker, &error);
    let source = input(&program, factory.as_ref(), error.clone());
    let source_projection = source.batch_projection();
    let mut operator = prepared(
        program.clone(),
        factory.clone(),
        error.clone(),
        tracker.clone(),
        &state,
    );
    operator
        .as_processor_mut()
        .unwrap()
        .push_chunk(&state, source)
        .unwrap();
    let before = tracker.current();
    tracker.install_limit_once(before + 1).unwrap();
    let failure = operator
        .as_processor_mut()
        .unwrap()
        .pull_chunk(&state)
        .unwrap_err();
    assert!(
        matches!(
            failure.cause(),
            ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))
        ),
        "{failure:?}"
    );
    assert_eq!(
        tracker.current(),
        before,
        "refused new construction rolls back without resetting old holds"
    );
    assert!(
        operator.as_processor_ref().unwrap().has_output(),
        "the original expansion remains available"
    );
    let ids = source_projection
        .record_batch()
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values().as_ref(), &[1, 2, 3]);
    drop(operator);
    drop(factory);
    drop(error);
    drop(state);
    drop(program);
    drop(source_projection);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn compiled_unpivot_cancel_preserves_original_caller_cause_before_candidate_creation() {
    let program = unpivot_program();
    let tracker = MemTracker::new_root("compiled Unpivot original cancellation cause");
    let state = RuntimeState::new(None, None, None, None, None, None, None);
    let error = state.error_state();
    let factory = runtime_factory(&tracker, &error);
    let source = input(&program, factory.as_ref(), error.clone());
    let mut operator = prepared(
        program.clone(),
        factory.clone(),
        error.clone(),
        tracker.clone(),
        &state,
    );
    operator
        .as_processor_mut()
        .unwrap()
        .push_chunk(&state, source)
        .unwrap();
    let before = tracker.current();
    let original = ExecutionFailure::from("original Unpivot caller shutdown");
    error.set_error(original.clone());
    let observed = operator
        .as_processor_mut()
        .unwrap()
        .pull_chunk(&state)
        .unwrap_err();
    assert_eq!(observed, original);
    assert_eq!(tracker.current(), before);
    drop(operator);
    drop(factory);
    drop(error);
    drop(state);
    drop(program);
    assert_eq!(tracker.current(), 0);
}
