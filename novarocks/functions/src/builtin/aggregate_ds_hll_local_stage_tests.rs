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

//! Typed local phases keep original control causes and owner destruction.
use super::*;
use std::num::NonZeroUsize;
fn handle() -> PreparedAggregateHandle {
    PreparedAggregateHandle::from_typed(
        Arc::new(kernel(
            "ds_hll_count_distinct",
            &[FunctionValueType::new(DataType::Int64, false)],
            AggregateKernelPhase::Single,
        )),
        &Compile,
    )
    .unwrap()
}
// Keep the exhaustive control matrix on an explicitly admitted small sketch.
// The independent native/value oracle retains its original lgK=17 source.
fn control_handle() -> PreparedAggregateHandle {
    PreparedAggregateHandle::from_typed(
        Arc::new(kernel(
            "ds_hll_count_distinct",
            &[
                FunctionValueType::new(DataType::Int64, false),
                FunctionValueType::new(DataType::Int64, false),
                FunctionValueType::new(DataType::Utf8, false),
            ],
            AggregateKernelPhase::Single,
        )),
        &Compile,
    )
    .unwrap()
}
fn partial_run(
    source: &PreparedAggregateHandle,
    control: &Control,
) -> Result<(), EvaluationFailure> {
    let host = Arc::new(Host::default());
    let partial = source.local_stages().unwrap().partial().clone();
    let result;
    {
        let mut states = AggregateStateColumn::try_new(
            partial.clone(),
            host.clone(),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
        states.push(&Control::default()).unwrap();
        let array =
            Arc::new(arrow_array::Int64Array::from_iter_values(1..=321)) as arrow_array::ArrayRef;
        let lg = Arc::new(arrow_array::Int64Array::from(vec![4])) as ArrayRef;
        let target = Arc::new(arrow_array::StringArray::from(vec!["HLL_6"])) as ArrayRef;
        let arguments = [
            EvaluatedArgument::Column(&array),
            EvaluatedArgument::Scalar(&lg),
            EvaluatedArgument::Scalar(&target),
        ];
        let input = SelectedAggregateUpdateInput::try_new(
            partial.contract(),
            Selection::all(321),
            &arguments,
            &[],
            &Control::default(),
        )
        .unwrap();
        let mapping = vec![0; 321];
        result = match states.prepare_update_batch_evaluation(&mapping, input, control) {
            Ok(mut frame) => {
                let result = frame.run(control);
                if result.is_err() {
                    assert!(matches!(
                        frame.run(&Control::default()),
                        Err(EvaluationFailure::Kernel(KernelFailure::InstanceFailed))
                    ));
                }
                result
            }
            Err(error) => Err(error),
        };
    }
    empty(&host);
    result
}
#[test]
fn ds_hll_local_stage_after_all_seven_actual_callback_causes_no_tail_drop() {
    // The original immutable phase recipe is prepared once. Every trial still
    // owns fresh state, allocator ledger, mapping, evaluated carriers and control.
    let mut source = control_handle();
    source.prepare_local_stages(&Compile).unwrap();

    let success = Control::default();
    partial_run(&source, &success).unwrap();
    let callbacks = success.trace.lock().unwrap().len();
    assert!(callbacks > 1);
    for stop in 0..callbacks {
        for cause in causes() {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((stop, cause.clone())),
            };
            assert!(
                matches!(partial_run(&source, &control), Err(EvaluationFailure::Kernel(actual)) if actual == cause)
            );
            assert_eq!(control.trace.lock().unwrap().len(), stop + 1);
        }
    }
}

fn merge_run(source: &PreparedAggregateHandle, control: &Control) -> Result<(), EvaluationFailure> {
    let host = Arc::new(Host::default());
    let stages = source.local_stages().unwrap();
    let partial = stages.partial().clone();
    let final_stage = stages.final_stage().clone();
    let result;
    {
        let mut partial_states = AggregateStateColumn::try_new(
            partial.clone(),
            host.clone(),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
        partial_states.push(&Control::default()).unwrap();
        let array =
            Arc::new(arrow_array::Int64Array::from_iter_values(1..=321)) as arrow_array::ArrayRef;
        let lg = Arc::new(arrow_array::Int64Array::from(vec![4])) as ArrayRef;
        let target = Arc::new(arrow_array::StringArray::from(vec!["HLL_6"])) as ArrayRef;
        let arguments = [
            EvaluatedArgument::Column(&array),
            EvaluatedArgument::Scalar(&lg),
            EvaluatedArgument::Scalar(&target),
        ];
        let input = SelectedAggregateUpdateInput::try_new(
            partial.contract(),
            Selection::all(321),
            &arguments,
            &[],
            &Control::default(),
        )
        .unwrap();
        let mapping = vec![0; 321];
        partial_states
            .prepare_update_batch_evaluation(&mapping, input, &Control::default())
            .unwrap()
            .run(&Control::default())
            .unwrap();
        let payload = partial_states
            .emit_evaluation(&[0], 1, &Control::default())
            .unwrap();
        let mut states = AggregateStateColumn::try_new(
            final_stage.clone(),
            host.clone(),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
        states.push(&Control::default()).unwrap();
        let input = SelectedAggregateMergeInput::try_new(
            final_stage.contract(),
            Selection::all(1),
            EvaluatedArgument::Column(&payload),
            &Control::default(),
        )
        .unwrap();
        result = match states.prepare_merge_batch_evaluation(&[0], input, control) {
            Ok(mut frame) => {
                let result = frame.run(control);
                if result.is_err() {
                    assert!(matches!(
                        frame.run(&Control::default()),
                        Err(EvaluationFailure::Kernel(KernelFailure::InstanceFailed))
                    ));
                }
                result
            }
            Err(error) => Err(error),
        };
    }
    empty(&host);
    result
}
#[test]
fn ds_hll_local_stage_after_final_merge_all_seven_callback_causes_no_tail_drop() {
    // The original immutable phase recipe is prepared once. Every trial still
    // owns fresh state, allocator ledger, mapping, evaluated carriers and control.
    let mut source = control_handle();
    source.prepare_local_stages(&Compile).unwrap();

    let success = Control::default();
    merge_run(&source, &success).unwrap();
    let callbacks = success.trace.lock().unwrap().len();
    assert!(callbacks > 1);
    for stop in 0..callbacks {
        for cause in causes() {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((stop, cause.clone())),
            };
            assert!(
                matches!(merge_run(&source, &control), Err(EvaluationFailure::Kernel(actual)) if actual == cause)
            );
            assert_eq!(control.trace.lock().unwrap().len(), stop + 1);
        }
    }
}
#[test]
fn ds_hll_local_stage_after_actual_state_allocation_causes_and_release() {
    for cause in causes() {
        for final_stage in [false, true] {
            let host = Arc::new(Host::default());
            let mut original = handle();
            original.prepare_local_stages(&Compile).unwrap();
            let stages = original.local_stages().unwrap();
            let phase = if final_stage {
                stages.final_stage()
            } else {
                stages.partial()
            };
            {
                let mut column = AggregateStateColumn::try_new(
                    phase.clone(),
                    host.clone(),
                    NonZeroUsize::new(1).unwrap(),
                )
                .unwrap();
                arm_refusal(&host, 0, cause.clone());
                assert_eq!(column.push(&Control::default()).unwrap_err(), cause);
                assert_eq!(column.len(), 0);
            }
            empty(&host);
        }
    }
}
struct PhaseCompile {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for PhaseCompile {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let ordinal = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(ordinal <= stop);
        }
        trace.push(units);
        match self.refusal {
            Some((stop, cause)) if stop == ordinal => Err(cause),
            _ => Ok(()),
        }
    }
}
#[test]
fn ds_hll_local_stage_after_three_compile_causes_never_publish_partial_recipe() {
    let control = PhaseCompile {
        trace: Mutex::default(),
        refusal: None,
    };
    handle().prepare_local_stages(&control).unwrap();
    let calls = control.trace.lock().unwrap().len();
    for stop in 0..calls {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = PhaseCompile {
                trace: Mutex::default(),
                refusal: Some((stop, cause)),
            };
            let mut source = handle();
            let original = source.contract().clone();
            assert_eq!(
                source.prepare_local_stages(&control).unwrap_err(),
                compile_failure(cause)
            );
            assert!(source.local_stages().is_none());
            assert!(Arc::ptr_eq(source.contract(), &original));
            assert_eq!(source.contract().phase(), AggregateKernelPhase::Single);
            assert_eq!(control.trace.lock().unwrap().len(), stop + 1);
        }
    }
}

#[test]
fn ds_hll_local_stage_after_final_data_exact_identity_domain_and_no_replay() {
    let host = Arc::new(Host::default());
    let mut source = handle();
    source.prepare_local_stages(&Compile).unwrap();
    let final_stage = source.local_stages().unwrap().final_stage().clone();
    let control = Control::default();
    let failure;
    {
        let mut column = AggregateStateColumn::try_new(
            final_stage.clone(),
            host.clone(),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
        column.push(&control).unwrap();
        let values = Arc::new(arrow_array::BinaryArray::from_iter_values([
            &[][..],
            &[1u8, 2, 3][..],
            &[][..],
        ])) as ArrayRef;
        let rows = [1];
        let selection = Selection::try_sparse(3, &rows).unwrap();
        let input = SelectedAggregateMergeInput::try_new(
            final_stage.contract(),
            selection,
            EvaluatedArgument::Column(&values),
            &control,
        )
        .unwrap();
        let mapping = [0];
        let mut frame = column
            .prepare_merge_batch_evaluation(&mapping, input, &control)
            .unwrap();
        failure = frame.run(&control).unwrap_err();
        let callbacks = control.trace.lock().unwrap().len();
        assert_eq!(
            frame.run(&control).unwrap_err(),
            EvaluationFailure::Kernel(KernelFailure::InstanceFailed)
        );
        assert_eq!(control.trace.lock().unwrap().len(), callbacks);
    }
    let EvaluationFailure::InvocationData(data) = failure else {
        panic!("actual full invocation Data");
    };
    assert_eq!(
        data.message(),
        "merge aggregate state: ds_hll preflight: HLL payload requires 8 bytes, got 3"
    );
    assert_eq!(data.input_rows(), &[1]);
    assert_eq!(data.state_indices(), &[0]);
    assert_eq!(data.aggregate_phase(), AggregateInvocationPhase::Merge);
    assert_eq!(data.aggregate_call_phase(), AggregateKernelPhase::Final);
    assert!(std::ptr::eq(
        data.aggregate_contract(),
        final_stage.contract().as_ref()
    ));
    assert!(Arc::ptr_eq(
        data.aggregate_contract().call(),
        source.contract().call()
    ));
    assert!(
        host.ledger.lock().unwrap().bytes > 0,
        "real diagnostic backing survives state destruction"
    );
    drop(data);
    empty(&host);
}
