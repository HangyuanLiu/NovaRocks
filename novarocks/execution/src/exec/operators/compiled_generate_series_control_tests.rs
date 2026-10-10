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
//! Processor lifecycle probes, independent of compile/source transport tests.
use super::*;
use crate::runtime::fragment::ExecutionFailureCause;
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use novarocks_types::SlotId;
use std::sync::Mutex;

struct Control {
    trace: Mutex<Vec<u32>>,
    fail_at: Option<usize>,
    cause: KernelFailure,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        trace.push(units);
        if self.fail_at == Some(at) {
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("integer series has no wait")
    }
}
fn instance(values: [i64; 3], control: Arc<Control>) -> (CompiledGenerateSeriesProcessor, Chunk) {
    let input = Arc::new(Schema::new(
        (0..3)
            .map(|ordinal| Field::new(format!("original-{ordinal}"), DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    let input_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
        input.as_ref(),
        &[SlotId::new(81), SlotId::new(6), SlotId::new(900)],
    )
    .unwrap();
    let output = ChunkSchema::try_ref_from_schema_and_slot_ids(
        &Schema::new(vec![Field::new("public-series", DataType::Int64, false)]),
        &[SlotId::new(71)],
    )
    .unwrap();
    let arrays = values
        .into_iter()
        .map(|value| Arc::new(Int64Array::from(vec![value])) as arrow::array::ArrayRef)
        .collect::<Vec<_>>();
    let chunk = Chunk::try_new_with_chunk_schema(
        RecordBatch::try_new(Arc::clone(&input), arrays).unwrap(),
        input_schema,
    )
    .unwrap();
    let processor = CompiledGenerateSeriesProcessor {
        name: "control-fixture".to_owned(),
        input,
        parameters: Arc::from([0, 1, 2]),
        output,
        control,
        pending: None,
        offset: 0,
        accepted: false,
        finishing: false,
        failed: false,
    };
    (processor, chunk)
}
fn control(fail_at: Option<usize>, cause: KernelFailure) -> Arc<Control> {
    Arc::new(Control {
        trace: Mutex::new(vec![]),
        fail_at,
        cause,
    })
}
#[test]
fn compiled_generate_series_control_seven_causes_exact_prefix_no_tail_and_failed_latch() {
    let state = RuntimeState::default();
    let values = [-600, 600, 1];
    let baseline = control(None, KernelFailure::Cancelled);
    let (mut original, chunk) = instance(values, Arc::clone(&baseline));
    original.push_chunk(&state, chunk).unwrap();
    let trace = baseline.trace.lock().unwrap().clone();
    assert!(trace.contains(&MAX_UNOBSERVED_KERNEL_WORK));
    assert!(
        trace
            .iter()
            .all(|units| *units <= MAX_UNOBSERVED_KERNEL_WORK)
    );
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("injected")),
        KernelFailure::Internal(KernelDiagnostic::new("injected")),
        KernelFailure::Operational(KernelDiagnostic::new("injected")),
        KernelFailure::InstanceFailed,
    ] {
        for at in 0..trace.len() {
            let injection = control(Some(at), cause.clone());
            let (mut processor, chunk) = instance(values, Arc::clone(&injection));
            let error = processor.push_chunk(&state, chunk.clone()).unwrap_err();
            assert_eq!(error.cause(), &ExecutionFailureCause::Kernel(cause.clone()));
            assert_eq!(*injection.trace.lock().unwrap(), trace[..=at]);
            assert!(!processor.has_output());
            assert!(processor.is_finished());
            let before = injection.trace.lock().unwrap().len();
            for result in [
                processor.push_chunk(&state, chunk),
                processor.set_finishing(&state),
            ] {
                assert_eq!(
                    result.unwrap_err().cause(),
                    &ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed)
                );
            }
            assert_eq!(
                processor.pull_chunk(&state).unwrap_err().cause(),
                &ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed)
            );
            assert_eq!(injection.trace.lock().unwrap().len(), before);
        }
    }
}
#[test]
fn compiled_generate_series_control_original_whole_data_has_no_late_footer_or_row_relabel() {
    let state = RuntimeState::default();
    let baseline = control(None, KernelFailure::Cancelled);
    let (mut processor, chunk) = instance([1, 3, 0], Arc::clone(&baseline));
    let original = processor.push_chunk(&state, chunk).unwrap_err();
    assert_eq!(
        original.cause(),
        &ExecutionFailureCause::Pipeline(
            "table function generate_series step size cannot equal zero".to_owned()
        )
    );
    let trace = baseline.trace.lock().unwrap().clone();
    let next = control(Some(trace.len()), KernelFailure::DeadlineExceeded);
    let (mut processor, chunk) = instance([1, 3, 0], Arc::clone(&next));
    assert_eq!(processor.push_chunk(&state, chunk).unwrap_err(), original);
    assert_eq!(*next.trace.lock().unwrap(), trace);
    assert_eq!(
        processor.pull_chunk(&state).unwrap_err().cause(),
        &ExecutionFailureCause::Kernel(KernelFailure::InstanceFailed)
    );
    assert_eq!(*next.trace.lock().unwrap(), trace);
}
#[test]
fn compiled_generate_series_control_finishing_owes_exact_output_then_latches_finished() {
    let state = RuntimeState::default();
    let control = control(None, KernelFailure::Cancelled);
    let (mut processor, chunk) = instance([1, 9, 2], control);
    assert!(processor.need_input());
    processor.push_chunk(&state, chunk).unwrap();
    assert!(!processor.need_input());
    processor.set_finishing(&state).unwrap();
    assert_eq!(processor.finishing_wait(), FinishingWait::OwedOutput);
    assert!(!processor.is_finished());
    let out = processor.pull_chunk(&state).unwrap().unwrap();
    assert_eq!(
        out.column_by_slot_id(SlotId::new(71))
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[1, 3, 5, 7, 9]
    );
    assert!(!out.schema().field(0).is_nullable());
    assert_eq!(out.schema().field(0).name(), "public-series");
    assert!(processor.is_finished());
    assert_eq!(processor.finishing_wait(), FinishingWait::Complete);
    assert!(processor.pull_chunk(&state).unwrap().is_none());
}
