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

//! Real lossless Column emission preserves every originating host cause.
use super::*;
use std::num::NonZeroUsize;
struct NoHostTail {
    host: Arc<Host>,
    trace: Mutex<Vec<u32>>,
}
impl KernelEvaluationControl for NoHostTail {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let refused = self
            .host
            .refusal
            .lock()
            .unwrap()
            .as_ref()
            .map(|(stop, _)| *stop);
        if let Some(stop) = refused {
            assert!(
                self.host.ledger.lock().unwrap().attempts <= stop,
                "optional callback after originating host allocation refusal"
            );
        }
        self.trace.lock().unwrap().push(units);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("emission never waits")
    }
}
fn prepared_handle() -> PreparedAggregateHandle {
    let kernel = kernel(
        "percentile_cont",
        FunctionValueType::new(DataType::Int64, false),
        FunctionValueType::new(DataType::Float64, false),
        AggregateKernelPhase::Single,
    );
    assert!(kernel.has_invocation_data());
    PreparedAggregateHandle::from_typed(Arc::new(kernel), &Compile).unwrap()
}
fn real_column(handle: &PreparedAggregateHandle, host: Arc<Host>) -> AggregateStateColumn {
    let mut column =
        AggregateStateColumn::try_new(handle.clone(), host, NonZeroUsize::new(2).unwrap()).unwrap();
    column.push(&Control::default()).unwrap();
    let values = Arc::new(arrow_array::Int64Array::from(vec![1, 4])) as ArrayRef;
    let rates = Arc::new(arrow_array::Float64Array::from(vec![0.5, 0.5])) as ArrayRef;
    let arguments = [
        EvaluatedArgument::Column(&values),
        EvaluatedArgument::Column(&rates),
    ];
    let mapping = [0, 0];
    let contract = column.handle().contract().clone();
    let control = Control::default();
    let input = SelectedAggregateUpdateInput::try_new(
        &contract,
        Selection::all(2),
        &arguments,
        &[],
        &control,
    )
    .unwrap();
    let mut frame = column
        .prepare_update_batch_evaluation(&mapping, input, &control)
        .unwrap();
    frame.run(&control).unwrap();
    assert_eq!(frame.rows_processed(), 2);
    drop(frame);
    column
}
#[test]
fn lossless_actual_column_emit_every_host_allocation_cause_has_no_footer() {
    let handle = prepared_handle();
    let reference = Arc::new(Host::default());
    let column = real_column(&handle, reference.clone());
    let before = reference.ledger.lock().unwrap().attempts;
    let trace = NoHostTail {
        host: reference.clone(),
        trace: Mutex::new(vec![]),
    };
    let failure = column.emit_evaluation(&[0], 1, &trace).unwrap_err();
    let EvaluationFailure::InvocationData(data) = failure else {
        panic!("original value-sensitive final Data")
    };
    assert_eq!(
        data.message(),
        "build aggregate final output: unsupported percentile_cont output type Int64"
    );
    let allocations = reference.ledger.lock().unwrap().attempts - before;
    assert!(
        allocations > 1,
        "actual metadata plus diagnostic and domain backing"
    );
    drop(data);
    drop(column);
    assert_eq!(reference.ledger.lock().unwrap().bytes, 0);
    for cause in causes() {
        for offset in 0..allocations {
            let host = Arc::new(Host::default());
            let column = real_column(&handle, host.clone());
            arm_refusal(&host, offset, cause.clone());
            let control = NoHostTail {
                host: host.clone(),
                trace: Mutex::new(vec![]),
            };
            let actual = column.emit_evaluation(&[0], 1, &control).unwrap_err();
            assert_eq!(actual, EvaluationFailure::Kernel(cause.clone()));
            assert!(!control.trace.lock().unwrap().is_empty());
            drop(actual);
            drop(column);
            assert_eq!(host.ledger.lock().unwrap().bytes, 0);
            assert!(host.ledger.lock().unwrap().live.is_empty());
        }
    }
}
#[test]
fn lossless_actual_column_emit_all_control_causes_and_atomic_data_prefix() {
    let handle = prepared_handle();
    let reference = Arc::new(Host::default());
    let column = real_column(&handle, reference.clone());
    let trace = Control::default();
    let failure = column.emit_evaluation(&[0], 1, &trace).unwrap_err();
    assert!(matches!(failure, EvaluationFailure::InvocationData(_)));
    let callbacks = trace.trace.lock().unwrap().len();
    drop(failure);
    drop(column);
    assert_eq!(reference.ledger.lock().unwrap().bytes, 0);
    for cause in causes() {
        for callback in 0..callbacks {
            let host = Arc::new(Host::default());
            let column = real_column(&handle, host.clone());
            let control = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((callback, cause.clone())),
            };
            let failure = column.emit_evaluation(&[0], 1, &control).unwrap_err();
            assert_eq!(failure, EvaluationFailure::Kernel(cause.clone()));
            assert_eq!(control.trace.lock().unwrap().len(), callback + 1);
            drop(failure);
            drop(column);
            assert_eq!(host.ledger.lock().unwrap().bytes, 0);
        }
    }
    let host = Arc::new(Host::default());
    let column = real_column(&handle, host.clone());
    let control = Control {
        trace: Mutex::new(vec![]),
        refusal: Some((callbacks, KernelFailure::Cancelled)),
    };
    let failure = column.emit_evaluation(&[0], 1, &control).unwrap_err();
    assert!(matches!(failure, EvaluationFailure::InvocationData(_)));
    assert_eq!(control.trace.lock().unwrap().len(), callbacks);
    drop(failure);
    drop(column);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}
