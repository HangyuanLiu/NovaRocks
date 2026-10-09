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
use arrow_array::{ArrayRef, Int32Array};
use arrow_schema::Field;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
}
impl crate::KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
        self.trace.lock().unwrap().push(units);
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("source geometry does not wait")
    }
}
fn source() -> StructArray {
    let child = Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef;
    StructArray::from(
        (0..321)
            .map(|ordinal| {
                (
                    Arc::new(Field::new(
                        format!("field_{ordinal}"),
                        DataType::Int32,
                        true,
                    )),
                    child.clone(),
                )
            })
            .collect::<Vec<_>>(),
    )
}
struct Observe {
    nodes: usize,
    stop: Option<(usize, KernelFailure)>,
}
impl<'source> BorrowedSourceObservation<'source> for Observe {
    fn node(
        &mut self,
        node: BorrowedSourceNode<'source>,
        _: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        let ordinal = self.nodes;
        self.nodes += 1;
        if let Some((stop, cause)) = &self.stop {
            assert!(ordinal <= *stop, "source callback after first failure");
            if ordinal == *stop {
                return Err(cause.clone());
            }
        }
        if matches!(node.array.data_type(), DataType::Struct(_)) {
            assert_eq!(node.child_count, 321);
            assert_eq!(node.data_buffers, 0);
        } else {
            assert_eq!(node.child_count, 0);
            assert_eq!(node.data_buffers, 1);
        }
        assert!(node.copy_nulls);
        Ok(())
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        crate::kernel_control::invalid("source observation invalid"),
        crate::kernel_control::internal("source observation internal"),
        KernelFailure::Operational(crate::KernelDiagnostic::new(
            "source observation operational",
        )),
        KernelFailure::InstanceFailed,
    ]
}
#[test]
fn array_cast_source_observation_same_author_has_no_default_work_or_footer() {
    let array = source();
    let legacy = Control::default();
    let mut work = EvaluationCheckpoints::new(&legacy);
    let before = copy_metadata_bytes(&array, &mut work).unwrap();
    work.finish().unwrap();
    let original_trace = legacy.trace.lock().unwrap().clone();
    assert!(original_trace.contains(&256));
    let observed = Control::default();
    let mut work = EvaluationCheckpoints::new(&observed);
    let mut observer = Observe {
        nodes: 0,
        stop: None,
    };
    let after = observe_source_metadata(&array, true, &mut work, &mut observer).unwrap();
    work.finish().unwrap();
    assert_eq!(before, after);
    assert_eq!(observer.nodes, 322);
    assert_eq!(*observed.trace.lock().unwrap(), original_trace);
}
#[test]
fn array_cast_source_observation_every_failure_is_immediate_and_unwrapped() {
    let array = source();
    for stop in [0, 1, 255, 256, 320, 321] {
        for cause in causes() {
            let control = Control::default();
            let mut work = EvaluationCheckpoints::new(&control);
            let mut observer = Observe {
                nodes: 0,
                stop: Some((stop, cause.clone())),
            };
            assert_eq!(
                observe_source_metadata(&array, true, &mut work, &mut observer),
                Err(cause)
            );
            assert_eq!(observer.nodes, stop + 1);
            // No finish/footer is called after the first callback failure.
            let trace = control.trace.lock().unwrap();
            assert!(trace.iter().all(|units| *units == 256));
        }
    }
}
