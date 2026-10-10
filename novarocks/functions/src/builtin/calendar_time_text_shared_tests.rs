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
//! Direct shared-core control goldens. Real owner/compiled tests remain required.
use super::calendar_time_text_shared::*;
use crate::{EvaluationCheckpoints, KernelDiagnostic, KernelEvaluationControl, KernelFailure};
use arrow_array::{Array, ArrayRef, StringArray};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
struct Control {
    seen: Mutex<Vec<u32>>,
    refuse: Option<usize>,
    cause: KernelFailure,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        let mut seen = self.seen.lock().unwrap();
        let at = seen.len();
        seen.push(n);
        if self.refuse == Some(at) {
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("TIME core must not wait")
    }
}
fn exercise(work: &mut EvaluationCheckpoints<'_>) -> Result<(), TimeTextError> {
    let source = StringArray::from(vec![Some("12:34:56"), None, Some("01:02:03.5")]);
    let values = clock_strings(&source, work)?;
    let pattern = "%f/é中/%H%%/%q".repeat(31);
    let format = StringArray::from(vec![pattern.as_str()]);
    let _ = format_output(&values, &format, source.len(), work)?;
    let duration = StringArray::from(vec![Some("27:02:03"), None]);
    let mut values = duration_strings(&duration, work)?;
    merge_source(&mut values, &[Some(-1), Some(5)], MergeMode::FillNull, work)?;
    let _ = any_null(&values, work)?;
    let _ = seconds_output(values, work)?;
    Ok(())
}
#[test]
fn shared_time_core_refusal_at_every_actual_callback_preserves_seven_causes_and_latch() {
    let normal = Control {
        seen: Mutex::new(vec![]),
        refuse: None,
        cause: KernelFailure::Cancelled,
    };
    let mut work = EvaluationCheckpoints::new(&normal);
    exercise(&mut work).unwrap();
    work.finish().unwrap();
    let calls = normal.seen.lock().unwrap().len();
    assert!(calls > 10);
    let causes = [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("program origin")),
        KernelFailure::Internal(KernelDiagnostic::new("internal origin")),
        KernelFailure::Operational(KernelDiagnostic::new("operational origin")),
        KernelFailure::InstanceFailed,
    ];
    for cause in causes {
        for at in 0..calls {
            let control = Control {
                seen: Mutex::new(vec![]),
                refuse: Some(at),
                cause: cause.clone(),
            };
            let mut work = EvaluationCheckpoints::new(&control);
            let error = exercise(&mut work)
                .and_then(|_| work.flush().map_err(TimeTextError::from))
                .unwrap_err();
            assert!(matches!(error,TimeTextError::Kernel(ref actual) if actual==&cause));
            let before = control.seen.lock().unwrap().len();
            assert_eq!(work.step().unwrap_err(), cause);
            assert_eq!(work.flush().unwrap_err(), cause);
            assert_eq!(control.seen.lock().unwrap().len(), before);
        }
    }
}
#[test]
fn shared_time_core_keeps_long_raw_carrier_error() {
    let dtype = arrow_schema::DataType::Timestamp(
        arrow_schema::TimeUnit::Microsecond,
        Some(Arc::<str>::from("z".repeat(800))),
    );
    // Unsupported nested carrier diagnostics are kept before the pure boundary.
    let field = Arc::new(arrow_schema::Field::new("field".repeat(150), dtype, true));
    let dtype = arrow_schema::DataType::List(field);
    let array = arrow_array::new_null_array(&dtype, 1);
    let mut work = EvaluationCheckpoints::new(&LegacyControl);
    let error = datetime_seconds(&array, &mut work)
        .unwrap_err()
        .into_legacy();
    assert_eq!(error, format!("unsupported datetime input type: {dtype:?}"));
    assert!(error.len() > 512);
}
#[test]
fn shared_time_core_empty_broadcast_and_original_formatter_bug() {
    let format = StringArray::from(vec![Some("%H:%i:%s/%S/%h/%f/%%/%q/é\0%")]);
    let mut work = EvaluationCheckpoints::new(&LegacyControl);
    let array: ArrayRef = format_output(&[Some(45296)], &format, 1, &mut work).unwrap();
    assert_eq!(
        array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "00:00:00/00/12/045296/%/%q/é\0%"
    );
    assert_eq!(
        format_output(
            &[],
            &StringArray::from(Vec::<Option<&str>>::new()),
            0,
            &mut work
        )
        .unwrap()
        .len(),
        0
    );
    assert_eq!(
        format_output(&[], &format, 1, &mut work)
            .unwrap_err()
            .into_legacy(),
        "time_format argument length mismatch"
    );
}
