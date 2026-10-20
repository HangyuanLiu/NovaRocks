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
fn shared_time_core_keeps_bounded_carrier_fact_without_retaining_the_type() {
    let dtype = arrow_schema::DataType::Timestamp(
        arrow_schema::TimeUnit::Microsecond,
        Some(Arc::<str>::from("z".repeat(800))),
    );
    // Unsupported nested carrier diagnostics are kept before the pure boundary.
    let field = Arc::new(arrow_schema::Field::new("field".repeat(150), dtype, true));
    let dtype = arrow_schema::DataType::List(field);
    let array = arrow_array::new_null_array(&dtype, 1);
    let mut work = EvaluationCheckpoints::new(&LegacyControl);
    let error = datetime_seconds(&array, &mut work).unwrap_err();
    use novarocks_failure_contract::{BoundedDetail, FailureClass, FailureProjection};
    assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    assert_eq!(
        error.to_string(),
        BoundedDetail::truncating_display(format_args!(
            "unsupported datetime input type: {dtype:?}"
        ))
        .as_str()
    );
    assert!(error.to_string().len() <= novarocks_failure_contract::DIAGNOSTIC_MAX_BYTES);
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
            .to_string(),
        "time_format argument length mismatch"
    );
}

#[test]
fn time_text_actual_duration_limits_and_virtual_backing_refuse_without_wrap_or_giant_allocation() {
    assert_eq!(
        parse_hms_duration_to_seconds("2562047788015215:30:07"),
        Some(i64::MAX)
    );
    assert_eq!(
        parse_hms_duration_to_seconds("2562047788015215:30:08"),
        None
    );
    assert_eq!(
        parse_hms_duration_to_seconds("9223372036854775807:00:00"),
        None
    );
    let mut work = EvaluationCheckpoints::new(&LegacyControl);
    let error = format_output(
        &[Some(1)],
        &StringArray::from(vec!["%H"]),
        usize::MAX,
        &mut work,
    )
    .unwrap_err();
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    assert_eq!(error.failure_fact().class(), FailureClass::Representation);
    let facts = error.failure_fact().representation().unwrap();
    assert_eq!(
        facts.boundary,
        novarocks_failure_contract::RepresentationBoundary::SingleOutput
    );
    assert_eq!(facts.extent_limit, Some(isize::MAX as u64));
    assert_eq!(
        facts.required, None,
        "actual usize::MAX row backing exceeds u64 bytes; no invented narrowed count"
    );
    assert!(
        std::error::Error::source(&error)
            .unwrap()
            .is::<std::alloc::LayoutError>()
    );
    let error = TimeTextError::Extent {
        required: i32::MAX as u128 + 1,
        largest: 8,
        prefix: 19,
    };
    let facts = error.failure_fact().representation().unwrap();
    assert_eq!(
        facts.boundary,
        novarocks_failure_contract::RepresentationBoundary::SingleOutput
    );
    assert_eq!(facts.extent_limit, Some(i32::MAX as u64));
    assert_eq!(facts.representable_prefix, Some(19));
    let error = TimeTextError::Arrow(arrow_schema::ArrowError::MemoryError(
        "invalid value".into(),
    ));
    assert_eq!(error.failure_fact().class(), FailureClass::Resource);
    assert!(
        std::error::Error::source(&error)
            .unwrap()
            .is::<arrow_schema::ArrowError>()
    );
}

#[test]
fn time_text_temporal_value_cast_is_data_but_unsupported_frozen_cast_is_not() {
    let mut work = EvaluationCheckpoints::new(&LegacyControl);
    let array: ArrayRef = Arc::new(arrow_array::Date32Array::from(vec![0, i32::MAX]));
    let DurationCastError::Data(source) = duration_cast_source(&array, &mut work).unwrap_err()
    else {
        panic!("actual temporal value formatting failure must remain Data");
    };
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    assert_eq!(source.failure_fact().class(), FailureClass::Data);
    assert!(
        std::error::Error::source(&source)
            .unwrap()
            .is::<arrow_schema::ArrowError>()
    );
    let row = source.row_error(7);
    assert_eq!(row.selected_ordinal(), 7);
    assert_eq!(row.failure_fact(), source.failure_fact());
    let array: ArrayRef = Arc::new(arrow_array::StructArray::new_empty_fields(1, None));
    let DurationCastError::Outer(source) = duration_cast_source(&array, &mut work).unwrap_err()
    else {
        panic!("unsupported frozen type cannot be a value-data failure");
    };
    assert_eq!(source.failure_fact().class(), FailureClass::Invariant);
    let source = TimeTextError::Extent {
        required: i32::MAX as u128 + 17,
        largest: i32::MAX as u128 + 1,
        prefix: 2,
    };
    let facts = source.failure_fact().representation().unwrap();
    assert_eq!(
        facts.boundary,
        novarocks_failure_contract::RepresentationBoundary::MinimumUnit
    );
    assert_eq!(facts.required, Some(i32::MAX as u64 + 1));
    assert_eq!(facts.representable_prefix, Some(2));
}
