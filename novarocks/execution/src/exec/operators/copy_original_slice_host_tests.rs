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

//! Same actual host as the complete take probes; standalone slice does not
//! replace original metadata/error authors or assert an upstream grant.
use super::*;
use novarocks_functions::selected_copy::slice_copy_in;
use arrow::buffer::{BooleanBuffer, NullBuffer};

#[test]
fn by_copy_original_slice_actual_metadata_grant_and_last_buffer_custody() {
    let source = input();
    let expected = source.slice(1, 2);
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr.clone(), None);
    let drops = Arc::new(AtomicUsize::new(0));
    let actual = slice_copy_in(
        source,
        1,
        2,
        SourceOwner(drops.clone()),
        host.clone(),
        &control,
    )
    .unwrap();
    assert_eq!(actual.values().to_data(), expected.to_data());
    assert!(actual.metadata_envelope() > 0);
    assert!(
        host.events()
            .iter()
            .any(|e| matches!(e, Event::OpaqueAttempt(bytes) if *bytes>0))
    );
    let result = actual.into_values();
    let data = result.to_data();
    let loan = data.child_data()[0].buffers()[0].clone();
    drop(data);
    drop(result);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(tr.current() > 0);
    drop(loan);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_released(&host);
}

#[test]
fn by_copy_original_slice_every_actual_host_and_callback_refusal_preserves_cause() {
    let source = input();
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr, None);
    drop(slice_copy_in(source.clone(), 1, 2, (), host.clone(), &control).unwrap());
    let attempts = *host.allocations.lock().unwrap();
    let trace = control.trace();
    assert_released(&host);
    for cause in causes() {
        for stop in 0..attempts {
            let tr = tracker();
            let host = Host::new(tr.clone(), Some((stop, cause.clone())));
            let control = Control::new(tr, None);
            assert!(
                matches!(slice_copy_in(source.clone(), 1, 2, (), host.clone(), &control),
                Err(CopyOperationError::Control(actual)) if actual==cause)
            );
            assert_eq!(*host.allocations.lock().unwrap(), stop + 1);
            assert_released(&host);
        }
        for stop in 0..trace.len() {
            let tr = tracker();
            let host = Host::new(tr.clone(), None);
            let control = Control::new(tr, Some((stop, cause.clone())));
            assert!(
                matches!(slice_copy_in(source.clone(), 1, 2, (), host.clone(), &control),
                Err(CopyOperationError::Control(actual)) if actual==cause)
            );
            assert_eq!(control.trace(), trace[..=stop]);
            assert_released(&host);
        }
    }
}

#[test]
fn by_copy_original_slice_preserves_full_carriers_empty_struct_and_original_panic() {
    let mut field = Field::new("original_nested", DataType::Utf8, true);
    field.set_metadata(std::collections::HashMap::from([(
        "PARQUET:field_id".into(),
        "7".into(),
    )]));
    let list: ArrayRef = Arc::new(
        ListArray::try_new(
            Arc::new(field),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1, 1, 3])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("remaining")])),
            None,
        )
        .unwrap(),
    );
    let cases: [ArrayRef; 6] = [
        list,
        input(),
        Arc::new(StructArray::new_empty_fields(3, None)),
        Arc::new(StructArray::new_empty_fields(
            3,
            Some(NullBuffer::new(BooleanBuffer::from(vec![
                true, false, true,
            ]))),
        )),
        Arc::new(NullArray::new(3)),
        Arc::new(LargeStringArray::from(vec!["a", "b", "c"])),
    ];
    for source in cases {
        for (offset, len) in [(0, 0), (1, 2), (3, 0), (2, 2)] {
            let original = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                source.slice(offset, len)
            }));
            let tr = tracker();
            let host = Host::new(tr.clone(), None);
            let control = Control::new(tr, None);
            let actual = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                slice_copy_in(source.clone(), offset, len, (), host.clone(), &control)
            }));
            match (original, actual) {
                (Ok(original), Ok(Ok(actual))) => {
                    assert_eq!(actual.values().to_data(), original.to_data());
                    assert_eq!(actual.values().len(), len);
                    drop(actual);
                }
                (Err(_), Err(_)) => {}
                (_, Ok(Err(error))) => {
                    panic!("unexpected original slice reclassification: {error:?}")
                }
                _ => panic!("original and retained slice disagree on panic/success"),
            }
            assert_released(&host);
        }
    }
}
