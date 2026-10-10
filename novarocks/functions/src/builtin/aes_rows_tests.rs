// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.
use super::super::{bytes_output, md5_shared::OwnedBytesArray};
use super::*;
use crate::{
    KernelDiagnostic, KernelEvaluationControl, KernelFailure, kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, BinaryArray, StringArray};
use arrow_schema::DataType;
use std::{sync::Mutex, time::Duration};
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal.as_ref() {
            assert!(at <= *stop, "callback after originating refusal");
        }
        trace.push(units);
        match self.refusal.as_ref() {
            Some((stop, cause)) if at == *stop => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("AES must not wait")
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("AES original invalid cause")),
        KernelFailure::Internal(KernelDiagnostic::new("AES original internal cause")),
        KernelFailure::Operational(KernelDiagnostic::new("AES original operational cause")),
        KernelFailure::InstanceFailed,
    ]
}
fn text(value: Option<&str>) -> OwnedBytesArray {
    OwnedBytesArray::Utf8(StringArray::from(vec![value]))
}
fn binary(value: &[u8]) -> OwnedBytesArray {
    OwnedBytesArray::Binary(BinaryArray::from(vec![Some(value)]))
}
fn run(
    operation: Operation,
    inputs: &[OwnedBytesArray],
    source: ToBase64ByteSource,
    control: &Control,
) -> Result<Row, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let rows = [0; 5];
    let out = evaluate_row_observed(
        operation,
        inputs,
        &rows[..inputs.len()],
        source,
        &mut |event| match event {
            Observation::Step => work.step(),
            Observation::OpaqueBoundary => work.flush(),
        },
    );
    work.finish_result(out)
}
#[test]
fn aes_original_source_sensitive_ctr_and_output_projection_are_same_core() {
    let zeros = [0; 16];
    let args = [
        text(Some("ÿ\u{80}")),
        binary(&zeros),
        binary(&zeros),
        text(Some("AES_128_CTR")),
    ];
    for (source, expected) in [
        (ToBase64ByteSource::Ordinary, vec![0xa5, 0x56, 0x89, 0x54]),
        (
            ToBase64ByteSource::NativeV1EncryptionLatin1,
            vec![0x99, 0x69],
        ),
    ] {
        let Row::Value(Some(value)) =
            run(Operation::Decrypt, &args, source, &Control::default()).unwrap()
        else {
            panic!("original CTR must produce bytes")
        };
        assert_eq!(value, expected);
        let output = bytes_output::build_bytes_output_lossy(
            vec![Some(value.clone())],
            Some(&DataType::Utf8),
        )
        .unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            String::from_utf8_lossy(&value)
        );
    }
}
#[test]
fn aes_original_null_empty_and_aad_errors_have_original_priority() {
    let mut args = [
        text(Some("a")),
        text(Some("k")),
        text(None),
        text(Some("AES_128_ECB")),
        text(Some("")),
    ];
    assert!(matches!(
        evaluate_row(
            Operation::Encrypt,
            &args,
            &[0; 5],
            ToBase64ByteSource::Ordinary
        ),
        Row::Data(DataError::AadRequiresGcm)
    ));
    args[0] = text(None);
    assert!(matches!(
        evaluate_row(
            Operation::Encrypt,
            &args,
            &[0; 5],
            ToBase64ByteSource::Ordinary
        ),
        Row::Value(None)
    ));
    args[0] = text(Some(""));
    assert!(matches!(
        evaluate_row(
            Operation::Decrypt,
            &args,
            &[0; 5],
            ToBase64ByteSource::Ordinary
        ),
        Row::Value(None)
    ));
    args[0] = text(Some("a"));
    args[3] = text(Some("AES_128_CBC"));
    assert!(matches!(
        evaluate_row(
            Operation::Decrypt,
            &args,
            &[0; 5],
            ToBase64ByteSource::Ordinary
        ),
        Row::Value(None)
    ));
}
#[test]
fn aes_every_original_owned_or_library_checkpoint_preserves_all_seven_causes() {
    let payload = "a\0b".repeat(260);
    let key = "key".repeat(120);
    let iv = "123";
    let aad = "aad".repeat(90);
    let encrypt_args = vec![
        text(Some(&payload)),
        text(Some(&key)),
        text(Some(iv)),
        text(Some("AES_128_GCM")),
        text(Some(&aad)),
    ];
    let Row::Value(Some(cipher)) = evaluate_row(
        Operation::Encrypt,
        &encrypt_args,
        &[0; 5],
        ToBase64ByteSource::Ordinary,
    ) else {
        panic!("original GCM must produce ciphertext")
    };
    let latin1 =
        bytes_output::build_bytes_output_latin1(vec![Some(cipher)], Some(&DataType::Utf8)).unwrap();
    let utf8 = latin1
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .clone();
    let decrypt_args = vec![
        OwnedBytesArray::Utf8(utf8),
        text(Some(&key)),
        text(Some("ignored")),
        text(Some("AES_128_GCM")),
        text(Some(&aad)),
    ];
    for (operation, inputs, source) in [
        (
            Operation::Encrypt,
            &encrypt_args,
            ToBase64ByteSource::Ordinary,
        ),
        (
            Operation::Decrypt,
            &decrypt_args,
            ToBase64ByteSource::Ordinary,
        ),
        (
            Operation::Decrypt,
            &decrypt_args,
            ToBase64ByteSource::NativeV1EncryptionLatin1,
        ),
    ] {
        let recorder = Control::default();
        assert!(matches!(
            run(operation, inputs, source, &recorder),
            Ok(Row::Value(Some(_)))
        ));
        let trace = recorder.trace.lock().unwrap().clone();
        assert!(trace.contains(&256));
        for stop in 0..trace.len() {
            for cause in causes() {
                let control = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((stop, cause.clone())),
                };
                assert!(
                    matches!(run(operation,inputs,source,&control),Err(actual)if actual==cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
}
#[test]
fn aes_bytes_output_null_empty_loop_observes_real_256_quantum_and_each_cause() {
    for projection in [
        bytes_output::TextProjection::Latin1,
        bytes_output::TextProjection::Utf8Lossy,
    ] {
        let values: Vec<_> = (0..321)
            .map(|i| if i % 2 == 0 { None } else { Some(vec![]) })
            .collect();
        let invoke = |control: &Control| -> Result<ArrayRef, KernelFailure> {
            control.checkpoint(0)?;
            let mut work = EvaluationCheckpoints::new(control);
            let output = bytes_output::build_bytes_output_observed(
                values.clone(),
                Some(&DataType::Utf8),
                projection,
                &mut |event| match event {
                    Observation::Step => work.step(),
                    Observation::OpaqueBoundary => work.flush(),
                },
            );
            work.finish_result(output)
        };
        let recorder = Control::default();
        let out = invoke(&recorder).unwrap();
        assert_eq!(out.len(), 321);
        assert_eq!(out.null_count(), 161);
        let trace = recorder.trace.lock().unwrap().clone();
        assert!(trace.contains(&256));
        for stop in 0..trace.len() {
            for cause in causes() {
                let control = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((stop, cause.clone())),
                };
                assert!(matches!(invoke(&control),Err(actual)if actual==cause));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
}
