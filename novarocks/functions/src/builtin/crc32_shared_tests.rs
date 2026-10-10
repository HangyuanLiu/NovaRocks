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
use super::*;
use crate::{EvaluatedArgument, FunctionValueType, ScalarEvaluationInstance};
use arrow_array::Int64Array;
use std::sync::{Arc, Mutex};
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    reject: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("XXH3 calculation never waits")
    }

    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.reject {
            assert!(at <= *stop, "callback after first refusal");
        }
        trace.push(units);
        if let Some((stop, cause)) = &self.reject {
            if at == *stop {
                return Err(cause.clone());
            }
        }
        Ok(())
    }
}

fn instance() -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        super::super::crc32_owner::prepared_for_test(&FunctionValueType::new(DataType::Utf8, true))
            .unwrap(),
    )
    .unwrap()
}
#[test]
fn shared_crc32_actual_instance_every_callback_preserves_all_seven_causes_and_latch() {
    let text = "é中a\0".repeat(129);
    let array = Arc::new(StringArray::from(vec![
        Some(text.as_str()),
        None,
        Some("hello"),
        Some("outside"),
    ])) as ArrayRef;
    let rows = [0, 1, 2];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let arguments = [EvaluatedArgument::Column(&array)];
    let probe = Control::default();
    let output = instance().evaluate(selection, &arguments, &probe).unwrap();
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(2),
        907060870
    );
    let trace = probe.trace.into_inner().unwrap();
    assert!(trace.len() > 8);
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("original invalid"),
        internal("original internal"),
        KernelFailure::Operational(crate::KernelDiagnostic::new("original operational")),
        KernelFailure::InstanceFailed,
    ] {
        for at in 0..trace.len() {
            let control = Control {
                trace: Mutex::new(Vec::new()),
                reject: Some((at, cause.clone())),
            };
            let mut instance = instance();
            assert_eq!(
                instance
                    .evaluate(selection, &arguments, &control)
                    .unwrap_err(),
                cause
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            assert_eq!(
                instance
                    .evaluate(selection, &arguments, &control)
                    .unwrap_err(),
                KernelFailure::InstanceFailed
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}
#[test]
fn shared_crc32_legacy_four_carriers_preserve_original_unsigned_checksum() {
    let text = vec![Some("123456789"), None, Some("你好"), Some("a\0b")];
    let bytes = text
        .iter()
        .map(|v| v.map(str::as_bytes))
        .collect::<Vec<_>>();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(text.clone())),
        Arc::new(LargeStringArray::from(text)),
        Arc::new(BinaryArray::from(bytes.clone())),
        Arc::new(LargeBinaryArray::from(bytes)),
    ];
    for array in arrays {
        assert_eq!(
            evaluate_legacy(&array).unwrap().to_data(),
            Int64Array::from(vec![
                Some(3421780262),
                None,
                Some(1352841281),
                Some(367556721)
            ])
            .to_data()
        );
    }
}
#[test]
fn shared_crc32_legacy_unsupported_dtype_keeps_full_string_before_diagnostics() {
    let field = Arc::new(arrow_schema::Field::new(
        "long_field_".repeat(128),
        DataType::Int64,
        true,
    ));
    let array = Arc::new(arrow_array::StructArray::from(vec![(
        field,
        Arc::new(Int64Array::from(vec![None])) as ArrayRef,
    )])) as ArrayRef;
    let expected = format!(
        "crc32 expects VARCHAR/BINARY input, got {:?}",
        array.data_type()
    );
    assert!(expected.len() > 512);
    assert_eq!(
        evaluate_legacy(&array).unwrap_err().as_bytes(),
        expected.as_bytes()
    );
}
