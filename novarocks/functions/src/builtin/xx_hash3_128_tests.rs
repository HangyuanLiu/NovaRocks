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
use arrow_array::{LargeBinaryArray, LargeStringArray};
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
fn prepared(sources: &[FunctionValueType]) -> std::sync::Arc<dyn crate::PreparedScalarKernel> {
    super::super::xx_hash3_128_owner::tests::prepared_for_test("xx_hash3_128", sources).unwrap()
}
#[test]
fn xx_hash3_128_every_actual_selected_checkpoint_preserves_seven_causes_and_failed_latch() {
    let text = "aé\0".repeat(129);
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![
            Some(text.as_str()),
            None,
            Some("hello"),
            Some(""),
        ])),
        Arc::new(LargeBinaryArray::from(vec![
            Some(b"prefix".as_slice()),
            Some(b"masked".as_slice()),
            Some(text.as_bytes()),
            Some(b"".as_slice()),
        ])),
    ];
    let sources = arrays
        .iter()
        .map(|a| FunctionValueType::new(a.data_type().clone(), true))
        .collect::<Vec<_>>();
    let prepared = prepared(&sources);
    let rows = [0, 2, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let arguments = arrays
        .iter()
        .map(EvaluatedArgument::Column)
        .collect::<Vec<_>>();
    let probe = Control::default();
    ScalarEvaluationInstance::instantiate(prepared.clone())
        .unwrap()
        .evaluate(selection, &arguments, &probe)
        .unwrap();
    let trace = probe.trace.into_inner().unwrap();
    assert!(trace.len() > 8);
    let causes = [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("original invalid"),
        internal("original internal"),
        KernelFailure::Operational(crate::KernelDiagnostic::new("original operational")),
        KernelFailure::InstanceFailed,
    ];
    for cause in causes {
        for at in 0..trace.len() {
            let control = Control {
                trace: Mutex::new(Vec::new()),
                reject: Some((at, cause.clone())),
            };
            let mut instance = ScalarEvaluationInstance::instantiate(prepared.clone()).unwrap();
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
fn xx_hash3_128_selected_large_constant_compact_and_sparse_addresses_keep_original_stream() {
    let source = Arc::new(LargeStringArray::from(vec![
        "inactive", "hello", "inactive",
    ])) as ArrayRef;
    let scalar = Arc::new(BinaryArray::from(vec![b"starrocks".as_slice()])) as ArrayRef;
    let sources = [
        FunctionValueType::new(DataType::LargeUtf8, false),
        FunctionValueType::new(DataType::Binary, false),
    ];
    let prepared = prepared(&sources);
    let rows = [1];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let arguments = [
        EvaluatedArgument::Column(&source),
        EvaluatedArgument::Scalar(&scalar),
    ];
    let out = ScalarEvaluationInstance::instantiate(prepared)
        .unwrap()
        .evaluate(selection, &arguments, &Control::default())
        .unwrap();
    let array = out
        .values()
        .as_any()
        .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
        .unwrap();
    let expected = ((1_559_307_639_436_096_304u128 << 64) | 8_859_976_453_967_563_600u128) as i128;
    assert_eq!(array.value(0), expected.to_be_bytes());
}
#[test]
fn xx_hash3_128_legacy_output_cast_callback_preserves_full_error_before_bounds() {
    let input = prepare_legacy_input(Arc::new(StringArray::from(vec!["hello"])), 0).unwrap();
    let name = "long_target_".to_string() + &"x".repeat(1025);
    let target =
        DataType::Struct(vec![arrow_schema::Field::new(&name, DataType::Int64, true)].into());
    let error = evaluate_legacy(&[input], 1, Some(&target)).unwrap_err();
    assert!(error.starts_with("xx_hash3_128: failed to cast output:"));
    assert!(error.contains(&name));
    assert!(error.len() > 512);
}
