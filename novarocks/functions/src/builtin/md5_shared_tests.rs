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
use crate::{
    EvaluatedArgument, FunctionValueType, KernelEvaluationControl, ScalarEvaluationInstance,
    kernel_control::{internal, invalid},
};
use arrow_array::{LargeBinaryArray, LargeStringArray, StructArray};
use std::sync::{Arc, Mutex};
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    reject: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("MD5 calculation never waits")
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
fn prepared(name: &str, sources: &[FunctionValueType]) -> Arc<dyn crate::PreparedScalarKernel> {
    match name {
        "md5" => super::super::string_md5_owner::prepared_for_test_with_policy(
            name,
            sources,
            novarocks_type_contract::DecimalOverflowPolicy::ReportError,
        )
        .unwrap(),
        "md5sum" => super::super::md5sum_owner::tests::prepared_for_test(name, sources).unwrap(),
        "md5sum_numeric" => {
            super::super::md5sum_numeric_owner::tests::prepared_for_test(name, sources).unwrap()
        }
        _ => panic!("uninstalled test owner"),
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("original invalid"),
        internal("original internal"),
        KernelFailure::Operational(crate::KernelDiagnostic::new("original operational")),
        KernelFailure::InstanceFailed,
    ]
}
#[test]
fn md5_family_actual_instances_keep_seven_causes_at_every_checkpoint_without_replay() {
    for name in ["md5", "md5sum", "md5sum_numeric"] {
        let text = "aé\0".repeat(129);
        let first = Arc::new(StringArray::from(vec![
            Some(text.as_str()),
            None,
            Some("hello"),
            Some(""),
        ])) as ArrayRef;
        let second = Arc::new(LargeBinaryArray::from(vec![
            Some(b"prefix".as_slice()),
            Some(b"masked".as_slice()),
            Some(text.as_bytes()),
            Some(b"".as_slice()),
        ])) as ArrayRef;
        let arrays = if name == "md5" {
            vec![first]
        } else {
            vec![first, second]
        };
        let sources = arrays
            .iter()
            .map(|array| FunctionValueType::new(array.data_type().clone(), true))
            .collect::<Vec<_>>();
        let prepared = prepared(name, &sources);
        let rows = [0, 1, 2, 3];
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
        assert!(trace.contains(&256));
        for cause in causes() {
            for at in 0..trace.len() {
                let control = Control {
                    trace: Mutex::new(vec![]),
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
}
#[test]
fn md5_family_selected_compact_and_scalar_addresses_skip_null_without_delimiters() {
    for name in ["md5sum", "md5sum_numeric"] {
        let first = Arc::new(LargeStringArray::from(vec![
            "inactive", "a", "inactive", "hello",
        ])) as ArrayRef;
        let compact = Arc::new(Int64Array::from(vec![Some(42), None])) as ArrayRef;
        let scalar = Arc::new(BinaryArray::from(vec![b"b".as_slice()])) as ArrayRef;
        let sources = [
            FunctionValueType::new(DataType::LargeUtf8, false),
            FunctionValueType::new(DataType::Int64, true),
            FunctionValueType::new(DataType::Binary, false),
        ];
        let rows = [1, 3];
        let selection = Selection::try_sparse(4, &rows).unwrap();
        let compact_values =
            crate::SelectedValues::try_new(selection, &DataType::Int64, compact, Box::default())
                .unwrap();
        let arguments = [
            EvaluatedArgument::Column(&first),
            EvaluatedArgument::SelectedColumn(&compact_values),
            EvaluatedArgument::Scalar(&scalar),
        ];
        let output = ScalarEvaluationInstance::instantiate(prepared(name, &sources))
            .unwrap()
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        let input = to_owned_bytes_array_with_varchar_cast(
            Arc::new(StringArray::from(vec!["a42b", "hellob"])),
            name,
            0,
        )
        .unwrap();
        let op = if name == "md5sum" {
            Operation::Md5sum
        } else {
            Operation::Md5sumNumeric
        };
        let target = if name == "md5sum" {
            DataType::Utf8
        } else {
            DataType::FixedSizeBinary(16)
        };
        let expected = evaluate_legacy(op, &[input], 2, Some(&target)).unwrap();
        assert_eq!(output.values().to_data(), expected.to_data());
        assert!(output.errors().is_empty());
    }
}
#[test]
fn md5_family_raw_errors_keep_full_output_cast_text_and_original_unsupported_input_error() {
    let input = to_owned_bytes_array(
        Arc::new(StringArray::from(vec!["hello"])),
        "md5sum_numeric",
        0,
    )
    .unwrap();
    let target = DataType::Struct(
        vec![Arc::new(arrow_schema::Field::new(
            "long".repeat(180),
            DataType::Int64,
            true,
        ))]
        .into(),
    );
    let raw = evaluate_legacy(Operation::Md5sumNumeric, &[input.clone()], 1, None).unwrap();
    let expected = arrow_cast::cast(&raw, &target)
        .map_err(|error| format!("md5sum_numeric: failed to cast output: {}", error))
        .unwrap_err();
    assert!(expected.len() > 512);
    assert_eq!(
        evaluate_legacy(Operation::Md5sumNumeric, &[input], 1, Some(&target)).unwrap_err(),
        expected
    );
    assert_eq!(
        to_owned_bytes_array(Arc::new(Int64Array::from(vec![1])), "md5", 0)
            .err()
            .unwrap(),
        "md5: arg0 must be VARCHAR or VARBINARY"
    );
}
#[test]
fn md5_normalizer_core_is_label_free_and_compatibility_preserves_original_prefixes() {
    let source = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
    assert!(matches!(
        to_owned_bytes_array_observed(source.clone(), 3, &mut |_| Ok(()))
            .err()
            .unwrap(),
        CoreError::BytesRequired { arg_idx: 3 }
    ));
    for label in ["md5", "md5sum", "aes_encrypt", "aes_decrypt"] {
        assert_eq!(
            to_owned_bytes_array(source.clone(), label, 3)
                .err()
                .unwrap(),
            format!("{label}: arg3 must be VARCHAR or VARBINARY")
        );
    }
}
#[test]
fn md5_varchar_fallback_keeps_original_typed_admission_failure() {
    let fields = vec![Arc::new(arrow_schema::Field::new(
        "x",
        DataType::Int64,
        false,
    ))]
    .into();
    let source = Arc::new(StructArray::new(
        fields,
        vec![Arc::new(Int64Array::from(vec![1]))],
        None,
    )) as ArrayRef;
    assert!(arrow_cast::cast(&source, &DataType::Utf8).is_err());
    assert!(matches!(
        to_owned_bytes_array_with_varchar_cast_observed(source.clone(), 4, &mut |_| Ok(()))
            .err()
            .unwrap(),
        CoreError::BytesRequired { arg_idx: 4 }
    ));
    assert_eq!(
        to_owned_bytes_array_with_varchar_cast(source, "md5sum", 4)
            .err()
            .unwrap(),
        "md5sum: arg4 must be VARCHAR or VARBINARY"
    );
}
#[test]
fn md5_typed_error_projection_never_changes_original_largeint_cause_or_kernel_failure() {
    let message = "original carrier error ".repeat(90);
    assert_eq!(
        compatibility::error_text(
            CoreError::LargeIntCarrier(message.clone()),
            "md5sum_numeric"
        ),
        message
    );
    let source = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
    let target = DataType::Struct(
        vec![Arc::new(arrow_schema::Field::new(
            "x",
            DataType::Int64,
            true,
        ))]
        .into(),
    );
    let typed = cast_output_observed(source.clone(), Some(&target), &mut |_| Ok(())).unwrap_err();
    let CoreError::OutputCast(cause) = typed else {
        panic!("exact original Arrow cast cause")
    };
    let expected = arrow_cast::cast(&source, &target).unwrap_err().to_string();
    assert_eq!(cause.to_string(), expected);
    assert_eq!(
        compatibility::error_text(CoreError::OutputCast(cause), "aes_encrypt"),
        format!("aes_encrypt: failed to cast output: {expected}")
    );
    for cause in causes() {
        for stage in 0..3 {
            let mut calls = 0;
            let mut observe = |_| {
                calls += 1;
                assert_eq!(calls, 1, "no callback after refusal");
                Err(cause.clone())
            };
            let failure = match stage {
                0 => to_owned_bytes_array_observed(source.clone(), 0, &mut observe)
                    .err()
                    .unwrap(),
                1 => {
                    to_owned_bytes_array_with_varchar_cast_observed(source.clone(), 0, &mut observe)
                        .err()
                        .unwrap()
                }
                _ => cast_output_observed(source.clone(), Some(&target), &mut observe)
                    .err()
                    .unwrap(),
            };
            assert_eq!(calls, 1);
            let CoreError::Kernel(actual) = failure else {
                panic!("unchanged typed control cause")
            };
            assert_eq!(actual, cause);
        }
    }
}
