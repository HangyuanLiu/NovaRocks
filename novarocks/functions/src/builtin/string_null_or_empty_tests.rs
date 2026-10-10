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
use crate::kernel_control::{internal, invalid};
use crate::{
    EvaluatedArgument, FunctionValueType, KernelEvaluationControl, ScalarEvaluationInstance,
};
use std::sync::Mutex;
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
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after first refusal");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("string computation never waits")
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

use arrow_array::{Int64Array, StructArray};
use arrow_schema::Field;
fn prepared(source: FunctionValueType) -> Arc<dyn crate::PreparedScalarKernel> {
    super::super::string_null_or_empty_owner::tests::prepared_for_test("null_or_empty", &[source])
        .unwrap()
}
#[test]
fn null_or_empty_actual_instance_seven_causes_every_checkpoint_and_no_replay() {
    let source = Arc::new(StringArray::from(vec![
        Some(""),
        None,
        Some("é\0"),
        Some("a"),
    ])) as ArrayRef;
    let args = [EvaluatedArgument::Column(&source)];
    let rows = [0, 1, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let prepared = prepared(FunctionValueType::new(DataType::Utf8, true));
    let ok = Control::default();
    ScalarEvaluationInstance::instantiate(prepared.clone())
        .unwrap()
        .evaluate(selection, &args, &ok)
        .unwrap();
    let trace = ok.trace.lock().unwrap().clone();
    for cause in causes() {
        for at in 0..trace.len() {
            let control = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((at, cause.clone())),
            };
            let mut instance = ScalarEvaluationInstance::instantiate(prepared.clone()).unwrap();
            assert_eq!(
                instance.evaluate(selection, &args, &control).unwrap_err(),
                cause
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            assert_eq!(
                instance.evaluate(selection, &args, &control).unwrap_err(),
                KernelFailure::InstanceFailed
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}
#[test]
fn null_or_empty_selected_compact_scalar_and_column_origins_keep_null_true() {
    let source = FunctionValueType::new(DataType::Utf8, true);
    let rows = [1, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let compact = Arc::new(StringArray::from(vec![None, Some("é")])) as ArrayRef;
    let selected =
        SelectedValues::try_new(selection, &DataType::Utf8, compact, Box::default()).unwrap();
    let values = Arc::new(StringArray::from(vec!["inactive", "", "inactive", "é"])) as ArrayRef;
    let scalar = Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef;
    for (arg, want) in [
        (
            EvaluatedArgument::SelectedColumn(&selected),
            vec![Some(true), Some(false)],
        ),
        (
            EvaluatedArgument::Column(&values),
            vec![Some(true), Some(false)],
        ),
        (
            EvaluatedArgument::Scalar(&scalar),
            vec![Some(true), Some(true)],
        ),
    ] {
        let arguments = [arg];
        let out = ScalarEvaluationInstance::instantiate(prepared(source.clone()))
            .unwrap()
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(
            out.values()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap(),
            &BooleanArray::from(want)
        );
        assert!(out.errors().is_empty());
    }
}
#[test]
fn null_or_empty_shared_raw_preserves_unsupported_all_null_empty_and_long_type_errors() {
    let input = Arc::new(Int64Array::from(vec![None; 3])) as ArrayRef;
    assert_eq!(
        evaluate_legacy(&input)
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap(),
        &BooleanArray::from(vec![Some(true); 3])
    );
    let empty = input.slice(0, 0);
    assert_eq!(evaluate_legacy(&empty).unwrap().len(), 0);
    let input = Arc::new(Int64Array::from(vec![None, Some(1)])) as ArrayRef;
    assert_eq!(
        evaluate_legacy(&input).unwrap_err(),
        "null_or_empty expects string or array, got Int64"
    );
    let field = Arc::new(Field::new("x".repeat(900), DataType::Int64, true));
    let input = Arc::new(StructArray::new(
        vec![field].into(),
        vec![Arc::new(Int64Array::from(vec![Some(1)]))],
        None,
    )) as ArrayRef;
    let expected = format!(
        "null_or_empty expects string or array, got {:?}",
        input.data_type()
    );
    assert!(expected.len() > 512);
    assert_eq!(evaluate_legacy(&input).unwrap_err(), expected);
}
