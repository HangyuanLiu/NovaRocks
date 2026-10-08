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

//! Observation probes, not a claim of whole-invocation equivalence.
//! Mount as a child of compiled_program/cast_tests.rs. Each case owns a fresh
//! actual compiler instance; a caught panic is never followed by reuse.
use super::*;
use crate::exec::expr::legacy_date_float_cast_baseline_tests::actual;
use arrow::array::{Date32Array, Float32Array, Float64Array, UInt32Array};
use std::panic::{AssertUnwindSafe, catch_unwind};

#[derive(Debug, Clone, PartialEq, Eq)]
struct FloatSnapshot {
    dtype: DataType,
    bits: Vec<Option<u64>>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
enum Observation {
    Rows(FloatSnapshot, Vec<(usize, String)>),
    Data(String),
    Kernel(String),
    Panic(String),
}
fn snapshot(values: &ArrayRef) -> FloatSnapshot {
    let bits = match values.data_type() {
        DataType::Float32 => values
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .iter()
            .map(|v| v.map(|v| u64::from(v.to_bits())))
            .collect(),
        DataType::Float64 => values
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .map(|v| v.map(f64::to_bits))
            .collect(),
        other => panic!("probe expected float output, got {other:?}"),
    };
    FloatSnapshot {
        dtype: values.data_type().clone(),
        bits,
    }
}
fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_owned()
    }
}
fn arena_observation(
    source: &ArrayRef,
    selected_rows: &[usize],
    target: DataType,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> Observation {
    // This is the real reference invocation domain: no inactive carrier enters
    // arena.eval. take preserves the selected Arrow NULL bitmap and order.
    let indices = UInt32Array::from(
        selected_rows
            .iter()
            .map(|r| u32::try_from(*r).unwrap())
            .collect::<Vec<_>>(),
    );
    let demanded = arrow::compute::take(source.as_ref(), &indices, None).unwrap();
    match catch_unwind(AssertUnwindSafe(|| {
        actual(&demanded, target, policy, allow)
    })) {
        Ok(Ok(values)) => Observation::Rows(snapshot(&values), Vec::new()),
        Ok(Err(error)) => Observation::Data(error),
        Err(payload) => Observation::Panic(panic_message(payload)),
    }
}
fn compiled_observation(
    program: &Arc<LocalProgram>,
    source: &ArrayRef,
    selected_rows: &[usize],
) -> Observation {
    let batch = RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            source.clone(),
            Arc::new(Int64Array::from(vec![42; source.len()])),
            Arc::new(BooleanArray::from(vec![true; source.len()])),
        ],
    )
    .unwrap();
    let mut evaluator = instance(program);
    let selection = Selection::try_sparse(source.len(), selected_rows).unwrap();
    let caught = catch_unwind(AssertUnwindSafe(|| {
        match evaluator.evaluate(&batch, selection, &Control) {
            Ok(values) => Observation::Rows(
                snapshot(values.values()),
                values
                    .errors()
                    .iter()
                    .map(|error| (error.selected_ordinal(), error.message().to_owned()))
                    .collect(),
            ),
            Err(error) => Observation::Kernel(format!("{error:?}")),
        }
    }));
    // Drop the instance after either a normal return or unwind. This probe does
    // not add a panic-to-KernelFailure adapter or assert direct panic latching.
    drop(evaluator);
    match caught {
        Ok(observed) => observed,
        Err(payload) => Observation::Panic(panic_message(payload)),
    }
}
struct Case {
    label: &'static str,
    carriers: Vec<i32>,
    valid: Vec<bool>,
    selected: Vec<usize>,
}
fn cases() -> Vec<Case> {
    vec![
        Case {
            label: "MIN_MAX_all",
            carriers: vec![i32::MIN, i32::MAX],
            valid: vec![true, true],
            selected: vec![0, 1],
        },
        Case {
            label: "MAX_MIN_all",
            carriers: vec![i32::MAX, i32::MIN],
            valid: vec![true, true],
            selected: vec![0, 1],
        },
        Case {
            label: "MIN_isolated",
            carriers: vec![i32::MIN],
            valid: vec![true],
            selected: vec![0],
        },
        Case {
            label: "MAX_isolated",
            carriers: vec![i32::MAX],
            valid: vec![true],
            selected: vec![0],
        },
        Case {
            label: "MIN_MAX_only_MIN",
            carriers: vec![i32::MIN, i32::MAX],
            valid: vec![true, true],
            selected: vec![0],
        },
        Case {
            label: "MIN_MAX_only_MAX",
            carriers: vec![i32::MIN, i32::MAX],
            valid: vec![true, true],
            selected: vec![1],
        },
        Case {
            label: "MAX_MIN_only_MIN",
            carriers: vec![i32::MAX, i32::MIN],
            valid: vec![true, true],
            selected: vec![1],
        },
        Case {
            label: "empty_demand",
            carriers: vec![i32::MIN, i32::MAX],
            valid: vec![true, true],
            selected: vec![],
        },
        Case {
            label: "MIN_hidden_MAX_NULL",
            carriers: vec![i32::MIN, i32::MAX],
            valid: vec![true, false],
            selected: vec![0, 1],
        },
        Case {
            label: "hidden_MIN_NULL_MAX",
            carriers: vec![i32::MIN, i32::MAX],
            valid: vec![false, true],
            selected: vec![0, 1],
        },
        Case {
            label: "MAX_hidden_MIN_NULL",
            carriers: vec![i32::MAX, i32::MIN],
            valid: vec![true, false],
            selected: vec![0, 1],
        },
        Case {
            label: "hidden_MAX_NULL_MIN",
            carriers: vec![i32::MAX, i32::MIN],
            valid: vec![false, true],
            selected: vec![0, 1],
        },
        Case {
            label: "safe_hidden_MAX_NULL",
            carriers: vec![0, i32::MAX],
            valid: vec![true, false],
            selected: vec![0, 1],
        },
        Case {
            label: "safe_inactive_MAX",
            carriers: vec![0, i32::MAX],
            valid: vec![true, true],
            selected: vec![0],
        },
    ]
}
fn run_probe(target: DataType) {
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            let program = compiled(
                FunctionValueType::new(DataType::Date32, true),
                FunctionValueType::new(target.clone(), true),
                Source::Column,
                Wrap::Bare,
                policy,
                allow,
            );
            let max: ArrayRef = Arc::new(Date32Array::from(vec![i32::MAX]));
            let max_raw = arena_observation(&max, &[0], target.clone(), policy, allow);
            let max_compiled = compiled_observation(&program, &max, &[0]);
            let arithmetic_panics = matches!(max_raw, Observation::Panic(_));
            for case in cases() {
                let source: ArrayRef = Arc::new(Date32Array::new(
                    case.carriers.into(),
                    Some(arrow_buffer::NullBuffer::from(case.valid)),
                ));
                let raw = arena_observation(&source, &case.selected, target.clone(), policy, allow);
                let compiled = compiled_observation(&program, &source, &case.selected);
                eprintln!(
                    "DATE_FLOAT_INVOCATION_PROBE target={target:?} policy={policy:?} allow={allow} case={} selected={:?} arena={raw:?} compiled={compiled:?}",
                    case.label, case.selected,
                );
                match (&raw, &compiled) {
                    (Observation::Rows(a, ae), Observation::Rows(b, be)) => {
                        assert!(ae.is_empty() && be.is_empty(), "{}", case.label);
                        assert_eq!(a, b, "{}", case.label);
                    }
                    (Observation::Data(message), Observation::Rows(_, errors)) => {
                        // Row diagnostics are the compiled API, whereas the
                        // old arena returns its first full-batch String.
                        assert_eq!(
                            errors.first().map(|(_, e)| e.as_str()),
                            Some(message.as_str()),
                            "{}",
                            case.label
                        );
                        if case.label == "MIN_MAX_only_MIN" || case.label == "MAX_MIN_only_MIN" {
                            assert_eq!(errors[0].0, 0);
                            assert_eq!(errors.len(), 1);
                        }
                    }
                    (Observation::Panic(a), Observation::Panic(b)) => {
                        assert_eq!(a, b, "{}", case.label)
                    }
                    (Observation::Data(message), Observation::Panic(_)) => {
                        // This assertion freezes the observed discrepancy; it
                        // deliberately does not call the paths equivalent.
                        assert_eq!(case.label, "MIN_MAX_all");
                        assert!(arithmetic_panics);
                        assert_eq!(compiled, max_compiled);
                        assert_eq!(
                            message,
                            &format!(
                                "CAST failed: from Date32 to {target:?}: invalid Date32 value -2147483648"
                            )
                        );
                    }
                    _ => panic!(
                        "unexpected probe classification {}: arena={raw:?}, compiled={compiled:?}",
                        case.label
                    ),
                }
                if case.label == "MIN_MAX_all" && arithmetic_panics {
                    assert!(matches!(raw, Observation::Data(_)));
                    assert!(matches!(compiled, Observation::Panic(_)));
                }
            }
        }
    }
}
#[test]
fn date_float_review_probe_real_arena_actual_compiler_f32_invocation_order() {
    run_probe(DataType::Float32);
}
#[test]
fn date_float_review_probe_real_arena_actual_compiler_f64_invocation_order() {
    run_probe(DataType::Float64);
}
