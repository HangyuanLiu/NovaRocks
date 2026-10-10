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
use arrow::datatypes::{Field, TimeUnit};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, PureCompileControl, ValueLogicalType,
};
use std::{collections::HashMap, sync::Mutex};

const SOURCE: usize = 1024 * 1024;
const WORK: usize = 3 * 1024 * 1024 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Encode);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after primary refusal");
        }
        trace.push(units);
        match self.refusal {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}

fn run(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source: usize,
    maximum: usize,
    control: &Control,
) -> Result<VerifiedTypeBinding, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = verify_type_binding(left, right, source, maximum, &mut work);
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    // Only the surrounding caller owns entry and ordinary/success publication.
    work.finish()?;
    result
}

fn nested(changed: bool) -> FunctionValueType {
    let mut metadata = HashMap::with_capacity(256);
    for index in 0..256 {
        metadata.insert(format!("deleted-{index}"), "temporary".to_owned());
    }
    metadata.retain(|key, _| key == "deleted-0");
    metadata.insert(
        "k".repeat(2051),
        if changed {
            "different".into()
        } else {
            "v".repeat(3073)
        },
    );
    FunctionValueType::new(
        DataType::Struct(
            vec![Field::new("payload", DataType::Int64, true).with_metadata(metadata)].into(),
        ),
        true,
    )
}

#[test]
fn borrowed_type_binding_preserves_exact_nested_metadata_and_scalar_attributes() {
    let left = nested(false);
    let independently_authored = nested(false);
    assert!(
        run(
            &left,
            &independently_authored,
            SOURCE,
            WORK,
            &Control::default()
        )
        .unwrap()
        .matches()
    );
    assert!(
        !run(&left, &nested(true), SOURCE, WORK, &Control::default())
            .unwrap()
            .matches()
    );
    let no_zone = FunctionValueType::new(DataType::Timestamp(TimeUnit::Nanosecond, None), true);
    let empty_zone = FunctionValueType::new(
        DataType::Timestamp(TimeUnit::Nanosecond, Some("".into())),
        true,
    );
    assert!(
        !run(&no_zone, &empty_zone, SOURCE, WORK, &Control::default())
            .unwrap()
            .matches()
    );
    #[allow(deprecated)]
    let dictionaries = [7, 9].map(|id| {
        FunctionValueType::new(
            DataType::Struct(
                vec![Field::new_dict(
                    "encoded",
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                    true,
                    id,
                    false,
                )]
                .into(),
            ),
            true,
        )
    });
    assert!(
        !run(
            &dictionaries[0],
            &dictionaries[1],
            SOURCE,
            WORK,
            &Control::default()
        )
        .unwrap()
        .matches()
    );
}

#[test]
fn borrowed_type_binding_flags_stop_before_scratch_without_skipping_equal_pointer_compare() {
    let left = FunctionValueType::new(DataType::Int64, false);
    let right = FunctionValueType::new(DataType::Int64, true);
    let early = run(&left, &right, SOURCE, SOURCE + 2, &Control::default()).unwrap();
    assert!(!early.matches());
    assert_eq!(early.work_upper_bound(), SOURCE + 2);
    assert!(matches!(
        run(&left, &left, SOURCE, SOURCE + 2, &Control::default()),
        Err(TypeCodecError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    let same = run(&left, &left, SOURCE, WORK, &Control::default()).unwrap();
    assert!(same.matches());
    let uuid = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::Uuid,
    )
    .unwrap();
    let physical = FunctionValueType::new(DataType::FixedSizeBinary(16), false);
    assert!(
        !run(&uuid, &physical, SOURCE, WORK, &Control::default())
            .unwrap()
            .matches()
    );
}

#[test]
fn borrowed_type_binding_deleted_map_invoice_and_work_limits_remain_mandatory() {
    let left = nested(false);
    let right = nested(false);
    let facts = run(&left, &right, SOURCE, WORK, &Control::default()).unwrap();
    assert!(
        run(
            &left,
            &right,
            SOURCE,
            facts.work_upper_bound(),
            &Control::default()
        )
        .unwrap()
        .matches()
    );
    assert!(matches!(
        run(
            &left,
            &right,
            SOURCE,
            facts.work_upper_bound() - 1,
            &Control::default()
        ),
        Err(TypeCodecError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    let larger = run(&left, &right, SOURCE + 4096, WORK, &Control::default()).unwrap();
    assert!(larger.work_upper_bound() > facts.work_upper_bound());
    assert!(matches!(
        run(&left, &right, 0, WORK, &Control::default()),
        Err(TypeCodecError::InvalidShape(_))
    ));
}

fn every_boundary(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source: usize,
    maximum: usize,
    expected_matches: Option<bool>,
) -> Vec<u32> {
    let recording = Control::default();
    let result = run(left, right, source, maximum, &recording);
    match expected_matches {
        Some(expected) => assert_eq!(result.unwrap().matches(), expected),
        None => assert!(matches!(result, Err(TypeCodecError::InvalidShape(_)))),
    }
    let expected = recording.trace.lock().unwrap().clone();
    for stop in 0..expected.len() {
        for cause in CAUSES {
            let refusing = Control {
                refusal: Some((stop, cause)),
                ..Control::default()
            };
            assert!(matches!(
                run(left, right, source, maximum, &refusing),
                Err(TypeCodecError::Control(actual)) if actual == cause
            ));
            assert_eq!(*refusing.trace.lock().unwrap(), expected[..=stop]);
        }
    }
    expected
}

#[test]
fn borrowed_type_binding_success_mismatch_and_ordinary_tails_preserve_every_control_prefix() {
    let left = nested(false);
    every_boundary(&left, &nested(false), SOURCE, WORK, Some(true));
    every_boundary(&left, &nested(true), SOURCE, WORK, Some(false));
    every_boundary(&left, &nested(false), 0, WORK, None);
}

#[test]
fn borrowed_type_binding_wide_real_comparison_observes_quantum_and_every_control_prefix() {
    let author = || {
        FunctionValueType::new(
            DataType::Struct(
                (0..320)
                    .map(|index| Field::new(format!("column-{index}"), DataType::Int64, false))
                    .collect(),
            ),
            true,
        )
    };
    let left = author();
    let right = author();
    let trace = every_boundary(&left, &right, SOURCE, WORK, Some(true));
    assert!(
        trace.contains(&256),
        "actual comparison must reach its quantum"
    );
}

#[test]
fn numeric_work_refusal_preserves_resource_before_later_controller_or_footer() {
    let left = nested(false);
    let right = nested(false);
    let upper = run(&left, &right, SOURCE, WORK, &Control::default())
        .unwrap()
        .work_upper_bound();
    for maximum in [0, SOURCE + 2, upper - 1] {
        let baseline = Control::default();
        assert!(matches!(
            run(&left, &right, SOURCE, maximum, &baseline),
            Err(TypeCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        let trace = baseline.trace.into_inner().unwrap();
        assert_eq!(trace.first(), Some(&0));
        // A later callback would introduce a second cause after numerical
        // admission already refused. It must never execute, including finish.
        for cause in CAUSES {
            let forbidden = Control {
                refusal: Some((trace.len(), cause)),
                ..Control::default()
            };
            assert!(matches!(
                run(&left, &right, SOURCE, maximum, &forbidden),
                Err(TypeCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(*forbidden.trace.lock().unwrap(), trace);
        }
        for stop in 0..trace.len() {
            for cause in CAUSES {
                let refusing = Control {
                    refusal: Some((stop, cause)),
                    ..Control::default()
                };
                assert!(matches!(run(&left, &right, SOURCE, maximum, &refusing),
                    Err(TypeCodecError::Control(actual)) if actual == cause));
                assert_eq!(*refusing.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
}

fn preflight_with_pending(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source: usize,
    maximum: usize,
    pending: usize,
    control: &Control,
) -> Result<BoundTypeComparisonFacts, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    // This unit-level priming exercises the actual borrowed owner's first
    // flush; the original caller's pending work is not a new type traversal.
    for _ in 0..pending {
        work.step()?;
    }
    let result = preflight_type_binding(left, right, source, maximum, &mut work);
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

#[test]
fn borrowed_type_known_prefix_refusal_precedes_pending_255_for_all_root_flags() {
    let physical = FunctionValueType::new(DataType::FixedSizeBinary(16), false);
    let same_flags_other_carrier = FunctionValueType::new(DataType::Int64, false);
    let nullable = FunctionValueType::new(DataType::FixedSizeBinary(16), true);
    let uuid = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::Uuid,
    )
    .unwrap();
    let scratch = mem::size_of::<[Option<(&DataType, usize)>; MAX_VALUE_TYPE_NODES]>();
    for (right, matching_flags) in [
        (&physical, true),
        (&same_flags_other_carrier, true),
        (&nullable, false),
        (&uuid, false),
    ] {
        let expected_prefix = SOURCE + if matching_flags { scratch } else { 0 } + 2;
        let bound = type_binding_prefix_work_upper_bound(&physical, right, SOURCE).unwrap();
        assert_eq!(bound.work_upper_bound(), expected_prefix);
        assert_eq!(bound.flags_match(), matching_flags);
        for cause in CAUSES {
            let control = Control {
                refusal: Some((1, cause)),
                ..Control::default()
            };
            assert!(matches!(
                preflight_with_pending(
                    &physical,
                    right,
                    SOURCE,
                    expected_prefix - 1,
                    255,
                    &control,
                ),
                Err(TypeCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(*control.trace.lock().unwrap(), [0]);
        }
    }
}

#[test]
fn borrowed_type_exact_prefix_deep_gate_and_original_success_tails_keep_their_trace() {
    let left = FunctionValueType::new(DataType::Int64, false);
    let nullable = FunctionValueType::new(DataType::Int64, true);
    let scratch = mem::size_of::<[Option<(&DataType, usize)>; MAX_VALUE_TYPE_NODES]>();
    let prefix = SOURCE + scratch + 2;
    // The plain root has one TypeNode: its original model contributes B,
    // four closed header units and one completed numerical visit.
    let full = prefix + SOURCE + 5;
    for pending in [0, 255] {
        for (right, maximum, expected_bound, expected_flags, expected_trace) in [
            (
                &left,
                full,
                full,
                true,
                vec![0, u32::try_from(pending).unwrap(), 3, 0, 1, 0],
            ),
            (
                &nullable,
                SOURCE + 2,
                SOURCE + 2,
                false,
                vec![0, u32::try_from(pending).unwrap(), 3, 0],
            ),
        ] {
            let control = Control::default();
            let result =
                preflight_with_pending(&left, right, SOURCE, maximum, pending, &control).unwrap();
            assert_eq!(result.work_upper_bound(), expected_bound);
            assert_eq!(result.flags_match(), expected_flags);
            assert_eq!(*control.trace.lock().unwrap(), expected_trace);
            for at in 0..expected_trace.len() {
                for cause in CAUSES {
                    let control = Control {
                        refusal: Some((at, cause)),
                        ..Control::default()
                    };
                    assert!(matches!(
                        preflight_with_pending(&left, right, SOURCE, maximum, pending, &control),
                        Err(TypeCodecError::Control(actual)) if actual == cause
                    ));
                    assert_eq!(*control.trace.lock().unwrap(), expected_trace[..=at]);
                }
            }
        }

        // An admitted exact prefix does not authorize the later datatype walk.
        // The first real TypeNode increases the bound and refuses before its
        // completed step; no opaque exit or ordinary footer follows it.
        let expected_trace = vec![0, u32::try_from(pending).unwrap(), 3, 0];
        let control = Control::default();
        assert!(matches!(
            preflight_with_pending(&left, &left, SOURCE, prefix, pending, &control),
            Err(TypeCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(*control.trace.lock().unwrap(), expected_trace);
        for cause in CAUSES {
            let control = Control {
                refusal: Some((expected_trace.len(), cause)),
                ..Control::default()
            };
            assert!(matches!(
                preflight_with_pending(&left, &left, SOURCE, prefix, pending, &control),
                Err(TypeCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(*control.trace.lock().unwrap(), expected_trace);
        }
    }
}

#[test]
fn borrowed_type_pure_prefix_keeps_ordinary_source_and_arithmetic_error_observations() {
    let left = FunctionValueType::new(DataType::Int64, false);
    for (source, maximum, expected_message, expected_trace) in [
        (
            0,
            0,
            "borrowed type source invoice omits original inline roots",
            vec![0, 255, 1],
        ),
        (
            usize::MAX,
            usize::MAX,
            "borrowed type comparison work sum overflow",
            vec![0, 255, 2],
        ),
    ] {
        let control = Control::default();
        assert!(matches!(
            preflight_with_pending(&left, &left, source, maximum, 255, &control),
            Err(TypeCodecError::InvalidShape(message)) if message == expected_message
        ));
        assert_eq!(*control.trace.lock().unwrap(), expected_trace);
        for at in 0..expected_trace.len() {
            for cause in CAUSES {
                let control = Control {
                    refusal: Some((at, cause)),
                    ..Control::default()
                };
                assert!(matches!(
                    preflight_with_pending(&left, &left, source, maximum, 255, &control),
                    Err(TypeCodecError::Control(actual)) if actual == cause
                ));
                assert_eq!(*control.trace.lock().unwrap(), expected_trace[..=at]);
            }
        }
    }
}

#[test]
fn admitted_comparison_preserves_original_bound_trace_and_every_actual_control_exit() {
    let left = nested(false);
    let right = nested(false);
    let run_admitted = |control: &Control| -> Result<VerifiedTypeBinding, TypeCodecError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
        let mut last = 0;
        let result = verify_type_binding_admitted(
            &left,
            &right,
            SOURCE,
            WORK,
            &mut |facts| {
                assert!(facts.work_upper_bound() >= last);
                last = facts.work_upper_bound();
                Ok::<_, TypeCodecError>(())
            },
            &mut work,
        );
        if matches!(&result, Err(TypeCodecError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    };
    let original_control = Control::default();
    let original = run(&left, &right, SOURCE, WORK, &original_control).unwrap();
    assert!(original.matches());
    let trace = original_control.trace.lock().unwrap().clone();
    let admitted_control = Control::default();
    let admitted = run_admitted(&admitted_control).unwrap();
    assert!(admitted.matches());
    assert_eq!(admitted.work_upper_bound(), original.work_upper_bound());
    assert_eq!(*admitted_control.trace.lock().unwrap(), trace);
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control {
                refusal: Some((at, cause)),
                ..Control::default()
            };
            assert!(
                matches!(run_admitted(&control), Err(TypeCodecError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn admitted_comparison_known_prefix_and_field_growth_precede_next_observation() {
    let left = FunctionValueType::new(
        DataType::Struct(
            vec![
                Field::new("x", DataType::Int64, true).with_metadata(HashMap::from([
                    ("a".to_owned(), "1".to_owned()),
                    ("b".to_owned(), "2".to_owned()),
                ])),
            ]
            .into(),
        ),
        false,
    );
    let prefix = SOURCE + mem::size_of::<[Option<(&DataType, usize)>; MAX_VALUE_TYPE_NODES]>() + 2;
    // The first real TypeNode contributes B+4 headers+one model visit.
    let first_node = prefix + SOURCE + 5;
    for stop_at in [prefix - 1, first_node] {
        let baseline = Control::default();
        let run = |control: &Control| -> Result<VerifiedTypeBinding, TypeCodecError> {
            let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
            for _ in 0..255 {
                work.step()?;
            }
            verify_type_binding_admitted(
                &left,
                &left,
                SOURCE,
                WORK,
                &mut |facts| {
                    if facts.work_upper_bound() > stop_at {
                        Err(TypeCodecError::Control(
                            CompileControlError::ResourceExhausted,
                        ))
                    } else {
                        Ok(())
                    }
                },
                &mut work,
            )
        };
        assert!(matches!(
            run(&baseline),
            Err(TypeCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        let trace = baseline.trace.lock().unwrap().clone();
        assert_eq!(
            trace,
            if stop_at < prefix {
                vec![0]
            } else {
                vec![0, 255, 3, 0]
            }
        );
        for cause in CAUSES {
            let control = Control {
                refusal: Some((trace.len(), cause)),
                ..Control::default()
            };
            assert!(matches!(
                run(&control),
                Err(TypeCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(*control.trace.lock().unwrap(), trace);
        }
    }
}
