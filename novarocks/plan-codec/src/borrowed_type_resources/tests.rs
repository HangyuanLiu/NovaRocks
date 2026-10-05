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
