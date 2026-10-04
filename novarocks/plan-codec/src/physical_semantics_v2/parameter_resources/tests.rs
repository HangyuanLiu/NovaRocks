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
use novarocks_type_contract::{CompileControlError, SemanticParameterKey};
use std::sync::Mutex;

const SOURCE: usize = 1 << 20;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Decode);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after primary refusal");
        }
        trace.push((phase, units));
        match self.stop {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn limits() -> ParameterProjectionLimits {
    ParameterProjectionLimits {
        max_parameters: MAX_SEMANTIC_PARAMETERS,
        max_timezone_request_bytes: 1 << 20,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 4 << 20,
        max_coexisting_source_and_request_bytes: 8 << 20,
        max_work: 1 << 30,
    }
}
fn entry(id: u32, value: wire::semantic_parameter::Value) -> wire::SemanticParameter {
    wire::SemanticParameter {
        id,
        value: Some(value),
    }
}
fn source() -> wire::SemanticParameters {
    use wire::semantic_parameter::Value::*;
    wire::SemanticParameters {
        entries: vec![
            entry(u32::MAX, StatementStartUtcMicros(i64::MIN)),
            entry(0, TimeZone("雪Z".into())),
            entry(2, AllowThrowException(true)),
            entry(3, DecimalOverflowToDouble(false)),
            entry(4, GroupConcatLegacy(true)),
            entry(5, GroupConcatMaxLen(i64::MIN)),
            entry(6, AllowThrowException(false)),
        ],
    }
}
#[test]
fn bounded_parameters_keep_all_raw_variants_scoped_keys_and_sparse_ids() {
    let input = source();
    let (table, facts) =
        decode_semantic_parameters(&input, SOURCE, limits(), &Control::default()).unwrap();
    assert_eq!(table.entries().len(), 7);
    assert_eq!(
        table.get(SemanticParameterId::new(u32::MAX)),
        Some(&SemanticParameterValue::StatementStartUtc(i64::MIN))
    );
    assert_eq!(
        table.get(SemanticParameterId::new(0)),
        Some(&SemanticParameterValue::TimeZone("雪Z".into()))
    );
    assert_eq!(
        table.get(SemanticParameterId::new(2)),
        Some(&SemanticParameterValue::AllowThrowException(true))
    );
    assert_eq!(
        table.get(SemanticParameterId::new(3)),
        Some(&SemanticParameterValue::DecimalOverflowToDouble(false))
    );
    assert_eq!(
        table.get(SemanticParameterId::new(4)),
        Some(&SemanticParameterValue::GroupConcatLegacy(true))
    );
    assert_eq!(
        table.get(SemanticParameterId::new(5)),
        Some(&SemanticParameterValue::GroupConcatMaxLen(i64::MIN))
    );
    assert_eq!(
        table.get(SemanticParameterId::new(6)),
        Some(&SemanticParameterValue::AllowThrowException(false))
    );
    assert_eq!(
        table.get(SemanticParameterId::new(6)).unwrap().key(),
        SemanticParameterKey::AllowThrowException
    );
    // This is a cumulative node-request bound, not n actual allocated nodes.
    assert_eq!(facts.parameter_count, 7);
    assert_eq!(facts.timezone_request_bytes_upper_bound, 4);
    assert_eq!(facts.allocation_requests_upper_bound, 8);
    let control = Control::default();
    let prepared = prepare_semantic_parameters_decode(&input, SOURCE, limits(), &control).unwrap();
    assert_eq!(*prepared.facts(), facts);
    let (prepared_table, prepared_facts) = prepared.emit().unwrap();
    assert_eq!(prepared_table, table);
    assert_eq!(prepared_facts, facts);
}
#[test]
fn bounded_parameters_all_six_resource_axes_and_actual_source_capacity_are_admitted() {
    let input = source();
    let (_, facts) =
        decode_semantic_parameters(&input, SOURCE, limits(), &Control::default()).unwrap();
    let exact = ParameterProjectionLimits {
        max_parameters: facts.parameter_count,
        max_timezone_request_bytes: facts.timezone_request_bytes_upper_bound,
        max_allocation_requests: facts.allocation_requests_upper_bound,
        max_allocation_request_bytes: facts.allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: facts
            .coexisting_source_and_request_bytes_upper_bound,
        max_work: facts.cumulative_work_upper_bound,
    };
    assert!(decode_semantic_parameters(&input, SOURCE, exact, &Control::default()).is_ok());
    for axis in 0..6 {
        let mut cap = exact;
        match axis {
            0 => cap.max_parameters -= 1,
            1 => cap.max_timezone_request_bytes -= 1,
            2 => cap.max_allocation_requests -= 1,
            3 => cap.max_allocation_request_bytes -= 1,
            4 => cap.max_coexisting_source_and_request_bytes -= 1,
            5 => cap.max_work -= 1,
            _ => unreachable!(),
        }
        assert!(matches!(
            decode_semantic_parameters(&input, SOURCE, cap, &Control::default()),
            Err(Error::InvalidShape(_))
        ));
    }
    let mut excess = source();
    if let Some(wire::semantic_parameter::Value::TimeZone(zone)) = &mut excess.entries[1].value {
        zone.reserve_exact(SOURCE * 2);
    }
    assert!(matches!(
        prepare_semantic_parameters_decode(&excess, SOURCE, limits(), &Control::default()),
        Err(Error::InvalidShape(_))
    ));
    let empty = wire::SemanticParameters {
        entries: Vec::with_capacity(100),
    };
    assert!(matches!(
        prepare_semantic_parameters_decode(
            &empty,
            size_of::<wire::SemanticParameters>(),
            limits(),
            &Control::default()
        ),
        Err(Error::InvalidShape(_))
    ));
    let wide = wire::SemanticParameters {
        entries: (0..320)
            .map(|id| {
                entry(
                    id,
                    wire::semantic_parameter::Value::AllowThrowException(false),
                )
            })
            .collect(),
    };
    let control = Control::default();
    assert!(matches!(
        prepare_semantic_parameters_decode(
            &wide,
            SOURCE,
            ParameterProjectionLimits {
                max_work: 256,
                ..limits()
            },
            &control
        ),
        Err(Error::InvalidShape(_))
    ));
    assert!(
        !control
            .trace
            .lock()
            .unwrap()
            .iter()
            .any(|(_, units)| *units == 256)
    );
}
#[test]
fn bounded_parameters_preserve_constructor_and_lazy_decoder_error_order() {
    use wire::semantic_parameter::Value::*;
    let cases = [
        (
            vec![
                entry(0, AllowThrowException(false)),
                entry(0, TimeZone("".into())),
                wire::SemanticParameter { id: 9, value: None },
            ],
            SemanticParameterError::DuplicateId(SemanticParameterId::new(0)),
        ),
        (
            vec![
                entry(0, StatementStartUtcMicros(1)),
                entry(1, StatementStartUtcMicros(2)),
                entry(2, TimeZone("".into())),
            ],
            SemanticParameterError::DuplicateStatementStart,
        ),
        (
            vec![
                entry(0, AllowThrowException(false)),
                entry(0, TimeZone("x".repeat(256))),
            ],
            SemanticParameterError::InvalidTimeZone,
        ),
    ];
    for (entries, expected) in cases {
        let input = wire::SemanticParameters { entries };
        assert!(
            matches!(decode_semantic_parameters(&input, SOURCE, limits(), &Control::default()), Err(Error::Parameter(actual)) if actual == expected)
        );
    }
    let missing = wire::SemanticParameters {
        entries: vec![
            entry(0, AllowThrowException(true)),
            wire::SemanticParameter { id: 1, value: None },
        ],
    };
    assert!(matches!(
        decode_semantic_parameters(&missing, SOURCE, limits(), &Control::default()),
        Err(Error::InvalidShape(
            "semantic parameter is missing its value variant"
        ))
    ));
    let too_many = wire::SemanticParameters {
        entries: vec![wire::SemanticParameter { id: 0, value: None }; MAX_SEMANTIC_PARAMETERS + 1],
    };
    assert!(matches!(
        decode_semantic_parameters(&too_many, SOURCE, limits(), &Control::default()),
        Err(Error::Parameter(SemanticParameterError::TooManyParameters))
    ));
}
#[test]
fn bounded_parameters_timezone_byte_edges_and_empty_request_counts_follow_original_owner() {
    for zone in ["Z".to_string(), "x".repeat(255), "雪".repeat(85)] {
        let input = wire::SemanticParameters {
            entries: vec![entry(
                0,
                wire::semantic_parameter::Value::TimeZone(zone.clone()),
            )],
        };
        let (table, facts) =
            decode_semantic_parameters(&input, SOURCE, limits(), &Control::default()).unwrap();
        assert_eq!(facts.timezone_request_bytes_upper_bound, zone.len());
        assert_eq!(facts.allocation_requests_upper_bound, 2);
        assert_eq!(
            table.get(SemanticParameterId::new(0)),
            Some(&SemanticParameterValue::TimeZone(zone.into_boxed_str()))
        );
    }
    for zone in ["".to_string(), "x".repeat(256), "a\nb".to_string()] {
        let input = wire::SemanticParameters {
            entries: vec![entry(0, wire::semantic_parameter::Value::TimeZone(zone))],
        };
        assert!(matches!(
            decode_semantic_parameters(&input, SOURCE, limits(), &Control::default()),
            Err(Error::Parameter(SemanticParameterError::InvalidTimeZone))
        ));
    }
    let empty = wire::SemanticParameters { entries: vec![] };
    let (_, facts) =
        decode_semantic_parameters(&empty, SOURCE, limits(), &Control::default()).unwrap();
    assert_eq!(facts.allocation_requests_upper_bound, 0);
    assert_eq!(facts.allocation_request_bytes_upper_bound, 0);
}
#[test]
fn bounded_parameters_every_actual_callback_retains_first_control_and_never_publishes_a_prefix() {
    let cases = [
        source(),
        wire::SemanticParameters { entries: vec![] },
        wire::SemanticParameters {
            entries: vec![entry(
                0,
                wire::semantic_parameter::Value::TimeZone("x".repeat(255)),
            )],
        },
        wire::SemanticParameters {
            entries: vec![
                entry(
                    0,
                    wire::semantic_parameter::Value::AllowThrowException(true),
                ),
                wire::SemanticParameter { id: 1, value: None },
            ],
        },
        wire::SemanticParameters {
            entries: vec![entry(
                0,
                wire::semantic_parameter::Value::TimeZone("a\nb".into()),
            )],
        },
    ];
    for (case, input) in cases.iter().enumerate() {
        let control = Control::default();
        let result = decode_semantic_parameters(input, SOURCE, limits(), &control);
        assert_eq!(result.is_err(), case >= 3);
        let trace = control.trace.into_inner().unwrap();
        assert_eq!(trace[0], (CompilePhase::Decode, 0));
        if case == 2 {
            assert!(trace.iter().any(|(_, units)| *units == 256));
        }
        if case == 3 {
            assert_eq!(trace.last().unwrap().1, 0);
        }
        for at in 0..trace.len() {
            for cause in CAUSES {
                let control = Control {
                    stop: Some((at, cause)),
                    ..Control::default()
                };
                assert!(
                    matches!(decode_semantic_parameters(input, SOURCE, limits(), &control), Err(Error::Control(actual)) if actual == cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}
