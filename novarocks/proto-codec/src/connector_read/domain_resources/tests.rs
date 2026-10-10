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
use novarocks_spi::connector::read_stack::{ConnectorValueType, ValueSet};
use novarocks_type_contract::{CompilePhase, PureCompileControl};
use std::{
    cell::Cell,
    sync::{Arc, Mutex},
};
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    events: Mutex<Vec<u32>>,
    stop: Mutex<Option<(usize, CompileControlError)>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut e = self.events.lock().unwrap();
        let at = e.len();
        let stop = *self.stop.lock().unwrap();
        if let Some((stop, _)) = stop {
            assert!(at <= stop, "callback after primary refusal");
        }
        e.push(units);
        match stop {
            Some((stop, c)) if stop == at => Err(c),
            _ => Ok(()),
        }
    }
}
fn domain(value: ConnectorValue) -> Domain {
    Domain::new(
        ValueSet::of_ranges(value.value_type(), vec![Range::equal(value)]).unwrap(),
        true,
    )
}
fn run(
    raw: &dto::Domain,
    stop: Option<(usize, CompileControlError)>,
) -> (
    Result<(Domain, DomainResourceFacts), DomainCodecError>,
    Vec<u32>,
) {
    let c = Control::default();
    *c.stop.lock().unwrap() = stop;
    let out = (|| {
        let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Decode)?;
        let out = decode_domain_observed(raw, "domain", &mut |_| Ok(()), &mut w);
        if !matches!(&out, Err(DomainCodecError::Control(_))) {
            w.finish()?;
        }
        out
    })();
    (out, c.events.into_inner().unwrap())
}
#[test]
fn encoder_counts_real_range_and_each_byte_arm_request_independently() {
    let cases = [
        (ConnectorValue::BigInt(7), 0usize),
        (ConnectorValue::try_decimal(123, 9, 2).unwrap(), 16),
        (ConnectorValue::Uuid([3; 16]), 16),
        (ConnectorValue::Varchar(Arc::from("雪")), 3),
        (ConnectorValue::Varbinary(Arc::from([0, 255, 7])), 3),
        (ConnectorValue::Fixed(Arc::from([1, 2, 3, 4])), 4),
    ];
    for (value, n) in cases {
        let domain = domain(value);
        let f = domain_encode_resource_facts(&domain).unwrap();
        assert_eq!(f.range_count, 1);
        assert_eq!(f.scalar_bytes, 2 * n);
        assert_eq!(
            f.allocation_requests_upper_bound,
            if n == 0 { 1 } else { 3 }
        );
        assert_eq!(
            f.allocation_request_bytes_upper_bound,
            Layout::array::<dto::Range>(1).unwrap().size() + 2 * n
        );
        let c = Control::default();
        let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
        let (actual, observed) = encode_domain_observed(&domain, &mut |_| Ok(()), &mut w).unwrap();
        w.finish().unwrap();
        assert_eq!(actual, encode_domain(&domain));
        assert_eq!(observed, f);
        assert_eq!(
            decode_domain(&actual, FieldPath::root("old")).unwrap(),
            domain
        );
    }
}
#[test]
fn decoder_prefunds_original_path_and_conditional_sort_requests_without_changing_ranges() {
    let raw = encode_domain(&Domain::new(
        ValueSet::none(ConnectorValueType::BigInt),
        false,
    ));
    let empty = domain_decode_resource_facts(&raw).unwrap();
    assert_eq!(empty.range_count, 0);
    assert_eq!(empty.allocation_requests_upper_bound, 16);
    assert_eq!(
        empty.allocation_request_bytes_upper_bound,
        12 * Layout::array::<FieldPathSegment>(16).unwrap().size() + 4 * 256
    );
    let ranges = (0..64)
        .map(|i| Range::equal(ConnectorValue::BigInt(i * 2)))
        .collect::<Vec<_>>();
    let source = Domain::new(
        ValueSet::of_ranges(ConnectorValueType::BigInt, ranges).unwrap(),
        true,
    );
    let raw = encode_domain(&source);
    let facts = domain_decode_resource_facts(&raw).unwrap();
    // Two original Range buffers coexist with the locked sort's optional one;
    // full 64 slots dominate its 48-slot small-sort scratch requirement.
    let range_bytes = Layout::array::<Range>(64).unwrap().size();
    assert_eq!(
        facts.allocation_requests_upper_bound,
        12 + 24 * 64 + 2 + 1 + 4
    );
    assert_eq!(
        facts.allocation_request_bytes_upper_bound,
        (12 + 24 * 64) * Layout::array::<FieldPathSegment>(16).unwrap().size()
            + 3 * range_bytes
            + 4 * 256
    );
    assert_eq!(run(&raw, None).0.unwrap().0, source);
    let mut missing = raw;
    missing.values.as_mut().unwrap().ranges[0].low = None;
    let error = run(&missing, None).0.unwrap_err();
    let DomainCodecError::Protocol(error) = error else {
        panic!("original protocol error")
    };
    assert_eq!(error.kind(), ProtocolErrorKind::MissingField);
    assert_eq!(error.path().to_string(), "domain.values.ranges[0].low");
}
#[test]
fn every_actual_success_and_ordinary_callback_keeps_original_control_and_footer() {
    let success = encode_domain(&domain(ConnectorValue::BigInt(7)));
    let mut wrong = success.clone();
    wrong
        .values
        .as_mut()
        .unwrap()
        .value_type
        .as_mut()
        .unwrap()
        .kind = 0;
    for (raw, expected) in [(&success, true), (&wrong, false)] {
        let (out, trace) = run(raw, None);
        assert_eq!(out.is_ok(), expected);
        assert!(!trace.is_empty());
        for at in 0..trace.len() {
            for cause in CAUSES {
                let (out, actual) = run(raw, Some((at, cause)));
                assert!(matches!(out,Err(DomainCodecError::Control(c))if c==cause));
                assert_eq!(actual, &trace[..=at]);
            }
        }
    }
}
#[test]
fn known_parent_request_refusal_wins_before_pending_control_and_nan_diagnostics_are_cumulative() {
    let raw = encode_domain(&domain(ConnectorValue::BigInt(7)));
    for pending in [0, 254, 255] {
        for cause in CAUSES {
            let c = Control::default();
            let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
            for _ in 0..pending {
                w.step().unwrap();
            }
            let before = c.events.lock().unwrap().clone();
            *c.stop.lock().unwrap() = Some((before.len(), cause));
            let calls = Cell::new(0);
            let result = decode_domain_observed(
                &raw,
                "domain",
                &mut |facts| {
                    calls.set(calls.get() + 1);
                    assert!(facts.allocation_requests_upper_bound > 0);
                    Err(CompileControlError::ResourceExhausted)
                },
                &mut w,
            );
            assert!(matches!(
                result,
                Err(DomainCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(calls.get(), 1);
            assert_eq!(*c.events.lock().unwrap(), before);
        }
    }
    // An unbounded high allows each NaN low through Range construction. The
    // original normalizer creates ordinary comparator diagnostics, not a mask
    // or a new semantic success. Their real String requests need an upper bound.
    let raw = dto::Domain {
        values: Some(dto::ValueSet {
            value_type: Some(super::super::value::encode_value_type(
                ConnectorValueType::Real,
            )),
            ranges: (0..4)
                .map(|_| dto::Range {
                    low: Some(dto::Bound {
                        kind: dto::BoundKind::Inclusive as i32,
                        value: Some(dto::Value {
                            value: Some(dto::value::Value::Real(f32::NAN)),
                        }),
                    }),
                    high: Some(dto::Bound {
                        kind: dto::BoundKind::Unbounded as i32,
                        value: None,
                    }),
                })
                .collect(),
        }),
        null_allowed: false,
    };
    let mut ordinary = raw.clone();
    for range in &mut ordinary.values.as_mut().unwrap().ranges {
        range.low.as_mut().unwrap().value.as_mut().unwrap().value =
            Some(dto::value::Value::Real(1.0));
    }
    let bad = domain_decode_resource_facts(&raw).unwrap();
    let good = domain_decode_resource_facts(&ordinary).unwrap();
    assert!(bad.allocation_requests_upper_bound > good.allocation_requests_upper_bound + 4);
    assert!(matches!(
        run(&raw, None).0,
        Err(DomainCodecError::Protocol(_))
    ));
}

#[test]
fn original_scalar_and_type_paths_have_independent_requests_and_error_oracles() {
    let type_facts = value_type_decode_resource_facts().unwrap();
    let path_bytes = Layout::array::<FieldPathSegment>(16).unwrap().size();
    assert_eq!(type_facts.allocation_requests_upper_bound, 5);
    assert_eq!(
        type_facts.allocation_request_bytes_upper_bound,
        4 * path_bytes + 256
    );
    let raw_type = super::super::value::encode_value_type(ConnectorValueType::BigInt);
    assert_eq!(
        super::super::value::decode_value_type(&raw_type, FieldPath::root("scalar_type")).unwrap(),
        ConnectorValueType::BigInt
    );
    let mut wrong_type = raw_type;
    wrong_type.kind = 0;
    let error = super::super::value::decode_value_type(&wrong_type, FieldPath::root("scalar_type"))
        .unwrap_err();
    assert_eq!(error.kind(), ProtocolErrorKind::InvalidEnum);
    assert_eq!(error.path().to_string(), "scalar_type.kind");
    let raw = dto::Value {
        value: Some(dto::value::Value::Varbinary(vec![0, 255, 7])),
    };
    let value_facts = value_decode_resource_facts(&raw).unwrap();
    let arc = novarocks_type_contract::owned_resources::layout::arc_layout(
        Layout::array::<u8>(3).unwrap(),
    )
    .unwrap()
    .size();
    assert_eq!(value_facts.scalar_bytes, 3);
    assert_eq!(value_facts.allocation_requests_upper_bound, 9);
    assert_eq!(
        value_facts.allocation_request_bytes_upper_bound,
        7 * path_bytes + 256 + arc
    );
    let error = super::super::value::decode_value(
        &raw,
        ConnectorValueType::BigInt,
        FieldPath::root("scalar_value"),
    )
    .unwrap_err();
    assert_eq!(error.kind(), ProtocolErrorKind::InvalidValue);
    assert_eq!(error.path().to_string(), "scalar_value");
    // Decimal's eager exact-length path has completed before the later invalid
    // precision path. Both original path allocations are covered, not B-based.
    let decimal = dto::Value {
        value: Some(dto::value::Value::Decimal(dto::DecimalValue {
            unscaled: vec![0; 16],
            precision: 0,
            scale: 0,
        })),
    };
    let f = value_decode_resource_facts(&decimal).unwrap();
    assert_eq!(f.allocation_requests_upper_bound, 8);
    assert_eq!(f.allocation_request_bytes_upper_bound, 7 * path_bytes + 256);
    let e = super::super::value::decode_value(
        &decimal,
        ConnectorValueType::Decimal {
            precision: 1,
            scale: 0,
        },
        FieldPath::root("decimal"),
    )
    .unwrap_err();
    assert_eq!(e.kind(), ProtocolErrorKind::OutOfRange);
    assert_eq!(e.path().to_string(), "decimal.decimal");
}

#[test]
fn admitted_count_matches_pure_author_and_gates_each_real_scalar_prefix() {
    let source = domain(ConnectorValue::Varbinary(Arc::from([0, 255, 7])));
    let raw = encode_domain(&source);
    for encode in [false, true] {
        let c = Control::default();
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        let mut snapshots = Vec::new();
        let actual = if encode {
            domain_encode_resource_facts_admitted(
                &source,
                &mut |f| {
                    snapshots.push(*f);
                    Ok(())
                },
                &mut work,
            )
            .unwrap()
        } else {
            domain_decode_resource_facts_admitted(
                &raw,
                &mut |f| {
                    snapshots.push(*f);
                    Ok(())
                },
                &mut work,
            )
            .unwrap()
        };
        work.finish().unwrap();
        let passive = if encode {
            domain_encode_resource_facts(&source).unwrap()
        } else {
            domain_decode_resource_facts(&raw).unwrap()
        };
        assert_eq!(actual, passive);
        assert_eq!(snapshots.len(), 3); // actual initial header + low + high
        assert_eq!(snapshots[0].range_count, 1);
        assert_eq!(
            (
                snapshots[0].scalar_bytes,
                snapshots[1].scalar_bytes,
                snapshots[2].scalar_bytes
            ),
            (0, 3, 6)
        );
        let (before_first_bytes, after_first_bytes) = (
            snapshots[0].allocation_request_bytes_upper_bound,
            snapshots[1].allocation_request_bytes_upper_bound,
        );
        let bytes = if encode {
            3
        } else {
            novarocks_type_contract::owned_resources::layout::arc_layout(
                Layout::array::<u8>(3).unwrap(),
            )
            .unwrap()
            .size()
        };
        assert_eq!(after_first_bytes - before_first_bytes, bytes);
        assert_eq!(
            snapshots[1].allocation_requests_upper_bound
                - snapshots[0].allocation_requests_upper_bound,
            1
        );
    }
    // The initial header's actual Count step fills pending254 to255. Capturing
    // the low scalar's length then finds Resource before the next step/callback.
    for cause in CAUSES {
        let c = Control::default();
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        for _ in 0..254 {
            work.step().unwrap();
        }
        let before = c.events.lock().unwrap().clone();
        *c.stop.lock().unwrap() = Some((before.len(), cause));
        let mut calls = 0;
        let result = domain_decode_resource_facts_admitted(
            &raw,
            &mut |f| {
                calls += 1;
                if f.scalar_bytes > 2 {
                    Err(CompileControlError::ResourceExhausted)
                } else {
                    Ok(())
                }
            },
            &mut work,
        );
        assert!(matches!(
            result,
            Err(DomainCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(calls, 2);
        assert_eq!(*c.events.lock().unwrap(), before);
    }
}
