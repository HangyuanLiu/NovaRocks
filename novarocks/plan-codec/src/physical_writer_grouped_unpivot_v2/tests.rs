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
use crate::physical_unpivot_v2::tests::{Control, Fixture, limits};
use novarocks_type_contract::CompileControlError;
use std::alloc::Layout;
const SOURCE: usize = 1 << 20;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
fn reference(pool: u32, ordinal: u32) -> p::ConstantReference {
    p::ConstantReference {
        pool: p::ConstantPoolId::new(pool),
        ordinal,
    }
}
fn source() -> p::WriterGroupedUnpivotSpec {
    let constants = |id| {
        Box::from([
            p::UnpivotConstant::Scalar(p::ExprId::new(id)),
            p::UnpivotConstant::Int32List(reference(0, 1)),
            p::UnpivotConstant::Utf8Map(reference(7, 1)),
        ])
    };
    p::WriterGroupedUnpivotSpec {
        // The component preserves raw target order/multiplicity. The Fragment
        // writer author independently rejects duplicate/unsorted targets.
        statistics_target_ordinals: Box::from([
            p::WriteTargetOrdinal::try_new(4095).unwrap(),
            p::WriteTargetOrdinal::try_new(0).unwrap(),
            p::WriteTargetOrdinal::try_new(4095).unwrap(),
        ]),
        grouping_input: p::ValueId::new(0),
        grouping_output: p::ValueId::new(u32::MAX),
        passthrough_output: p::ValueId::new(7),
        value_output: p::ValueId::new(7),
        literal_outputs: Box::from([
            p::ValueId::new(u32::MAX),
            p::ValueId::new(0),
            p::ValueId::new(u32::MAX),
        ]),
        mappings: Box::from([
            p::WriterGroupedUnpivotMapping {
                write_target_ordinal: p::WriteTargetOrdinal::try_new(4095).unwrap(),
                input: p::ValueId::new(0),
                constants: constants(0),
            },
            p::WriterGroupedUnpivotMapping {
                write_target_ordinal: p::WriteTargetOrdinal::try_new(0).unwrap(),
                input: p::ValueId::new(7),
                constants: constants(u32::MAX),
            },
        ]),
        max_output_rows: u64::MAX,
        max_output_bytes: 1 << 63,
    }
}
fn expected() -> wire::WriterGroupedUnpivot {
    let constants = |id| {
        vec![
            wire::UnpivotConstant {
                kind: Some(wire::unpivot_constant::Kind::ScalarExprId(id)),
            },
            wire::UnpivotConstant {
                kind: Some(wire::unpivot_constant::Kind::Int32List(
                    wire::ConstantReference {
                        pool_id: Some(0),
                        row_ordinal: 1,
                    },
                )),
            },
            wire::UnpivotConstant {
                kind: Some(wire::unpivot_constant::Kind::Utf8Map(
                    wire::ConstantReference {
                        pool_id: Some(7),
                        row_ordinal: 1,
                    },
                )),
            },
        ]
    };
    wire::WriterGroupedUnpivot {
        statistics_target_ordinals: vec![4095, 0, 4095],
        grouping_input_value_id: Some(0),
        grouping_output_value_id: Some(u32::MAX),
        passthrough_output_value_id: Some(7),
        value_output_id: Some(7),
        literal_output_ids: vec![u32::MAX, 0, u32::MAX],
        mappings: vec![
            wire::WriterGroupedUnpivotMapping {
                write_target_ordinal: 4095,
                input_value_id: Some(0),
                constants: constants(0),
            },
            wire::WriterGroupedUnpivotMapping {
                write_target_ordinal: 0,
                input_value_id: Some(7),
                constants: constants(u32::MAX),
            },
        ],
        max_output_rows: u64::MAX,
        max_output_bytes: 1 << 63,
    }
}
fn exact(f: WriterGroupedUnpivotProjectionFacts) -> WriterGroupedUnpivotProjectionLimits {
    WriterGroupedUnpivotProjectionLimits {
        max_input_nodes: 0,
        max_value_references: f.value_reference_count,
        max_list_items: f.list_item_count,
        max_allocation_requests: f.allocation_requests_upper_bound,
        max_allocation_request_bytes: f.allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: f.coexisting_source_and_request_bytes_upper_bound,
        max_work: f.cumulative_work_upper_bound,
        ..limits()
    }
}
fn under(
    mut l: WriterGroupedUnpivotProjectionLimits,
    at: usize,
) -> WriterGroupedUnpivotProjectionLimits {
    match at {
        0 => l.max_value_references -= 1,
        1 => l.max_list_items -= 1,
        2 => l.max_allocation_requests -= 1,
        3 => l.max_allocation_request_bytes -= 1,
        4 => l.max_coexisting_source_and_request_bytes -= 1,
        5 => l.max_work -= 1,
        _ => panic!(),
    }
    l
}
fn assert_prefix<T>(c: &Control, run: impl Fn() -> Result<T, Error>) {
    c.arm(None);
    let _ = run();
    let trace = c.trace();
    assert!(!trace.is_empty());
    for stop in 0..trace.len() {
        for cause in CAUSES {
            c.arm(Some((stop, cause)));
            assert!(matches!(run(), Err(Error::Control(actual)) if actual == cause));
            assert_eq!(c.trace(), trace[..=stop]);
        }
    }
    c.arm(None);
}

#[test]
fn grouped_unpivot_exact_raw_payload_and_independent_allocation_layouts() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, expressions, read| {
        c.arm(None);
        let (encoded, f) =
            encode_writer_grouped_unpivot(&source(), values, expressions, SOURCE, limits())
                .unwrap();
        assert_eq!(encoded, expected());
        c.arm(None);
        let (decoded, r) =
            decode_writer_grouped_unpivot(&expected(), read, SOURCE, limits()).unwrap();
        assert_eq!(decoded, source());
        assert_eq!(f.input_node_count, 0);
        assert_eq!(f.value_reference_count, 9);
        assert_eq!(f.list_item_count, 14);
        assert_eq!(f.allocation_requests_upper_bound, 5);
        assert_eq!(r.allocation_requests_upper_bound, 10);
        let size = |layout: Layout| layout.size();
        let send = size(Layout::array::<u32>(6).unwrap())
            + size(Layout::array::<wire::WriterGroupedUnpivotMapping>(2).unwrap())
            + size(Layout::array::<wire::UnpivotConstant>(6).unwrap());
        let receive = 2
            * (size(Layout::array::<p::WriteTargetOrdinal>(3).unwrap())
                + size(Layout::array::<p::ValueId>(3).unwrap())
                + size(Layout::array::<p::WriterGroupedUnpivotMapping>(2).unwrap())
                + size(Layout::array::<p::UnpivotConstant>(6).unwrap()));
        assert_eq!(f.allocation_request_bytes_upper_bound, send);
        assert_eq!(r.allocation_request_bytes_upper_bound, receive);
        assert_eq!(
            f.coexisting_source_and_request_bytes_upper_bound,
            SOURCE + send
        );
        assert_eq!(
            r.coexisting_source_and_request_bytes_upper_bound,
            SOURCE + receive
        );
        assert!(std::ptr::eq(expressions.pools(), read.pools()));
        let mut zero = source();
        zero.max_output_rows = 0;
        zero.max_output_bytes = 0;
        c.arm(None);
        let (wire, _) =
            encode_writer_grouped_unpivot(&zero, values, expressions, SOURCE, limits()).unwrap();
        assert_eq!(
            decode_writer_grouped_unpivot(&wire, read, SOURCE, limits())
                .unwrap()
                .0,
            zero
        );
        let mut empty = source();
        empty.statistics_target_ordinals = Box::default();
        empty.literal_outputs = Box::default();
        empty.mappings = Box::default();
        c.arm(None);
        let (encoded, facts) =
            encode_writer_grouped_unpivot(&empty, values, expressions, SOURCE, limits()).unwrap();
        assert_eq!(facts.allocation_requests_upper_bound, 0);
        assert_eq!(facts.allocation_request_bytes_upper_bound, 0);
        assert_eq!(facts.value_reference_count, 4);
        let (decoded, facts) =
            decode_writer_grouped_unpivot(&encoded, read, SOURCE, limits()).unwrap();
        assert_eq!(decoded, empty);
        assert_eq!(facts.allocation_requests_upper_bound, 0);
    });
}
#[test]
fn grouped_unpivot_prepared_tokens_keep_original_namespaces_and_refuse_foreign_loans() {
    let fixture = Fixture::new();
    let other = Fixture::new();
    let c = Control::default();
    let different = Control::default();
    let input = source();
    let received = expected();
    fixture.with_tokens(&c, |values, expressions, read| {
        c.arm(None);
        let prepared =
            prepare_writer_grouped_unpivot_encode(&input, values, expressions, SOURCE, limits())
                .unwrap();
        let f = *prepared.facts();
        assert_eq!(prepared.emit().unwrap(), (expected(), f));
        c.arm(None);
        let prepared =
            prepare_writer_grouped_unpivot_decode(&received, read, SOURCE, limits()).unwrap();
        let f = *prepared.facts();
        assert_eq!(prepared.emit().unwrap(), (source(), f));
        c.arm(None);
        other.with_tokens(&c, |_, foreign, _| {
            c.arm(None);
            assert!(matches!(
                encode_writer_grouped_unpivot(&input, values, foreign, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
        });
        different.arm(None);
        other.with_tokens(&different, |_, foreign, _| {
            c.arm(None);
            assert!(matches!(
                encode_writer_grouped_unpivot(&input, values, foreign, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
        });
    });
}
#[test]
fn grouped_unpivot_closed_presence_and_selected_address_errors_remain_distinct() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, expressions, read| {
        for at in 0..7 {
            let mut raw = expected();
            match at {
                0 => raw.grouping_input_value_id = None,
                1 => raw.grouping_output_value_id = None,
                2 => raw.passthrough_output_value_id = None,
                3 => raw.value_output_id = None,
                4 => raw.mappings[0].input_value_id = None,
                5 => raw.mappings[0].constants[0].kind = None,
                6 => {
                    raw.mappings[0].constants[1].kind = Some(
                        wire::unpivot_constant::Kind::Int32List(wire::ConstantReference {
                            pool_id: None,
                            row_ordinal: 1,
                        }),
                    )
                }
                _ => unreachable!(),
            }
            c.arm(None);
            assert!(matches!(
                decode_writer_grouped_unpivot(&raw, read, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
        }
        for id in [7, 99] {
            let mut input = source();
            input.mappings[0].constants[0] = p::UnpivotConstant::Scalar(p::ExprId::new(id));
            c.arm(None);
            assert!(matches!(
                encode_writer_grouped_unpivot(&input, values, expressions, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
            let mut raw = expected();
            raw.mappings[0].constants[0].kind =
                Some(wire::unpivot_constant::Kind::ScalarExprId(id));
            c.arm(None);
            assert!(matches!(
                decode_writer_grouped_unpivot(&raw, read, SOURCE, limits()),
                Err(Error::InvalidShape(_))
            ));
        }
        for invalid_ordinal in [4096, u32::MAX] {
            for mapping in [false, true] {
                let mut raw = expected();
                if mapping {
                    raw.mappings[0].write_target_ordinal = invalid_ordinal;
                } else {
                    raw.statistics_target_ordinals[0] = invalid_ordinal;
                }
                c.arm(None);
                assert!(matches!(
                    decode_writer_grouped_unpivot(&raw, read, SOURCE, limits()),
                    Err(Error::Identity(novarocks_connector_contract::ConnectorIdentityError::InvalidWriteTargetOrdinal))
                ));
            }
        }
        for selected in [reference(99, 0), reference(0, 2)] {
            let mut input = source();
            input.mappings[0].constants[1] = p::UnpivotConstant::Int32List(selected);
            c.arm(None);
            assert!(matches!(
                encode_writer_grouped_unpivot(&input, values, expressions, SOURCE, limits()),
                Err(Error::Constant(_))
            ));
            let mut raw = expected();
            raw.mappings[0].constants[1].kind = Some(wire::unpivot_constant::Kind::Int32List(
                wire::ConstantReference {
                    pool_id: Some(selected.pool.get()),
                    row_ordinal: selected.ordinal,
                },
            ));
            c.arm(None);
            assert!(matches!(
                decode_writer_grouped_unpivot(&raw, read, SOURCE, limits()),
                Err(Error::Constant(_))
            ));
        }
        let mut input = source();
        input.grouping_output = p::ValueId::new(99);
        c.arm(None);
        assert!(matches!(
            encode_writer_grouped_unpivot(&input, values, expressions, SOURCE, limits()),
            Err(Error::InvalidShape(_))
        ));
        let mut raw = expected();
        raw.grouping_output_value_id = Some(99);
        c.arm(None);
        assert!(matches!(
            decode_writer_grouped_unpivot(&raw, read, SOURCE, limits()),
            Err(Error::InvalidShape(_))
        ));
        // This component only verifies original addresses. Swapped collection
        // profiles and raw target order still require the writer Fragment gate.
        let mut input = source();
        input.mappings[0].constants[1] = p::UnpivotConstant::Int32List(reference(7, 1));
        c.arm(None);
        assert!(
            encode_writer_grouped_unpivot(&input, values, expressions, SOURCE, limits()).is_ok()
        );
    });
}
#[test]
fn grouped_unpivot_six_resource_axes_and_actual_wire_capacities_precede_emission() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, expressions, read| {
        c.arm(None);
        let (_, f) =
            encode_writer_grouped_unpivot(&source(), values, expressions, SOURCE, limits())
                .unwrap();
        c.arm(None);
        assert!(
            encode_writer_grouped_unpivot(&source(), values, expressions, SOURCE, exact(f)).is_ok()
        );
        for axis in 0..6 {
            c.arm(None);
            assert!(matches!(
                encode_writer_grouped_unpivot(
                    &source(),
                    values,
                    expressions,
                    SOURCE,
                    under(exact(f), axis)
                ),
                Err(Error::InvalidShape(_))
            ));
        }
        c.arm(None);
        let (_, f) = decode_writer_grouped_unpivot(&expected(), read, SOURCE, limits()).unwrap();
        c.arm(None);
        assert!(decode_writer_grouped_unpivot(&expected(), read, SOURCE, exact(f)).is_ok());
        for axis in 0..6 {
            c.arm(None);
            assert!(matches!(
                decode_writer_grouped_unpivot(&expected(), read, SOURCE, under(exact(f), axis)),
                Err(Error::InvalidShape(_))
            ));
        }
        let mut capacity = expected();
        capacity.statistics_target_ordinals.reserve(SOURCE);
        c.arm(None);
        assert!(matches!(
            decode_writer_grouped_unpivot(&capacity, read, SOURCE, limits()),
            Err(Error::InvalidShape(_))
        ));
        // Even before walking 320 inner constants, their actual count must
        // obey the outer caller's source/list/work envelope.
        let mut wide = source();
        wide.mappings[0].constants =
            vec![p::UnpivotConstant::Scalar(p::ExprId::new(0)); 320].into_boxed_slice();
        let mut l = limits();
        l.max_list_items = 20;
        c.arm(None);
        assert!(matches!(
            encode_writer_grouped_unpivot(&wide, values, expressions, SOURCE, l),
            Err(Error::InvalidShape(_))
        ));
        assert!(!c.trace().iter().any(|(_, units)| *units == 256));
        c.arm(None);
        assert!(matches!(
            encode_writer_grouped_unpivot(&source(), values, expressions, 0, limits()),
            Err(Error::InvalidShape(_))
        ));
    });
}
#[test]
fn grouped_unpivot_every_small_callback_preserves_three_primary_causes_and_ordinary_tails() {
    let fixture = Fixture::new();
    let c = Control::default();
    let input = source();
    let raw = expected();
    fixture.with_tokens(&c, |values, expressions, read| {
        assert_prefix(&c, || {
            encode_writer_grouped_unpivot(&input, values, expressions, SOURCE, limits())
        });
        assert_prefix(&c, || {
            decode_writer_grouped_unpivot(&raw, read, SOURCE, limits())
        });
        assert_prefix(&c, || {
            prepare_writer_grouped_unpivot_encode(&input, values, expressions, SOURCE, limits())
        });
        assert_prefix(&c, || {
            prepare_writer_grouped_unpivot_decode(&raw, read, SOURCE, limits())
        });
        let mut bad = source();
        bad.mappings[1].constants[2] = p::UnpivotConstant::Scalar(p::ExprId::new(7));
        assert_prefix(&c, || {
            encode_writer_grouped_unpivot(&bad, values, expressions, SOURCE, limits())
        });
        let mut bad = expected();
        bad.mappings[1].constants[2].kind = Some(wire::unpivot_constant::Kind::ScalarExprId(7));
        assert_prefix(&c, || {
            decode_writer_grouped_unpivot(&bad, read, SOURCE, limits())
        });
        // A prepared emitter does not re-prepare. Each rerun obtains a fresh
        // admitted token before arming only this emitter's original meter.
        c.arm(None);
        let token =
            prepare_writer_grouped_unpivot_encode(&input, values, expressions, SOURCE, limits())
                .unwrap();
        c.arm(None);
        token.emit().unwrap();
        let trace = c.trace();
        for stop in 0..trace.len() {
            for cause in CAUSES {
                c.arm(None);
                let token = prepare_writer_grouped_unpivot_encode(
                    &input,
                    values,
                    expressions,
                    SOURCE,
                    limits(),
                )
                .unwrap();
                c.arm(Some((stop, cause)));
                assert!(matches!(token.emit(), Err(Error::Control(actual)) if actual == cause));
                assert_eq!(c.trace(), trace[..=stop]);
            }
        }
        c.arm(None);
        let token = prepare_writer_grouped_unpivot_decode(&raw, read, SOURCE, limits()).unwrap();
        c.arm(None);
        token.emit().unwrap();
        let trace = c.trace();
        for stop in 0..trace.len() {
            for cause in CAUSES {
                c.arm(None);
                let token =
                    prepare_writer_grouped_unpivot_decode(&raw, read, SOURCE, limits()).unwrap();
                c.arm(Some((stop, cause)));
                assert!(matches!(token.emit(), Err(Error::Control(actual)) if actual == cause));
                assert_eq!(c.trace(), trace[..=stop]);
            }
        }
        c.arm(None);
    });
}
#[test]
fn grouped_unpivot_wide_inner_constants_observe_real_quantum_and_no_retry() {
    let fixture = Fixture::new();
    let c = Control::default();
    let mut input = source();
    input.mappings[0].constants =
        vec![p::UnpivotConstant::Scalar(p::ExprId::new(0)); 320].into_boxed_slice();
    let mut raw = expected();
    raw.mappings[0].constants = vec![
        wire::UnpivotConstant {
            kind: Some(wire::unpivot_constant::Kind::ScalarExprId(0))
        };
        320
    ];
    fixture.with_tokens(&c, |values, expressions, read| {
        c.arm(None);
        encode_writer_grouped_unpivot(&input, values, expressions, SOURCE, limits()).unwrap();
        let send = c.trace();
        c.arm(None);
        decode_writer_grouped_unpivot(&raw, read, SOURCE, limits()).unwrap();
        let receive = c.trace();
        for (decode, trace) in [(false, send), (true, receive)] {
            let quantum = trace.iter().position(|(_, units)| *units == 256).unwrap();
            for stop in [0, quantum, trace.len() - 1] {
                for cause in CAUSES {
                    c.arm(Some((stop, cause)));
                    let refused = if decode {
                        decode_writer_grouped_unpivot(&raw, read, SOURCE, limits()).map(|_| ())
                    } else {
                        encode_writer_grouped_unpivot(&input, values, expressions, SOURCE, limits())
                            .map(|_| ())
                    };
                    assert!(matches!(refused, Err(Error::Control(actual)) if actual == cause));
                    assert_eq!(c.trace(), trace[..=stop]);
                }
            }
        }
        c.arm(None);
    });
}
