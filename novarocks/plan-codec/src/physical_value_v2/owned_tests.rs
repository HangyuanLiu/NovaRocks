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

#[test]
fn observed_empty_value_namespace_header_overflow_precedes_pending_control() {
    let control = Control::default();
    let mut payload_cap = payload_limits();
    payload_cap.max_coexisting_source_and_request_bytes = usize::MAX;
    let payloads = encode_connector_payloads(&[], usize::MAX, payload_cap, &control).unwrap();
    let decoded_payloads =
        decode_connector_payloads(&[], usize::MAX, payload_cap, &control).unwrap();
    let types = encode_type_table_sources(&[], &[], type_limits(), &control).unwrap();
    let decoded_types = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let mut cap = limits();
    cap.max_definitions = usize::MAX;
    cap.max_origin_references = usize::MAX;
    cap.max_allocation_requests = usize::MAX;
    cap.max_allocation_request_bytes = usize::MAX;
    cap.max_coexisting_source_and_request_bytes = usize::MAX;
    cap.max_work = usize::MAX;
    for sending in [true, false] {
        for cause in CAUSES {
            control.arm(Some((1, cause)));
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
            for _ in 0..255 {
                work.step().unwrap();
            }
            let mut prefixes = Vec::new();
            let mut parent = |facts: &ValueProjectionFacts| {
                prefixes.push(*facts);
                Ok(())
            };
            let result = if sending {
                encode_values_observed_in(
                    &[],
                    &payloads,
                    &types,
                    usize::MAX,
                    cap,
                    &mut parent,
                    &mut work,
                )
                .map(|_| ())
            } else {
                decode_values_observed_in(
                    &[],
                    &decoded_payloads,
                    &decoded_types,
                    usize::MAX,
                    cap,
                    &mut parent,
                    &mut work,
                )
                .map(|_| ())
            };
            assert!(matches!(
                finish(result, work),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(prefixes.len(), 1);
            assert_eq!(prefixes[0].allocation_requests_upper_bound, 0);
            assert_eq!(
                prefixes[0].coexisting_source_and_request_bytes_upper_bound,
                usize::MAX
            );
            assert_eq!(control.trace(), [(CompilePhase::Validate, 0)]);
        }
    }
}

fn fixture(
    run: impl FnOnce(
        &Control,
        &p::ValueDef,
        &[ValueSource<'_>],
        &EncodedConnectorPayloads<'_, '_>,
        &DecodedConnectorPayloads<'_, '_>,
        &EncodedTypeTable<'_>,
        &DecodedTypeTable,
    ),
) {
    let control = Control::default();
    let value = def(
        u32::MAX,
        ty(DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::Utf8),
        )),
        provider(),
    );
    let roots = [(0, value.ty.clone())];
    let types = encode_type_table_sources(&roots, &[], type_limits(), &control).unwrap();
    let decoded_types = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let payload_sources = [(0, payload(&value))];
    let payloads =
        encode_connector_payloads(&payload_sources, PRIOR, payload_limits(), &control).unwrap();
    let decoded_payloads =
        decode_connector_payloads(payloads.as_wire(), PRIOR, payload_limits(), &control).unwrap();
    let inputs = [ValueSource {
        source: &value,
        value_type_id: 0,
    }];
    run(
        &control,
        &value,
        &inputs,
        &payloads,
        &decoded_payloads,
        &types,
        &decoded_types,
    );
}

#[test]
fn observed_values_preserve_sparse_original_namespaces_and_independent_request_layout() {
    fixture(
        |control, value, inputs, payloads, decoded_payloads, types, decoded_types| {
            control.arm(None);
            let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate).unwrap();
            let mut prefixes = Vec::new();
            let result = encode_values_observed_in(
                inputs,
                payloads,
                types,
                SOURCE,
                limits(),
                &mut |f| {
                    prefixes.push(*f);
                    Ok(())
                },
                &mut work,
            );
            let encoded = finish(result, work).unwrap();
            assert_eq!(
                encoded.as_wire(),
                [expected(
                    u32::MAX,
                    0,
                    wire::value_origin::Kind::ProviderField(wire::ProviderFieldOrigin {
                        scan_node_id: Some(0),
                        column_payload_id: Some(0)
                    })
                )]
            );
            assert!(std::ptr::eq(encoded.types(), types));
            assert!(std::ptr::eq(encoded.payloads(), payloads));
            assert_eq!(encoded.facts().allocation_requests_upper_bound, 2);
            assert_eq!(
                encoded.facts().allocation_request_bytes_upper_bound,
                size_of::<usize>() + size_of::<wire::ValueDefinition>()
            );
            assert_eq!(prefixes.last(), Some(encoded.facts()));
            assert!(
                control
                    .trace()
                    .iter()
                    .all(|(phase, _)| *phase == CompilePhase::Validate)
            );
            control.arm(None);
            let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate).unwrap();
            let mut prefixes = Vec::new();
            let result = decode_values_observed_in(
                encoded.as_wire(),
                decoded_payloads,
                decoded_types,
                SOURCE,
                limits(),
                &mut |f| {
                    prefixes.push(*f);
                    Ok(())
                },
                &mut work,
            );
            let decoded = finish(result, work).unwrap();
            assert_eq!(
                decoded.into_values().as_slice(),
                std::slice::from_ref(value)
            );
            let final_facts = prefixes.last().unwrap();
            assert_eq!(final_facts.allocation_requests_upper_bound, 5);
            assert_eq!(
                final_facts.allocation_request_bytes_upper_bound,
                size_of::<usize>()
                    + size_of::<p::ValueDef>()
                    + 2 * size_of::<DataType>()
                    + bytes_shared_upper().unwrap()
            );
            assert_eq!(final_facts.origin_reference_count, 2);
            assert!(
                control
                    .trace()
                    .iter()
                    .all(|(phase, _)| *phase == CompilePhase::Validate)
            );
            assert!(
                prefixes
                    .windows(2)
                    .all(|w| w[1].allocation_requests_upper_bound
                        >= w[0].allocation_requests_upper_bound
                        && w[1].allocation_request_bytes_upper_bound
                            >= w[0].allocation_request_bytes_upper_bound
                        && w[1].cumulative_work_upper_bound >= w[0].cumulative_work_upper_bound)
            );
        },
    );
}

#[test]
fn observed_value_headers_clone_work_and_all_six_axes_refuse_before_output() {
    fixture(
        |control, _, inputs, payloads, decoded_payloads, types, decoded_types| {
            control.arm(None);
            let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate).unwrap();
            let result = encode_values_observed_in(
                inputs,
                payloads,
                types,
                SOURCE,
                limits(),
                &mut |_| Ok(()),
                &mut work,
            );
            let encoded = finish(result, work).unwrap();
            control.arm(None);
            let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate).unwrap();
            let result = decode_values_observed_in(
                encoded.as_wire(),
                decoded_payloads,
                decoded_types,
                SOURCE,
                limits(),
                &mut |_| Ok(()),
                &mut work,
            );
            let decoded = finish(result, work).unwrap();
            for (sending, facts) in [(true, *encoded.facts()), (false, *decoded.facts())] {
                for axis in 0..=6 {
                    let cap = if axis == 6 {
                        exact(facts)
                    } else {
                        smaller(exact(facts), axis)
                    };
                    control.arm(None);
                    let mut work =
                        CompileCheckpoints::try_new(control, CompilePhase::Validate).unwrap();
                    let result = if sending {
                        encode_values_observed_in(
                            inputs,
                            payloads,
                            types,
                            SOURCE,
                            cap,
                            &mut |_| Ok(()),
                            &mut work,
                        )
                        .map(|_| ())
                    } else {
                        decode_values_observed_in(
                            encoded.as_wire(),
                            decoded_payloads,
                            decoded_types,
                            SOURCE,
                            cap,
                            &mut |_| Ok(()),
                            &mut work,
                        )
                        .map(|_| ())
                    };
                    let result = finish(result, work);
                    if axis == 6 {
                        result.unwrap();
                    } else {
                        assert!(matches!(
                            result,
                            Err(Error::Control(CompileControlError::ResourceExhausted))
                        ));
                    }
                }
                for resource_axis in 0..2 {
                    for cause in CAUSES {
                        control.arm(Some((1, cause)));
                        let mut work =
                            CompileCheckpoints::try_new(control, CompilePhase::Validate).unwrap();
                        for _ in 0..255 {
                            work.step().unwrap();
                        }
                        let mut cap = limits();
                        if resource_axis == 0 {
                            cap.max_allocation_requests = 0;
                        } else {
                            cap.max_work =
                                physical_type_v2::value_type_clone_preflight_work_upper_bound();
                        }
                        let mut called = false;
                        let mut parent = |_: &ValueProjectionFacts| {
                            called = true;
                            Ok(())
                        };
                        let result = if sending {
                            encode_values_observed_in(
                                inputs,
                                payloads,
                                types,
                                SOURCE,
                                cap,
                                &mut parent,
                                &mut work,
                            )
                            .map(|_| ())
                        } else {
                            decode_values_observed_in(
                                encoded.as_wire(),
                                decoded_payloads,
                                decoded_types,
                                SOURCE,
                                cap,
                                &mut parent,
                                &mut work,
                            )
                            .map(|_| ())
                        };
                        assert!(matches!(
                            finish(result, work),
                            Err(Error::Control(CompileControlError::ResourceExhausted))
                        ));
                        assert!(!called);
                        assert_eq!(control.trace(), [(CompilePhase::Validate, 0)]);
                    }
                }
            }
        },
    );
}

#[test]
fn observed_values_keep_every_actual_success_ordinary_and_parent_control_prefix() {
    fixture(
        |control, _, inputs, payloads, decoded_payloads, types, decoded_types| {
            control.arm(None);
            let wire = encode_values(inputs, payloads, types, SOURCE, limits())
                .unwrap()
                .into_wire();
            let mut malformed = wire.clone();
            malformed[0].value_type_id = Some(17);
            for (sending, ordinary) in [(true, false), (false, false), (false, true)] {
                let run = || {
                    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
                    let result = if sending {
                        encode_values_observed_in(
                            inputs,
                            payloads,
                            types,
                            SOURCE,
                            limits(),
                            &mut |_| Ok(()),
                            &mut work,
                        )
                        .map(|_| ())
                    } else {
                        decode_values_observed_in(
                            if ordinary { &malformed } else { &wire },
                            decoded_payloads,
                            decoded_types,
                            SOURCE,
                            limits(),
                            &mut |_| Ok(()),
                            &mut work,
                        )
                        .map(|_| ())
                    };
                    finish(result, work)
                };
                control.arm(None);
                let baseline_result = run();
                if ordinary {
                    assert!(matches!(
                        baseline_result,
                        Err(Error::InvalidShape("value type ID is unknown"))
                    ));
                } else {
                    baseline_result.unwrap();
                }
                let baseline = control.trace();
                assert!(!baseline.is_empty());
                assert!(baseline.iter().any(|(_, units)| *units > 0));
                for at in 0..baseline.len() {
                    for cause in CAUSES {
                        control.arm(Some((at, cause)));
                        assert!(matches!(run(),Err(Error::Control(c)) if c==cause));
                        assert_eq!(control.trace(), baseline[..=at]);
                    }
                }
            }
            control.arm(None);
            let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate).unwrap();
            let mut calls = 0;
            let result = encode_values_observed_in(
                inputs,
                payloads,
                types,
                SOURCE,
                limits(),
                &mut |_| {
                    calls += 1;
                    Err(CompileControlError::DeadlineExceeded)
                },
                &mut work,
            );
            assert!(matches!(
                finish(result, work),
                Err(Error::Control(CompileControlError::DeadlineExceeded))
            ));
            assert_eq!(calls, 1);
            assert_eq!(control.trace(), [(CompilePhase::Validate, 0)]);
        },
    );
}

#[test]
fn observed_value_namespace_refuses_foreign_meter_and_preserves_source_floor_category() {
    fixture(|control, _, inputs, payloads, _, types, _| {
        let foreign = Control::default();
        let mut work = CompileCheckpoints::try_new(&foreign, CompilePhase::Validate).unwrap();
        assert!(matches!(
            encode_values_observed_in(
                inputs,
                payloads,
                types,
                SOURCE,
                limits(),
                &mut |_| Ok(()),
                &mut work
            ),
            Err(Error::InvalidShape(
                "value namespace belongs to another control"
            ))
        ));
        assert_eq!(foreign.trace(), [(CompilePhase::Validate, 0)]);
        control.arm(None);
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate).unwrap();
        let result = encode_values_observed_in(
            inputs,
            payloads,
            types,
            0,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        );
        assert!(matches!(
            finish(result, work),
            Err(Error::InvalidShape(
                "value source invoice omits original backing"
            ))
        ));
        // The known namespace header rejects this invoice before completed
        // work. The ordinary caller still observes its original finish.
        assert_eq!(
            control.trace(),
            [(CompilePhase::Validate, 0), (CompilePhase::Validate, 0)]
        );
        control.arm(None);
        let namespace = encode_values(inputs, payloads, types, SOURCE, limits()).unwrap();
        let mut work = CompileCheckpoints::try_new(&foreign, CompilePhase::Validate).unwrap();
        assert!(matches!(
            namespace.value_observed(u32::MAX, &mut work),
            Err(Error::InvalidShape(
                "value namespace belongs to another control"
            ))
        ));
    });
}

#[test]
fn observed_values_wide_actual_index_quantum_preserves_all_ordered_source_ids() {
    const WIDE_SOURCE: usize = 128 * 1024;
    let control = Control::default();
    let roots = [(0, ty(DataType::Int32))];
    let types = encode_type_table_sources(&roots, &[], type_limits(), &control).unwrap();
    let decoded_types = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let payloads = encode_connector_payloads(&[], 0, payload_limits(), &control).unwrap();
    let decoded_payloads = decode_connector_payloads(&[], 0, payload_limits(), &control).unwrap();
    let values: Vec<_> = (0..320)
        .rev()
        .map(|id| {
            def(
                id,
                roots[0].1.clone(),
                p::ValueOrigin::NodeOutput {
                    node: p::NodeId::new(0),
                    output_ordinal: id,
                },
            )
        })
        .collect();
    let inputs: Vec<_> = values
        .iter()
        .map(|source| ValueSource {
            source,
            value_type_id: 0,
        })
        .collect();
    control.arm(None);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let result = encode_values_observed_in(
        &inputs,
        &payloads,
        &types,
        WIDE_SOURCE,
        limits(),
        &mut |_| Ok(()),
        &mut work,
    );
    let encoded = finish(result, work).unwrap();
    let sending = control.trace();
    assert_eq!(
        encoded.as_wire().iter().map(|v| v.id).collect::<Vec<_>>(),
        (0..320).rev().collect::<Vec<_>>()
    );
    control.arm(None);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let result = decode_values_observed_in(
        encoded.as_wire(),
        &decoded_payloads,
        &decoded_types,
        WIDE_SOURCE,
        limits(),
        &mut |_| Ok(()),
        &mut work,
    );
    assert_eq!(finish(result, work).unwrap().into_values(), values);
    let receiving = control.trace();
    for (is_encode, baseline) in [(true, sending), (false, receiving)] {
        let quantum = baseline
            .iter()
            .position(|(_, units)| *units == 256)
            .expect("actual count-sized index fill observes 256 completed operations");
        for at in [0, quantum, baseline.len() - 1] {
            for cause in CAUSES {
                control.arm(Some((at, cause)));
                let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate);
                let result = match work.as_mut() {
                    Ok(work) => {
                        if is_encode {
                            encode_values_observed_in(
                                &inputs,
                                &payloads,
                                &types,
                                WIDE_SOURCE,
                                limits(),
                                &mut |_| Ok(()),
                                work,
                            )
                            .map(|_| ())
                        } else {
                            decode_values_observed_in(
                                encoded.as_wire(),
                                &decoded_payloads,
                                &decoded_types,
                                WIDE_SOURCE,
                                limits(),
                                &mut |_| Ok(()),
                                work,
                            )
                            .map(|_| ())
                        }
                    }
                    Err(c) => Err(Error::Control(*c)),
                };
                let result = match work {
                    Ok(work) => finish(result, work),
                    Err(_) => result,
                };
                assert!(matches!(result,Err(Error::Control(c))if c==cause));
                assert_eq!(control.trace(), baseline[..=at]);
            }
        }
    }
}

#[test]
fn observed_nested_dictionary_requests_are_admitted_at_the_original_clone_prefix() {
    let control = Control::default();
    let ty = ty(DataType::Dictionary(
        Box::new(DataType::Int8),
        Box::new(DataType::Dictionary(
            Box::new(DataType::Int16),
            Box::new(DataType::Utf8),
        )),
    ));
    let roots = [(u32::MAX, ty.clone())];
    let types = encode_type_table_sources(&roots, &[], type_limits(), &control).unwrap();
    let receiving_types = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let payloads = encode_connector_payloads(&[], 0, payload_limits(), &control).unwrap();
    let receiving_payloads = decode_connector_payloads(&[], 0, payload_limits(), &control).unwrap();
    let source = def(
        0,
        ty,
        p::ValueOrigin::NodeOutput {
            node: p::NodeId::new(u32::MAX),
            output_ordinal: 0,
        },
    );
    let inputs = [ValueSource {
        source: &source,
        value_type_id: u32::MAX,
    }];
    let raw = encode_values(&inputs, &payloads, &types, SOURCE, limits())
        .unwrap()
        .into_wire();
    control.arm(None);
    let mut prefixes = Vec::new();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let result = decode_values_observed_in(
        &raw,
        &receiving_payloads,
        &receiving_types,
        SOURCE,
        limits(),
        &mut |f| {
            prefixes.push(*f);
            Ok(())
        },
        &mut work,
    );
    let decoded = finish(result, work).unwrap();
    assert_eq!(decoded.facts().allocation_requests_upper_bound, 6);
    assert_eq!(
        decoded.facts().allocation_request_bytes_upper_bound,
        size_of::<usize>() + size_of::<p::ValueDef>() + 4 * size_of::<DataType>()
    );
    assert_eq!(decoded.into_values(), [source]);
    assert!(
        prefixes
            .iter()
            .any(|f| f.allocation_requests_upper_bound == 4)
    );
    assert!(
        prefixes
            .iter()
            .any(|f| f.allocation_requests_upper_bound == 6)
    );
    for cause in CAUSES {
        control.arm(None);
        let mut armed = false;
        let mut trace_at_outer = Vec::new();
        let mut cap = limits();
        cap.max_allocation_requests = 5;
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let result = decode_values_observed_in(
            &raw,
            &receiving_payloads,
            &receiving_types,
            SOURCE,
            cap,
            &mut |f| {
                if f.allocation_requests_upper_bound == 4 && !armed {
                    // The real outer Dictionary prefix, before its next completed
                    // clone step, arms the next actual controller callback.
                    trace_at_outer = control.trace();
                    *control.stop.lock().unwrap() = Some((trace_at_outer.len(), cause));
                    armed = true;
                }
                Ok(())
            },
            &mut work,
        );
        assert!(matches!(
            finish(result, work),
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        assert!(
            armed,
            "the actual clone author must expose its outer request prefix"
        );
        assert_eq!(
            control.trace(),
            trace_at_outer,
            "known inner Dictionary requests refuse before the armed late observer"
        );
        control.arm(None);
        let mut reached = false;
        let mut at_refusal = Vec::new();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let result = decode_values_observed_in(
            &raw,
            &receiving_payloads,
            &receiving_types,
            SOURCE,
            limits(),
            &mut |f| {
                if f.allocation_requests_upper_bound == 6 {
                    reached = true;
                    at_refusal = control.trace();
                    Err(cause)
                } else {
                    Ok(())
                }
            },
            &mut work,
        );
        assert!(matches!(finish(result,work),Err(Error::Control(actual))if actual==cause));
        assert!(reached);
        assert_eq!(control.trace(), at_refusal);
    }
}
