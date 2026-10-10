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

fn caller_encode(
    input: Option<&p::ResultPort>,
    ids: &[u32],
    values: &EncodedValues<'_, '_, '_>,
    l: NodeProjectionLimits,
    parent: &mut NodeAdmit<'_>,
    c: &Control,
) -> Result<(Option<wire::ResultPort>, NodeProjectionFacts), Error> {
    let mut work = CompileCheckpoints::try_new(c, CompilePhase::Encode)?;
    let result = prepare_result_encode_in(input, ids, values, SOURCE, l, parent, &mut work)
        .and_then(|prepared| prepared.emit_in(parent, &mut work));
    finish(work, result)
}
fn caller_decode(
    input: Option<&wire::ResultPort>,
    values: &DecodedValues<'_, '_, '_>,
    l: NodeProjectionLimits,
    parent: &mut NodeAdmit<'_>,
    c: &Control,
) -> Result<(Option<p::ResultPort>, NodeProjectionFacts), Error> {
    let mut work = CompileCheckpoints::try_new(c, CompilePhase::Decode)?;
    let result = prepare_result_decode_in(input, values, SOURCE, l, parent, &mut work)
        .and_then(|prepared| prepared.emit_in(parent, &mut work));
    finish(work, result)
}
fn snapshot_fits(f: &NodeProjectionFacts, ceiling: NodeProjectionFacts) {
    assert!(f.input_node_count <= ceiling.input_node_count);
    assert!(f.value_reference_count <= ceiling.value_reference_count);
    assert!(f.list_item_count <= ceiling.list_item_count);
    assert!(f.allocation_requests_upper_bound <= ceiling.allocation_requests_upper_bound);
    assert!(f.allocation_request_bytes_upper_bound <= ceiling.allocation_request_bytes_upper_bound);
    assert!(
        f.coexisting_source_and_request_bytes_upper_bound
            <= ceiling.coexisting_source_and_request_bytes_upper_bound
    );
    assert!(f.cumulative_work_upper_bound <= ceiling.cumulative_work_upper_bound);
}
#[test]
fn caller_result_independent_wire_and_exact_snapshot_replay_keep_sparse_repeats() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, read| {
        let input = fixture.result();
        c.arm(None);
        let plain = encode(Some(&input), &[0, 7, 0], values, SOURCE, limits(), &c).unwrap();
        c.arm(None);
        let mut snapshots = Vec::new();
        let encoded = caller_encode(
            Some(&input),
            &[0, 7, 0],
            values,
            limits(),
            &mut |f| {
                snapshots.push(*f);
                Ok(())
            },
            &c,
        )
        .unwrap();
        assert_eq!(encoded.0, Some(expected()));
        assert_eq!(encoded, plain);
        assert_eq!(snapshots.last(), Some(&encoded.1));
        for f in &snapshots {
            snapshot_fits(f, encoded.1);
        }
        assert_eq!(c.trace().first(), Some(&(CompilePhase::Encode, 0)));
        c.arm(None);
        assert_eq!(
            caller_encode(
                Some(&input),
                &[0, 7, 0],
                values,
                exact_limits(encoded.1),
                &mut |f| {
                    snapshot_fits(f, encoded.1);
                    Ok(())
                },
                &c
            )
            .unwrap(),
            encoded
        );
        c.arm(None);
        let plain = decode(encoded.0.as_ref(), read, SOURCE, limits(), &c).unwrap();
        c.arm(None);
        snapshots.clear();
        let decoded = caller_decode(
            encoded.0.as_ref(),
            read,
            limits(),
            &mut |f| {
                snapshots.push(*f);
                Ok(())
            },
            &c,
        )
        .unwrap();
        assert_eq!(decoded.0, Some(input));
        assert_eq!(decoded, plain);
        assert_eq!(snapshots.last(), Some(&decoded.1));
        for f in &snapshots {
            snapshot_fits(f, decoded.1);
        }
        c.arm(None);
        assert_eq!(
            caller_decode(
                encoded.0.as_ref(),
                read,
                exact_limits(decoded.1),
                &mut |f| {
                    snapshot_fits(f, decoded.1);
                    Ok(())
                },
                &c
            )
            .unwrap(),
            decoded
        );
    });
}
#[test]
fn caller_result_none_empty_and_nominal_dictionary_roots_keep_distinct_values() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, read| {
        c.arm(None);
        let absent = caller_encode(None, &[], values, limits(), &mut |_| Ok(()), &c).unwrap();
        assert!(absent.0.is_none());
        assert!(
            caller_decode(None, read, limits(), &mut |_| Ok(()), &c)
                .unwrap()
                .0
                .is_none()
        );
        let empty = p::ResultPort {
            scalar_schema: None,
            fragment: p::FragmentId::new(0),
            output: p::OutputPort {
                node: p::NodeId::new(0),
                columns: Box::default(),
            },
            fields: Box::default(),
        };
        let empty_wire = caller_encode(Some(&empty), &[], values, limits(), &mut |_| Ok(()), &c)
            .unwrap()
            .0;
        assert!(empty_wire.is_some());
        assert_eq!(
            caller_decode(empty_wire.as_ref(), read, limits(), &mut |_| Ok(()), &c)
                .unwrap()
                .0,
            Some(empty)
        );
        for id in [7, 9, u32::MAX] {
            let input = fixture.single(id);
            let encoded =
                caller_encode(Some(&input), &[id], values, limits(), &mut |_| Ok(()), &c).unwrap();
            let decoded =
                caller_decode(encoded.0.as_ref(), read, limits(), &mut |_| Ok(()), &c).unwrap();
            assert_eq!(decoded.0, Some(input));
            let dictionary_bytes = if id == u32::MAX {
                2 * Layout::new::<DataType>().size()
            } else {
                0
            };
            let roots = 2 * Layout::array::<p::ValueId>(1).unwrap().size()
                + 2 * Layout::array::<p::ResultField>(1).unwrap().size();
            // Direct Dictionary owns two Boxes; Struct's FieldRef child is shared.
            assert_eq!(
                decoded.1.allocation_request_bytes_upper_bound,
                roots + 2 * "value".len() + dictionary_bytes
            );
            c.arm(None);
            assert_eq!(
                caller_decode(
                    encoded.0.as_ref(),
                    read,
                    exact_limits(decoded.1),
                    &mut |f| {
                        snapshot_fits(f, decoded.1);
                        Ok(())
                    },
                    &c
                )
                .unwrap(),
                decoded
            );
        }
    });
}
#[test]
fn caller_result_known_root_requests_refuse_before_pending_quantum_and_late_cause() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, read| {
        let input = fixture.result();
        let raw = expected();
        let encode_bytes = Layout::array::<u32>(3).unwrap().size()
            + Layout::array::<wire::ResultField>(3).unwrap().size();
        let decode_bytes = 2
            * (Layout::array::<p::ValueId>(3).unwrap().size()
                + Layout::array::<p::ResultField>(3).unwrap().size());
        assert!(encode_bytes > 1 && decode_bytes > 1);
        for pending in [0, 254, 255] {
            for cause in CAUSES {
                for decode in [false, true] {
                    c.arm(Some((1, cause)));
                    let phase = if decode {
                        CompilePhase::Decode
                    } else {
                        CompilePhase::Encode
                    };
                    let mut work = CompileCheckpoints::try_new(&c, phase).unwrap();
                    for _ in 0..pending {
                        work.step().unwrap();
                    }
                    let bytes = if decode { decode_bytes } else { encode_bytes };
                    let mut calls = 0;
                    let mut parent = |f: &NodeProjectionFacts| {
                        calls += 1;
                        assert_eq!(f.allocation_request_bytes_upper_bound, bytes);
                        assert_eq!(
                            f.coexisting_source_and_request_bytes_upper_bound,
                            SOURCE + bytes
                        );
                        if f.allocation_request_bytes_upper_bound > bytes - 1 {
                            Err(CompileControlError::ResourceExhausted)
                        } else {
                            Ok(())
                        }
                    };
                    let result = if decode {
                        prepare_result_decode_in(
                            Some(&raw),
                            read,
                            SOURCE,
                            limits(),
                            &mut parent,
                            &mut work,
                        )
                        .map(|_| ())
                    } else {
                        prepare_result_encode_in(
                            Some(&input),
                            &[0, 7, 0],
                            values,
                            SOURCE,
                            limits(),
                            &mut parent,
                            &mut work,
                        )
                        .map(|_| ())
                    };
                    control_error(finish(work, result), CompileControlError::ResourceExhausted);
                    assert_eq!(calls, 1);
                    assert_eq!(c.trace(), [(phase, 0)]);
                }
            }
        }
    });
}
#[test]
fn caller_result_actual_dictionary_capture_gate_precedes_completed_lookup() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, read| {
        let input = fixture.single(u32::MAX);
        c.arm(None);
        let raw = caller_encode(
            Some(&input),
            &[u32::MAX],
            values,
            limits(),
            &mut |_| Ok(()),
            &c,
        )
        .unwrap()
        .0
        .unwrap();
        let roots = 2
            * (Layout::array::<p::ValueId>(1).unwrap().size()
                + Layout::array::<p::ResultField>(1).unwrap().size());
        let known = roots + 2 * "value".len() + 2 * Layout::new::<DataType>().size();
        let mut cut = None;
        c.arm(None);
        caller_decode(
            Some(&raw),
            read,
            limits(),
            &mut |f| {
                if f.allocation_request_bytes_upper_bound == known && cut.is_none() {
                    cut = Some(c.trace());
                }
                Ok(())
            },
            &c,
        )
        .unwrap();
        let prefix = cut.unwrap();
        assert!(!prefix.is_empty());
        for cause in CAUSES {
            c.arm(Some((prefix.len(), cause)));
            let result = caller_decode(
                Some(&raw),
                read,
                limits(),
                &mut |f| {
                    if f.allocation_request_bytes_upper_bound > known - 1 {
                        Err(CompileControlError::ResourceExhausted)
                    } else {
                        Ok(())
                    }
                },
                &c,
            );
            control_error(result, CompileControlError::ResourceExhausted);
            assert_eq!(c.trace(), prefix);
        }
    });
}
#[test]
fn caller_result_every_actual_small_callback_and_emit_parent_refusal_keep_first_cause() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, read| {
        let input = fixture.single(0);
        c.arm(None);
        let raw = caller_encode(Some(&input), &[0], values, limits(), &mut |_| Ok(()), &c)
            .unwrap()
            .0
            .unwrap();
        for ordinary in [false, true] {
            let mut input = input.clone();
            let mut raw = raw.clone();
            if ordinary {
                input.fields[0].value = p::ValueId::new(42);
                raw.fields[0].value_id = Some(42);
            }
            for decode in [false, true] {
                let invoke = || {
                    if decode {
                        caller_decode(Some(&raw), read, limits(), &mut |_| Ok(()), &c).map(|_| ())
                    } else {
                        caller_encode(Some(&input), &[0], values, limits(), &mut |_| Ok(()), &c)
                            .map(|_| ())
                    }
                };
                c.arm(None);
                let baseline = invoke();
                if ordinary {
                    assert!(matches!(baseline, Err(Error::InvalidShape(_))));
                } else {
                    baseline.unwrap();
                }
                let trace = c.trace();
                assert!(trace.len() > 1);
                for at in 0..trace.len() {
                    for cause in CAUSES {
                        c.arm(Some((at, cause)));
                        control_error(invoke(), cause);
                        assert_eq!(c.trace(), trace[..=at]);
                    }
                }
            }
        }
        c.arm(None);
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
        let prepared = prepare_result_encode_in(
            Some(&input),
            &[0],
            values,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        let before = c.trace();
        control_error(
            prepared.emit_in(&mut |_| Err(CompileControlError::Cancelled), &mut work),
            CompileControlError::Cancelled,
        );
        assert_eq!(c.trace(), before);
        c.arm(None);
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        let prepared = prepare_result_decode_in(
            Some(&raw),
            read,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        let before = c.trace();
        control_error(
            prepared.emit_in(
                &mut |_| Err(CompileControlError::DeadlineExceeded),
                &mut work,
            ),
            CompileControlError::DeadlineExceeded,
        );
        assert_eq!(c.trace(), before);
    });
}
#[test]
fn caller_result_foreign_controller_refuses_before_parent_or_namespace_observation() {
    let fixture = Fixture::new();
    let c = Control::default();
    let foreign = Control::default();
    fixture.with_tokens(&c, |values, read| {
        let input = fixture.result();
        let raw = expected();
        c.arm(None);
        foreign.arm(None);
        let mut work = CompileCheckpoints::try_new(&foreign, CompilePhase::Encode).unwrap();
        assert!(matches!(
            prepare_result_encode_in(
                Some(&input),
                &[0, 7, 0],
                values,
                SOURCE,
                limits(),
                &mut |_| panic!("foreign parent reached"),
                &mut work
            ),
            Err(Error::InvalidShape(_))
        ));
        assert!(c.trace().is_empty());
        assert_eq!(foreign.trace(), [(CompilePhase::Encode, 0)]);
        c.arm(None);
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        let prepared = prepare_result_decode_in(
            Some(&raw),
            read,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        let before = c.trace();
        let mut other = CompileCheckpoints::try_new(&foreign, CompilePhase::Decode).unwrap();
        assert!(matches!(
            prepared.emit_in(
                &mut |_| panic!("foreign emitter parent reached"),
                &mut other
            ),
            Err(Error::InvalidShape(_))
        ));
        assert_eq!(c.trace(), before);
    });
}

#[test]
fn caller_result_wide_actual_text_copy_keeps_quantum_and_sampled_first_causes() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, read| {
        let mut input = fixture.single(0);
        input.fields[0].name = "中\0".repeat(320).into_boxed_str();
        c.arm(None);
        let encoded =
            caller_encode(Some(&input), &[0], values, limits(), &mut |_| Ok(()), &c).unwrap();
        let raw = encoded.0.unwrap();
        assert_eq!(
            raw.fields[0].name.as_bytes(),
            input.fields[0].name.as_bytes()
        );
        for decode in [false, true] {
            let invoke = || {
                if decode {
                    caller_decode(Some(&raw), read, limits(), &mut |_| Ok(()), &c).map(|result| {
                        assert_eq!(result.0, Some(input.clone()));
                    })
                } else {
                    caller_encode(Some(&input), &[0], values, limits(), &mut |_| Ok(()), &c).map(
                        |result| {
                            assert_eq!(result.0, Some(raw.clone()));
                        },
                    )
                }
            };
            c.arm(None);
            invoke().unwrap();
            let trace = c.trace();
            assert!(trace.iter().any(|(_, units)| *units == 256));
            let quantum = trace.iter().position(|(_, units)| *units == 256).unwrap();
            for at in [0, 1, quantum, trace.len() / 2, trace.len() - 1] {
                for cause in CAUSES {
                    c.arm(Some((at, cause)));
                    control_error(invoke(), cause);
                    assert_eq!(c.trace(), trace[..=at]);
                }
            }
        }
    });
}

#[test]
fn caller_result_numeric_comparison_overflow_is_resource_before_ordinary_finish() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, read| {
        let input = fixture.single(0);
        c.arm(None);
        let encoded =
            caller_encode(Some(&input), &[0], values, limits(), &mut |_| Ok(()), &c).unwrap();
        let raw = encoded.0.as_ref().unwrap();
        c.arm(None);
        let decoded = caller_decode(Some(raw), read, limits(), &mut |_| Ok(()), &c).unwrap();
        let mut unbounded = limits();
        unbounded.max_input_nodes = usize::MAX;
        unbounded.max_value_references = usize::MAX;
        unbounded.max_list_items = usize::MAX;
        unbounded.max_allocation_requests = usize::MAX;
        unbounded.max_allocation_request_bytes = usize::MAX;
        unbounded.max_coexisting_source_and_request_bytes = usize::MAX;
        unbounded.max_work = usize::MAX;
        for deep in [false, true] {
            for decode in [false, true] {
                let requested = if decode {
                    2 * (Layout::array::<p::ValueId>(1).unwrap().size()
                        + Layout::array::<p::ResultField>(1).unwrap().size()
                        + "value".len())
                } else {
                    Layout::array::<u32>(1).unwrap().size()
                        + Layout::array::<wire::ResultField>(1).unwrap().size()
                        + "value".len()
                };
                let facts = if decode { decoded.1 } else { encoded.1 };
                assert_eq!(facts.allocation_request_bytes_upper_bound, requested);
                let source = if deep {
                    usize::MAX / 2
                } else {
                    usize::MAX - requested
                };
                if deep {
                    assert!(
                        crate::borrowed_type_resources::type_binding_prefix_work_upper_bound_in(
                            &input.fields[0].ty,
                            &input.fields[0].ty,
                            source
                        )
                        .is_ok(),
                        "the root prefix must fit before actual TypeNode growth"
                    );
                    let plain_control = Control::default();
                    let mut plain_work =
                        CompileCheckpoints::try_new(&plain_control, CompilePhase::Encode).unwrap();
                    assert!(matches!(
                        crate::borrowed_type_resources::verify_type_binding(
                            &input.fields[0].ty,
                            &input.fields[0].ty,
                            source,
                            usize::MAX,
                            &mut plain_work
                        ),
                        Err(physical_type_v2::TypeCodecError::InvalidShape(_))
                    ));
                } else {
                    assert!(matches!(
                        crate::borrowed_type_resources::type_binding_prefix_work_upper_bound(
                            &input.fields[0].ty,
                            &input.fields[0].ty,
                            source
                        ),
                        Err(physical_type_v2::TypeCodecError::InvalidShape(_))
                    ));
                }
                let phase = if decode {
                    CompilePhase::Decode
                } else {
                    CompilePhase::Encode
                };
                let invoke = || {
                    let mut work = CompileCheckpoints::try_new(&c, phase)?;
                    let mut calls = 0;
                    let mut parent = |_: &NodeProjectionFacts| {
                        calls += 1;
                        Ok(())
                    };
                    let result = if decode {
                        prepare_result_decode_in(
                            Some(raw),
                            read,
                            source,
                            unbounded,
                            &mut parent,
                            &mut work,
                        )
                        .map(|_| ())
                    } else {
                        prepare_result_encode_in(
                            Some(&input),
                            &[0],
                            values,
                            source,
                            unbounded,
                            &mut parent,
                            &mut work,
                        )
                        .map(|_| ())
                    };
                    let before = c.trace();
                    let result = finish(work, result);
                    assert!(calls > 1, "the actual namespace comparison must be reached");
                    assert_eq!(
                        c.trace(),
                        before,
                        "numeric refusal must not publish an ordinary footer"
                    );
                    result
                };
                c.arm(None);
                control_error(invoke(), CompileControlError::ResourceExhausted);
                let trace = c.trace();
                for cause in CAUSES {
                    c.arm(Some((trace.len(), cause)));
                    control_error(invoke(), CompileControlError::ResourceExhausted);
                    assert_eq!(c.trace(), trace);
                }
            }
        }
    });
}

#[test]
fn caller_result_single_semantic_scalar_source_roundtrips_both_explicit_states() {
    let fixture = Fixture::new();
    let c = Control::default();
    // This fixture also admits the existing complete root schema profile.
    let root_bytes = crate::physical_fragment_envelope_v2::root_projection_request_bytes().unwrap();
    let mut scalar_limits = limits();
    scalar_limits.max_allocation_request_bytes += root_bytes;
    scalar_limits.max_coexisting_source_and_request_bytes += root_bytes;
    fixture.with_tokens(&c, |values, read| {
        for slot in [None, Some(37)] {
            let semantic = novarocks_result_contract::ScalarSchema::try_new(
                novarocks_result_contract::ScalarField {
                    value_type: novarocks_result_contract::ScalarValueType::String,
                    nullable: true,
                },
            )
            .unwrap();
            let semantic = match slot {
                Some(slot) => semantic.bind_native_slots(&[slot]).unwrap(),
                None => semantic,
            };
            let mut input = fixture.single(0);
            input.scalar_schema = Some(semantic);
            let encoded = caller_encode(
                Some(&input),
                &[0],
                values,
                scalar_limits,
                &mut |_| Ok(()),
                &c,
            )
            .unwrap();
            assert_eq!(
                encoded
                    .0
                    .as_ref()
                    .unwrap()
                    .scalar_schema
                    .as_ref()
                    .unwrap()
                    .source_slot,
                slot
            );
            let decoded =
                caller_decode(encoded.0.as_ref(), read, scalar_limits, &mut |_| Ok(()), &c)
                    .unwrap();
            assert_eq!(decoded.0, Some(input));
        }
    });
}
