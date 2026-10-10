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

fn monotone(before: &ValueOriginProjectionFacts, after: &ValueOriginProjectionFacts) {
    assert!(before.reference_count <= after.reference_count);
    assert!(before.allocation_requests_upper_bound <= after.allocation_requests_upper_bound);
    assert!(
        before.allocation_request_bytes_upper_bound <= after.allocation_request_bytes_upper_bound
    );
    assert!(
        before.coexisting_source_and_request_bytes_upper_bound
            <= after.coexisting_source_and_request_bytes_upper_bound
    );
    assert!(before.cumulative_work_upper_bound <= after.cumulative_work_upper_bound);
}

#[test]
fn origin_admitted_sole_grammar_preserves_all_variants_and_monotone_actual_clone_facts() {
    for bytes in [&[][..], &[0, 255, 1][..]] {
        let control = Control::default();
        let origin = provider(ConnectorCodecCategory::ReadColumn, bytes);
        let inputs = [(u32::MAX, original(&origin))];
        let encoded =
            encode_connector_payloads(&inputs, NAMESPACE_SOURCE, ns_limits(), &control).unwrap();
        let wire = encode_value_origin(&origin, &encoded, SOURCE, limits())
            .unwrap()
            .0;
        let decoded =
            decode_connector_payloads(encoded.as_wire(), NAMESPACE_SOURCE, ns_limits(), &control)
                .unwrap();
        let cases = nonprovider_cases();
        for (origin, expected) in cases
            .iter()
            .map(|(origin, wire)| (origin, *wire))
            .chain(std::iter::once((&origin, wire)))
        {
            let payload = decoded.payload(u32::MAX).unwrap().unwrap();
            let mut work =
                CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
            let before_trace = trace(&control).len();
            let mut snapshots = Vec::new();
            let before = preflight_encode_admitted(
                origin,
                &encoded,
                SOURCE,
                limits(),
                &mut |facts| {
                    if let Some(previous) = snapshots.last() {
                        monotone(previous, facts);
                    }
                    snapshots.push(*facts);
                    Ok(())
                },
                &mut work,
            )
            .unwrap();
            let (actual, after) = encode_admitted(
                origin,
                &encoded,
                SOURCE,
                exact(before),
                &mut |_| Ok(()),
                &mut work,
            )
            .unwrap();
            assert_eq!(actual, expected);
            assert_eq!(before, after);
            let was_unique = payload.payload().is_unique();
            snapshots.clear();
            let before = preflight_decode_admitted(
                &expected,
                &decoded,
                SOURCE,
                limits(),
                &mut |facts| {
                    if let Some(previous) = snapshots.last() {
                        monotone(previous, facts);
                    }
                    snapshots.push(*facts);
                    Ok(())
                },
                &mut work,
            )
            .unwrap();
            assert_eq!(
                payload.payload().is_unique(),
                was_unique,
                "borrowed preflight cannot promote Bytes"
            );
            let (actual, after) = decode_admitted(
                &expected,
                &decoded,
                SOURCE,
                exact(before),
                &mut |_| Ok(()),
                &mut work,
            )
            .unwrap();
            assert_eq!(&actual, origin);
            assert_eq!(before, after);
            // Explicit clone bracketing can legitimately flush zero units.
            // Its phase must remain the caller's, including those observations.
            assert!(
                trace(&control)[before_trace..]
                    .iter()
                    .all(|(phase, _)| *phase == CompilePhase::LowerProgram)
            );
            work.finish().unwrap();
        }
    }
}

#[test]
fn origin_admitted_all_four_axes_and_parent_refusal_precede_actual_payload_promotion() {
    let control = Control::default();
    let origin = provider(ConnectorCodecCategory::ReadColumn, &[1, 2, 3]);
    let inputs = [(0, original(&origin))];
    let encoded =
        encode_connector_payloads(&inputs, NAMESPACE_SOURCE, ns_limits(), &control).unwrap();
    let raw = encode_value_origin(&origin, &encoded, SOURCE, limits())
        .unwrap()
        .0;
    let decoded =
        decode_connector_payloads(encoded.as_wire(), NAMESPACE_SOURCE, ns_limits(), &control)
            .unwrap();
    let payload = decoded.payload(0).unwrap().unwrap();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let facts =
        preflight_decode_admitted(&raw, &decoded, SOURCE, limits(), &mut |_| Ok(()), &mut work)
            .unwrap();
    for axis in 0..4 {
        let mut limit = exact(facts);
        let n = match axis {
            0 => &mut limit.max_allocation_requests,
            1 => &mut limit.max_allocation_request_bytes,
            2 => &mut limit.max_coexisting_source_and_request_bytes,
            _ => &mut limit.max_work,
        };
        *n -= 1;
        assert!(matches!(
            decode_admitted(&raw, &decoded, SOURCE, limit, &mut |_| Ok(()), &mut work),
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        assert!(payload.payload().is_unique());
    }
    for cause in CAUSES {
        assert!(
            matches!(decode_admitted(&raw, &decoded, SOURCE, limits(), &mut |facts| {
            if facts.allocation_requests_upper_bound != 0 { Err(cause) } else { Ok(()) }
        }, &mut work), Err(Error::Control(actual)) if actual == cause)
        );
        assert!(payload.payload().is_unique());
    }
    let no_caps = ValueOriginProjectionLimits {
        max_allocation_requests: usize::MAX,
        max_allocation_request_bytes: usize::MAX,
        max_coexisting_source_and_request_bytes: usize::MAX,
        max_work: usize::MAX,
    };
    assert!(matches!(
        preflight_decode_admitted(
            &raw,
            &decoded,
            usize::MAX,
            no_caps,
            &mut |_| Ok(()),
            &mut work
        ),
        Err(Error::Control(CompileControlError::ResourceExhausted))
    ));
    assert!(payload.payload().is_unique());
    work.finish().unwrap();
}

#[derive(Default)]
struct ArmedControl {
    armed: Mutex<Option<CompileControlError>>,
    callbacks: Mutex<usize>,
}
impl PureCompileControl for ArmedControl {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        *self.callbacks.lock().unwrap() += 1;
        match *self.armed.lock().unwrap() {
            Some(cause) => Err(cause),
            None => Ok(()),
        }
    }
}
#[test]
fn origin_admitted_known_work_and_parent_gate_win_before_pending_255_callback() {
    let control = ArmedControl::default();
    let encoded = encode_connector_payloads(&[], 0, ns_limits(), &control).unwrap();
    let decoded = decode_connector_payloads(&[], 0, ns_limits(), &control).unwrap();
    let (origin, raw) = nonprovider_cases().remove(0);
    for cause in CAUSES {
        for receiving in [false, true] {
            for parent_refusal in [false, true] {
                *control.armed.lock().unwrap() = None;
                let mut work =
                    CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
                for _ in 0..255 {
                    work.step().unwrap();
                }
                let callbacks = *control.callbacks.lock().unwrap();
                *control.armed.lock().unwrap() = Some(cause);
                let mut limit = limits();
                if !parent_refusal {
                    limit.max_work = 0;
                }
                let mut parent =
                    |_: &ValueOriginProjectionFacts| Err(CompileControlError::ResourceExhausted);
                let result = if receiving {
                    preflight_decode_admitted(&raw, &decoded, SOURCE, limit, &mut parent, &mut work)
                } else {
                    preflight_encode_admitted(
                        &origin,
                        &encoded,
                        SOURCE,
                        limit,
                        &mut parent,
                        &mut work,
                    )
                };
                assert!(matches!(
                    result,
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
                assert_eq!(*control.callbacks.lock().unwrap(), callbacks);
            }
        }
    }
}

#[test]
fn origin_admitted_actual_success_and_ordinary_failure_keep_every_caller_control_prefix() {
    let origin = provider(ConnectorCodecCategory::ReadColumn, &[0, 255]);
    let inputs = [(u32::MAX, original(&origin))];
    let baseline = Control::default();
    let encoded =
        encode_connector_payloads(&inputs, NAMESPACE_SOURCE, ns_limits(), &baseline).unwrap();
    let raw = encode_value_origin(&origin, &encoded, SOURCE, limits())
        .unwrap()
        .0;
    for ordinary in [false, true] {
        every_prefix(
            |control| {
                let encoded =
                    encode_connector_payloads(&inputs, NAMESPACE_SOURCE, ns_limits(), control)?;
                let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
                let result = encode_admitted(
                    &origin,
                    &encoded,
                    if ordinary { 0 } else { SOURCE },
                    limits(),
                    &mut |_| Ok(()),
                    &mut work,
                )
                .map(|_| ());
                finish(result, work)
            },
            ordinary,
        );
        every_prefix(
            |control| {
                let decoded = decode_connector_payloads(
                    encoded.as_wire(),
                    NAMESPACE_SOURCE,
                    ns_limits(),
                    control,
                )?;
                let mut input = raw;
                if ordinary {
                    input.kind = None;
                }
                let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
                let result = decode_admitted(
                    &input,
                    &decoded,
                    SOURCE,
                    limits(),
                    &mut |_| Ok(()),
                    &mut work,
                )
                .map(|_| ());
                finish(result, work)
            },
            ordinary,
        );
    }
    let other = Control::default();
    let mut work = CompileCheckpoints::try_new(&other, CompilePhase::Decode).unwrap();
    let before = trace(&other).len();
    assert!(matches!(
        encode_admitted(
            &origin,
            &encoded,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut work
        ),
        Err(Error::InvalidShape(
            "value origin caller work uses a different namespace control"
        ))
    ));
    assert_eq!(trace(&other).len(), before);
}

#[test]
fn origin_admitted_original_320_source_walk_uses_caller_quantum_and_sparse_max_id() {
    let origin = provider(ConnectorCodecCategory::ReadColumn, &[9]);
    let others: Vec<_> = (0..320)
        .map(|_| payload(ConnectorCodecCategory::ReadColumn, &[9]))
        .collect();
    let mut inputs: Vec<_> = others
        .iter()
        .enumerate()
        .map(|(at, payload)| (at as u32, payload))
        .collect();
    inputs.push((u32::MAX, original(&origin)));
    let baseline = Control::default();
    let namespace =
        encode_connector_payloads(&inputs, NAMESPACE_SOURCE, ns_limits(), &baseline).unwrap();
    let mut work = CompileCheckpoints::try_new(&baseline, CompilePhase::LowerProgram).unwrap();
    let before = trace(&baseline).len();
    let (actual, _) = encode_admitted(
        &origin,
        &namespace,
        SOURCE,
        limits(),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    assert_eq!(
        actual,
        wrap(wire::value_origin::Kind::ProviderField(
            wire::ProviderFieldOrigin {
                scan_node_id: Some(0),
                column_payload_id: Some(u32::MAX),
            }
        ))
    );
    work.finish().unwrap();
    let positive = trace(&baseline);
    let at = before
        + positive[before..]
            .iter()
            .position(|(_, units)| *units == 256)
            .unwrap();
    assert_eq!(positive[at].0, CompilePhase::LowerProgram);
    for cause in CAUSES {
        let control = Control {
            stop: Some((at, cause)),
            ..Control::default()
        };
        let namespace =
            encode_connector_payloads(&inputs, NAMESPACE_SOURCE, ns_limits(), &control).unwrap();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
        assert!(
            matches!(encode_admitted(&origin, &namespace, SOURCE, limits(), &mut |_| Ok(()), &mut work), Err(Error::Control(actual)) if actual == cause)
        );
        assert_eq!(trace(&control), positive[..=at]);
    }
}

#[test]
fn origin_admitted_original_namespace_header_overflow_precedes_pending_255_control() {
    let control = ArmedControl::default();
    let ns = ConnectorPayloadProjectionLimits {
        max_definitions: usize::MAX,
        max_payload_bytes: usize::MAX,
        max_allocation_requests: usize::MAX,
        max_allocation_request_bytes: usize::MAX,
        max_coexisting_source_and_request_bytes: usize::MAX,
        max_work: usize::MAX,
    };
    let encoded = encode_connector_payloads(&[], usize::MAX, ns, &control).unwrap();
    let decoded = decode_connector_payloads(&[], usize::MAX, ns, &control).unwrap();
    let limit = ValueOriginProjectionLimits {
        max_allocation_requests: usize::MAX,
        max_allocation_request_bytes: usize::MAX,
        max_coexisting_source_and_request_bytes: usize::MAX,
        max_work: usize::MAX,
    };
    let (origin, raw) = nonprovider_cases().remove(0);
    for receiving in [false, true] {
        for cause in CAUSES {
            *control.armed.lock().unwrap() = None;
            let mut work =
                CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
            for _ in 0..255 {
                work.step().unwrap();
            }
            let before = *control.callbacks.lock().unwrap();
            *control.armed.lock().unwrap() = Some(cause);
            let result = if receiving {
                preflight_decode_admitted(
                    &raw,
                    &decoded,
                    usize::MAX,
                    limit,
                    &mut |_| Ok(()),
                    &mut work,
                )
            } else {
                preflight_encode_admitted(
                    &origin,
                    &encoded,
                    usize::MAX,
                    limit,
                    &mut |_| Ok(()),
                    &mut work,
                )
            };
            assert!(matches!(
                result,
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(*control.callbacks.lock().unwrap(), before);
        }
    }
    *control.armed.lock().unwrap() = None;
    assert!(matches!(
        encode_value_origin(&origin, &encoded, usize::MAX, limit),
        Err(Error::Payload(ConnectorPayloadCodecError::InvalidShape(_)))
    ));
}
