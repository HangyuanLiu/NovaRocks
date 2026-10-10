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
use std::sync::Mutex;
const SOURCE: usize = 1024 * 1024;
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
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after refusal");
        }
        trace.push((phase, units));
        match self.stop {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn trace(control: &Control) -> Vec<(CompilePhase, u32)> {
    control.trace.lock().unwrap().clone()
}
fn limits() -> ProviderBindingProjectionLimits {
    ProviderBindingProjectionLimits {
        max_definitions: 1024,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 4 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 8 * 1024 * 1024,
        max_work: 64 * 1024 * 1024,
    }
}
fn binding(instance: &str, catalog: &str) -> ConnectorReadBinding {
    ConnectorReadBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::try_from_canonical(instance).unwrap(),
        },
        CatalogHandle::new(
            ConnectorInstanceId::try_from_canonical(catalog).unwrap(),
            CatalogVersion::from_bytes([0; 32]),
        ),
    )
}
fn expected(id: u32, instance: &str, name: &str) -> wire::ProviderBindingDefinition {
    wire::ProviderBindingDefinition {
        id,
        provider_id: "iceberg".into(),
        instance_id: instance.into(),
        catalog: Some(catalog::CatalogHandle {
            catalog_name: name.into(),
            version: vec![0; 32],
        }),
    }
}
#[test]
fn provider_binding_namespace_independent_wire_all_fields_and_typed_receiving_preserve_exact_sources()
 {
    let sources = [
        binding("lake", "lake"),
        binding("_a.b-c", "different_catalog"),
    ];
    let inputs = [(u32::MAX, &sources[0]), (0, &sources[1])];
    let control = Control::default();
    let token = encode_provider_bindings(&inputs, SOURCE, limits(), &control).unwrap();
    assert_eq!(
        token.as_wire(),
        [
            expected(u32::MAX, "lake", "lake"),
            expected(0, "_a.b-c", "different_catalog")
        ]
    );
    assert_eq!(token.source_count(), 2);
    assert!(std::ptr::eq(
        token.binding(u32::MAX).unwrap().unwrap(),
        &sources[0]
    ));
    assert_eq!(token.source_id(&sources[1]).unwrap(), 0);
    let received = decode_provider_bindings(token.as_wire(), SOURCE, limits(), &control).unwrap();
    assert!(std::ptr::eq(received.as_wire(), token.as_wire()));
    assert_eq!(received.binding(0).unwrap(), Some(&sources[1]));
    assert_eq!(received.binding(u32::MAX).unwrap(), Some(&sources[0]));
    assert!(received.binding(42).unwrap().is_none());
    // Neutral construction preserves this mismatch; the actual read-reference
    // validator must reject it later. No installed/catalog binding is granted.
    assert_ne!(
        received
            .binding(0)
            .unwrap()
            .unwrap()
            .descriptor()
            .instance_id,
        *received
            .binding(0)
            .unwrap()
            .unwrap()
            .catalog_handle()
            .catalog_name()
    );
    assert_eq!(
        received
            .binding(0)
            .unwrap()
            .unwrap()
            .catalog_handle()
            .version()
            .as_bytes(),
        &[0; 32]
    );
    assert!(token.retained_invoice_floor().unwrap() > SOURCE);
    assert!(received.retained_invoice_floor().unwrap() > SOURCE);
}
#[test]
fn provider_binding_namespace_sparse_aliases_duplicate_ids_and_unique_original_pointer_are_exact() {
    let source = binding("lake", "lake");
    let equivalent = source.clone();
    let aliases = [(u32::MAX, &source), (0, &source)];
    let control = Control::default();
    let token = encode_provider_bindings(&aliases, SOURCE, limits(), &control).unwrap();
    assert!(matches!(
        token.source_id(&source),
        Err(Error::InvalidShape(
            "provider binding source association is ambiguous"
        ))
    ));
    assert!(matches!(
        token.source_id(&equivalent),
        Err(Error::InvalidShape(
            "provider binding source owner is not in this namespace"
        ))
    ));
    let duplicate = [(0, &source), (0, &equivalent)];
    assert!(encode_provider_bindings(&duplicate, SOURCE, limits(), &control).is_err());
    assert!(
        decode_provider_bindings(
            &[expected(0, "lake", "lake"), expected(0, "lake", "lake")],
            SOURCE,
            limits(),
            &control
        )
        .is_err()
    );
    let empty = encode_provider_bindings(&[], 0, limits(), &control).unwrap();
    assert!(empty.as_wire().is_empty());
    assert_eq!(empty.facts().allocation_requests_upper_bound, 0);
    let empty = decode_provider_bindings(&[], 0, limits(), &control).unwrap();
    assert!(empty.binding(u32::MAX).unwrap().is_none());
    assert_eq!(empty.facts().allocation_requests_upper_bound, 0);
}
#[test]
fn provider_binding_namespace_receiving_requires_complete_catalog_and_sole_canonical_identity_grammar()
 {
    let control = Control::default();
    let mut absent = expected(0, "lake", "lake");
    absent.catalog = None;
    assert!(decode_provider_bindings(&[absent], SOURCE, limits(), &control).is_err());
    for n in [0, 31, 33] {
        let mut raw = expected(0, "lake", "lake");
        raw.catalog.as_mut().unwrap().version = vec![0; n];
        assert!(decode_provider_bindings(&[raw], SOURCE, limits(), &control).is_err());
    }
    for name in ["", "UPPER", "bad|name", "é"] {
        let mut raw = expected(0, "lake", "lake");
        raw.provider_id = name.into();
        assert!(matches!(
            decode_provider_bindings(&[raw], SOURCE, limits(), &control),
            Err(Error::Identity(ConnectorIdentityError::InvalidProviderId))
        ));
        let raw = expected(0, name, "lake");
        assert!(matches!(
            decode_provider_bindings(&[raw], SOURCE, limits(), &control),
            Err(Error::Identity(
                ConnectorIdentityError::InvalidCanonicalInstanceId
            ))
        ));
        let raw = expected(0, "lake", name);
        assert!(matches!(
            decode_provider_bindings(&[raw], SOURCE, limits(), &control),
            Err(Error::Identity(
                ConnectorIdentityError::InvalidCanonicalInstanceId
            ))
        ));
    }
    let mut raw = expected(u32::MAX, "_a.b-c", "other_catalog");
    raw.provider_id = "provider-a_b9".into();
    let token =
        decode_provider_bindings(std::slice::from_ref(&raw), SOURCE, limits(), &control).unwrap();
    assert_eq!(
        token
            .binding(u32::MAX)
            .unwrap()
            .unwrap()
            .descriptor()
            .provider_id
            .as_str(),
        "provider-a_b9"
    );
    assert_eq!(
        token
            .binding(u32::MAX)
            .unwrap()
            .unwrap()
            .descriptor()
            .instance_id
            .as_str(),
        "_a.b-c"
    );
}
fn exact(f: ProviderBindingProjectionFacts) -> ProviderBindingProjectionLimits {
    ProviderBindingProjectionLimits {
        max_definitions: f.definition_count,
        max_allocation_requests: f.allocation_requests_upper_bound,
        max_allocation_request_bytes: f.allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: f.coexisting_source_and_request_bytes_upper_bound,
        max_work: f.cumulative_work_upper_bound,
    }
}
fn lower(
    mut limits: ProviderBindingProjectionLimits,
    at: usize,
) -> ProviderBindingProjectionLimits {
    let field = match at {
        0 => &mut limits.max_definitions,
        1 => &mut limits.max_allocation_requests,
        2 => &mut limits.max_allocation_request_bytes,
        3 => &mut limits.max_coexisting_source_and_request_bytes,
        _ => &mut limits.max_work,
    };
    *field -= 1;
    limits
}
#[test]
fn provider_binding_namespace_all_five_limits_capacity_floors_aliases_and_checked_arithmetic_are_independent()
 {
    let source = binding("lake", "lake");
    let inputs = [(0, &source)];
    let control = Control::default();
    let token = encode_provider_bindings(&inputs, SOURCE, limits(), &control).unwrap();
    let e = exact(*token.facts());
    assert!(encode_provider_bindings(&inputs, SOURCE, e, &control).is_ok());
    for at in 0..5 {
        assert!(encode_provider_bindings(&inputs, SOURCE, lower(e, at), &control).is_err());
    }
    let received = decode_provider_bindings(token.as_wire(), SOURCE, limits(), &control).unwrap();
    let d = exact(*received.facts());
    assert!(decode_provider_bindings(token.as_wire(), SOURCE, d, &control).is_ok());
    for at in 0..5 {
        assert!(decode_provider_bindings(token.as_wire(), SOURCE, lower(d, at), &control).is_err());
    }
    assert!(encode_provider_bindings(&inputs, 0, limits(), &control).is_err());
    let mut raw = expected(0, "lake", "lake");
    raw.instance_id.reserve(8192);
    assert!(
        decode_provider_bindings(
            &[raw],
            size_of::<wire::ProviderBindingDefinition>(),
            limits(),
            &control
        )
        .is_err()
    );
    assert!(bytes::<usize>(usize::MAX).is_err());
    assert!(add(usize::MAX, 1).is_err());
    assert!(mul(usize::MAX, 2).is_err());
    // Prove the known lower-floor formula does not double-count a shared Arc.
    // This is threshold admission evidence, not an allocator/host MEM receipt.
    let instance = ConnectorInstanceId::try_from_canonical("lake").unwrap();
    let shared = ConnectorReadBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: instance.clone(),
        },
        CatalogHandle::new(instance, CatalogVersion::from_bytes([0; 32])),
    );
    let inputs = [(0, &shared)];
    let known = bytes::<(u32, &ConnectorReadBinding)>(1).unwrap()
        + size_of::<ConnectorReadBinding>()
        + provider_arc(7).unwrap()
        + provider_arc(4).unwrap();
    assert!(encode_provider_bindings(&inputs, known, limits(), &control).is_ok());
    assert!(encode_provider_bindings(&inputs, known - 1, limits(), &control).is_err());
}
fn every_prefix(mut invoke: impl FnMut(&Control) -> Result<(), Error>, ordinary: bool) {
    let baseline = Control::default();
    assert_eq!(invoke(&baseline).is_err(), ordinary);
    let positive = trace(&baseline);
    for at in 0..positive.len() {
        for cause in CAUSES {
            let control = Control {
                stop: Some((at, cause)),
                ..Control::default()
            };
            assert!(matches!(invoke(&control), Err(Error::Control(actual)) if actual == cause));
            assert_eq!(trace(&control), positive[..=at]);
        }
    }
}
#[test]
fn provider_binding_namespace_success_ordinary_failure_and_native_constructor_tails_preserve_every_original_cause()
 {
    let source = binding("lake", "lake");
    let inputs = [(u32::MAX, &source), (0, &source)];
    every_prefix(
        |control| encode_provider_bindings(&inputs, SOURCE, limits(), control).map(|_| ()),
        false,
    );
    let raw = [
        expected(0, "lake", "lake"),
        expected(u32::MAX, "lake", "lake"),
    ];
    every_prefix(
        |control| decode_provider_bindings(&raw, SOURCE, limits(), control).map(|_| ()),
        false,
    );
    let duplicate = [(0, &source), (0, &source)];
    every_prefix(
        |control| encode_provider_bindings(&duplicate, SOURCE, limits(), control).map(|_| ()),
        true,
    );
    let invalid = [expected(0, "UPPER", "lake")];
    every_prefix(
        |control| decode_provider_bindings(&invalid, SOURCE, limits(), control).map(|_| ()),
        true,
    );
}
#[test]
fn provider_binding_namespace_source_lookup_and_retained_floor_keep_original_control_after_preparation()
 {
    let source = binding("lake", "lake");
    let inputs = [(0, &source)];
    for operation in 0..3 {
        let baseline = Control::default();
        let token = encode_provider_bindings(&inputs, SOURCE, limits(), &baseline).unwrap();
        let before = trace(&baseline).len();
        match operation {
            0 => {
                token.binding(u32::MAX).unwrap();
            }
            1 => {
                token.source_id(&source).unwrap();
            }
            _ => {
                token.retained_invoice_floor().unwrap();
            }
        }
        let positive = trace(&baseline);
        for at in before..positive.len() {
            for cause in CAUSES {
                let control = Control {
                    stop: Some((at, cause)),
                    ..Control::default()
                };
                let token = encode_provider_bindings(&inputs, SOURCE, limits(), &control).unwrap();
                let result = match operation {
                    0 => token.binding(u32::MAX).map(|_| ()),
                    1 => token.source_id(&source).map(|_| ()),
                    _ => token.retained_invoice_floor().map(|_| ()),
                };
                assert!(matches!(result, Err(Error::Control(actual)) if actual == cause));
                assert_eq!(trace(&control), positive[..=at]);
            }
        }
    }
}
#[test]
fn provider_binding_namespace_real_wide_both_directions_observe_quantum_and_preserve_arbitrary_sparse_ids()
 {
    let source = binding("lake", "lake");
    let inputs: Vec<_> = (0..320).rev().map(|id| (id, &source)).collect();
    let baseline = Control::default();
    let token = encode_provider_bindings(&inputs, SOURCE, limits(), &baseline).unwrap();
    let positive = trace(&baseline);
    let at = positive
        .iter()
        .position(|(_, units)| *units == 256)
        .unwrap();
    for cause in CAUSES {
        let control = Control {
            stop: Some((at, cause)),
            ..Control::default()
        };
        assert!(
            matches!(encode_provider_bindings(&inputs, SOURCE, limits(), &control), Err(Error::Control(actual)) if actual == cause)
        );
        assert_eq!(trace(&control), positive[..=at]);
    }
    assert_eq!(token.as_wire()[0].id, 319);
    assert_eq!(token.as_wire()[319].id, 0);
    let baseline = Control::default();
    let received = decode_provider_bindings(token.as_wire(), SOURCE, limits(), &baseline).unwrap();
    let positive = trace(&baseline);
    let at = positive
        .iter()
        .position(|(_, units)| *units == 256)
        .unwrap();
    for cause in CAUSES {
        let control = Control {
            stop: Some((at, cause)),
            ..Control::default()
        };
        assert!(
            matches!(decode_provider_bindings(token.as_wire(), SOURCE, limits(), &control), Err(Error::Control(actual)) if actual == cause)
        );
        assert_eq!(trace(&control), positive[..=at]);
    }
    assert_eq!(received.binding(0).unwrap(), Some(&source));
}
