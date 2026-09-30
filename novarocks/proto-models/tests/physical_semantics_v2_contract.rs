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

use std::{
    collections::BTreeSet,
    fmt::Debug,
    mem::{align_of, size_of},
};

use novarocks_proto_models::{
    FILE_DESCRIPTOR_SET, generated_resource_layouts, physical_control_v2 as control,
    physical_semantics_v2 as v2,
    resource_layout::{GeneratedResourceLayout, ObjectKind},
};
use prost::Message;
use prost_reflect::{DescriptorPool, Kind, MessageDescriptor};

fn roundtrip<M: Message + Default + PartialEq + Debug>(value: &M) {
    assert_eq!(M::decode(value.encode_to_vec().as_slice()).unwrap(), *value);
}
fn pool() -> DescriptorPool {
    DescriptorPool::decode(FILE_DESCRIPTOR_SET).unwrap()
}
fn effects(scope: v2::proof_scope::Kind) -> v2::CallEffects {
    v2::CallEffects {
        value_stability: v2::ValueStability::Volatile as i32,
        own_row_error: v2::OwnRowError::MayRaise as i32,
        failure_behavior: v2::FailureBehavior::ReturnsNull as i32,
        null_behavior: v2::NullBehavior::CalledOnNull as i32,
        argument_control: Some(v2::ArgumentControl {
            kind: Some(v2::argument_control::Kind::HigherOrder(
                control::HigherOrderControl {
                    body_ordinal: u32::MAX,
                    body_demand: control::EvaluationDemand::TruthOnly as i32,
                },
            )),
        }),
        instance_state: v2::InstanceState::ScalarInstance as i32,
        observable_effects: Some(v2::ObservableEffects {
            rng_sampling: true,
            warnings: true,
            controlled_wait: true,
        }),
        environment: vec![
            v2::SemanticParameterRef {
                id: Some(0),
                expected_key: v2::SemanticParameterKey::TimeZone as i32,
            },
            v2::SemanticParameterRef {
                id: Some(u32::MAX),
                expected_key: v2::SemanticParameterKey::AllowThrowException as i32,
            },
        ],
        proof_scope: Some(v2::ProofScope { kind: Some(scope) }),
    }
}

#[test]
fn all_six_parameter_variants_preserve_raw_values_and_oneof_presence() {
    use v2::semantic_parameter::Value;
    let values = [
        Value::StatementStartUtcMicros(i64::MIN),
        Value::TimeZone("+08:00".into()),
        Value::AllowThrowException(false),
        Value::DecimalOverflowToDouble(true),
        Value::GroupConcatLegacy(false),
        Value::GroupConcatMaxLen(i64::MAX),
    ];
    let parameters = v2::SemanticParameters {
        entries: values
            .into_iter()
            .enumerate()
            .map(|(ordinal, value)| v2::SemanticParameter {
                id: if ordinal == 5 {
                    u32::MAX
                } else {
                    ordinal as u32
                },
                value: Some(value),
            })
            .collect(),
    };
    roundtrip(&parameters);
    let pool = pool();
    let descriptor = pool
        .get_message_by_name("novarocks.physical_semantics_v2.SemanticParameter")
        .unwrap();
    assert_eq!(
        descriptor
            .oneofs()
            .find(|oneof| oneof.name() == "value")
            .unwrap()
            .fields()
            .count(),
        6
    );
    for entry in &parameters.entries {
        let absent = v2::SemanticParameter {
            id: entry.id,
            value: None,
        };
        assert_ne!(entry.encode_to_vec(), absent.encode_to_vec());
    }
    let zero = v2::SemanticParameter {
        id: 0,
        value: Some(Value::GroupConcatMaxLen(0)),
    };
    roundtrip(&zero);
    assert_ne!(
        zero.encode_to_vec(),
        v2::SemanticParameter::default().encode_to_vec()
    );
}

#[test]
fn all_nine_effect_fields_higher_order_channels_and_both_proof_scopes_survive() {
    let value = effects(v2::proof_scope::Kind::DomainId(0));
    roundtrip(&value);
    let unconditional = effects(v2::proof_scope::Kind::Unconditional(control::Empty {}));
    roundtrip(&unconditional);
    assert_ne!(value.encode_to_vec(), unconditional.encode_to_vec());
    let max_domain = effects(v2::proof_scope::Kind::DomainId(u32::MAX));
    roundtrip(&max_domain);
    assert_ne!(value.encode_to_vec(), max_domain.encode_to_vec());
    let descriptor = pool()
        .get_message_by_name("novarocks.physical_semantics_v2.CallEffects")
        .unwrap();
    assert_eq!(
        descriptor
            .fields()
            .map(|field| (field.number(), field.name().to_owned()))
            .collect::<Vec<_>>(),
        vec![
            (1, "value_stability".into()),
            (2, "own_row_error".into()),
            (3, "failure_behavior".into()),
            (4, "null_behavior".into()),
            (5, "argument_control".into()),
            (6, "instance_state".into()),
            (7, "observable_effects".into()),
            (8, "environment".into()),
            (9, "proof_scope".into()),
        ]
    );
    for simple in [
        v2::SimpleArgumentControl::Eager,
        v2::SimpleArgumentControl::TypeOnly,
        v2::SimpleArgumentControl::If,
        v2::SimpleArgumentControl::Coalesce,
        v2::SimpleArgumentControl::SimpleCase,
        v2::SimpleArgumentControl::SearchedCase,
        v2::SimpleArgumentControl::Aggregate,
        v2::SimpleArgumentControl::Window,
        v2::SimpleArgumentControl::Table,
    ] {
        roundtrip(&v2::ArgumentControl {
            kind: Some(v2::argument_control::Kind::Simple(simple as i32)),
        });
    }
    roundtrip(&v2::ArgumentControl {
        kind: Some(v2::argument_control::Kind::HigherOrder(
            control::HigherOrderControl {
                body_ordinal: 0,
                body_demand: control::EvaluationDemand::Value as i32,
            },
        )),
    });
    // These tests cover representation, not valid owner declarations or
    // application admission of the deliberately independent field values.
}

#[test]
fn six_typed_sites_and_context_references_preserve_zero_max_and_absence() {
    use v2::call_site::Kind;
    let node = v2::NodeCallSite {
        node_id: Some(0),
        call: u32::MAX,
    };
    let sites = [
        Kind::ExpressionUseId(0),
        Kind::Aggregate(node),
        Kind::TopNState(node),
        Kind::WriterPartial(node),
        Kind::WriterFinal(node),
        Kind::TableNodeId(u32::MAX),
    ];
    let calls = v2::FrozenCalls {
        entries: sites
            .into_iter()
            .enumerate()
            .map(|(ordinal, site)| v2::FrozenCall {
                site: Some(v2::CallSite { kind: Some(site) }),
                context: Some(v2::EffectContext {
                    use_id: Some(if ordinal == 0 { 0 } else { u32::MAX }),
                    domain_id: Some(if ordinal == 0 { u32::MAX } else { 0 }),
                    demand: if ordinal == 0 {
                        control::EvaluationDemand::TruthOnly
                    } else {
                        control::EvaluationDemand::Value
                    } as i32,
                }),
                effects: Some(effects(v2::proof_scope::Kind::Unconditional(
                    control::Empty {},
                ))),
            })
            .collect(),
    };
    roundtrip(&calls);
    let descriptor = pool()
        .get_message_by_name("novarocks.physical_semantics_v2.CallSite")
        .unwrap();
    assert_eq!(
        descriptor
            .oneofs()
            .find(|oneof| oneof.name() == "kind")
            .unwrap()
            .fields()
            .count(),
        6
    );
    for id in [0, u32::MAX] {
        let reference = v2::SemanticParameterRef {
            id: Some(id),
            expected_key: v2::SemanticParameterKey::TimeZone as i32,
        };
        roundtrip(&reference);
        assert_ne!(
            reference.encode_to_vec(),
            v2::SemanticParameterRef {
                id: None,
                ..reference
            }
            .encode_to_vec()
        );
        let context = v2::EffectContext {
            use_id: Some(id),
            domain_id: Some(id),
            demand: control::EvaluationDemand::Value as i32,
        };
        roundtrip(&context);
        assert_ne!(
            context.encode_to_vec(),
            v2::EffectContext {
                use_id: None,
                ..context
            }
            .encode_to_vec()
        );
        assert_ne!(
            context.encode_to_vec(),
            v2::EffectContext {
                domain_id: None,
                ..context
            }
            .encode_to_vec()
        );
        let node = v2::NodeCallSite {
            node_id: Some(id),
            call: 0,
        };
        roundtrip(&node);
        assert_ne!(
            node.encode_to_vec(),
            v2::NodeCallSite {
                node_id: None,
                ..node
            }
            .encode_to_vec()
        );
        for kind in [Kind::ExpressionUseId(id), Kind::TableNodeId(id)] {
            let site = v2::CallSite { kind: Some(kind) };
            roundtrip(&site);
            assert_ne!(
                site.encode_to_vec(),
                v2::CallSite::default().encode_to_vec()
            );
        }
    }
    for name in ["SemanticParameterRef", "EffectContext", "NodeCallSite"] {
        let descriptor = pool()
            .get_message_by_name(&format!("novarocks.physical_semantics_v2.{name}"))
            .unwrap();
        for field in descriptor
            .fields()
            .filter(|field| field.name() == "id" || field.name().ends_with("_id"))
        {
            assert!(field.supports_presence(), "{}", field.full_name());
        }
    }
}

#[test]
fn semantics_descriptor_is_nonrecursive_and_every_actual_layout_is_registered() {
    fn depth(message: MessageDescriptor, path: &mut BTreeSet<String>) -> usize {
        assert!(
            path.insert(message.full_name().to_owned()),
            "recursive carrier: {}",
            message.full_name()
        );
        let mut maximum = 1;
        for field in message.fields() {
            if let Kind::Message(child) = field.kind() {
                assert!(matches!(
                    child.package_name(),
                    "novarocks.physical_semantics_v2" | "novarocks.physical_control_v2"
                ));
                maximum = maximum.max(1 + depth(child, path));
            }
        }
        path.remove(message.full_name());
        maximum
    }
    let pool = pool();
    assert_eq!(
        depth(
            pool.get_message_by_name("novarocks.physical_semantics_v2.FrozenCalls")
                .unwrap(),
            &mut BTreeSet::new()
        ),
        5
    );
    assert_eq!(
        depth(
            pool.get_message_by_name("novarocks.physical_semantics_v2.SemanticParameters")
                .unwrap(),
            &mut BTreeSet::new()
        ),
        2
    );
    let layouts = generated_resource_layouts()
        .filter(|layout| {
            layout
                .schema_id
                .starts_with("novarocks.physical_semantics_v2.")
        })
        .collect::<Vec<_>>();
    let actual = layouts
        .iter()
        .map(|layout| layout.schema_id.to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(actual.len(), layouts.len());
    let mut expected = BTreeSet::new();
    for message in pool
        .all_messages()
        .filter(|message| message.package_name() == "novarocks.physical_semantics_v2")
    {
        expected.insert(message.full_name().to_owned());
        let layout = layouts
            .iter()
            .find(|layout| layout.schema_id == message.full_name())
            .unwrap();
        assert_eq!(layout.kind, ObjectKind::Message);
        let tags = layout
            .fields
            .iter()
            .flat_map(|field| field.wire.iter().map(|wire| wire.number))
            .collect::<Vec<_>>();
        let distinct = tags.iter().copied().collect::<BTreeSet<_>>();
        assert_eq!(tags.len(), distinct.len());
        assert_eq!(
            distinct,
            message.fields().map(|field| field.number()).collect()
        );
        for oneof in message.oneofs().filter(|oneof| {
            !oneof
                .fields()
                .any(|field| field.field_descriptor_proto().proto3_optional == Some(true))
        }) {
            expected.insert(oneof.full_name().to_owned());
            let layout = layouts
                .iter()
                .find(|layout| layout.schema_id == oneof.full_name())
                .unwrap();
            assert_eq!(layout.kind, ObjectKind::Oneof);
            assert_eq!(
                layout
                    .fields
                    .iter()
                    .flat_map(|field| field.wire.iter().map(|wire| wire.number))
                    .collect::<BTreeSet<_>>(),
                oneof.fields().map(|field| field.number()).collect()
            );
        }
    }
    assert_eq!(actual, expected);
    fn dimensions<T: GeneratedResourceLayout>() {
        assert_eq!(T::RESOURCE_LAYOUT.size, size_of::<T>());
        assert_eq!(T::RESOURCE_LAYOUT.alignment, align_of::<T>());
    }
    dimensions::<v2::SemanticParameters>();
    dimensions::<v2::SemanticParameter>();
    dimensions::<v2::semantic_parameter::Value>();
    dimensions::<v2::FrozenCalls>();
    dimensions::<v2::FrozenCall>();
    dimensions::<v2::CallEffects>();
    dimensions::<v2::CallSite>();
    dimensions::<v2::call_site::Kind>();
    dimensions::<v2::ArgumentControl>();
    dimensions::<v2::argument_control::Kind>();
    dimensions::<v2::ProofScope>();
    dimensions::<v2::proof_scope::Kind>();
    // Actual Rust layout coverage is an input to later resource modelling;
    // it does not prove bounded decoding allocations or a complete codec.
}
