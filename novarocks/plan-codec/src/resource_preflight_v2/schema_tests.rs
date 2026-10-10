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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use novarocks_proto_models::generated_resource_layouts;
use novarocks_proto_models::physical_package_v2::FragmentPackage;
use novarocks_proto_models::resource_layout::{
    Cardinality, GeneratedResourceLayout, ObjectKind, ObjectLayout, RustLayout, Storage, WireKind,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};

use super::{FragmentDecodeResourceModel, ResourceModelError, schema};

#[derive(Default)]
struct OriginalScope {
    observations: Mutex<Vec<u32>>,
}
impl PureCompileControl for OriginalScope {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Validate);
        assert!(units <= 256);
        self.observations.lock().unwrap().push(units);
        Ok(())
    }
}

type Registry = BTreeMap<&'static str, &'static ObjectLayout>;

fn registry() -> Registry {
    let mut result = BTreeMap::new();
    for layout in generated_resource_layouts() {
        assert!(result.insert(layout.schema_id, layout).is_none());
    }
    result
}

// Follow actual Rust storage, independently of the schema builder's wire join.
fn storage_references(storage: Storage, pending: &mut Vec<&'static str>) {
    match storage {
        Storage::Message(id) | Storage::Oneof(id) => pending.push(id),
        Storage::Option(inner) | Storage::Vec(inner) => storage_references(inner.storage, pending),
        Storage::Scalar | Storage::String => {}
        Storage::Bytes | Storage::Box(_) | Storage::HashMap { .. } | Storage::BTreeMap { .. } => {
            panic!("the actual package gained storage without this model's coverage")
        }
    }
}

fn actual_closure(registry: &Registry) -> BTreeSet<&'static str> {
    let mut pending = vec![FragmentPackage::SCHEMA_ID];
    let mut seen = BTreeSet::new();
    while let Some(id) = pending.pop() {
        if seen.insert(id) {
            for field in registry[id].fields {
                storage_references(field.rust.storage, &mut pending);
            }
        }
    }
    seen
}

fn wire_depth(id: &str, registry: &Registry, active: &mut BTreeSet<String>) -> usize {
    assert!(active.insert(id.to_owned()), "recursive message graph");
    let children = registry[id]
        .fields
        .iter()
        .flat_map(|field| field.wire)
        .filter(|wire| wire.kind == WireKind::Message)
        .map(|wire| wire_depth(wire.target.unwrap(), registry, active));
    let depth = 1 + children.max().unwrap_or(0);
    active.remove(id);
    depth
}

fn assert_rust_layout(actual: &RustLayout, expected: &RustLayout) {
    assert_eq!(actual.size, expected.size);
    assert_eq!(actual.alignment, expected.alignment);
    match (actual.storage, expected.storage) {
        (Storage::Scalar, Storage::Scalar) | (Storage::String, Storage::String) => {}
        (Storage::Message(a), Storage::Message(b)) | (Storage::Oneof(a), Storage::Oneof(b)) => {
            assert_eq!(a, b);
        }
        (Storage::Option(a), Storage::Option(b)) | (Storage::Vec(a), Storage::Vec(b)) => {
            assert_rust_layout(a, b);
        }
        _ => panic!("actual payload storage differs from its generated ABI"),
    }
}

fn unwrapped(layout: &RustLayout) -> &RustLayout {
    match layout.storage {
        Storage::Option(inner) => inner,
        _ => layout,
    }
}

#[test]
fn actual_fragment_model_covers_the_complete_generated_storage_closure() {
    let source = registry();
    let closure = actual_closure(&source);
    let depth = wire_depth(FragmentPackage::SCHEMA_ID, &source, &mut BTreeSet::new());
    let scope = OriginalScope::default();
    let model = FragmentDecodeResourceModel::try_new(&scope).unwrap();
    assert_eq!(model.max_message_depth, depth);
    assert_eq!(
        model.schema.objects[model.schema.root].layout.schema_id,
        FragmentPackage::SCHEMA_ID
    );
    let modeled: BTreeSet<_> = model
        .schema
        .objects
        .iter()
        .map(|object| object.layout.schema_id)
        .collect();
    let expected: BTreeSet<_> = closure
        .into_iter()
        .filter(|id| source[id].kind == ObjectKind::Message)
        .collect();
    assert_eq!(modeled, expected);
    let observations = scope.observations.lock().unwrap();
    assert_eq!(observations.first(), Some(&0));
    assert!(observations.contains(&256));
}

#[test]
fn every_wire_tag_joins_the_actual_message_or_oneof_payload_abi() {
    let source = registry();
    let model = FragmentDecodeResourceModel::try_new(&OriginalScope::default()).unwrap();
    let mut oneof_variants = 0;
    for object in &model.schema.objects {
        let mut expected = BTreeMap::new();
        for field in source[object.layout.schema_id].fields {
            if let Storage::Oneof(id) = unwrapped(&field.rust).storage {
                let variants = source[id];
                assert_eq!(variants.kind, ObjectKind::Oneof);
                for wire in field.wire {
                    let variant = variants
                        .fields
                        .iter()
                        .find(|variant| variant.wire[0].number == wire.number)
                        .unwrap();
                    assert_eq!(variant.wire.len(), 1);
                    assert!(
                        expected
                            .insert(wire.number, (wire, &variant.rust))
                            .is_none()
                    );
                    oneof_variants += 1;
                }
            } else {
                assert_eq!(field.wire.len(), 1);
                assert!(
                    expected
                        .insert(field.wire[0].number, (&field.wire[0], &field.rust))
                        .is_none()
                );
            }
        }
        assert_eq!(object.fields.len(), expected.len());
        for (tag, field) in &object.fields {
            let (wire, rust) = expected[tag];
            assert_eq!(field.wire.name, wire.name);
            assert_eq!(field.wire.number, wire.number);
            assert_eq!(field.wire.kind, wire.kind);
            assert_eq!(field.wire.cardinality, wire.cardinality);
            assert_eq!(field.wire.packed, wire.packed);
            assert_eq!(field.wire.target, wire.target);
            assert_rust_layout(field.rust, rust);
            if field.wire.kind == WireKind::Message {
                let target = model.schema.objects[field.message.unwrap()].layout;
                assert_eq!(target.kind, ObjectKind::Message);
                assert_eq!(Some(target.schema_id), field.wire.target);
            } else {
                assert!(field.message.is_none());
            }
        }
    }
    assert!(oneof_variants > 100);
}

#[test]
fn enum_targets_are_not_messages_and_byte_vectors_are_payloads() {
    let model = FragmentDecodeResourceModel::try_new(&OriginalScope::default()).unwrap();
    let mut enums = 0;
    let mut bytes = 0;
    for object in &model.schema.objects {
        for field in object.fields.values() {
            if field.wire.kind == WireKind::Enum {
                enums += 1;
                assert!(field.wire.target.is_some());
                assert!(field.message.is_none());
            }
            if field.wire.kind == WireKind::Bytes {
                bytes += 1;
                assert!(field.message.is_none());
                let payload = if field.wire.cardinality == Cardinality::Repeated {
                    field.repeated.unwrap()
                } else {
                    assert!(field.repeated.is_none());
                    unwrapped(field.rust)
                };
                let Storage::Vec(element) = payload.storage else {
                    panic!("bytes lack actual Vec backing")
                };
                assert!(matches!(element.storage, Storage::Scalar));
                assert_eq!(element.size, 1);
                assert_eq!(element.alignment, 1);
            }
            if field.wire.cardinality == Cardinality::Repeated {
                let Storage::Vec(element) = field.rust.storage else {
                    panic!("repeated field lacks Vec backing")
                };
                assert_rust_layout(field.repeated.unwrap(), element);
            } else {
                assert!(field.repeated.is_none());
            }
        }
    }
    assert!(enums > 0);
    assert!(bytes > 0);
}

#[test]
fn closed_public_model_uses_the_canonical_fragment_root_dimensions() {
    let source = registry();
    let canonical = source[FragmentPackage::SCHEMA_ID];
    let model = FragmentDecodeResourceModel::try_new(&OriginalScope::default()).unwrap();
    let root = model.schema.objects[model.schema.root].layout;
    assert_eq!(root.kind, ObjectKind::Message);
    assert_eq!(root.size, std::mem::size_of::<FragmentPackage>());
    assert_eq!(root.alignment, std::mem::align_of::<FragmentPackage>());
    assert_eq!(root.size, canonical.size);
    assert_eq!(root.alignment, canonical.alignment);
    assert_eq!(root.size, FragmentPackage::RESOURCE_LAYOUT.size);
    assert_eq!(root.alignment, FragmentPackage::RESOURCE_LAYOUT.alignment);
}

static INVALID_ROOT: ObjectLayout = ObjectLayout {
    schema_id: "fixture.mismatched_root",
    kind: ObjectKind::Message,
    size: 0,
    alignment: 1,
    fields: &[],
};
struct InconsistentRoot;
impl GeneratedResourceLayout for InconsistentRoot {
    const SCHEMA_ID: &'static str = FragmentPackage::SCHEMA_ID;
    const RESOURCE_LAYOUT: &'static ObjectLayout = &INVALID_ROOT;
}
struct UnregisteredRoot;
impl GeneratedResourceLayout for UnregisteredRoot {
    const SCHEMA_ID: &'static str = "fixture.mismatched_root";
    const RESOURCE_LAYOUT: &'static ObjectLayout = &INVALID_ROOT;
}

#[test]
fn private_schema_builder_refuses_inconsistent_or_unregistered_root_identity() {
    let scope = OriginalScope::default();
    let mut work = CompileCheckpoints::try_new(&scope, CompilePhase::Validate).unwrap();
    assert!(matches!(
        schema::build::<InconsistentRoot>(&mut work),
        Err(ResourceModelError::Schema(_))
    ));
    assert!(matches!(
        schema::build::<UnregisteredRoot>(&mut work),
        Err(ResourceModelError::Schema(_))
    ));
}
