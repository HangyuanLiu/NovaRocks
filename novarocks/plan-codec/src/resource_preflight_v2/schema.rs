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

//! Reachable wire fields joined to their actual generated Rust payload layouts.
//! This is schema coverage, not a container-growth or allocation-peak model.

use std::collections::BTreeMap;

use novarocks_proto_models::generated_resource_layouts;
use novarocks_proto_models::resource_layout::{
    Cardinality, GeneratedResourceLayout, ObjectKind, ObjectLayout, RustLayout, Storage, WireField,
    WireKind,
};
use novarocks_type_contract::CompileCheckpoints;

use super::ResourceModelError;

pub(super) struct ModelField {
    pub(super) wire: &'static WireField,
    /// The actual parent field or oneof payload, including its inline wrapper.
    pub(super) rust: &'static RustLayout,
    pub(super) repeated: Option<&'static RustLayout>,
    pub(super) message: Option<usize>,
}

pub(super) struct ModelObject {
    pub(super) layout: &'static ObjectLayout,
    pub(super) fields: BTreeMap<u32, ModelField>,
}

pub(super) struct Schema {
    pub(super) root: usize,
    /// Only messages are scanner frames. Oneof enums are flattened by wire tag.
    pub(super) objects: Vec<ModelObject>,
}

type Registry = BTreeMap<&'static str, &'static ObjectLayout>;

pub(super) fn build<T: GeneratedResourceLayout>(
    work: &mut CompileCheckpoints<'_>,
) -> Result<Schema, ResourceModelError> {
    let mut registry = Registry::new();
    for layout in generated_resource_layouts() {
        work.step()?;
        if registry.insert(layout.schema_id, layout).is_some() {
            return Err(ResourceModelError::Schema(
                "duplicate generated schema identity",
            ));
        }
    }
    if T::RESOURCE_LAYOUT.schema_id != T::SCHEMA_ID {
        return Err(ResourceModelError::Schema("root schema identity differs"));
    }
    // Always use the registry's actual generated layout, not a caller's layout.
    let root = message_layout(&registry, T::SCHEMA_ID, work)?;
    if T::RESOURCE_LAYOUT.size != root.size || T::RESOURCE_LAYOUT.alignment != root.alignment {
        return Err(ResourceModelError::Schema(
            "root generated layout dimensions differ",
        ));
    }
    let mut state = BuildState {
        registry,
        indices: BTreeMap::from([(root.schema_id, 0)]),
        objects: vec![ModelObject {
            layout: root,
            fields: BTreeMap::new(),
        }],
    };
    let mut index = 0;
    while index < state.objects.len() {
        work.step()?;
        let layout = state.objects[index].layout;
        let mut fields = BTreeMap::new();
        for field in layout.fields {
            work.step()?;
            if field.wire.is_empty() {
                return Err(ResourceModelError::Schema(
                    "generated field has no wire tags",
                ));
            }
            match field.rust.storage {
                Storage::Option(inner) if matches!(inner.storage, Storage::Oneof(_)) => {
                    let Storage::Oneof(target) = inner.storage else {
                        unreachable!();
                    };
                    state.flatten_oneof(field.wire, target, &mut fields, work)?;
                }
                Storage::Oneof(_) => {
                    return Err(ResourceModelError::Schema(
                        "oneof parent lacks optional slot",
                    ));
                }
                _ => {
                    if field.wire.len() != 1 {
                        return Err(ResourceModelError::Schema(
                            "ordinary field has multiple wire tags",
                        ));
                    }
                    state.insert_field(&mut fields, &field.wire[0], &field.rust, false, work)?;
                }
            }
        }
        state.objects[index].fields = fields;
        index += 1;
    }
    check_acyclic(&state.objects, work)?;
    Ok(Schema {
        root: 0,
        objects: state.objects,
    })
}

fn message_layout(
    registry: &Registry,
    id: &str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<&'static ObjectLayout, ResourceModelError> {
    work.step()?;
    let layout = registry.get(id).copied().ok_or(ResourceModelError::Schema(
        "unresolved generated message reference",
    ))?;
    if layout.kind != ObjectKind::Message {
        return Err(ResourceModelError::Schema(
            "message reference targets a oneof",
        ));
    }
    Ok(layout)
}

struct BuildState {
    registry: Registry,
    indices: BTreeMap<&'static str, usize>,
    objects: Vec<ModelObject>,
}

impl BuildState {
    fn flatten_oneof(
        &mut self,
        parent: &'static [WireField],
        target: &'static str,
        fields: &mut BTreeMap<u32, ModelField>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ResourceModelError> {
        work.step()?;
        let variants = self
            .registry
            .get(target)
            .copied()
            .ok_or(ResourceModelError::Schema(
                "unresolved generated oneof reference",
            ))?;
        if variants.kind != ObjectKind::Oneof || variants.fields.len() != parent.len() {
            return Err(ResourceModelError::Schema("oneof layout coverage differs"));
        }
        let mut by_tag = BTreeMap::new();
        for variant in variants.fields {
            work.step()?;
            if variant.wire.len() != 1 {
                return Err(ResourceModelError::Schema(
                    "oneof payload has multiple wire tags",
                ));
            }
            if by_tag.insert(variant.wire[0].number, variant).is_some() {
                return Err(ResourceModelError::Schema("duplicate oneof payload tag"));
            }
        }
        for wire in parent {
            work.step()?;
            let variant = by_tag
                .remove(&wire.number)
                .ok_or(ResourceModelError::Schema(
                    "oneof parent has no matching payload",
                ))?;
            let other = &variant.wire[0];
            if wire.name != other.name
                || wire.kind != other.kind
                || wire.cardinality != other.cardinality
                || wire.packed != other.packed
                || wire.target != other.target
                || wire.map_entry.is_some()
                || other.map_entry.is_some()
                || wire.cardinality != Cardinality::Optional
            {
                return Err(ResourceModelError::Schema(
                    "oneof wire correspondence differs",
                ));
            }
            self.insert_field(fields, wire, &variant.rust, true, work)?;
        }
        if !by_tag.is_empty() {
            return Err(ResourceModelError::Schema("uncovered oneof payload"));
        }
        Ok(())
    }

    fn insert_field(
        &mut self,
        fields: &mut BTreeMap<u32, ModelField>,
        wire: &'static WireField,
        rust: &'static RustLayout,
        oneof_payload: bool,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ResourceModelError> {
        work.step()?;
        if wire.number == 0 || wire.number > (1 << 29) - 1 {
            return Err(ResourceModelError::Schema("invalid generated wire tag"));
        }
        if wire.map_entry.is_some() {
            return Err(ResourceModelError::Schema(
                "map storage has no resource model",
            ));
        }
        let (leaf, repeated) = match (wire.cardinality, rust.storage, oneof_payload) {
            (Cardinality::Repeated, Storage::Vec(inner), false) => (inner, Some(inner)),
            (Cardinality::Optional, Storage::Option(inner), false) => (inner, None),
            (Cardinality::Optional, _, true) => (rust, None),
            (Cardinality::Singular | Cardinality::Required, _, false) => (rust, None),
            _ => {
                return Err(ResourceModelError::Schema(
                    "wire cardinality and Rust wrapper differ",
                ));
            }
        };
        work.step()?;
        let message = match (wire.kind, leaf.storage) {
            (WireKind::Message, Storage::Message(id)) => {
                if wire.target != Some(id) {
                    return Err(ResourceModelError::Schema(
                        "wire and Rust message targets differ",
                    ));
                }
                let layout = message_layout(&self.registry, id, work)?;
                if leaf.size != layout.size || leaf.alignment != layout.alignment {
                    return Err(ResourceModelError::Schema("message payload layout differs"));
                }
                let target = match self.indices.get(id).copied() {
                    Some(index) => index,
                    None => {
                        let index = self.objects.len();
                        self.indices.insert(id, index);
                        self.objects.push(ModelObject {
                            layout,
                            fields: BTreeMap::new(),
                        });
                        index
                    }
                };
                Some(target)
            }
            (WireKind::String, Storage::String) if wire.target.is_none() => None,
            (WireKind::Bytes, Storage::Vec(inner))
                if wire.target.is_none()
                    && matches!(inner.storage, Storage::Scalar)
                    && inner.size == 1
                    && inner.alignment == 1 =>
            {
                work.step()?;
                None
            }
            (kind, Storage::Scalar) if scalar_wire(kind) => {
                if leaf.size != scalar_size(kind) {
                    return Err(ResourceModelError::Schema("scalar payload size differs"));
                }
                if (kind == WireKind::Enum) != wire.target.is_some() {
                    return Err(ResourceModelError::Schema("scalar wire target differs"));
                }
                None
            }
            (
                _,
                Storage::Box(_)
                | Storage::Bytes
                | Storage::HashMap { .. }
                | Storage::BTreeMap { .. },
            ) => {
                return Err(ResourceModelError::Schema("unsupported generated storage"));
            }
            _ => {
                return Err(ResourceModelError::Schema(
                    "wire kind and Rust payload differ",
                ));
            }
        };
        if wire.packed && (repeated.is_none() || !scalar_wire(wire.kind)) {
            return Err(ResourceModelError::Schema(
                "packed field is not repeated scalar",
            ));
        }
        if fields
            .insert(
                wire.number,
                ModelField {
                    wire,
                    rust,
                    repeated,
                    message,
                },
            )
            .is_some()
        {
            return Err(ResourceModelError::Schema("duplicate message wire tag"));
        }
        Ok(())
    }
}

fn scalar_wire(kind: WireKind) -> bool {
    !matches!(kind, WireKind::String | WireKind::Bytes | WireKind::Message)
}

fn scalar_size(kind: WireKind) -> usize {
    match kind {
        WireKind::Bool => 1,
        WireKind::Double
        | WireKind::Int64
        | WireKind::Uint64
        | WireKind::Fixed64
        | WireKind::Sfixed64
        | WireKind::Sint64 => 8,
        WireKind::Float
        | WireKind::Int32
        | WireKind::Fixed32
        | WireKind::Uint32
        | WireKind::Enum
        | WireKind::Sfixed32
        | WireKind::Sint32 => 4,
        WireKind::String | WireKind::Message | WireKind::Bytes => 0,
    }
}

fn check_acyclic(
    objects: &[ModelObject],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ResourceModelError> {
    let mut states = Vec::with_capacity(objects.len());
    for _ in objects {
        work.step()?;
        states.push(0_u8);
    }
    let mut stack = Vec::new();
    for start in 0..objects.len() {
        work.step()?;
        if states[start] != 0 {
            continue;
        }
        states[start] = 1;
        stack.push((start, objects[start].fields.values()));
        while let Some((_, fields)) = stack.last_mut() {
            work.step()?;
            let Some(field) = fields.next() else {
                let (index, _) = stack.pop().expect("active schema traversal");
                states[index] = 2;
                continue;
            };
            let Some(child) = field.message else { continue };
            match states[child] {
                1 => {
                    return Err(ResourceModelError::Schema(
                        "recursive generated message graph",
                    ));
                }
                2 => {}
                _ => {
                    states[child] = 1;
                    stack.push((child, objects[child].fields.values()));
                }
            }
        }
    }
    Ok(())
}
