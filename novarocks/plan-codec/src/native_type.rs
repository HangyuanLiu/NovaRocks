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

//! Native plan-wire `TypeDesc` construction and decoding.
//!
//! A `TypeDesc` carries one SQL type as a flat node array in canonical
//! preorder (see `common.proto`). This module owns the only constructors that
//! assemble such arrays and the only decoder that validates one -- against
//! the logical type budget, through the shared flat tree kernel -- before it
//! builds any Arrow type. It is a pure conversion of a sealed wire DTO and
//! never depends on a role runtime, Connector, or request assembly state.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use novarocks_proto_codec::flat_type_tree::{
    FlatEdge, FlatTreeLimits, FlatTreeViolation, validate_preorder,
};
use novarocks_proto_models::common;
use novarocks_type_contract::LogicalTypeLimits;
use novarocks_types::logical::{LogicalType, field_with_logical_type};

/// Protobuf message levels of any flat `TypeDesc`, the descriptor itself
/// included: `TypeDesc` -> `TypeNode` -> `TypeStructNode` -> `TypeStructMember`.
/// Constant whatever the SQL nesting depth.
pub(crate) const TYPE_DESC_WIRE_DEPTH: usize = 4;

const TIME_UNIT_MICROS: i32 = 2;
const TIME_UNIT_NANOS: i32 = 3;

/// A descriptor of one scalar type.
pub fn scalar_type_desc(scalar: common::ScalarType) -> common::TypeDesc {
    common::TypeDesc {
        nodes: vec![common::TypeNode {
            kind: Some(common::type_node::Kind::Scalar(scalar)),
        }],
    }
}

/// The scalar a descriptor consists of, when its root is a scalar.
pub fn root_scalar(desc: &common::TypeDesc) -> Option<&common::ScalarType> {
    match desc.nodes.first()?.kind.as_ref()? {
        common::type_node::Kind::Scalar(scalar) => Some(scalar),
        _ => None,
    }
}

/// A list descriptor over a complete element descriptor.
pub fn list_type_desc(element: common::TypeDesc) -> common::TypeDesc {
    let mut nodes = Vec::with_capacity(1 + element.nodes.len());
    nodes.push(common::TypeNode::default());
    let element = graft(&mut nodes, element);
    nodes[0].kind = Some(common::type_node::Kind::List(element));
    common::TypeDesc { nodes }
}

/// A map descriptor over complete key and value descriptors.
pub fn map_type_desc(key: common::TypeDesc, value: common::TypeDesc) -> common::TypeDesc {
    let mut nodes = Vec::with_capacity(1 + key.nodes.len() + value.nodes.len());
    nodes.push(common::TypeNode::default());
    let key = graft(&mut nodes, key);
    let value = graft(&mut nodes, value);
    nodes[0].kind = Some(common::type_node::Kind::Map(common::TypeMapNode {
        key,
        value,
    }));
    common::TypeDesc { nodes }
}

/// A struct descriptor over ordered, named, complete member descriptors.
pub fn struct_type_desc(
    members: impl IntoIterator<Item = (String, common::TypeDesc)>,
) -> common::TypeDesc {
    let mut nodes = vec![common::TypeNode::default()];
    let fields = members
        .into_iter()
        .map(|(name, member)| common::TypeStructMember {
            name,
            child: graft(&mut nodes, member),
        })
        .collect();
    nodes[0].kind = Some(common::type_node::Kind::Strct(common::TypeStructNode {
        fields,
    }));
    common::TypeDesc { nodes }
}

/// Append a complete child tree after the nodes already present and return
/// its root index. Every child reference inside it shifts by the same offset,
/// so the result stays in canonical preorder: the parent precedes its
/// children and each child subtree stays contiguous, in declared order.
fn graft(nodes: &mut Vec<common::TypeNode>, child: common::TypeDesc) -> u32 {
    let offset = u32::try_from(nodes.len()).expect("a type tree within u32 node indices");
    for mut node in child.nodes {
        match node.kind.as_mut() {
            Some(common::type_node::Kind::List(element)) => *element += offset,
            Some(common::type_node::Kind::Map(map)) => {
                map.key += offset;
                map.value += offset;
            }
            Some(common::type_node::Kind::Strct(strct)) => {
                for member in &mut strct.fields {
                    member.child += offset;
                }
            }
            Some(common::type_node::Kind::Scalar(_)) | None => {}
        }
        nodes.push(node);
    }
    offset
}

pub fn decode_type(desc: &common::TypeDesc) -> Result<DataType, String> {
    decode_tree(desc).map(|(data_type, _)| data_type)
}

pub fn decode_field_type(
    name: &str,
    nullable: bool,
    desc: &common::TypeDesc,
) -> Result<Field, String> {
    let (data_type, logical_type) = decode_tree(desc)?;
    Ok(field(name, data_type, nullable, logical_type))
}

fn field(
    name: &str,
    data_type: DataType,
    nullable: bool,
    logical_type: Option<LogicalType>,
) -> Field {
    let field = Field::new(name, data_type, nullable);
    match logical_type {
        Some(logical_type) => field_with_logical_type(field, logical_type),
        None => field,
    }
}

/// Validate the whole tree against the logical type budget, then build it
/// bottom-up in reverse preorder. Returns the root type and the logical marker
/// its root carries.
fn decode_tree(desc: &common::TypeDesc) -> Result<(DataType, Option<LogicalType>), String> {
    let nodes = &desc.nodes;
    let limits = LogicalTypeLimits::default();
    validate_preorder(
        nodes.len(),
        FlatTreeLimits {
            max_depth: limits.max_depth,
            max_nodes: limits.max_nodes,
        },
        |index, edges| node_edges(&nodes[index], edges),
    )
    .map_err(|error| match error {
        TreeError::Violation(violation) => format!("TypeDesc {violation}"),
        TreeError::Node(detail) => detail,
    })?;
    check_struct_members(nodes, limits)?;

    let mut built: Vec<Option<(DataType, Option<LogicalType>)>> = Vec::with_capacity(nodes.len());
    built.resize_with(nodes.len(), || None);
    for index in (0..nodes.len()).rev() {
        let kind = nodes[index]
            .kind
            .as_ref()
            .expect("validated nodes carry a kind");
        let data_type = match kind {
            common::type_node::Kind::Scalar(scalar) => {
                let data_type = decode_scalar_type(scalar)?;
                built[index] = Some((data_type, logical_type_from_scalar(scalar)));
                continue;
            }
            common::type_node::Kind::List(element) => {
                let (data_type, logical) = take(&mut built, *element);
                DataType::List(Arc::new(field("item", data_type, true, logical)))
            }
            common::type_node::Kind::Map(map) => {
                let (key_type, key_logical) = take(&mut built, map.key);
                let (value_type, value_logical) = take(&mut built, map.value);
                let entries = Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Arc::new(field("key", key_type, true, key_logical)),
                        Arc::new(field("value", value_type, true, value_logical)),
                    ])),
                    false,
                );
                DataType::Map(Arc::new(entries), false)
            }
            common::type_node::Kind::Strct(strct) => {
                let fields = strct
                    .fields
                    .iter()
                    .map(|member| {
                        let (data_type, logical) = take(&mut built, member.child);
                        Arc::new(field(&member.name, data_type, true, logical))
                    })
                    .collect::<Vec<_>>();
                DataType::Struct(Fields::from(fields))
            }
        };
        built[index] = Some((data_type, None));
    }
    let root = built
        .into_iter()
        .next()
        .flatten()
        .expect("a validated tree has a root");
    // The kernel bounded the wire shape; the complete logical type proves the
    // built carrier is consistent with the same budget.
    novarocks_types::logical_type::logical_type_from_engine_arrow(&root.0)
        .map_err(|detail| format!("TypeDesc does not form a valid logical type: {detail}"))?;
    Ok(root)
}

/// Claim a built child. Validation proved every child index is larger than
/// its parent's and referenced exactly once, so the child is built and
/// unclaimed when its parent is built in reverse preorder.
fn take(
    built: &mut [Option<(DataType, Option<LogicalType>)>],
    child: u32,
) -> (DataType, Option<LogicalType>) {
    built[child as usize]
        .take()
        .expect("a validated child is built before its parent")
}

enum TreeError {
    Violation(FlatTreeViolation),
    Node(String),
}

impl From<FlatTreeViolation> for TreeError {
    fn from(violation: FlatTreeViolation) -> Self {
        Self::Violation(violation)
    }
}

fn node_edges(node: &common::TypeNode, edges: &mut Vec<FlatEdge>) -> Result<(), TreeError> {
    let level = |child: u32| FlatEdge { child, depth: 1 };
    match node.kind.as_ref() {
        None => return Err(TreeError::Node("TypeNode.kind missing".into())),
        Some(common::type_node::Kind::Scalar(_)) => {}
        Some(common::type_node::Kind::List(element)) => edges.push(level(*element)),
        Some(common::type_node::Kind::Map(map)) => {
            edges.push(level(map.key));
            edges.push(level(map.value));
        }
        Some(common::type_node::Kind::Strct(strct)) => {
            edges.extend(strct.fields.iter().map(|member| level(member.child)));
        }
    }
    Ok(())
}

/// Struct member names are non-empty, unique within their struct and within
/// the logical text budget.
fn check_struct_members(
    nodes: &[common::TypeNode],
    limits: LogicalTypeLimits,
) -> Result<(), String> {
    let mut text = 0_usize;
    for node in nodes {
        let Some(common::type_node::Kind::Strct(strct)) = node.kind.as_ref() else {
            continue;
        };
        let mut names = HashSet::with_capacity(strct.fields.len());
        for member in &strct.fields {
            if member.name.is_empty() || !names.insert(member.name.as_str()) {
                return Err("TypeDesc struct has an empty or duplicate field name".into());
            }
            text = text.saturating_add(member.name.len());
        }
    }
    if text > limits.max_text_bytes {
        return Err(format!(
            "TypeDesc struct names use {text} bytes, above the limit {}",
            limits.max_text_bytes
        ));
    }
    Ok(())
}

fn decode_scalar_type(scalar: &common::ScalarType) -> Result<DataType, String> {
    use common::PrimitiveType;

    let primitive = PrimitiveType::try_from(scalar.r#type)
        .map_err(|_| format!("unknown primitive type {}", scalar.r#type))?;
    match primitive {
        PrimitiveType::Unspecified => Err("primitive type is unspecified".to_string()),
        PrimitiveType::NullType => Ok(DataType::Null),
        PrimitiveType::Boolean => Ok(DataType::Boolean),
        PrimitiveType::Tinyint => Ok(DataType::Int8),
        PrimitiveType::Smallint => Ok(DataType::Int16),
        PrimitiveType::Int => Ok(DataType::Int32),
        PrimitiveType::Bigint => Ok(DataType::Int64),
        PrimitiveType::Largeint => Ok(DataType::FixedSizeBinary(16)),
        PrimitiveType::Float => Ok(DataType::Float32),
        PrimitiveType::Double => Ok(DataType::Float64),
        PrimitiveType::Decimal32
        | PrimitiveType::Decimal64
        | PrimitiveType::Decimal128
        | PrimitiveType::Decimal256 => decode_decimal_type(primitive, scalar),
        PrimitiveType::Date => Ok(DataType::Date32),
        PrimitiveType::Datetime => {
            let unit = match scalar.time_unit {
                None | Some(TIME_UNIT_MICROS) => TimeUnit::Microsecond,
                Some(TIME_UNIT_NANOS) => TimeUnit::Nanosecond,
                Some(value) => {
                    return Err(format!(
                        "unsupported DATETIME time_unit {value}; only unset/{TIME_UNIT_MICROS}/{TIME_UNIT_NANOS} supported"
                    ));
                }
            };
            let zone = scalar
                .time_zone
                .as_ref()
                .map(|zone| Arc::from(zone.as_str()));
            Ok(DataType::Timestamp(unit, zone))
        }
        PrimitiveType::Time => Ok(DataType::Time64(TimeUnit::Microsecond)),
        PrimitiveType::Varchar | PrimitiveType::Char | PrimitiveType::Json => Ok(DataType::Utf8),
        PrimitiveType::Varbinary
        | PrimitiveType::Binary
        | PrimitiveType::Hll
        | PrimitiveType::Bitmap
        | PrimitiveType::Object
        | PrimitiveType::Percentile => Ok(DataType::Binary),
        PrimitiveType::Variant => Ok(DataType::LargeBinary),
    }
}

fn decode_decimal_type(
    primitive: common::PrimitiveType,
    scalar: &common::ScalarType,
) -> Result<DataType, String> {
    let precision = scalar
        .precision
        .ok_or_else(|| "decimal precision missing".to_string())
        .and_then(|v| u8::try_from(v).map_err(|_| format!("invalid decimal precision {v}")))?;
    let scale = scalar
        .scale
        .ok_or_else(|| "decimal scale missing".to_string())
        .and_then(|v| i8::try_from(v).map_err(|_| format!("invalid decimal scale {v}")))?;
    let (max_precision, label) = match primitive {
        common::PrimitiveType::Decimal32 => (9, "Decimal32"),
        common::PrimitiveType::Decimal64 => (18, "Decimal64"),
        common::PrimitiveType::Decimal128 => (38, "Decimal128"),
        common::PrimitiveType::Decimal256 => (76, "Decimal256"),
        _ => unreachable!(),
    };
    validate_decimal(precision, scale, max_precision, label)?;
    if primitive == common::PrimitiveType::Decimal256 || precision > 38 {
        Ok(DataType::Decimal256(precision, scale))
    } else {
        Ok(DataType::Decimal128(precision, scale))
    }
}

fn validate_decimal(
    precision: u8,
    scale: i8,
    max_precision: u8,
    label: &str,
) -> Result<(), String> {
    if precision == 0 || precision > max_precision {
        return Err(format!(
            "{label} precision {precision} must be between 1 and {max_precision}"
        ));
    }
    if scale < 0 || i32::from(scale) > i32::from(precision) {
        return Err(format!(
            "{label} scale {scale} must be between 0 and precision {precision}"
        ));
    }
    Ok(())
}

fn logical_type_from_scalar(scalar: &common::ScalarType) -> Option<LogicalType> {
    match common::PrimitiveType::try_from(scalar.r#type).ok()? {
        common::PrimitiveType::Json => Some(LogicalType::Json),
        common::PrimitiveType::Hll => Some(LogicalType::Hll),
        common::PrimitiveType::Bitmap => Some(LogicalType::Bitmap),
        common::PrimitiveType::Object => Some(LogicalType::Object),
        common::PrimitiveType::Percentile => Some(LogicalType::Percentile),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field};
    use novarocks_type_contract::LogicalTypeLimits;
    use novarocks_types::logical::{LogicalType, logical_type_of_field};

    use super::*;
    use novarocks_proto_models::common;

    fn scalar(primitive: common::PrimitiveType) -> common::TypeDesc {
        scalar_type_desc(common::ScalarType {
            r#type: primitive as i32,
            ..Default::default()
        })
    }

    fn nested_lists(levels: usize) -> common::TypeDesc {
        let mut desc = scalar(common::PrimitiveType::Int);
        for _ in 0..levels {
            desc = list_type_desc(desc);
        }
        desc
    }

    #[test]
    fn decodes_nested_and_decimal_types_without_a_role_codec() {
        let decimal = scalar_type_desc(common::ScalarType {
            r#type: common::PrimitiveType::Decimal128 as i32,
            precision: Some(18),
            scale: Some(2),
            ..Default::default()
        });
        assert_eq!(
            decode_type(&list_type_desc(decimal)).expect("decode nested decimal type"),
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Decimal128(18, 2),
                true
            )))
        );
    }

    #[test]
    fn constructors_emit_canonical_preorder_and_markers_survive() {
        let desc = struct_type_desc([
            (
                "m".to_string(),
                map_type_desc(
                    scalar(common::PrimitiveType::Varchar),
                    list_type_desc(scalar(common::PrimitiveType::Json)),
                ),
            ),
            ("j".to_string(), scalar(common::PrimitiveType::Json)),
        ]);
        // struct, map, key, list, json element, json member
        assert_eq!(desc.nodes.len(), 6);
        let Some(common::type_node::Kind::Strct(strct)) = desc.nodes[0].kind.as_ref() else {
            panic!("root struct");
        };
        assert_eq!(
            strct
                .fields
                .iter()
                .map(|member| member.child)
                .collect::<Vec<_>>(),
            vec![1, 5]
        );
        let data_type = decode_type(&desc).expect("valid nested type");
        let DataType::Struct(fields) = &data_type else {
            panic!("struct");
        };
        assert_eq!(logical_type_of_field(&fields[1]), Some(LogicalType::Json));
        let DataType::Map(entries, false) = fields[0].data_type() else {
            panic!("map");
        };
        let DataType::Struct(entry_fields) = entries.data_type() else {
            panic!("entries");
        };
        let DataType::List(element) = entry_fields[1].data_type() else {
            panic!("list value");
        };
        assert_eq!(logical_type_of_field(element), Some(LogicalType::Json));
        assert!(root_scalar(&desc).is_none());
        assert_eq!(
            root_scalar(&scalar(common::PrimitiveType::Json)).map(|s| s.r#type),
            Some(common::PrimitiveType::Json as i32)
        );
    }

    #[test]
    fn logical_depth_64_decodes_and_65_is_refused() {
        let limit = LogicalTypeLimits::default().max_depth;
        decode_type(&nested_lists(limit - 1)).expect("64 levels");
        assert!(decode_type(&nested_lists(limit)).is_err());
    }

    #[test]
    fn node_and_text_budgets_are_enforced() {
        let limits = LogicalTypeLimits::default();
        let members = |count: usize, name: &dyn Fn(usize) -> String| {
            struct_type_desc(
                (0..count).map(|index| (name(index), scalar(common::PrimitiveType::Int))),
            )
        };
        let short = |index: usize| format!("f{index}");
        decode_type(&members(limits.max_nodes - 1, &short)).expect("4096 nodes");
        assert!(decode_type(&members(limits.max_nodes, &short)).is_err());
        let wide = |index: usize| format!("{index:0>1024}");
        let fitting = limits.max_text_bytes / 1024;
        decode_type(&members(fitting, &wide)).expect("exactly the text budget");
        assert!(decode_type(&members(fitting + 1, &wide)).is_err());
    }

    #[test]
    fn malformed_trees_are_refused() {
        let pair = struct_type_desc([
            ("a".to_string(), scalar(common::PrimitiveType::Int)),
            ("b".to_string(), scalar(common::PrimitiveType::Bigint)),
        ]);
        let with_children = |children: Vec<u32>| {
            let mut desc = pair.clone();
            let Some(common::type_node::Kind::Strct(strct)) = desc.nodes[0].kind.as_mut() else {
                panic!("struct");
            };
            for (member, child) in strct.fields.iter_mut().zip(children) {
                member.child = child;
            }
            desc
        };
        decode_type(&pair).expect("valid pair");
        assert!(decode_type(&common::TypeDesc::default()).is_err());
        assert!(decode_type(&with_children(vec![1, 9])).is_err());
        assert!(decode_type(&with_children(vec![1, 1])).is_err());
        assert!(decode_type(&with_children(vec![2, 1])).is_err());
        assert!(decode_type(&with_children(vec![0, 1])).is_err());
        let mut orphan = pair.clone();
        orphan.nodes.push(orphan.nodes[1].clone());
        assert!(decode_type(&orphan).is_err());
        let mut missing_kind = pair.clone();
        missing_kind.nodes[2].kind = None;
        assert!(decode_type(&missing_kind).is_err());
        let duplicate = struct_type_desc([
            ("a".to_string(), scalar(common::PrimitiveType::Int)),
            ("a".to_string(), scalar(common::PrimitiveType::Int)),
        ]);
        assert!(decode_type(&duplicate).is_err());
        let empty = struct_type_desc([(String::new(), scalar(common::PrimitiveType::Int))]);
        assert!(decode_type(&empty).is_err());
    }

    /// A peer still sending the retired recursive grammar (field 1 carried a
    /// scalar directly) produces a descriptor without nodes, which is refused
    /// rather than read as some default type.
    #[test]
    fn retired_recursive_payload_is_refused() {
        use prost::Message;
        // TypeDesc { scalar (field 1) = ScalarType { type (field 1) = INT } }
        let retired = [0x0a, 0x02, 0x08, common::PrimitiveType::Int as u8];
        let desc =
            common::TypeDesc::decode(retired.as_slice()).expect("unknown fields are skipped");
        assert!(desc.nodes.is_empty());
        assert!(decode_type(&desc).is_err());
    }

    #[test]
    fn deep_descriptor_decodes_under_the_default_protobuf_recursion_limit() {
        use prost::Message;
        let limit = LogicalTypeLimits::default().max_depth;
        let mut desc = scalar(common::PrimitiveType::Json);
        for level in 0..limit - 1 {
            desc = struct_type_desc([(format!("n{level}"), desc)]);
        }
        let bytes = desc.encode_to_vec();
        let decoded = common::TypeDesc::decode(bytes.as_slice()).expect("flat nesting is constant");
        let data_type = decode_type(&decoded).expect("64 logical levels");
        let mut depth = 1;
        let mut current = &data_type;
        while let DataType::Struct(fields) = current {
            current = fields[0].data_type();
            depth += 1;
        }
        assert_eq!(depth, limit);
    }
}
