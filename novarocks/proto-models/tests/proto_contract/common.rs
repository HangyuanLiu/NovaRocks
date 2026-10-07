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

use prost::Message;

use novarocks_proto_models::common;

#[derive(Clone, Debug, PartialEq)]
enum InternalType {
    Scalar {
        prim: i32,
        precision: Option<i32>,
        scale: Option<i32>,
    },
    List(Box<InternalType>),
    Map(Box<InternalType>, Box<InternalType>),
    Struct(Vec<(String, InternalType)>),
}

fn type_to_proto(t: &InternalType) -> common::TypeDesc {
    let mut nodes = Vec::new();
    push_type(t, &mut nodes);
    common::TypeDesc { nodes }
}

/// Append `t` in canonical preorder and return its node index.
fn push_type(t: &InternalType, nodes: &mut Vec<common::TypeNode>) -> u32 {
    use common::type_node::Kind;

    let index = nodes.len() as u32;
    nodes.push(common::TypeNode::default());
    let kind = match t {
        InternalType::Scalar {
            prim,
            precision,
            scale,
        } => Kind::Scalar(common::ScalarType {
            r#type: *prim,
            len: None,
            precision: *precision,
            scale: *scale,
            time_unit: None,
            time_zone: None,
        }),
        InternalType::List(el) => Kind::List(push_type(el, nodes)),
        InternalType::Map(k, v) => {
            let key = push_type(k, nodes);
            let value = push_type(v, nodes);
            Kind::Map(common::TypeMapNode { key, value })
        }
        InternalType::Struct(fields) => Kind::Strct(common::TypeStructNode {
            fields: fields
                .iter()
                .map(|(name, ft)| common::TypeStructMember {
                    name: name.clone(),
                    child: push_type(ft, nodes),
                })
                .collect(),
        }),
    };
    nodes[index as usize].kind = Some(kind);
    index
}

fn type_from_proto(p: &common::TypeDesc) -> Result<InternalType, String> {
    if p.nodes.is_empty() {
        return Err("TypeDesc.nodes missing".to_string());
    }
    node_from_proto(&p.nodes, 0)
}

fn node_from_proto(nodes: &[common::TypeNode], index: u32) -> Result<InternalType, String> {
    use common::type_node::Kind;

    let node = nodes
        .get(index as usize)
        .ok_or_else(|| format!("TypeDesc node {index} missing"))?;
    let kind = node.kind.as_ref().ok_or("TypeNode.kind missing")?;
    Ok(match kind {
        Kind::Scalar(s) => InternalType::Scalar {
            prim: s.r#type,
            precision: s.precision,
            scale: s.scale,
        },
        Kind::List(el) => InternalType::List(Box::new(node_from_proto(nodes, *el)?)),
        Kind::Map(m) => InternalType::Map(
            Box::new(node_from_proto(nodes, m.key)?),
            Box::new(node_from_proto(nodes, m.value)?),
        ),
        Kind::Strct(s) => InternalType::Struct(
            s.fields
                .iter()
                .map(|f| Ok((f.name.clone(), node_from_proto(nodes, f.child)?)))
                .collect::<Result<Vec<_>, String>>()?,
        ),
    })
}

fn roundtrip_message<M>(value: &M) -> M
where
    M: Message + Default,
{
    M::decode(value.encode_to_vec().as_slice()).expect("decode proto message")
}

fn sample_recursive_type() -> InternalType {
    use common::PrimitiveType;

    // Map<VARCHAR, List<Struct<a: DECIMAL128(10,2), b: List<BIGINT>>>>
    InternalType::Map(
        Box::new(InternalType::Scalar {
            prim: PrimitiveType::Varchar as i32,
            precision: None,
            scale: None,
        }),
        Box::new(InternalType::List(Box::new(InternalType::Struct(vec![
            (
                "a".to_string(),
                InternalType::Scalar {
                    prim: PrimitiveType::Decimal128 as i32,
                    precision: Some(10),
                    scale: Some(2),
                },
            ),
            (
                "b".to_string(),
                InternalType::List(Box::new(InternalType::Scalar {
                    prim: PrimitiveType::Bigint as i32,
                    precision: None,
                    scale: None,
                })),
            ),
        ])))),
    )
}

fn literal_boundary_accepts(value: &common::LiteralValue) -> Result<(), String> {
    use common::literal_value::Value;

    match value.value.as_ref() {
        Some(Value::NullValue(false)) => {
            Err("LiteralValue.null_value false is invalid".to_string())
        }
        _ => Ok(()),
    }
}

#[test]
fn recursive_type_desc_survives_proto_roundtrip() {
    let original = sample_recursive_type();
    let proto = type_to_proto(&original);

    let decoded: common::TypeDesc = roundtrip_message(&proto);
    assert_eq!(proto, decoded);

    let back = type_from_proto(&decoded).expect("convert TypeDesc back");
    assert_eq!(original, back);
}

#[test]
fn missing_type_desc_kind_reports_boundary_error() {
    let err = type_from_proto(&common::TypeDesc::default()).expect_err("missing nodes");
    assert_eq!(err, "TypeDesc.nodes missing");
    let err = type_from_proto(&common::TypeDesc {
        nodes: vec![common::TypeNode::default()],
    })
    .expect_err("missing kind");
    assert_eq!(err, "TypeNode.kind missing");
}

#[test]
fn literal_value_oneof_arms_survive_proto_roundtrip() {
    use common::literal_value::Value;

    let values = vec![
        common::LiteralValue {
            value: Some(Value::NullValue(true)),
        },
        common::LiteralValue {
            value: Some(Value::BoolValue(false)),
        },
        common::LiteralValue {
            value: Some(Value::IntValue(-42)),
        },
        common::LiteralValue {
            value: Some(Value::LargeintValue((-1i128).to_be_bytes().to_vec())),
        },
        common::LiteralValue {
            value: Some(Value::FloatValue(-12.5)),
        },
        common::LiteralValue {
            value: Some(Value::StringValue("hello".to_string())),
        },
        common::LiteralValue {
            value: Some(Value::BinaryValue(vec![0x00, 0xff, 0x2a])),
        },
        common::LiteralValue {
            value: Some(Value::Date32Value(19_000)),
        },
        common::LiteralValue {
            value: Some(Value::DecimalValue(common::DecimalLiteral {
                value: (-12345i128).to_be_bytes().to_vec(),
                precision: 10,
                scale: 2,
            })),
        },
    ];

    for original in values {
        let decoded: common::LiteralValue = roundtrip_message(&original);
        assert_eq!(original, decoded);
        literal_boundary_accepts(&decoded).expect("valid literal boundary");
    }
}

#[test]
fn null_value_false_is_wire_representable_but_boundary_invalid() {
    use common::literal_value::Value;

    let original = common::LiteralValue {
        value: Some(Value::NullValue(false)),
    };

    let decoded: common::LiteralValue = roundtrip_message(&original);
    assert_eq!(original, decoded);

    let err = literal_boundary_accepts(&decoded).expect_err("null false must be rejected");
    assert_eq!(err, "LiteralValue.null_value false is invalid");
}

#[test]
fn status_unique_id_and_output_column_survive_proto_roundtrip() {
    let status = common::Status {
        code: 7,
        message: "backend rejected fragment".to_string(),
    };
    assert_eq!(status, roundtrip_message(&status));

    let unique_id = common::UniqueId {
        hi: 0x1122_3344_5566_7788,
        lo: -0x0102_0304_0506_0708,
    };
    assert_eq!(unique_id, roundtrip_message(&unique_id));

    let output_column = common::OutputColumn {
        column_id: 42,
        name: "sum_revenue".to_string(),
        r#type: Some(type_to_proto(&InternalType::Scalar {
            prim: common::PrimitiveType::Decimal128 as i32,
            precision: Some(18),
            scale: Some(2),
        })),
        nullable: false,
        is_internal: true,
    };
    assert_eq!(output_column, roundtrip_message(&output_column));
}
