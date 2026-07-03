//! NIDL-0 conversion-layer ergonomics spike. TEMPORARY.
//!
//! TOMBSTONE: delete this file, its `#[cfg(test)] mod proto_spike;` line in
//! src/service/mod.rs, idl/novarocks/spike.proto, and the crate::proto::spike
//! wiring in src/build.rs when NIDL-3 (plan-face contract freeze) lands.
//!
//! FINDINGS (filled from this run):
//!   1. Recursive message Box placement: prost emitted SpikeType.kind as
//!      Option<spike_type::Kind>, Kind::List(Box<SpikeList>),
//!      SpikeList.element as Option<Box<SpikeType>>, SpikeField.field_type as
//!      Option<SpikeType>, and repeated SpikeNode.children as Vec<SpikeNode>.
//!   2. Large oneof exhaustive match: 12 arms are tolerable as a no-`_` match;
//!      adding/removing an arm becomes a compile-time exhaustiveness change.
//!      The `Kind::Strct` name is spike-only awkwardness; NIDL-3 should choose
//!      clearer field names and not cargo-cult this spelling.
//!   3. Option-centralization density: 4 ok_or checks cover the 4 proto fields
//!      that the internal model treats as required: SpikeType.kind,
//!      SpikeList.element, SpikeField.field_type, and SpikeNode.payload.
//!   4. Roundtrip template: encode_to_vec() -> decode() -> conversion ->
//!      assert_eq! is a compact template for proving conversion-layer fidelity.

#![cfg(test)]

use prost::Message;

use crate::proto::spike;

// Internal (planner-side-analogue) types: non-null, exhaustive Rust enums -
// the shape the conversion layer protects from proto3's Option/i32 degradation.
#[derive(Clone, Debug, PartialEq)]
enum InternalType {
    Scalar(i32),
    List(Box<InternalType>),
    Struct(Vec<(String, InternalType)>),
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum InternalPayload {
    Arm(u8, i64), // arm ordinal (1..=12) + value; models a wide oneof
}

#[derive(Clone, Debug, PartialEq)]
struct InternalNode {
    node_id: i32,
    children: Vec<InternalNode>,
    payload: InternalPayload,
}

// --- encode side (internal -> proto) ---

fn type_to_proto(t: &InternalType) -> spike::SpikeType {
    use spike::spike_type::Kind;

    let kind = match t {
        InternalType::Scalar(id) => Kind::Scalar(spike::SpikeScalar { type_id: *id }),
        InternalType::List(el) => Kind::List(Box::new(spike::SpikeList {
            element: Some(Box::new(type_to_proto(el))),
        })),
        InternalType::Struct(fields) => Kind::Strct(spike::SpikeStruct {
            fields: fields
                .iter()
                .map(|(name, ft)| spike::SpikeField {
                    name: name.clone(),
                    field_type: Some(type_to_proto(ft)),
                })
                .collect(),
        }),
    };

    spike::SpikeType { kind: Some(kind) }
}

fn node_to_proto(n: &InternalNode) -> spike::SpikeNode {
    use spike::spike_node::Payload;

    let InternalPayload::Arm(ord, value) = n.payload;
    let unit = spike::SpikeUnit { value };
    let payload = match ord {
        1 => Payload::Arm01(unit),
        2 => Payload::Arm02(unit),
        3 => Payload::Arm03(unit),
        4 => Payload::Arm04(unit),
        5 => Payload::Arm05(unit),
        6 => Payload::Arm06(unit),
        7 => Payload::Arm07(unit),
        8 => Payload::Arm08(unit),
        9 => Payload::Arm09(unit),
        10 => Payload::Arm10(unit),
        11 => Payload::Arm11(unit),
        12 => Payload::Arm12(unit),
        other => panic!("spike arm out of range: {other}"),
    };

    spike::SpikeNode {
        node_id: n.node_id,
        children: n.children.iter().map(node_to_proto).collect(),
        payload: Some(payload),
    }
}

// --- decode side (proto -> internal), Option centralization via ok_or ---

fn type_from_proto(p: &spike::SpikeType) -> Result<InternalType, String> {
    use spike::spike_type::Kind;

    let kind = p.kind.as_ref().ok_or("SpikeType.kind missing")?;
    Ok(match kind {
        Kind::Scalar(s) => InternalType::Scalar(s.type_id),
        Kind::List(l) => {
            let el = l.element.as_ref().ok_or("SpikeList.element missing")?;
            InternalType::List(Box::new(type_from_proto(el)?))
        }
        Kind::Strct(s) => InternalType::Struct(
            s.fields
                .iter()
                .map(|f| {
                    let ft = f
                        .field_type
                        .as_ref()
                        .ok_or("SpikeField.field_type missing")?;
                    Ok((f.name.clone(), type_from_proto(ft)?))
                })
                .collect::<Result<Vec<_>, String>>()?,
        ),
    })
}

// Finding #2: this match is the exhaustiveness proof. If a oneof arm is added or
// removed, this fails to compile with no `_` arm. This validates enum width and
// exhaustiveness; payload ownership variety is intentionally limited because all
// arms use SpikeUnit in the Task 4 schema.
fn node_from_proto(p: &spike::SpikeNode) -> Result<InternalNode, String> {
    use spike::spike_node::Payload;

    let payload = p.payload.as_ref().ok_or("SpikeNode.payload missing")?;
    let (ord, value) = match payload {
        Payload::Arm01(u) => (1u8, u.value),
        Payload::Arm02(u) => (2, u.value),
        Payload::Arm03(u) => (3, u.value),
        Payload::Arm04(u) => (4, u.value),
        Payload::Arm05(u) => (5, u.value),
        Payload::Arm06(u) => (6, u.value),
        Payload::Arm07(u) => (7, u.value),
        Payload::Arm08(u) => (8, u.value),
        Payload::Arm09(u) => (9, u.value),
        Payload::Arm10(u) => (10, u.value),
        Payload::Arm11(u) => (11, u.value),
        Payload::Arm12(u) => (12, u.value),
    };

    Ok(InternalNode {
        node_id: p.node_id,
        children: p
            .children
            .iter()
            .map(node_from_proto)
            .collect::<Result<Vec<_>, String>>()?,
        payload: InternalPayload::Arm(ord, value),
    })
}

fn sample_type() -> InternalType {
    // List<Struct<a: Scalar(1), b: List<Scalar(2)>>>
    InternalType::List(Box::new(InternalType::Struct(vec![
        ("a".to_string(), InternalType::Scalar(1)),
        (
            "b".to_string(),
            InternalType::List(Box::new(InternalType::Scalar(2))),
        ),
    ])))
}

fn sample_node() -> InternalNode {
    InternalNode {
        node_id: 7,
        payload: InternalPayload::Arm(3, 300),
        children: vec![
            InternalNode {
                node_id: 8,
                payload: InternalPayload::Arm(1, 100),
                children: vec![],
            },
            InternalNode {
                node_id: 9,
                payload: InternalPayload::Arm(12, 120),
                children: vec![],
            },
        ],
    }
}

#[test]
fn recursive_type_survives_proto_roundtrip() {
    let original = sample_type();
    let bytes = type_to_proto(&original).encode_to_vec();
    let decoded = spike::SpikeType::decode(bytes.as_slice()).expect("decode SpikeType");
    let back = type_from_proto(&decoded).expect("convert back");
    assert_eq!(original, back);
}

#[test]
fn wide_oneof_node_survives_proto_roundtrip() {
    let original = sample_node();
    let bytes = node_to_proto(&original).encode_to_vec();
    let decoded = spike::SpikeNode::decode(bytes.as_slice()).expect("decode SpikeNode");
    let back = node_from_proto(&decoded).expect("convert back");
    assert_eq!(original, back);
}
