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

use super::*;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use novarocks_types::logical::LogicalType;
use std::collections::HashMap;
use std::num::NonZeroUsize;

fn layout(slot: u32, logical: Option<LogicalType>) -> StaticLayout {
    StaticLayout::try_new_exact(
        Arc::new(Schema::new(vec![Field::new("json", DataType::Utf8, true)])),
        Arc::from([SlotId::new(slot)]),
        vec![(StaticFieldSchema::new(logical, vec![]), Some(slot as i32))],
    )
    .unwrap()
}
fn output(probe: &StaticLayout, result: u32, nullable: bool) -> StaticLayout {
    StaticLayout::try_new_exact(
        Arc::new(Schema::new(vec![
            probe.schema().field(0).clone(),
            Field::new("member", DataType::Boolean, nullable),
        ])),
        Arc::from([probe.slots()[0], SlotId::new(result)]),
        vec![
            (
                probe.slot_metadata_at(0).unwrap().0.clone(),
                probe.slot_metadata_at(0).unwrap().1,
            ),
            (StaticFieldSchema::new(None, vec![]), None),
        ],
    )
    .unwrap()
}
fn spec() -> MembershipSpec {
    MembershipSpec {
        probe: SlotId::new(1),
        build: SlotId::new(2),
        result: SlotId::new(3),
        negated: false,
        comparison: MembershipComparison::JsonInListV1,
        distribution: MembershipDistribution::BroadcastBuild,
    }
}
#[test]
fn membership_exact_layout_preserves_probe_field_semantics_and_reaches_both_children() {
    let probe = layout(1, Some(LogicalType::Json));
    let build = layout(2, Some(LogicalType::Json));
    let out = output(&probe, 3, true);
    let source = |id, layout: &StaticLayout| {
        ProgramNode::new(
            id,
            ProgramNodeKind::Values {
                values: StaticValues::try_new(
                    RecordBatch::new_empty(layout.schema().clone()),
                    layout.clone(),
                )
                .unwrap(),
            },
            layout.clone(),
        )
    };
    let nodes = vec![
        source(1, &probe),
        source(2, &build),
        ProgramNode::new(
            3,
            ProgramNodeKind::Membership {
                probe: ProgramNodeId::new(0),
                build: ProgramNodeId::new(1),
                spec: spec(),
            },
            out.clone(),
        ),
    ];
    let expressions =
        Arc::new(ImmutableExpressions::try_new(vec![], false, HashMap::new(), None).unwrap());
    let profile = CompileProfile::new(
        NonZeroUsize::new(1).unwrap(),
        None,
        out.identity().unwrap(),
        crate::KernelAbiVersion::CURRENT,
    );
    LocalProgram::try_new(
        nodes,
        ProgramNodeId::new(2),
        expressions,
        profile,
        BindingRequirements::try_new(vec![]).unwrap(),
    )
    .unwrap();
    assert_eq!(out.slot_metadata_at(0), probe.slot_metadata_at(0));
}
#[test]
fn membership_plain_carrier_unknown_semantics_and_cross_child_slot_are_rejected() {
    let probe = layout(1, Some(LogicalType::Json));
    let build = layout(2, Some(LogicalType::Json));
    let out = output(&probe, 3, true);
    let plain = layout(1, None);
    let unknown = StaticLayout::try_new(probe.schema().clone(), Arc::from(probe.slots())).unwrap();
    for invalid in [&plain, &unknown] {
        assert!(validate_membership_layout(&spec(), invalid, &build, &out).is_err());
    }
    assert!(validate_membership_layout(&spec(), &build, &probe, &out).is_err());
}
#[test]
fn membership_nullable_result_and_fresh_identity_are_required() {
    let probe = layout(1, Some(LogicalType::Json));
    let build = layout(2, Some(LogicalType::Json));
    assert!(
        validate_membership_layout(&spec(), &probe, &build, &output(&probe, 3, false)).is_err()
    );
    let mut alias = spec();
    alias.result = SlotId::new(2);
    assert!(validate_membership_layout(&alias, &probe, &build, &output(&probe, 2, true)).is_err());
}
