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

//! Original change-event facts projected into the checked local vocabulary.
//! Allocations remain subject to the caller's formal preparation scope.

use crate::{assert_rows::reserve_vec, lowering::FragmentCompileError};
use arrow_schema::Schema;
use novarocks_local_program::{
    ChangeEventOutputExpr, ChangeEventSpec, ProgramExprId, ProgramNodeId, ProgramNodeKind,
    StaticLayout,
};
use novarocks_physical_plan::{ExprId, FragmentPackage, NodeKind, PhysicalNode};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};
use novarocks_types::SlotId;
use std::{collections::BTreeMap, sync::Arc};

pub(crate) fn lower_change_events(
    package: &FragmentPackage,
    node: &PhysicalNode,
    local: ProgramNodeId,
    child: ProgramNodeId,
    slots: &[SlotId],
    expressions: &BTreeMap<ExprId, ProgramExprId>,
    control: &dyn PureCompileControl,
) -> Result<(ProgramNodeKind, StaticLayout), FragmentCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core(package, node, local, (child, expressions), slots, &mut work);
    if matches!(&result, Err(FragmentCompileError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

fn lower_core(
    package: &FragmentPackage,
    node: &PhysicalNode,
    local: ProgramNodeId,
    input: (ProgramNodeId, &BTreeMap<ExprId, ProgramExprId>),
    slots: &[SlotId],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(ProgramNodeKind, StaticLayout), FragmentCompileError> {
    let (child, expressions) = input;
    let NodeKind::ChangeEventExpand {
        events,
        effect_output,
    } = &node.kind
    else {
        return Err(FragmentCompileError::Invalid(
            "change-event source kind differs",
        ));
    };
    if slots.len() != node.output.columns.len() {
        return Err(FragmentCompileError::Invalid(
            "change-event output width differs",
        ));
    }
    let mut output_slots = Vec::new();
    let mut fields = Vec::new();
    let mut addresses = BTreeMap::new();
    let mut effect_slot = None;
    reserve_vec(&mut output_slots, slots.len(), work)?;
    reserve_vec(&mut fields, slots.len(), work)?;
    let result_port = package.result().filter(|r| r.output == node.output);
    for (ordinal, (&value, &slot)) in node.output.columns.iter().zip(slots).enumerate() {
        let fresh = addresses.insert(value, slot).is_none();
        work.step()?;
        if !fresh {
            return Err(FragmentCompileError::Unsupported {
                node: Some(node.id),
                feature: "repeated change-event output occurrences",
            });
        }
        if value == *effect_output {
            effect_slot = Some(slot);
        }
        let ty = &package
            .fragment()
            .values()
            .get(&value)
            .ok_or(FragmentCompileError::Invalid(
                "missing change-event output value",
            ))?
            .ty;
        work.flush()?;
        let name = match result_port {
            Some(result) => {
                let field = result
                    .fields
                    .get(ordinal)
                    .ok_or(FragmentCompileError::Invalid(
                        "missing change-event result field",
                    ))?;
                field.alias.as_deref().unwrap_or(&field.name).to_owned()
            }
            None => format!("local_{}_{}", local.index(), ordinal),
        };
        work.flush()?;
        let field = ty.try_to_field(name)?;
        work.flush()?;
        fields.push(field);
        output_slots.push(slot);
        work.step()?;
    }
    let effect_slot_id = effect_slot.ok_or(FragmentCompileError::Invalid(
        "missing change-event effect output",
    ))?;
    let resolve = |id: ExprId| {
        expressions
            .get(&id)
            .copied()
            .ok_or(FragmentCompileError::Invalid(
                "missing change-event expression definition",
            ))
    };
    let mut lowered = Vec::new();
    reserve_vec(&mut lowered, events.len(), work)?;
    for event in events {
        let predicate = event.predicate.map(resolve).transpose();
        work.step()?;
        let predicate = predicate?;
        let mut assignments = Vec::new();
        reserve_vec(&mut assignments, event.assignments.len(), work)?;
        for (value, expression) in &event.assignments {
            let output_slot_id =
                addresses
                    .get(value)
                    .copied()
                    .ok_or(FragmentCompileError::Invalid(
                        "change-event assignment output is absent",
                    ));
            let expr = expression.map(resolve).transpose();
            work.step()?;
            assignments.push(ChangeEventOutputExpr {
                output_slot_id: output_slot_id?,
                expr: expr?,
            });
            work.step()?;
        }
        lowered.push(ChangeEventSpec {
            predicate,
            effect: event.effect,
            assignments,
        });
        work.step()?;
    }
    work.flush()?;
    let schema = Arc::new(Schema::new(fields));
    let output_layout_slots: Arc<[SlotId]> = Arc::from(slots);
    work.flush()?;
    let layout = StaticLayout::try_new_for_compile(schema, output_layout_slots, work.control())?;
    Ok((
        ProgramNodeKind::ChangeEventExpand {
            input: child,
            events: lowered,
            output_slot_ids: output_slots,
            effect_slot_id,
        },
        layout,
    ))
}
