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

//! Normalize every proved UnionAll branch with the existing Project vocabulary.
//! The sole channel and expression authors supply exact input occurrences and
//! definitions. This leaf neither casts values nor creates runtime capabilities.
//! Type/layout clones and opaque Arc construction remain caller admission work,
//! not a host allocation grant or a model of Arrow's internal copy behavior.

use crate::{FragmentCompileError, assert_rows::reserve_vec, channels::UnionChannelBranch};
use arrow_schema::{Field, Schema};
use novarocks_local_program::{
    DiagnosticSourceNodeId, MAX_PROGRAM_NODES, MAX_PROGRAM_TYPED_CHANNELS, MAX_STATIC_EXPRESSIONS,
    ProgramChannelLayoutRole, ProgramChannelSite, ProgramExprId, ProgramNode, ProgramNodeId,
    ProgramNodeKind, StaticLayout,
};
use novarocks_physical_plan::{FragmentPackage, NodeKind, PhysicalNode, SetOperationKind};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
use novarocks_types::SlotId;
use std::{alloc::Layout, sync::Arc};

/// Emit ordered subordinate normalizers followed by the actual UnionAll.
/// `definitions` are already authored SlotId reads with each source's complete
/// type; their arena, lexical bindings, total counts and identity allocation
/// remain with the original compiler owners. Shared layouts preserve the
/// published output types without changing the source expression definitions.
pub(crate) fn lower_union(
    package: &FragmentPackage,
    node: &PhysicalNode,
    local: ProgramNodeId,
    branches: &[UnionChannelBranch],
    definitions: &[Vec<ProgramExprId>],
    slots: &[SlotId],
    control: &dyn PureCompileControl,
) -> Result<Vec<ProgramNode>, FragmentCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core(
        package,
        node,
        local,
        branches,
        definitions,
        slots,
        &mut work,
    );
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
    branches: &[UnionChannelBranch],
    definitions: &[Vec<ProgramExprId>],
    slots: &[SlotId],
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<ProgramNode>, FragmentCompileError> {
    let fragment = package.fragment();
    let original = fragment.nodes().get(&node.id);
    let same = original.is_some_and(|original| std::ptr::eq(original, node));
    work.step()?;
    if !same {
        return Err(FragmentCompileError::Invalid(
            "UnionAll source is outside its original fragment",
        ));
    }
    let NodeKind::SetOp {
        kind: SetOperationKind::UnionAll,
        input_mappings,
    } = &node.kind
    else {
        return Err(FragmentCompileError::Invalid(
            "UnionAll source kind differs",
        ));
    };
    let width = node.output.columns.len();
    let shape = node.inputs.len() >= 2
        && input_mappings.len() == node.inputs.len()
        && branches.len() == node.inputs.len()
        && definitions.len() == branches.len()
        && slots.len() == width;
    work.step()?;
    if !shape {
        return Err(FragmentCompileError::Invalid(
            "UnionAll branch or output occurrence shape differs",
        ));
    }
    // Bound this leaf's real expanded contributions before traversing inner
    // mappings or reserving outputs. The final owners also admit global totals.
    let node_count = branches
        .len()
        .checked_add(1)
        .filter(|&count| count <= MAX_PROGRAM_NODES)
        .ok_or(CompileControlError::ResourceExhausted)?;
    let expression_count = branches
        .len()
        .checked_mul(width)
        .filter(|&count| count <= MAX_STATIC_EXPRESSIONS)
        .ok_or(CompileControlError::ResourceExhausted)?;
    let channel_count = node_count
        .checked_mul(width)
        .filter(|&count| count <= MAX_PROGRAM_TYPED_CHANNELS)
        .ok_or(CompileControlError::ResourceExhausted)?;
    Layout::array::<ProgramNode>(node_count).map_err(|_| CompileControlError::ResourceExhausted)?;
    Layout::array::<ProgramExprId>(expression_count)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    Layout::array::<SlotId>(channel_count).map_err(|_| CompileControlError::ResourceExhausted)?;
    work.step()?;

    for ((branch, definition), (&child_id, mapping)) in branches
        .iter()
        .zip(definitions)
        .zip(node.inputs.iter().zip(input_mappings))
    {
        let child = fragment.nodes().get(&child_id);
        let shape = mapping.len() == width
            && branch.sources.len() == width
            && definition.len() == width
            && branch.normalizer != local
            && branch.normalizer != branch.input
            && branch.input != local;
        work.step()?;
        if !shape {
            return Err(FragmentCompileError::Invalid(
                "UnionAll normalization branch shape differs",
            ));
        }
        let child = child.ok_or(FragmentCompileError::Invalid(
            "UnionAll physical input is absent",
        ))?;
        for ((source, &mapped), &output) in
            branch.sources.iter().zip(mapping).zip(&node.output.columns)
        {
            let source_ordinal = match source.input.source {
                ProgramChannelSite::Layout {
                    node: owner,
                    role: ProgramChannelLayoutRole::NodeOutput,
                    ordinal,
                } if owner == branch.input => usize::try_from(ordinal).ok(),
                _ => None,
            };
            let same = source_ordinal.and_then(|ordinal| child.output.columns.get(ordinal))
                == Some(&mapped);
            let source_value = fragment.values().get(&mapped);
            let output_value = fragment.values().get(&output);
            work.step()?;
            if !same {
                return Err(FragmentCompileError::Invalid(
                    "UnionAll mapping differs from its proved input occurrence",
                ));
            }
            let source_value = source_value.ok_or(FragmentCompileError::Invalid(
                "UnionAll mapped value is absent",
            ))?;
            let output_value = output_value.ok_or(FragmentCompileError::Invalid(
                "UnionAll output value is absent",
            ))?;
            exact_type(&source.ty, &source_value.ty, work)?;
            let widens = !source_value.ty.nullable || output_value.ty.nullable;
            work.step()?;
            if !widens {
                return Err(FragmentCompileError::Invalid(
                    "UnionAll output narrows input nullability",
                ));
            }
            work.flush()?;
            let mut expected = source_value.ty.clone();
            expected.nullable = output_value.ty.nullable;
            work.step()?;
            work.flush()?;
            exact_type(&expected, &output_value.ty, work)?;
        }
    }

    let layout = output_layout(package, node, local, slots, work)?;
    let mut nodes = Vec::new();
    reserve_vec(&mut nodes, node_count, work)?;
    let mut inputs = Vec::new();
    reserve_vec(&mut inputs, branches.len(), work)?;
    let diagnostic = DiagnosticSourceNodeId::new(node.id.get());
    for (branch, definitions) in branches.iter().zip(definitions) {
        let exprs = copy_items(definitions, work)?;
        let expr_slot_ids = copy_items(slots, work)?;
        let sources = source_diagnostic(diagnostic, work)?;
        let kind = ProgramNodeKind::Project {
            input: branch.input,
            is_subordinate: true,
            validate_final_result_input: false,
            exprs,
            expr_slot_ids,
            expr_slot_schemas: None,
            output_indices: None,
        };
        let output = layout.clone();
        work.step()?;
        work.flush()?;
        let normalizer = ProgramNode::new_local(branch.normalizer, sources, kind, output);
        work.step()?;
        work.flush()?;
        nodes.push(normalizer);
        inputs.push(branch.normalizer);
        work.step()?;
    }
    let sources = source_diagnostic(diagnostic, work)?;
    work.flush()?;
    let union =
        ProgramNode::new_local(local, sources, ProgramNodeKind::UnionAll { inputs }, layout);
    work.step()?;
    work.flush()?;
    nodes.push(union);
    work.step()?;
    Ok(nodes)
}

fn exact_type(
    left: &FunctionValueType,
    right: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCompileError> {
    work.flush()?;
    let same = left.exactly_equals_observed::<FragmentCompileError>(right, || {
        work.step().map_err(Into::into)
    })?;
    work.flush()?;
    if !same {
        return Err(FragmentCompileError::Invalid(
            "UnionAll complete value types differ",
        ));
    }
    Ok(())
}

fn output_layout(
    package: &FragmentPackage,
    node: &PhysicalNode,
    local: ProgramNodeId,
    slots: &[SlotId],
    work: &mut CompileCheckpoints<'_>,
) -> Result<StaticLayout, FragmentCompileError> {
    let mut result_names = package
        .result()
        .filter(|result| result.output.columns.len() == node.output.columns.len());
    if let Some(result) = result_names {
        for (actual, expected) in node.output.columns.iter().zip(&result.output.columns) {
            let same = actual == expected;
            work.step()?;
            if !same {
                result_names = None;
                break;
            }
        }
    }
    let mut fields = Vec::<Field>::new();
    if package.original_metadata_namespace().is_none() {
        reserve_vec(&mut fields, slots.len(), work)?;
    }
    let mut original_fields = match package.original_metadata_namespace() {
        Some(_) => {
            let mut original_fields = Vec::new();
            reserve_vec(&mut original_fields, slots.len(), work)?;
            Some(original_fields)
        }
        None => None,
    };
    for (ordinal, &value) in node.output.columns.iter().enumerate() {
        let definition = package.fragment().values().get(&value);
        let result_name = result_names.and_then(|result| result.fields.get(ordinal));
        work.step()?;
        let definition = definition.ok_or(FragmentCompileError::Invalid(
            "UnionAll output value is absent",
        ))?;
        let name = if let Some(field) = result_name {
            copy_name(field.alias.as_deref().unwrap_or(&field.name), work)?
        } else {
            work.flush()?;
            let name = format!("local_{}_{}", local.index(), ordinal);
            work.step()?;
            work.flush()?;
            name
        };
        work.flush()?;
        if let Some(original_fields) = original_fields.as_mut() {
            let field = novarocks_type_contract::owned_resources::metadata_materialization::materialize_value_field(&definition.ty, name);
            work.step()?;
            work.flush()?;
            original_fields.push(field?);
        } else {
            let field = definition.ty.try_to_field(name);
            work.step()?;
            work.flush()?;
            fields.push(field?);
        }
        work.step()?;
    }
    let copied_slots = copy_items(slots, work)?;
    work.flush()?;
    let materialized = match (original_fields, package.original_metadata_namespace()) {
        (Some(fields), Some(namespace)) => Some(novarocks_type_contract::owned_resources::metadata_materialization::TypedSchemaMaterializations::new(fields, namespace.clone()).into_original_schema()),
        _ => None,
    };
    let schema = if let Some(source) = &materialized {
        source.schema_owner().schema().clone()
    } else {
        Arc::new(Schema::new(fields))
    };
    let slots = Arc::from(copied_slots);
    work.step()?;
    work.flush()?;
    let layout = match materialized {
        Some(source) => {
            StaticLayout::try_new_materialized_for_compile(source, slots, work.control())?
        }
        None => StaticLayout::try_new_for_compile(schema, slots, work.control())?,
    };
    work.flush()?;
    Ok(layout)
}

fn copy_items<T: Copy>(
    source: &[T],
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<T>, FragmentCompileError> {
    let mut copied = Vec::new();
    reserve_vec(&mut copied, source.len(), work)?;
    for &item in source {
        copied.push(item);
        work.step()?;
    }
    Ok(copied)
}

fn source_diagnostic(
    source: DiagnosticSourceNodeId,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<DiagnosticSourceNodeId>, FragmentCompileError> {
    let mut sources = Vec::new();
    reserve_vec(&mut sources, 1, work)?;
    sources.push(source);
    work.step()?;
    Ok(sources)
}

fn copy_name(
    source: &str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<String, FragmentCompileError> {
    Layout::array::<u8>(source.len()).map_err(|_| CompileControlError::ResourceExhausted)?;
    let mut copied = String::new();
    work.flush()?;
    let reserved = copied.try_reserve_exact(source.len());
    reserved.map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    let mut start = 0usize;
    while start < source.len() {
        let mut end = start.saturating_add(256).min(source.len());
        while !source.is_char_boundary(end) {
            end -= 1;
            work.step()?;
        }
        copied.push_str(&source[start..end]);
        for _ in start..end {
            work.step()?;
        }
        start = end;
    }
    Ok(copied)
}
