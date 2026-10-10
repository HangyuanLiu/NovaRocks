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

//! Materialize admitted constant cells into the existing static Values owner.
//! Every other cell stays a dynamic `ValuesCell` root that the one compiled
//! evaluator runs at runtime; nothing is evaluated or folded here. This is not
//! an expression evaluator or an opaque Arrow memory grant.

use crate::{assert_rows::reserve_vec, expressions::LoweredExpressions, lowering::FragmentCompileError};
use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, UInt64Array, new_empty_array};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::{
    interleave::interleave,
    take::{TakeOptions, take},
};
use novarocks_functions::{
    ConstantError, ConstantValue, KernelFailure,
    selected_copy::{self, CopyError},
};
use novarocks_local_program::{
    ProgramExprId, ProgramNodeKind, StaticExprKind, StaticLayout, StaticValues, StaticValuesCell,
};
use novarocks_physical_plan::{ExprId, ExprKind, ExpressionRootRole, FragmentPackage, PhysicalNode};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ExpressionUseId, PureCompileControl,
    ValueTypeVisit, validate_arrow_carrier_parameters_observed,
    validate_value_type_structure_observed,
};
use novarocks_types::SlotId;
use std::{collections::BTreeSet, sync::Arc};

/// One admitted Values cell: a compile-time constant materialized into the
/// backing, or a dynamic cell kept as its own runtime root.
#[derive(Clone, Copy)]
enum Cell<'a> {
    Constant(&'a ConstantValue),
    Dynamic(ProgramExprId),
}

fn cell<'a>(
    package: &FragmentPackage,
    expressions: &'a LoweredExpressions,
    id: ExprId,
) -> Result<Cell<'a>, FragmentCompileError> {
    let source = package
        .fragment()
        .expressions()
        .get(id)
        .ok_or(FragmentCompileError::Invalid(
            "missing Values cell definition",
        ))?;
    let local = *expressions
        .ids
        .get(&id)
        .ok_or(FragmentCompileError::Invalid("Values cell was not lowered"))?;
    if !matches!(source.kind, ExprKind::Literal(_) | ExprKind::Constant(_)) {
        // The compiled evaluator runs it once, over an empty port, with the
        // same value, NULL and row-error effects as any other root.
        return Ok(Cell::Dynamic(local));
    }
    let lowered = expressions
        .arena
        .node(local)
        .ok_or(FragmentCompileError::Invalid(
            "missing lowered Values definition",
        ))?;
    let StaticExprKind::Constant(value) = lowered.kind() else {
        return Err(FragmentCompileError::Invalid(
            "Values constant source changed",
        ));
    };
    Ok(Cell::Constant(value))
}
fn constant<'a>(
    package: &FragmentPackage,
    expressions: &'a LoweredExpressions,
    id: ExprId,
) -> Result<Option<&'a ConstantValue>, FragmentCompileError> {
    Ok(match cell(package, expressions, id)? {
        Cell::Constant(value) => Some(value),
        Cell::Dynamic(_) => None,
    })
}
fn copy_error(error: CopyError, node: novarocks_physical_plan::NodeId) -> FragmentCompileError {
    match error {
        CopyError::Extent | CopyError::Control(KernelFailure::ResourceExhausted) => {
            FragmentCompileError::Control(CompileControlError::ResourceExhausted)
        }
        CopyError::Control(KernelFailure::Cancelled) => {
            FragmentCompileError::Control(CompileControlError::Cancelled)
        }
        CopyError::Control(KernelFailure::DeadlineExceeded) => {
            FragmentCompileError::Control(CompileControlError::DeadlineExceeded)
        }
        CopyError::Unsupported(_) => FragmentCompileError::Unsupported {
            node: Some(node),
            feature: "Values combined copy carrier lacks selected-copy preflight",
        },
        error => FragmentCompileError::Owner {
            phase: "Values copy",
            error: Box::new(error),
        },
    }
}
fn observe(work: &mut CompileCheckpoints<'_>, boundary: bool) -> Result<(), KernelFailure> {
    let result = if boundary { work.flush() } else { work.step() };
    result.map_err(|cause| match cause {
        CompileControlError::Cancelled => KernelFailure::Cancelled,
        CompileControlError::DeadlineExceeded => KernelFailure::DeadlineExceeded,
        CompileControlError::ResourceExhausted => KernelFailure::ResourceExhausted,
    })
}

pub(crate) fn lower_values(
    package: &FragmentPackage,
    node: &PhysicalNode,
    expressions: &LoweredExpressions,
    slots: &[SlotId],
    control: &dyn PureCompileControl,
) -> Result<(ProgramNodeKind, StaticLayout), FragmentCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core(package, node, expressions, slots, &mut work);
    if matches!(&result, Err(FragmentCompileError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn lower_core(
    package: &FragmentPackage,
    node: &PhysicalNode,
    expressions: &LoweredExpressions,
    slots: &[SlotId],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(ProgramNodeKind, StaticLayout), FragmentCompileError> {
    let novarocks_physical_plan::NodeKind::Values { rows } = &node.kind else {
        return Err(FragmentCompileError::Invalid("Values source kind differs"));
    };
    if !node.inputs.is_empty() || slots.len() != node.output.columns.len() {
        return Err(FragmentCompileError::Invalid(
            "Values input or channel width differs",
        ));
    }
    for row in rows {
        let same = row.len() == slots.len();
        work.step()?;
        if !same {
            return Err(FragmentCompileError::Invalid("Values row width differs"));
        }
    }
    // Validate every source before materializing any column. No dynamic call is
    // evaluated, folded, or removed from its original required occurrence: it
    // stays a ValuesCell root at its exact position.
    let mut constant_cells: Vec<usize> = Vec::new();
    reserve_vec(&mut constant_cells, slots.len(), work)?;
    for _ in 0..slots.len() {
        constant_cells.push(0);
        work.step()?;
    }
    let mut dynamic = Vec::new();
    for (row_index, row) in rows.iter().enumerate() {
        for (ordinal, &id) in row.iter().enumerate() {
            let value = cell(package, expressions, id);
            work.step()?;
            let value = value?;
            let source = package
                .fragment()
                .expressions()
                .get(id)
                .ok_or(FragmentCompileError::Invalid("missing Values source"))?;
            let output = package
                .fragment()
                .values()
                .get(&node.output.columns[ordinal])
                .ok_or(FragmentCompileError::Invalid("missing Values output"))?;
            if source.owner != node.id {
                return Err(FragmentCompileError::Invalid("foreign Values cell owner"));
            }
            let ty = match value {
                Cell::Constant(value) => value.value_type(),
                Cell::Dynamic(_) => &source.ty,
            };
            if !ty.exactly_equals_observed::<FragmentCompileError>(&output.ty, || {
                work.step().map_err(Into::into)
            })? {
                return Err(FragmentCompileError::Invalid(
                    "Values cell full type differs from output",
                ));
            }
            match value {
                Cell::Constant(_) => constant_cells[ordinal] += 1,
                Cell::Dynamic(definition) => dynamic.push(StaticValuesCell {
                    row: u32::try_from(row_index)
                        .map_err(|_| CompileControlError::ResourceExhausted)?,
                    column: u32::try_from(ordinal)
                        .map_err(|_| CompileControlError::ResourceExhausted)?,
                    definition,
                }),
            }
        }
    }
    // A column without a constant cell keeps an empty constant backing.
    // Arrow's sole empty constructor selects the first Union child even for
    // zero rows. The shared type walk also reaches nested empty Unions.
    for (output, count) in node.output.columns.iter().zip(&constant_cells) {
        work.step()?;
        if *count != 0 {
            continue;
        }
        let ty = &package
            .fragment()
            .values()
            .get(output)
            .ok_or(FragmentCompileError::Invalid("missing Values output type"))?
            .ty;
        work.flush()?;
        validate_value_type_structure_observed::<FragmentCompileError>(&ty.data_type, |visit| {
            if let ValueTypeVisit::TypeNode(data_type) = visit {
                validate_arrow_carrier_parameters_observed::<ConstantError>(data_type, || {
                    work.step().map_err(Into::into)
                })
                .map_err(|error| match error {
                    ConstantError::Control(cause) => FragmentCompileError::Control(cause),
                    ConstantError::Limit(_) => {
                        FragmentCompileError::Control(CompileControlError::ResourceExhausted)
                    }
                    _ => FragmentCompileError::Unsupported {
                        node: Some(node.id),
                        feature: "Values empty carrier has invalid Arrow constructor parameters",
                    },
                })?;
            }
            let unsupported = matches!(visit, ValueTypeVisit::TypeNode(DataType::Union(fields, _)) if fields.is_empty());
            work.step()?;
            if unsupported {
                return Err(FragmentCompileError::Unsupported {
                    node: Some(node.id),
                    feature: "Values empty Union has no Arrow constructor child",
                });
            }
            Ok(())
        })?;
        work.flush()?;
    }
    let mut fields: Vec<Field> = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
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
    reserve_vec(&mut columns, slots.len(), work)?;
    for (ordinal, output) in node.output.columns.iter().enumerate() {
        let ty = &package
            .fragment()
            .values()
            .get(output)
            .ok_or(FragmentCompileError::Invalid("missing Values output type"))?
            .ty;
        work.flush()?;
        let name = if package
            .result()
            .is_some_and(|result| result.output.columns == node.output.columns)
        {
            let field = package
                .result()
                .and_then(|result| result.fields.get(ordinal))
                .ok_or(FragmentCompileError::Invalid("missing Values result label"))?;
            field.alias.as_deref().unwrap_or(&field.name).to_owned()
        } else {
            format!("local_values_{}_{}", node.id.get(), ordinal)
        };
        if let Some(original_fields) = original_fields.as_mut() {
            let field = novarocks_type_contract::owned_resources::metadata_materialization::materialize_value_field(ty, name);
            work.flush()?;
            original_fields.push(field?);
        } else {
            let field = ty.try_to_field(name);
            work.flush()?;
            fields.push(field?);
        }
        let column = if constant_cells[ordinal] == 0 {
            // The sole Arrow empty constructor follows the admitted type. Its
            // internal type walk/allocation remains opaque, not a MEM grant.
            work.flush()?;
            let array = new_empty_array(&ty.data_type);
            work.flush()?;
            array
        } else {
            // Only this column's constant cells, in row order; a dynamic cell
            // has no placeholder in the backing.
            let count = constant_cells[ordinal];
            let mut sources = Vec::new();
            let mut choices = Vec::new();
            let mut indices = Vec::new();
            let mut raw_indices = Vec::new();
            reserve_vec(&mut sources, count, work)?;
            reserve_vec(&mut choices, count, work)?;
            reserve_vec(&mut indices, count, work)?;
            reserve_vec(&mut raw_indices, count, work)?;
            let mut first: Option<&ConstantValue> = None;
            let mut same_backing = true;
            for row in rows {
                let value = constant(package, expressions, row[ordinal]);
                work.step()?;
                let Some(value) = value? else {
                    continue;
                };
                let first = *first.get_or_insert(value);
                same_backing &= value.pool().backing_identity() == first.pool().backing_identity();
                sources.push(Arc::clone(value.pool().array()));
                choices.push((sources.len() - 1, value.ordinal() as usize));
                indices.push(Some(u64::from(value.ordinal())));
                raw_indices.push(u64::from(value.ordinal()));
                work.step()?;
            }
            let first = first.ok_or(FragmentCompileError::Invalid(
                "Values constant cell count changed",
            ))?;
            if same_backing {
                selected_copy::preflight_take(
                    first.pool().array().as_ref(),
                    &indices,
                    |boundary| observe(work, boundary),
                )
                .map_err(|error| copy_error(error, node.id))?;
                work.flush()?;
                let indices = UInt64Array::from(raw_indices);
                let copied = take(
                    first.pool().array().as_ref(),
                    &indices,
                    Some(TakeOptions { check_bounds: true }),
                );
                work.flush()?;
                copied.map_err(|error| FragmentCompileError::Owner {
                    phase: "Values take",
                    error: Box::new(error),
                })?
            } else {
                selected_copy::preflight_guarded_interleave(
                    &ty.data_type,
                    &sources,
                    &choices,
                    |boundary| observe(work, boundary),
                )
                .map_err(|error| copy_error(error, node.id))?;
                let mut borrowed: Vec<&dyn Array> = Vec::new();
                reserve_vec(&mut borrowed, sources.len(), work)?;
                for source in &sources {
                    borrowed.push(source.as_ref());
                    work.step()?;
                }
                work.flush()?;
                let copied = interleave(&borrowed, &choices);
                work.flush()?;
                copied.map_err(|error| FragmentCompileError::Owner {
                    phase: "Values interleave",
                    error: Box::new(error),
                })?
            }
        };
        columns.push(column);
        work.step()?;
    }
    work.flush()?;
    let layout = match (original_fields, package.original_metadata_namespace()) {
        (Some(fields), Some(namespace)) => {
            let source = novarocks_type_contract::owned_resources::metadata_materialization::TypedSchemaMaterializations::new(fields, namespace.clone()).into_original_schema();
            StaticLayout::try_new_materialized_for_compile(
                source,
                Arc::from(slots),
                work.control(),
            )?
        }
        _ => StaticLayout::try_new_for_compile(
            Arc::new(Schema::new(fields)),
            Arc::from(slots),
            work.control(),
        )?,
    };
    work.flush()?;
    if !dynamic.is_empty() {
        let values = StaticValues::try_new_with_cells_for_compile(
            rows.len(),
            columns,
            dynamic,
            layout.clone(),
            work.control(),
        )?;
        work.flush()?;
        return Ok((ProgramNodeKind::Values { values }, layout));
    }
    let batch = RecordBatch::try_new_with_options(
        layout.schema().clone(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows.len())),
    );
    work.flush()?;
    let batch = batch.map_err(|error| FragmentCompileError::Owner {
        phase: "Values batch",
        error: Box::new(error),
    })?;
    let values = StaticValues::try_new_for_compile(batch, layout.clone(), work.control())?;
    work.flush()?;
    Ok((ProgramNodeKind::Values { values }, layout))
}

pub(crate) fn retired_values_uses(
    package: &FragmentPackage,
    expressions: &LoweredExpressions,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BTreeSet<ExpressionUseId>, FragmentCompileError> {
    let mut retired = BTreeSet::new();
    for (site, id) in package.expression_uses().bindings() {
        if let ExpressionRootRole::ValuesCell { .. } = site.role {
            let invocation = package.expression_uses().flow().uses().get(id).ok_or(
                FragmentCompileError::Invalid("missing Values root invocation"),
            )?;
            let cell = constant(package, expressions, invocation.definition);
            work.step()?;
            // A dynamic cell keeps its root use and every argument use.
            if cell?.is_some() {
                if !invocation.arguments.is_empty() {
                    return Err(FragmentCompileError::Invalid(
                        "constant Values root has arguments",
                    ));
                }
                retired.insert(*id);
            }
        }
        work.step()?;
    }
    Ok(retired)
}
