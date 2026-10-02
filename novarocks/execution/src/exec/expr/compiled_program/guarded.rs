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

//! Iterative actual-use continuations. Owned row domains never borrow a moving
//! frame. Kernel calls temporarily borrow one stable domain, then erase that
//! borrow into owned values before resuming the parent continuation.
use super::*;
use arrow::{
    array::{Array, BooleanArray},
    compute::interleave,
};
use novarocks_local_program::{ProgramLexicalBindings, ProgramNodeId};

pub(super) fn supports_result(ty: &DataType) -> bool {
    *ty == DataType::Boolean || ty.primitive_width().is_some()
}

enum OwnedValue {
    Constant(novarocks_functions::ConstantValue),
    Column(ArrayRef),
    Selected(ArrayRef, Box<[RowDataError]>),
}
impl OwnedValue {
    fn into_value<'a>(
        self,
        selection: Selection<'a>,
        work: &mut Work<'_>,
    ) -> Result<Value<'a>, KernelFailure> {
        Ok(match self {
            Self::Constant(value) => Value::Constant(value),
            Self::Column(value) => Value::Column(value),
            Self::Selected(array, errors) => {
                let ty = array.data_type().clone();
                Value::Selected(SelectedValues::try_new_observed(
                    selection,
                    &ty,
                    array,
                    errors,
                    || work.step(),
                )?)
            }
        })
    }
    fn from_selected(output: SelectedValues<'_>) -> Self {
        let (_, array, errors) = output.into_parts();
        Self::Selected(array, errors)
    }
}
struct Child {
    // Compact parent positions, distinct from original batch rows.
    ordinals: Vec<usize>,
    value: OwnedValue,
}
struct Frame {
    occurrence: ProgramUseRef,
    rows: Vec<usize>,
    parent_ordinals: Vec<usize>,
    next: usize,
    children: Vec<Child>,
    routes: Vec<Option<bool>>,
    remaining: Vec<usize>,
    choices: Vec<Option<(usize, usize)>>,
    errors: BTreeMap<usize, RowDataError>,
}
impl Frame {
    fn new(
        occurrence: ProgramUseRef,
        rows: Vec<usize>,
        parent_ordinals: Vec<usize>,
        work: &mut Work<'_>,
    ) -> Result<Self, KernelFailure> {
        let mut remaining = Vec::with_capacity(rows.len());
        let mut choices = Vec::with_capacity(rows.len());
        for ordinal in 0..rows.len() {
            remaining.push(ordinal);
            choices.push(None);
            work.step()?;
        }
        Ok(Self {
            occurrence,
            rows,
            parent_ordinals,
            next: 0,
            children: Vec::new(),
            routes: Vec::new(),
            remaining,
            choices,
            errors: BTreeMap::new(),
        })
    }
    fn next_ordinals(
        &self,
        shape: ControlShape,
        arity: usize,
        work: &mut Work<'_>,
    ) -> Result<Option<Vec<usize>>, KernelFailure> {
        if self.next >= arity || (shape == ControlShape::Coalesce && self.remaining.is_empty()) {
            return Ok(None);
        }
        let mut ordinals = Vec::new();
        match shape {
            ControlShape::If if self.next != 0 => {
                let desired = self.next == 1;
                for (ordinal, &route) in self.routes.iter().enumerate() {
                    if route == Some(desired) {
                        ordinals.push(ordinal);
                    }
                    work.step()?;
                }
            }
            ControlShape::Coalesce => {
                for &ordinal in &self.remaining {
                    ordinals.push(ordinal);
                    work.step()?;
                }
            }
            _ => {
                for ordinal in 0..self.rows.len() {
                    ordinals.push(ordinal);
                    work.step()?;
                }
            }
        }
        Ok(Some(ordinals))
    }
    fn attach(
        &mut self,
        child: Child,
        shape: ControlShape,
        batch_rows: usize,
        work: &mut Work<'_>,
    ) -> Result<(), KernelFailure> {
        if !matches!(shape, ControlShape::If | ControlShape::Coalesce) {
            self.children.push(child);
            return Ok(());
        }
        let child_index = self.children.len();
        let mut rows = Vec::with_capacity(child.ordinals.len());
        for &ordinal in &child.ordinals {
            rows.push(self.rows[ordinal]);
            work.step()?;
        }
        let selection = Selection::try_sparse_observed(batch_rows, &rows, || work.step())?;
        let value = child.value.into_value(selection, work)?;
        let ty = value.argument().array().data_type().clone();
        let output = value.materialize(selection, &ty, work)?;
        let mut child_errors = output.errors().iter().peekable();
        let mut remaining = Vec::new();
        if shape == ControlShape::If && child_index == 0 {
            let booleans = output
                .values()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| invalid("actual IF condition is not its frozen Boolean carrier"))?;
            self.routes.reserve(self.rows.len());
            for (ordinal, _) in rows.iter().enumerate() {
                let route = if child_errors
                    .peek()
                    .is_some_and(|error| error.selected_ordinal() == ordinal)
                {
                    let error = child_errors
                        .next()
                        .ok_or_else(|| internal("missing IF condition error"))?;
                    self.errors
                        .insert(ordinal, RowDataError::new(ordinal, error.message()));
                    None
                } else {
                    Some(!booleans.is_null(ordinal) && booleans.value(ordinal))
                };
                self.routes.push(route);
                work.step()?;
            }
        } else {
            work.flush()?;
            visit_selected_nulls(
                EvaluatedArgument::SelectedColumn(&output),
                selection,
                work.control,
                |ordinal, _, is_null| {
                    let parent = child.ordinals[ordinal];
                    if child_errors
                        .peek()
                        .is_some_and(|error| error.selected_ordinal() == ordinal)
                    {
                        let error = child_errors
                            .next()
                            .ok_or_else(|| internal("missing guarded child error"))?;
                        self.errors
                            .insert(parent, RowDataError::new(parent, error.message()));
                    } else if shape == ControlShape::Coalesce && is_null {
                        remaining.push(parent);
                    } else {
                        self.choices[parent] = Some((child_index, ordinal));
                    }
                    Ok(())
                },
            )?;
        }
        if shape == ControlShape::Coalesce {
            self.remaining = remaining;
        }
        self.children.push(Child {
            ordinals: child.ordinals,
            value: OwnedValue::from_selected(output),
        });
        Ok(())
    }
}

pub(super) fn evaluate_tree<'a>(
    checked: &ProgramLexicalBindings,
    root: ProgramExpressionRootSite,
    input: &RecordBatch,
    input_node: ProgramNodeId,
    selection: Selection<'a>,
    instances: &mut BTreeMap<ProgramUseRef, ScalarEvaluationInstance>,
    work: &mut Work<'_>,
) -> Result<Value<'a>, KernelFailure> {
    let typed = checked.channels().expressions();
    let resolved = typed.resolved_calls();
    let snapshot = resolved.snapshot();
    let flow = &snapshot.flows()[&root.arena()];
    let definitions = &snapshot.roots().arenas()[&root.arena()];
    let root_use = ProgramUseRef {
        arena: root.arena(),
        use_id: snapshot.bindings()[&root],
    };
    // Admit the controller's per-row storage representation before its first
    // allocation. This is an extent gate, not a host memory grant.
    let element_width = std::mem::size_of::<RowDataError>()
        .max(std::mem::size_of::<Option<(usize, usize)>>())
        .max(std::mem::size_of::<usize>());
    if selection
        .len()
        .checked_mul(element_width)
        .is_none_or(|bytes| bytes > isize::MAX as usize)
    {
        return Err(KernelFailure::ResourceExhausted);
    }
    work.step()?;
    let mut rows = Vec::with_capacity(selection.len());
    for row in selection.iter() {
        rows.push(row);
        work.step()?;
    }
    let mut frames = vec![Frame::new(root_use, rows, Vec::new(), work)?];
    while let Some(mut frame) = frames.pop() {
        work.step()?;
        let invocation = &flow.uses()[&frame.occurrence.use_id];
        let definition = definitions
            .node(invocation.definition)
            .ok_or_else(|| invalid("missing compiled definition"))?;
        let FunctionArgumentType::Value(result_type) = typed
            .definition_type(frame.occurrence.arena, invocation.definition)
            .ok_or_else(|| invalid("missing exact compiled definition type"))?
        else {
            return Err(invalid(
                "ordinary root cannot materialize a lambda definition",
            ));
        };
        // An empty actual domain never enters children, constructors or kernels.
        let next = if frame.rows.is_empty() {
            None
        } else {
            frame.next_ordinals(invocation.control, invocation.arguments.len(), work)?
        };
        if let Some(ordinals) = next {
            let child_use = invocation.arguments[frame.next];
            frame.next += 1;
            let mut rows = Vec::with_capacity(ordinals.len());
            for &ordinal in &ordinals {
                rows.push(frame.rows[ordinal]);
                work.step()?;
            }
            let child = Frame::new(
                ProgramUseRef {
                    arena: frame.occurrence.arena,
                    use_id: child_use,
                },
                rows,
                ordinals,
                work,
            )?;
            frames.push(frame);
            frames.push(child);
            continue;
        }
        let local_selection =
            Selection::try_sparse_observed(input.num_rows(), &frame.rows, || work.step())?;
        let value = if frame.rows.is_empty() {
            work.flush()?;
            let array = new_empty_array(&result_type.data_type);
            work.flush()?;
            OwnedValue::Selected(array, Box::default())
        } else {
            match definition.kind() {
                StaticExprKind::Constant(constant) => {
                    work.flush()?;
                    validate_evaluated_argument_observed(
                        EvaluatedArgument::Constant(constant),
                        local_selection,
                        result_type,
                        work.control,
                    )?;
                    OwnedValue::Constant(constant.clone())
                }
                StaticExprKind::SlotId(_) => {
                    let Some(ProgramLexicalSource::Input(ProgramChannelSite::Layout {
                        node,
                        role: ProgramChannelLayoutRole::NodeOutput,
                        ordinal,
                    })) = checked.slots().get(&frame.occurrence)
                    else {
                        return Err(invalid("slot requires its actual compiled input source"));
                    };
                    if *node != input_node {
                        return Err(invalid("slot source differs from actual root input port"));
                    }
                    OwnedValue::Column(Arc::clone(
                        input
                            .columns()
                            .get(*ordinal as usize)
                            .ok_or_else(|| invalid("slot source ordinal is absent"))?,
                    ))
                }
                StaticExprKind::BoundCall { .. } => {
                    let call = &resolved.calls()[&ProgramCallSite::Expression(frame.occurrence)];
                    match call.specialization().prepared() {
                        PreparedPureKernel::Scalar(prepared) => {
                            let mut children = Vec::with_capacity(frame.children.len());
                            for child in std::mem::take(&mut frame.children) {
                                children.push(child.value.into_value(local_selection, work)?);
                                work.step()?;
                            }
                            OwnedValue::from_selected(evaluate_scalar(
                                frame.occurrence,
                                prepared,
                                &children,
                                local_selection,
                                instances,
                                work,
                            )?)
                        }
                        PreparedPureKernel::ControlIntrinsic(_) => assemble(
                            &frame.children,
                            &frame.choices,
                            &mut frame.errors,
                            &result_type.data_type,
                            local_selection,
                            work,
                        )?,
                        _ => return Err(invalid("compiled occurrence has a different lifecycle")),
                    }
                }
                _ => return Err(invalid("unsupported compiled root definition")),
            }
        };
        if let Some(parent) = frames.last_mut() {
            let shape = flow.uses()[&parent.occurrence.use_id].control;
            parent.attach(
                Child {
                    ordinals: frame.parent_ordinals,
                    value,
                },
                shape,
                input.num_rows(),
                work,
            )?;
        } else {
            return value.into_value(selection, work);
        }
    }
    Err(internal("actual compiled root result is absent"))
}

fn assemble(
    children: &[Child],
    choices: &[Option<(usize, usize)>],
    row_errors: &mut BTreeMap<usize, RowDataError>,
    ty: &DataType,
    selection: Selection<'_>,
    work: &mut Work<'_>,
) -> Result<OwnedValue, KernelFailure> {
    // This first exact guarded carrier protocol is statically admitted before
    // any branch state is entered. Other carriers need their multi-source author.
    if !supports_result(ty) {
        return Err(invalid(
            "guarded result requires its dedicated carrier protocol",
        ));
    }
    work.flush()?;
    let mut sources = vec![new_null_array(ty, 1)];
    work.flush()?;
    let mut source_ids = Vec::with_capacity(children.len());
    for child in children {
        let id = match &child.value {
            OwnedValue::Selected(array, _) if array.data_type() == ty => {
                sources.push(Arc::clone(array));
                Some(sources.len() - 1)
            }
            _ => None, // The IF condition is not a result source.
        };
        source_ids.push(id);
        work.step()?;
    }
    let mut indices = Vec::with_capacity(choices.len());
    for &choice in choices {
        indices.push(match choice {
            Some((child, ordinal)) => (
                source_ids[child]
                    .ok_or_else(|| internal("missing actual guarded result source"))?,
                ordinal,
            ),
            None => (0, 0),
        });
        work.step()?;
    }
    super::super::constant_eval::preflight_fixed_interleave(ty, &sources, &indices, |boundary| {
        if boundary { work.flush() } else { work.step() }
    })
    .map_err(|error| match error {
        super::super::constant_eval::CopyError::Control(error) => error,
        super::super::constant_eval::CopyError::Extent => KernelFailure::ResourceExhausted,
        _ => invalid("guarded result requires its dedicated carrier protocol"),
    })?;
    let mut arrays = Vec::with_capacity(sources.len());
    for source in &sources {
        arrays.push(source.as_ref());
        work.step()?;
    }
    work.flush()?;
    let array = interleave(&arrays, &indices)
        .map_err(|_| internal("checked guarded result could not be assembled"))?;
    work.flush()?;
    let mut errors = Vec::with_capacity(row_errors.len());
    for error in std::mem::take(row_errors).into_values() {
        errors.push(error);
        work.step()?;
    }
    Ok(OwnedValue::from_selected(SelectedValues::try_new_observed(
        selection,
        ty,
        array,
        errors.into_boxed_slice(),
        || work.step(),
    )?))
}
