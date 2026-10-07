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
use super::boolean_region::BooleanRows;
use super::*;
use arrow::{
    array::{Array, BooleanArray},
    compute::interleave,
};
use novarocks_local_program::ProgramNodeId;
use novarocks_type_contract::EvaluationDemand;

pub(super) fn supports_result(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Boolean
            | DataType::Utf8
            | DataType::Binary
            | DataType::LargeUtf8
            | DataType::LargeBinary
    ) || ty.primitive_width().is_some()
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
    matched: Vec<usize>,
    operand: Option<ArrayRef>,
    choices: Vec<Option<(usize, usize)>>,
    errors: BTreeMap<usize, RowDataError>,
    boolean: Option<BooleanRows>,
}
impl Frame {
    fn new(
        occurrence: ProgramUseRef,
        shape: ControlShape,
        result_type: &DataType,
        rows: Vec<usize>,
        parent_ordinals: Vec<usize>,
        work: &mut Work<'_>,
    ) -> Result<Self, KernelFailure> {
        if supports_result(result_type) {
            novarocks_functions::selected_copy::guarded_interleave_extent(result_type, rows.len())
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            work.step()?;
        }
        let tracks_remaining = matches!(
            shape,
            ControlShape::Coalesce
                | ControlShape::Conjunction
                | ControlShape::Disjunction
                | ControlShape::Case { .. }
        );
        let tracks_choices = matches!(
            shape,
            ControlShape::If | ControlShape::Coalesce | ControlShape::Case { .. }
        );
        let mut remaining = Vec::new();
        let mut choices = Vec::new();
        if tracks_remaining {
            remaining.reserve(rows.len());
        }
        if tracks_choices {
            choices.reserve(rows.len());
        }
        if tracks_remaining || tracks_choices {
            for ordinal in 0..rows.len() {
                if tracks_remaining {
                    remaining.push(ordinal);
                }
                if tracks_choices {
                    choices.push(None);
                }
                work.step()?;
            }
        }
        Ok(Self {
            occurrence,
            rows,
            parent_ordinals,
            next: 0,
            children: Vec::new(),
            routes: Vec::new(),
            remaining,
            matched: Vec::new(),
            operand: None,
            choices,
            errors: BTreeMap::new(),
            boolean: None,
        })
    }
    fn next_ordinals(
        &mut self,
        shape: ControlShape,
        arity: usize,
        next_is_pure: bool,
        work: &mut Work<'_>,
    ) -> Result<Option<Vec<usize>>, KernelFailure> {
        if matches!(shape, ControlShape::Conjunction | ControlShape::Disjunction)
            && self.boolean.is_none()
        {
            self.boolean = Some(BooleanRows::new(self.rows.len(), work)?);
        }
        if matches!(shape, ControlShape::Conjunction | ControlShape::Disjunction) && !next_is_pure {
            self.boolean
                .as_mut()
                .ok_or_else(|| internal("missing Boolean continuation"))?
                .boundary(&mut self.errors, &mut self.remaining, work)?;
        }
        if self.next >= arity
            || (matches!(
                shape,
                ControlShape::Coalesce | ControlShape::Conjunction | ControlShape::Disjunction
            ) && self.remaining.is_empty())
        {
            return Ok(None);
        }
        let mut ordinals = Vec::new();
        match shape {
            ControlShape::Case { simple, arms, .. } => {
                let offset = usize::from(simple);
                let then = self.next >= offset
                    && self.next < offset + arms as usize * 2
                    && !(self.next - offset).is_multiple_of(2);
                let domain = if then { &self.matched } else { &self.remaining };
                if domain.is_empty() && self.remaining.is_empty() {
                    return Ok(None);
                }
                for &ordinal in domain {
                    ordinals.push(ordinal);
                    work.step()?;
                }
            }
            ControlShape::If if self.next != 0 => {
                let desired = self.next == 1;
                for (ordinal, &route) in self.routes.iter().enumerate() {
                    if route == Some(desired) {
                        ordinals.push(ordinal);
                    }
                    work.step()?;
                }
            }
            ControlShape::Coalesce | ControlShape::Conjunction | ControlShape::Disjunction => {
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
    #[expect(
        clippy::too_many_arguments,
        reason = "Keep the immutable recipe owner, selected child, parent semantics and work scope explicit"
    )]
    fn attach(
        &mut self,
        child: Child,
        program: &novarocks_local_program::LocalProgram,
        shape: ControlShape,
        demand: EvaluationDemand,
        child_is_pure: bool,
        batch_rows: usize,
        work: &mut Work<'_>,
    ) -> Result<(), KernelFailure> {
        if !matches!(
            shape,
            ControlShape::If
                | ControlShape::Coalesce
                | ControlShape::Conjunction
                | ControlShape::Disjunction
                | ControlShape::Case { .. }
        ) {
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
        if let ControlShape::Case { simple, arms, .. } = shape {
            let ordinal = self
                .next
                .checked_sub(1)
                .ok_or_else(|| internal("CASE child completed before its continuation"))?;
            let offset = usize::from(simple);
            if simple && ordinal == 0 {
                let mut errors = output.errors().iter().peekable();
                let mut remaining = Vec::with_capacity(child.ordinals.len());
                for (local, &parent) in child.ordinals.iter().enumerate() {
                    if errors
                        .peek()
                        .is_some_and(|error| error.selected_ordinal() == local)
                    {
                        let error = errors
                            .next()
                            .ok_or_else(|| internal("missing CASE operand error"))?;
                        self.errors
                            .insert(parent, RowDataError::new(parent, error.message()));
                    } else {
                        remaining.push(parent);
                    }
                    work.step()?;
                }
                self.remaining = remaining;
                self.operand = Some(Arc::clone(output.values()));
                // The compact operand is retained once. Its errors are now
                // terminal parent evidence, excluded from all later labels.
                work.flush()?;
                drop(output);
                work.flush()?;
                return Ok(());
            }
            if ordinal >= offset
                && ordinal < offset + arms as usize * 2
                && (ordinal - offset).is_multiple_of(2)
            {
                let booleans = if simple {
                    None
                } else {
                    Some(
                        output
                            .values()
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .ok_or_else(|| {
                                invalid(
                                    "searched CASE WHEN differs from its frozen Boolean carrier",
                                )
                            })?,
                    )
                };
                let parent_selection =
                    Selection::try_sparse_observed(batch_rows, &self.rows, || work.step())?;
                let operand = if simple {
                    let array = self
                        .operand
                        .as_ref()
                        .ok_or_else(|| internal("missing once-evaluated simple CASE operand"))?;
                    Some(SelectedValues::try_new_observed(
                        parent_selection,
                        array.data_type(),
                        Arc::clone(array),
                        Box::default(),
                        || work.step(),
                    )?)
                } else {
                    None
                };
                let recipe = if simple {
                    Some(
                        program
                            .comparison_recipe(
                                novarocks_local_program::ProgramComparisonSite::CaseWhen {
                                    occurrence: self.occurrence,
                                    arm: ((ordinal - offset) / 2) as u32,
                                },
                            )
                            .ok_or_else(|| invalid("missing exact CASE WHEN comparison recipe"))?,
                    )
                } else {
                    None
                };
                let mut errors = output.errors().iter().peekable();
                let mut remaining = Vec::with_capacity(child.ordinals.len());
                self.matched.clear();
                for (local, &parent) in child.ordinals.iter().enumerate() {
                    if errors
                        .peek()
                        .is_some_and(|error| error.selected_ordinal() == local)
                    {
                        let error = errors
                            .next()
                            .ok_or_else(|| internal("missing CASE WHEN error"))?;
                        self.errors
                            .insert(parent, RowDataError::new(parent, error.message()));
                    } else {
                        let matches = if let (Some(recipe), Some(operand)) =
                            (recipe, operand.as_ref())
                        {
                            work.flush()?;
                            recipe.compare_rows(
                                EvaluatedArgument::SelectedColumn(operand),
                                parent,
                                self.rows[parent],
                                EvaluatedArgument::SelectedColumn(&output),
                                local,
                                self.rows[parent],
                                work.control,
                            )? == Some(true)
                        } else {
                            let booleans = booleans
                                .ok_or_else(|| internal("missing searched CASE Boolean carrier"))?;
                            !booleans.is_null(local) && booleans.value(local)
                        };
                        if matches {
                            self.matched.push(parent);
                        } else {
                            remaining.push(parent);
                        }
                    }
                    work.step()?;
                }
                self.remaining = remaining;
                work.flush()?;
                drop(output);
                work.flush()?;
                return Ok(());
            }
            let mut errors = output.errors().iter().peekable();
            for (local, &parent) in child.ordinals.iter().enumerate() {
                if errors
                    .peek()
                    .is_some_and(|error| error.selected_ordinal() == local)
                {
                    let error = errors
                        .next()
                        .ok_or_else(|| internal("missing CASE result error"))?;
                    self.errors
                        .insert(parent, RowDataError::new(parent, error.message()));
                } else {
                    self.choices[parent] = Some((child_index, local));
                }
                work.step()?;
            }
            self.matched.clear();
            if ordinal == offset + arms as usize * 2 {
                self.remaining.clear();
            }
            self.children.push(Child {
                ordinals: child.ordinals,
                value: OwnedValue::from_selected(output),
            });
            return Ok(());
        }
        if matches!(shape, ControlShape::Conjunction | ControlShape::Disjunction) {
            self.remaining = self
                .boolean
                .as_mut()
                .ok_or_else(|| internal("missing Boolean continuation"))?
                .consume(
                    &output,
                    &child.ordinals,
                    shape,
                    demand,
                    child_is_pure,
                    &mut self.errors,
                    work,
                )?;
            // A wide Boolean region retains only row state, not every operand's
            // selected array. Drop the consumed backing at an opaque boundary.
            work.flush()?;
            drop(output);
            work.flush()?;
            return Ok(());
        }
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

#[expect(
    clippy::too_many_arguments,
    reason = "Keep the checked root, exact input port, selected domain, use-owned state/effects and caller control explicit"
)]
pub(super) fn evaluate_tree<'a>(
    program: &novarocks_local_program::LocalProgram,
    root: ProgramExpressionRootSite,
    input: &RecordBatch,
    input_node: Option<(ProgramNodeId, ProgramChannelLayoutRole)>,
    selection: Selection<'a>,
    instances: &mut BTreeMap<ProgramUseRef, ScalarEvaluationInstance>,
    effects: &BTreeMap<ProgramUseRef, ScopedExpressionEffects>,
    work: &mut Work<'_>,
) -> Result<Value<'a>, KernelFailure> {
    let checked = program.checked();
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
    let root_invocation = &flow.uses()[&root_use.use_id];
    let root_type = definitions
        .node(root_invocation.definition)
        .ok_or_else(|| invalid("missing actual root definition"))?
        .data_type();
    let mut frames = vec![Frame::new(
        root_use,
        root_invocation.control,
        root_type,
        rows,
        Vec::new(),
        work,
    )?];
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
            let next_is_pure = invocation
                .arguments
                .get(frame.next)
                .map(|child| {
                    let occurrence = ProgramUseRef {
                        arena: frame.occurrence.arena,
                        use_id: *child,
                    };
                    effects
                        .get(&occurrence)
                        .ok_or_else(|| invalid("missing exact operand effects"))?
                        .for_use(flow.uses()[child].context)
                        .map_err(|_| invalid("operand effect context differs"))
                        .map(|summary| summary.permits_boolean_reordering())
                })
                .transpose()?
                .unwrap_or(true);
            frame.next_ordinals(
                invocation.control,
                invocation.arguments.len(),
                next_is_pure,
                work,
            )?
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
                flow.uses()[&child_use].control,
                definitions
                    .node(flow.uses()[&child_use].definition)
                    .ok_or_else(|| invalid("missing actual child definition"))?
                    .data_type(),
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
                        role,
                        ordinal,
                    })) = checked.slots().get(&frame.occurrence)
                    else {
                        return Err(invalid("slot requires its actual compiled input source"));
                    };
                    // An empty-port root has no input source at all; every
                    // other root reads exactly its own (node, role) port.
                    if Some((*node, *role)) != input_node {
                        return Err(invalid("slot source differs from actual root input port"));
                    }
                    OwnedValue::Column(Arc::clone(
                        input
                            .columns()
                            .get(*ordinal as usize)
                            .ok_or_else(|| invalid("slot source ordinal is absent"))?,
                    ))
                }
                StaticExprKind::NaryAnd { .. } | StaticExprKind::NaryOr { .. } => {
                    let state = frame
                        .boolean
                        .take()
                        .ok_or_else(|| internal("missing Boolean continuation"))?;
                    let (array, errors) = state.finish(
                        invocation.control,
                        invocation.context.demand,
                        std::mem::take(&mut frame.errors),
                        work,
                    )?;
                    OwnedValue::from_selected(SelectedValues::try_new_observed(
                        local_selection,
                        &result_type.data_type,
                        array,
                        errors,
                        || work.step(),
                    )?)
                }
                StaticExprKind::PreparedCast { .. } => {
                    if frame.children.len() != 1 {
                        return Err(invalid("cast requires its exact operand"));
                    }
                    let child = std::mem::take(&mut frame.children)
                        .into_iter()
                        .next()
                        .ok_or_else(|| internal("missing cast operand"))?
                        .value
                        .into_value(local_selection, work)?;
                    let recipe = program
                        .cast_recipe(frame.occurrence)
                        .ok_or_else(|| invalid("missing exact cast recipe"))?;
                    if recipe.is_identity() {
                        // The value passes unchanged, with its row errors;
                        // only the frozen nullability widens.
                        OwnedValue::from_selected(child.materialize(
                            local_selection,
                            &recipe.result_type().data_type,
                            work,
                        )?)
                    } else {
                        OwnedValue::from_selected(evaluate_cast(
                            recipe,
                            &child,
                            local_selection,
                            work,
                        )?)
                    }
                }
                StaticExprKind::PreparedArithmetic { .. } => {
                    if frame.children.len() != 2 {
                        return Err(invalid("arithmetic requires its exact ordered operands"));
                    }
                    let mut children = std::mem::take(&mut frame.children).into_iter();
                    let left = children
                        .next()
                        .ok_or_else(|| internal("missing arithmetic left operand"))?
                        .value
                        .into_value(local_selection, work)?;
                    let right = children
                        .next()
                        .ok_or_else(|| internal("missing arithmetic right operand"))?
                        .value
                        .into_value(local_selection, work)?;
                    let recipe = program
                        .arithmetic_recipe(frame.occurrence)
                        .ok_or_else(|| invalid("missing exact arithmetic recipe"))?;
                    OwnedValue::from_selected(evaluate_arithmetic(
                        recipe,
                        &left,
                        &right,
                        local_selection,
                        work,
                    )?)
                }
                kind if kind.ordinary_comparison().is_some()
                    || matches!(kind, StaticExprKind::PreparedNullSafeComparison { .. }) =>
                {
                    if frame.children.len() != 2 {
                        return Err(invalid("comparison requires its exact ordered operands"));
                    }
                    let mut children = std::mem::take(&mut frame.children).into_iter();
                    let left = children
                        .next()
                        .ok_or_else(|| internal("missing comparison left operand"))?
                        .value
                        .into_value(local_selection, work)?;
                    let right = children
                        .next()
                        .ok_or_else(|| internal("missing comparison right operand"))?
                        .value
                        .into_value(local_selection, work)?;
                    let recipe =
                        if matches!(kind, StaticExprKind::PreparedNullSafeComparison { .. }) {
                            ComparisonRecipe::NullSafe(
                                program
                                    .null_safe_comparison_recipe(frame.occurrence)
                                    .ok_or_else(|| {
                                        invalid("missing exact null-safe comparison recipe")
                                    })?,
                            )
                        } else {
                            ComparisonRecipe::Ordinary(
                                program
                                    .comparison_recipe(
                                        novarocks_local_program::ProgramComparisonSite::Binary(
                                            frame.occurrence,
                                        ),
                                    )
                                    .ok_or_else(|| invalid("missing exact comparison recipe"))?,
                            )
                        };
                    OwnedValue::from_selected(evaluate_comparison(
                        recipe,
                        &left,
                        &right,
                        local_selection,
                        work,
                    )?)
                }
                StaticExprKind::Case { .. } => assemble(
                    &frame.children,
                    &frame.choices,
                    &mut frame.errors,
                    &result_type.data_type,
                    local_selection,
                    work,
                )?,
                StaticExprKind::Not(_)
                | StaticExprKind::IsNull(_)
                | StaticExprKind::IsNotNull(_) => {
                    if frame.children.len() != 1 {
                        return Err(invalid("unary occurrence requires its exact operand"));
                    }
                    let child = frame
                        .children
                        .pop()
                        .ok_or_else(|| internal("missing actual unary operand"))?;
                    let value = child.value.into_value(local_selection, work)?;
                    OwnedValue::from_selected(super::unary::evaluate(
                        definition.kind(),
                        &value,
                        local_selection,
                        work,
                    )?)
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
        if let Some(operand) = frame.operand.take() {
            work.flush()?;
            drop(operand);
            work.flush()?;
        }
        if let Some(parent) = frames.last_mut() {
            let parent_invocation = &flow.uses()[&parent.occurrence.use_id];
            let shape = parent_invocation.control;
            let child_is_pure = effects
                .get(&frame.occurrence)
                .ok_or_else(|| invalid("missing actual child effects"))?
                .for_use(invocation.context)
                .map_err(|_| invalid("child effects differ from actual occurrence"))?
                .permits_boolean_reordering();
            parent.attach(
                Child {
                    ordinals: frame.parent_ordinals,
                    value,
                },
                program,
                shape,
                parent_invocation.context.demand,
                child_is_pure,
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
    // The exact guarded carrier protocol is admitted before entering branch
    // state. Actual payload admission uses the complete source-choice plan.
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
    novarocks_functions::selected_copy::preflight_guarded_interleave(
        ty,
        &sources,
        &indices,
        |boundary| {
            if boundary { work.flush() } else { work.step() }
        },
    )
    .map_err(|error| match error {
        novarocks_functions::selected_copy::CopyError::Control(error) => error,
        novarocks_functions::selected_copy::CopyError::Extent => KernelFailure::ResourceExhausted,
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

/// The shared Boolean journal/assembly is independent of the numeric algorithm.
/// Null-safe comparison retains its own prepared recipe and scalar semantics.
enum ComparisonRecipe<'a> {
    Ordinary(&'a novarocks_functions::PreparedComparisonRecipe),
    NullSafe(&'a novarocks_functions::PreparedNullSafeComparisonRecipe),
}

fn evaluate_comparison<'a>(
    recipe: ComparisonRecipe<'_>,
    left: &Value<'_>,
    right: &Value<'_>,
    selection: Selection<'a>,
    work: &mut Work<'_>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    let mut left_errors = left.errors().iter().peekable();
    let mut right_errors = right.errors().iter().peekable();
    // Representation accounting precedes fallible reservations. It is not a
    // formal host memory grant or an Arrow allocation-origin guarantee.
    selection
        .len()
        .checked_mul(
            std::mem::size_of::<Option<bool>>()
                + std::mem::size_of::<RowDataError>()
                + novarocks_functions::MAX_ROW_ERROR_MESSAGE_BYTES,
        )
        .and_then(|n| {
            selection
                .len()
                .checked_add(7)
                .and_then(|bits| n.checked_add(bits / 8))
        })
        .filter(|n| *n <= isize::MAX as usize)
        .ok_or(KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut errors = Vec::new();
    let mut values = Vec::new();
    errors
        .try_reserve_exact(selection.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    values
        .try_reserve_exact(selection.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    for (ordinal, row) in selection.iter().enumerate() {
        let l = if left_errors
            .peek()
            .is_some_and(|error| error.selected_ordinal() == ordinal)
        {
            left_errors.next()
        } else {
            None
        };
        let r = if right_errors
            .peek()
            .is_some_and(|error| error.selected_ordinal() == ordinal)
        {
            right_errors.next()
        } else {
            None
        };
        if let Some(error) = l.or(r) {
            errors.push(error.clone());
            values.push(None);
        } else {
            work.flush()?;
            let compared = match recipe {
                ComparisonRecipe::Ordinary(recipe) => recipe.compare_rows(
                    left.argument(),
                    ordinal,
                    row,
                    right.argument(),
                    ordinal,
                    row,
                    work.control,
                )?,
                ComparisonRecipe::NullSafe(recipe) => Some(recipe.compare_rows(
                    left.argument(),
                    ordinal,
                    row,
                    right.argument(),
                    ordinal,
                    row,
                    work.control,
                )?),
            };
            values.push(compared);
        }
        work.step()?;
    }
    work.flush()?;
    let array = Arc::new(BooleanArray::from(values));
    work.flush()?;
    SelectedValues::try_new_observed(
        selection,
        &DataType::Boolean,
        array,
        errors.into_boxed_slice(),
        || work.step(),
    )
}

fn evaluate_arithmetic<'a>(
    recipe: &novarocks_functions::PreparedArithmeticRecipe,
    left: &Value<'_>,
    right: &Value<'_>,
    selection: Selection<'a>,
    work: &mut Work<'_>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    use arrow::array::{
        Decimal128Array, Decimal256Array, Float64Array, Int16Array, Int32Array, Int64Array,
        builder::FixedSizeBinaryBuilder,
    };
    use arrow_buffer::i256;
    use novarocks_functions::ArithmeticRowResult as R;
    let ty = &recipe.result_type().data_type;
    // This is a checked representation bound and fallible capacity reservation,
    // not a host Account grant or an Arrow allocation-origin receipt.
    let bitmap = selection
        .len()
        .checked_add(7)
        .map(|n| n / 8)
        .and_then(|n| n.checked_add(63))
        .map(|n| n / 64 * 64)
        .ok_or(KernelFailure::ResourceExhausted)?;
    selection
        .len()
        .checked_mul(
            std::mem::size_of::<Option<i256>>()
                + 32
                + std::mem::size_of::<RowDataError>()
                + novarocks_functions::MAX_ROW_ERROR_MESSAGE_BYTES,
        )
        .and_then(|n| n.checked_add(bitmap))
        .filter(|bytes| *bytes <= isize::MAX as usize)
        .ok_or(KernelFailure::ResourceExhausted)?;
    work.flush()?;
    enum Output {
        I16(Vec<Option<i16>>),
        I32(Vec<Option<i32>>),
        I64(Vec<Option<i64>>),
        Float(Vec<Option<f64>>),
        Decimal128(Vec<Option<i128>>),
        Decimal256(Vec<Option<i256>>),
        LargeInt(FixedSizeBinaryBuilder),
    }
    let mut output = match ty {
        DataType::Int16 => Output::I16(Vec::new()),
        DataType::Int32 => Output::I32(Vec::new()),
        DataType::Int64 => Output::I64(Vec::new()),
        DataType::Float64 => Output::Float(Vec::new()),
        DataType::Decimal128(..) => Output::Decimal128(Vec::new()),
        DataType::Decimal256(..) => Output::Decimal256(Vec::new()),
        DataType::FixedSizeBinary(16)
            if recipe.result_type().logical_type
                == novarocks_type_contract::ValueLogicalType::LargeInt =>
        {
            Output::LargeInt(FixedSizeBinaryBuilder::with_capacity(selection.len(), 16))
        }
        _ => {
            return Err(internal(
                "arithmetic recipe has an unsupported frozen result carrier",
            ));
        }
    };
    match &mut output {
        Output::I16(values) => values.try_reserve_exact(selection.len()),
        Output::I32(values) => values.try_reserve_exact(selection.len()),
        Output::I64(values) => values.try_reserve_exact(selection.len()),
        Output::Float(values) => values.try_reserve_exact(selection.len()),
        Output::Decimal128(values) => values.try_reserve_exact(selection.len()),
        Output::Decimal256(values) => values.try_reserve_exact(selection.len()),
        Output::LargeInt(_) => Ok(()),
    }
    .map_err(|_| KernelFailure::ResourceExhausted)?;
    let mut errors = Vec::new();
    errors
        .try_reserve_exact(selection.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut left_errors = left.errors().iter().peekable();
    let mut right_errors = right.errors().iter().peekable();
    for (ordinal, row) in selection.iter().enumerate() {
        let l = if left_errors
            .peek()
            .is_some_and(|e| e.selected_ordinal() == ordinal)
        {
            left_errors.next()
        } else {
            None
        };
        let r = if right_errors
            .peek()
            .is_some_and(|e| e.selected_ordinal() == ordinal)
        {
            right_errors.next()
        } else {
            None
        };
        let result = if let Some(error) = l.or(r) {
            R::RowError(error.clone())
        } else {
            work.flush()?;
            recipe.evaluate_row(
                left.argument(),
                ordinal,
                row,
                right.argument(),
                ordinal,
                row,
                work.control,
            )?
        };
        let result = match result {
            R::RowError(error) => {
                errors.push(RowDataError::new(ordinal, error.message()));
                R::Null
            }
            result => result,
        };
        match (&mut output, result) {
            (Output::I16(values), R::Signed(value)) => values.push(i16::try_from(value).ok()),
            (Output::I32(values), R::Signed(value)) => values.push(i32::try_from(value).ok()),
            (Output::I64(values), R::Signed(value)) => values.push(Some(value)),
            (Output::Float(values), R::Float(value)) => values.push(Some(value)),
            (Output::Decimal128(values), R::Decimal128(value)) => values.push(Some(value)),
            (Output::Decimal256(values), R::Decimal256(value)) => values.push(Some(value)),
            (Output::LargeInt(values), R::LargeInt(value)) => values
                .append_value(value.to_be_bytes())
                .map_err(|_| internal("LargeInt result differs from its fixed carrier width"))?,
            (Output::I16(values), R::Null) => values.push(None),
            (Output::I32(values), R::Null) => values.push(None),
            (Output::I64(values), R::Null) => values.push(None),
            (Output::Float(values), R::Null) => values.push(None),
            (Output::Decimal128(values), R::Null) => values.push(None),
            (Output::Decimal256(values), R::Null) => values.push(None),
            (Output::LargeInt(values), R::Null) => values.append_null(),
            _ => {
                return Err(internal(
                    "arithmetic body returned a foreign frozen result carrier",
                ));
            }
        }
        work.step()?;
    }
    work.flush()?;
    let array: ArrayRef = match (ty, output) {
        (DataType::Int16, Output::I16(values)) => Arc::new(Int16Array::from(values)),
        (DataType::Int32, Output::I32(values)) => Arc::new(Int32Array::from(values)),
        (DataType::Int64, Output::I64(values)) => Arc::new(Int64Array::from(values)),
        (DataType::Float64, Output::Float(values)) => Arc::new(Float64Array::from(values)),
        (DataType::Decimal128(precision, scale), Output::Decimal128(values)) => Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|_| {
                    internal("Decimal128 result metadata differs from its prepared type")
                })?,
        ),
        (DataType::Decimal256(precision, scale), Output::Decimal256(values)) => Arc::new(
            Decimal256Array::from(values)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|_| {
                    internal("Decimal256 result metadata differs from its prepared type")
                })?,
        ),
        (DataType::FixedSizeBinary(16), Output::LargeInt(mut values)) => Arc::new(values.finish()),
        _ => {
            return Err(internal(
                "arithmetic recipe has an unsupported frozen result carrier",
            ));
        }
    };
    work.flush()?;
    SelectedValues::try_new_observed(selection, ty, array, errors.into_boxed_slice(), || {
        work.step()
    })
}

fn evaluate_cast<'a>(
    recipe: &novarocks_functions::PreparedCastRecipe,
    child: &Value<'_>,
    selection: Selection<'a>,
    work: &mut Work<'_>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    use arrow::array::{Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array};
    use novarocks_functions::CastRowResult as R;
    let ty = &recipe.result_type().data_type;
    // Checked representation and fallible reservation are not a host memory grant.
    let bitmap = selection
        .len()
        .checked_add(7)
        .map(|n| n / 8)
        .and_then(|n| n.checked_add(63))
        .map(|n| n / 64 * 64)
        .ok_or(KernelFailure::ResourceExhausted)?;
    selection
        .len()
        .checked_mul(
            std::mem::size_of::<Option<i64>>()
                + std::mem::size_of::<RowDataError>()
                + novarocks_functions::MAX_ROW_ERROR_MESSAGE_BYTES,
        )
        .and_then(|n| n.checked_add(bitmap))
        .filter(|n| *n <= isize::MAX as usize)
        .ok_or(KernelFailure::ResourceExhausted)?;
    work.flush()?;
    enum Output {
        Boolean(Vec<Option<bool>>),
        I8(Vec<Option<i8>>),
        I16(Vec<Option<i16>>),
        I32(Vec<Option<i32>>),
        I64(Vec<Option<i64>>),
        Timestamp(Vec<Option<i64>>),
        U8(Vec<Option<u8>>),
        U16(Vec<Option<u16>>),
        U32(Vec<Option<u32>>),
        U64(Vec<Option<u64>>),
        F32(Vec<Option<f32>>),
        F64(Vec<Option<f64>>),
    }
    let mut output = match ty {
        DataType::Boolean => Output::Boolean(Vec::new()),
        DataType::Int8 => Output::I8(Vec::new()),
        DataType::Int16 => Output::I16(Vec::new()),
        DataType::Int32 => Output::I32(Vec::new()),
        DataType::Int64 => Output::I64(Vec::new()),
        DataType::Timestamp(_, None) => Output::Timestamp(Vec::new()),
        DataType::UInt8 => Output::U8(Vec::new()),
        DataType::UInt16 => Output::U16(Vec::new()),
        DataType::UInt32 => Output::U32(Vec::new()),
        DataType::UInt64 => Output::U64(Vec::new()),
        DataType::Float32 => Output::F32(Vec::new()),
        DataType::Float64 => Output::F64(Vec::new()),
        _ => return Err(internal("cast recipe has a foreign frozen result carrier")),
    };
    match &mut output {
        Output::Boolean(v) => v.try_reserve_exact(selection.len()),
        Output::I8(v) => v.try_reserve_exact(selection.len()),
        Output::I16(v) => v.try_reserve_exact(selection.len()),
        Output::I32(v) => v.try_reserve_exact(selection.len()),
        Output::I64(v) => v.try_reserve_exact(selection.len()),
        Output::Timestamp(v) => v.try_reserve_exact(selection.len()),
        Output::U8(v) => v.try_reserve_exact(selection.len()),
        Output::U16(v) => v.try_reserve_exact(selection.len()),
        Output::U32(v) => v.try_reserve_exact(selection.len()),
        Output::U64(v) => v.try_reserve_exact(selection.len()),
        Output::F32(v) => v.try_reserve_exact(selection.len()),
        Output::F64(v) => v.try_reserve_exact(selection.len()),
    }
    .map_err(|_| KernelFailure::ResourceExhausted)?;
    let mut errors = Vec::new();
    errors
        .try_reserve_exact(selection.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut inherited = child.errors().iter().peekable();
    for (ordinal, row) in selection.iter().enumerate() {
        let value = if inherited
            .peek()
            .is_some_and(|e| e.selected_ordinal() == ordinal)
        {
            R::RowError(inherited.next().expect("checked inherited error").clone())
        } else {
            work.flush()?;
            recipe.evaluate_row(child.argument(), ordinal, row, work.control)?
        };
        let value = match value {
            R::RowError(error) => {
                errors.push(RowDataError::new(ordinal, error.message()));
                R::Null
            }
            other => other,
        };
        match (&mut output, value) {
            (Output::Boolean(v), R::Boolean(n)) => v.push(Some(n)),
            (Output::I8(v), R::Signed(n)) => v.push(Some(
                i8::try_from(n).map_err(|_| internal("cast returned an out-of-range Int8"))?,
            )),
            (Output::I16(v), R::Signed(n)) => {
                v.push(Some(i16::try_from(n).map_err(|_| {
                    internal("cast returned an out-of-range Int16")
                })?))
            }
            (Output::I32(v), R::Signed(n)) => {
                v.push(Some(i32::try_from(n).map_err(|_| {
                    internal("cast returned an out-of-range Int32")
                })?))
            }
            (Output::I64(v), R::Signed(n)) => v.push(Some(n)),
            (Output::Timestamp(v), R::Timestamp(n)) => v.push(Some(n)),
            (Output::U8(v), R::Unsigned(n)) => {
                v.push(Some(u8::try_from(n).map_err(|_| {
                    internal("cast returned an out-of-range UInt8")
                })?))
            }
            (Output::U16(v), R::Unsigned(n)) => {
                v.push(Some(u16::try_from(n).map_err(|_| {
                    internal("cast returned an out-of-range UInt16")
                })?))
            }
            (Output::U32(v), R::Unsigned(n)) => {
                v.push(Some(u32::try_from(n).map_err(|_| {
                    internal("cast returned an out-of-range UInt32")
                })?))
            }
            (Output::U64(v), R::Unsigned(n)) => v.push(Some(n)),
            (Output::F32(v), R::Float32(n)) => v.push(Some(n)),
            (Output::F64(v), R::Float64(n)) => v.push(Some(n)),
            (Output::I8(v), R::Null) => v.push(None),
            (Output::Boolean(v), R::Null) => v.push(None),
            (Output::I16(v), R::Null) => v.push(None),
            (Output::I32(v), R::Null) => v.push(None),
            (Output::I64(v), R::Null) => v.push(None),
            (Output::Timestamp(v), R::Null) => v.push(None),
            (Output::U8(v), R::Null) => v.push(None),
            (Output::U16(v), R::Null) => v.push(None),
            (Output::U32(v), R::Null) => v.push(None),
            (Output::U64(v), R::Null) => v.push(None),
            (Output::F32(v), R::Null) => v.push(None),
            (Output::F64(v), R::Null) => v.push(None),
            _ => {
                return Err(internal(
                    "cast body returned a foreign result representation",
                ));
            }
        }
        work.step()?;
    }
    work.flush()?;
    // Arrow construction is an opaque observed boundary, not internally cooperative allocation.
    let array: ArrayRef = match output {
        Output::Boolean(v) => Arc::new(BooleanArray::from(v)),
        Output::I8(v) => Arc::new(Int8Array::from(v)),
        Output::I16(v) => Arc::new(Int16Array::from(v)),
        Output::I32(v) => Arc::new(Int32Array::from(v)),
        Output::I64(v) => Arc::new(Int64Array::from(v)),
        Output::Timestamp(v) => match ty {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None) => {
                Arc::new(arrow::array::TimestampSecondArray::from(v))
            }
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None) => {
                Arc::new(arrow::array::TimestampMillisecondArray::from(v))
            }
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
                Arc::new(arrow::array::TimestampMicrosecondArray::from(v))
            }
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None) => {
                Arc::new(arrow::array::TimestampNanosecondArray::from(v))
            }
            _ => {
                return Err(internal(
                    "timestamp cast has a foreign frozen result carrier",
                ));
            }
        },
        Output::U8(v) => Arc::new(arrow::array::UInt8Array::from(v)),
        Output::U16(v) => Arc::new(arrow::array::UInt16Array::from(v)),
        Output::U32(v) => Arc::new(arrow::array::UInt32Array::from(v)),
        Output::U64(v) => Arc::new(arrow::array::UInt64Array::from(v)),
        Output::F32(v) => Arc::new(Float32Array::from(v)),
        Output::F64(v) => Arc::new(Float64Array::from(v)),
    };
    work.flush()?;
    SelectedValues::try_new_observed(selection, ty, array, errors.into_boxed_slice(), || {
        work.step()
    })
}
