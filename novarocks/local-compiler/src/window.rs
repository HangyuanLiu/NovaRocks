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

//! Lower one admitted physical Window into the local Analytic owner and
//! prepare each of its calls from the exact frozen window facts.
//!
//! Every call becomes a `Prepared` local call whose kernel is the one this
//! compiler attaches at `ProgramCallSite::Window`. It is prepared through the
//! installed pure window owner (or the aggregate owner's window adapter for an
//! aggregate OVER) from the frozen binding, the original request, the frozen
//! call effects and the actual effects of its argument roots. No
//! implementation is selected by name: the display name copied into an
//! aggregate binding is a diagnostic tag only.
//!
//! Roots: the physical `WindowCall` root and each of its frame-offset uses are
//! retired from the local flow. A relational call context is never a flow
//! occurrence, and an offset is a frozen constant of the call's frame rather
//! than an evaluated input. Each argument use, then each function ORDER use,
//! becomes an independent `WindowInput` root of the Analytic node.
//!
//! Frames belong to each call. A call without a frame receives the SQL
//! default, derived only by `effective_frame` and frozen explicitly into the
//! program; the prepared options keep the frozen absence of a frame.

use crate::{
    assert_rows::reserve_vec, expressions::ExpressionLoweringError, lowering::FragmentCompileError,
};
use arrow_schema::{Field, Schema};
use novarocks_functions::{
    AggregateBindingSelection, AggregateKernelPhase, AggregateOverloadIdentity,
    AggregatePreparationOptions, AggregateWindowPreparationOptions, CallEffectInput,
    FunctionBindingSelection, FunctionResultType, FunctionSpecializationFailure,
    PureCallPreparation, PureCallSpecialization, PureEngineFunctionCatalog, PureKernelAbi,
    ResolvedAggregateSignature, ScopedExpressionEffects, WindowCallOptions,
};
use novarocks_local_program::{
    AnalyticOutputColumn, ProgramCallSite, ProgramExprId, ProgramExpressionRootSite,
    ProgramNodeExpressionRole, ProgramNodeId, ProgramNodeKind, ProgramRootUseBinding, StaticLayout,
    StaticWindowFunction, WindowBoundary, WindowFrame as ProgramWindowFrame, WindowFunctionKind,
    WindowType,
};
use novarocks_physical_plan::{
    AggregatePhase, ExprId, ExprKind, ExpressionRootRole, FragmentPackage, FunctionArgumentType,
    NodeId, NodeKind, PhysicalCallDefinition, PhysicalCallSite, PhysicalNode,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, EvaluationDemand, ExpressionEffects,
    ExpressionUseId, FunctionKind, PureCompileControl, WindowBound, WindowFrame,
    WindowFrameExclusion, WindowFrameUnits,
};
use novarocks_types::SlotId;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// Admit one physical Window occurrence before any lowering work. Each call's
/// installed kernel is found through the pure catalog by its frozen function
/// identity and overload, never by a SQL name; a call shape no installed
/// kernel prepares is refused here by its feature.
pub(crate) fn admit_window(
    package: &FragmentPackage,
    node: &PhysicalNode,
    functions: &PureEngineFunctionCatalog,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCompileError> {
    let NodeKind::Window(spec) = &node.kind else {
        return Err(FragmentCompileError::Invalid(
            "window admission for another node family",
        ));
    };
    if node.inputs.len() != 1 || spec.expressions.is_empty() {
        return Err(FragmentCompileError::Invalid(
            "window input or call list differs",
        ));
    }
    let unsupported = |feature| FragmentCompileError::Unsupported {
        node: Some(node.id),
        feature,
    };
    for item in spec.expressions.iter() {
        let definition = package
            .fragment()
            .expressions()
            .get(item.expression)
            .ok_or(FragmentCompileError::Invalid(
                "missing window call definition",
            ))?;
        let ExprKind::WindowCall {
            function,
            distinct,
            function_order_by,
            frame,
            aggregate_binding,
            ..
        } = &definition.kind
        else {
            return Err(FragmentCompileError::Invalid(
                "window expression is not a window call",
            ));
        };
        if *distinct {
            return Err(unsupported("DISTINCT window call"));
        }
        if !function_order_by.is_empty() {
            return Err(unsupported("window function ORDER BY"));
        }
        if let Some(frame) = frame {
            if frame.units == WindowFrameUnits::Groups {
                return Err(unsupported("GROUPS window frame"));
            }
            if frame.exclusion != WindowFrameExclusion::NoOthers {
                return Err(unsupported("window frame exclusion other than NO OTHERS"));
            }
            if frame.units == WindowFrameUnits::Range && has_offset(frame) {
                return Err(unsupported("RANGE window frame offset"));
            }
        }
        let (abi, missing) = match (function.kind, aggregate_binding.as_deref()) {
            (FunctionKind::Window, None) => (
                PureKernelAbi::WindowV1,
                "window function without an installed pure window kernel",
            ),
            (FunctionKind::Aggregate, Some(binding)) if binding.phase == AggregatePhase::Single => {
                (
                    PureKernelAbi::AggregateWindowV1,
                    "aggregate OVER without an installed pure window kernel",
                )
            }
            _ => {
                return Err(FragmentCompileError::Invalid(
                    "window call binds no exact window or single-phase aggregate",
                ));
            }
        };
        for argument in function.argument_types.iter() {
            let lambda = matches!(argument, FunctionArgumentType::Lambda { .. });
            work.step()?;
            if lambda {
                return Err(unsupported("window call with a lambda argument"));
            }
        }
        work.flush()?;
        let declaration = functions.metadata().pure_overload_declaration_observed(
            &function.function_id,
            function.kind,
            &function.overload,
            work.control(),
        );
        work.flush()?;
        let installed = match declaration {
            Ok(declaration) => Some(declaration.implementation().abi),
            Err(FunctionSpecializationFailure::Control(cause)) => return Err(cause.into()),
            Err(_) => None,
        };
        if installed != Some(abi) {
            return Err(unsupported(missing));
        }
        work.step()?;
    }
    Ok(())
}

fn has_offset<O>(frame: &WindowFrame<O>) -> bool {
    [&frame.start, &frame.end]
        .iter()
        .any(|bound| matches!(bound, WindowBound::Preceding(_) | WindowBound::Following(_)))
}

/// One call's frozen frame with its checked constant offsets, in the prepared
/// options' vocabulary. `None` keeps the frozen absence of a frame.
fn frozen_frame(
    package: &FragmentPackage,
    frame: Option<&novarocks_physical_plan::WindowFrame>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<WindowFrame<u64>>, ExpressionLoweringError> {
    let Some(frame) = frame else {
        return Ok(None);
    };
    let mut bound =
        |bound: &WindowBound<ExprId>| -> Result<WindowBound<u64>, ExpressionLoweringError> {
            let mut offset = |id: &ExprId| {
                novarocks_physical_plan::window_offset_observed(
                    package.fragment(),
                    package.constants(),
                    *id,
                    work,
                )?
                .ok_or(ExpressionLoweringError::Invalid(
                    "window frame offset is not a checked constant",
                ))
            };
            Ok(match bound {
                WindowBound::UnboundedPreceding => WindowBound::UnboundedPreceding,
                WindowBound::Preceding(id) => WindowBound::Preceding(offset(id)?),
                WindowBound::CurrentRow => WindowBound::CurrentRow,
                WindowBound::Following(id) => WindowBound::Following(offset(id)?),
                WindowBound::UnboundedFollowing => WindowBound::UnboundedFollowing,
            })
        };
    Ok(Some(WindowFrame {
        units: frame.units,
        start: bound(&frame.start)?,
        end: bound(&frame.end)?,
        exclusion: frame.exclusion,
    }))
}

/// The one host derivation of a call's effective frame. An explicit frame is
/// carried exactly. Without one, SQL's default frame is RANGE BETWEEN
/// UNBOUNDED PRECEDING AND CURRENT ROW, whose CURRENT ROW extends over the
/// row's ORDER BY peers; without ORDER BY every row of a partition is its
/// peer, so the frame is the whole partition.
pub(crate) fn effective_frame(
    frozen: Option<&WindowFrame<u64>>,
    ordered: bool,
) -> Result<ProgramWindowFrame, FragmentCompileError> {
    let Some(frame) = frozen else {
        return Ok(if ordered {
            ProgramWindowFrame {
                start: None,
                end: Some(WindowBoundary::CurrentRow),
                window_type: WindowType::Range,
            }
        } else {
            ProgramWindowFrame {
                start: None,
                end: None,
                window_type: WindowType::Rows,
            }
        });
    };
    let window_type = match frame.units {
        WindowFrameUnits::Rows => WindowType::Rows,
        WindowFrameUnits::Range => WindowType::Range,
        WindowFrameUnits::Groups => {
            return Err(FragmentCompileError::Unsupported {
                node: None,
                feature: "GROUPS window frame",
            });
        }
    };
    let offset = |offset: u64| {
        i64::try_from(offset).map_err(|_| FragmentCompileError::Unsupported {
            node: None,
            feature: "window frame offset beyond the signed 64-bit range",
        })
    };
    let bounded = |bound: WindowBound<u64>| -> Result<WindowBoundary, FragmentCompileError> {
        Ok(match bound {
            WindowBound::CurrentRow => WindowBoundary::CurrentRow,
            WindowBound::Preceding(value) => WindowBoundary::Preceding(offset(value)?),
            WindowBound::Following(value) => WindowBoundary::Following(offset(value)?),
            WindowBound::UnboundedPreceding | WindowBound::UnboundedFollowing => {
                return Err(FragmentCompileError::Invalid(
                    "window frame bound is unbounded on its wrong side",
                ));
            }
        })
    };
    Ok(ProgramWindowFrame {
        start: match frame.start {
            WindowBound::UnboundedPreceding => None,
            bound => Some(bounded(bound)?),
        },
        end: match frame.end {
            WindowBound::UnboundedFollowing => None,
            bound => Some(bounded(bound)?),
        },
        window_type,
    })
}

/// Lower one admitted Window whose output is its whole input followed by one
/// value per call, in call order. Pass-through columns keep their input
/// channels; `slots` is the planned output, input slots first.
pub(crate) fn lower_window(
    package: &FragmentPackage,
    node: &PhysicalNode,
    input: ProgramNodeId,
    input_layout: &StaticLayout,
    expressions: &BTreeMap<ExprId, ProgramExprId>,
    slots: &[SlotId],
    control: &dyn PureCompileControl,
) -> Result<(ProgramNodeKind, StaticLayout), FragmentCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core(
        package,
        node,
        input,
        input_layout,
        expressions,
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
    input: ProgramNodeId,
    input_layout: &StaticLayout,
    expressions: &BTreeMap<ExprId, ProgramExprId>,
    slots: &[SlotId],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(ProgramNodeKind, StaticLayout), FragmentCompileError> {
    let NodeKind::Window(spec) = &node.kind else {
        return Err(FragmentCompileError::Invalid("Window kind differs"));
    };
    let width = input_layout.slots().len();
    let total = width
        .checked_add(spec.expressions.len())
        .ok_or(CompileControlError::ResourceExhausted)?;
    if node.inputs.len() != 1 || node.output.columns.len() != total || slots.len() != total {
        return Err(FragmentCompileError::Invalid(
            "window input, output or channel width differs",
        ));
    }
    let lowered = |id: &ExprId, missing: &'static str| {
        expressions
            .get(id)
            .copied()
            .ok_or(FragmentCompileError::Invalid(missing))
    };
    let mut partition_exprs = Vec::new();
    reserve_vec(&mut partition_exprs, spec.partition_by.len(), work)?;
    for key in spec.partition_by.iter() {
        partition_exprs.push(lowered(&key.expr, "missing lowered window partition key")?);
        work.step()?;
    }
    let mut order_by_exprs = Vec::new();
    reserve_vec(&mut order_by_exprs, spec.order_by.len(), work)?;
    for key in spec.order_by.iter() {
        order_by_exprs.push(lowered(&key.expr, "missing lowered window order key")?);
        work.step()?;
    }
    let ordered = !spec.order_by.is_empty();
    let values = package.fragment().values();
    let mut functions = Vec::new();
    reserve_vec(&mut functions, spec.expressions.len(), work)?;
    for (ordinal, item) in spec.expressions.iter().enumerate() {
        let definition = package
            .fragment()
            .expressions()
            .get(item.expression)
            .ok_or(FragmentCompileError::Invalid(
                "missing window call definition",
            ))?;
        let ExprKind::WindowCall {
            args,
            frame,
            ignore_nulls,
            aggregate_binding,
            ..
        } = &definition.kind
        else {
            return Err(FragmentCompileError::Invalid(
                "window expression is not a window call",
            ));
        };
        if node.output.columns.get(width + ordinal) != Some(&item.output) {
            return Err(FragmentCompileError::Invalid(
                "window output is not its input followed by its call outputs",
            ));
        }
        let output = values
            .get(&item.output)
            .ok_or(FragmentCompileError::Invalid("missing window output value"))?;
        work.flush()?;
        let same = definition
            .ty
            .exactly_equals_observed::<FragmentCompileError>(&output.ty, || {
                work.step().map_err(Into::into)
            })?;
        if !same {
            return Err(FragmentCompileError::Invalid(
                "window output type differs from its call",
            ));
        }
        let mut inputs = Vec::new();
        reserve_vec(&mut inputs, args.len(), work)?;
        for argument in args.iter() {
            inputs.push(lowered(argument, "missing lowered window argument")?);
            work.step()?;
        }
        let frozen = frozen_frame(package, frame.as_ref(), work)?;
        let aggregate_binding = match aggregate_binding.as_deref() {
            Some(binding) => Some(aggregate_signature(binding, work)?),
            None => None,
        };
        work.flush()?;
        functions.push(StaticWindowFunction {
            kind: WindowFunctionKind::Prepared,
            args: inputs,
            return_type: definition.ty.data_type.clone(),
            aggregate_binding,
            frame: Some(effective_frame(frozen.as_ref(), ordered)?),
            ignore_nulls: *ignore_nulls,
        });
        work.step()?;
    }
    let mut output_columns = Vec::new();
    reserve_vec(&mut output_columns, total, work)?;
    for &slot in input_layout.slots() {
        output_columns.push(AnalyticOutputColumn::InputSlotId(slot));
        work.step()?;
    }
    for call in 0..spec.expressions.len() {
        output_columns.push(AnalyticOutputColumn::Window(call));
        work.step()?;
    }
    let result = package
        .result()
        .filter(|result| result.output.columns == node.output.columns);
    let mut fields: Vec<Field> = Vec::new();
    if package.original_metadata_namespace().is_none() {
        reserve_vec(&mut fields, total, work)?;
    }
    let mut original_fields = match package.original_metadata_namespace() {
        Some(_) => {
            let mut original_fields = Vec::new();
            reserve_vec(&mut original_fields, total, work)?;
            Some(original_fields)
        }
        None => None,
    };
    for (ordinal, value) in node.output.columns.iter().enumerate() {
        let ty = &values
            .get(value)
            .ok_or(FragmentCompileError::Invalid("missing window output type"))?
            .ty;
        // Full result labels are authoritative only when this window's entire
        // ordered output is the result port's.
        let name = match result {
            Some(result) => {
                let field = result
                    .fields
                    .get(ordinal)
                    .ok_or(FragmentCompileError::Invalid("missing window result label"))?;
                field.alias.as_deref().unwrap_or(&field.name).to_owned()
            }
            None => format!("local_window_{}_{}", node.id.get(), ordinal),
        };
        work.flush()?;
        if let Some(original_fields) = original_fields.as_mut() {
            let field = novarocks_type_contract::owned_resources::metadata_materialization::materialize_value_field(ty, name);
            work.flush()?;
            original_fields.push(field?);
        } else {
            let field = ty.try_to_field(name);
            work.flush()?;
            fields.push(field?);
        }
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
    Ok((
        ProgramNodeKind::Analytic {
            input,
            partition_exprs,
            order_by_exprs,
            functions,
            output_columns,
        },
        layout,
    ))
}

/// The mandatory legacy carrier of an aggregate OVER binding. Its display
/// name is a diagnostic tag; the frozen preparation owns the call.
fn aggregate_signature(
    binding: &novarocks_physical_plan::AggregateBinding,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(Arc<str>, ResolvedAggregateSignature), FragmentCompileError> {
    let function = &binding.function;
    let mut argument_types = Vec::new();
    reserve_vec(&mut argument_types, function.argument_types.len(), work)?;
    for argument in function.argument_types.iter() {
        let FunctionArgumentType::Value(value) = argument else {
            return Err(FragmentCompileError::Invalid(
                "aggregate window channel is not a value",
            ));
        };
        work.flush()?;
        argument_types.push(value.data_type.clone());
        work.step()?;
    }
    let overload =
        AggregateOverloadIdentity::try_new(function.overload.as_str()).map_err(|error| {
            FragmentCompileError::Owner {
                phase: "aggregate window overload identity",
                error: Box::new(error),
            }
        })?;
    work.flush()?;
    let name: Arc<str> = Arc::from(function.function_id.as_str());
    work.flush()?;
    Ok((
        name,
        ResolvedAggregateSignature {
            overload,
            argument_types,
            intermediate_type: binding.intermediate_type.data_type.clone(),
            output_type: function.result_type.data_type.clone(),
            state_format: binding.state_format.clone(),
        },
    ))
}

/// The occurrences a compiled Window retires and the argument roots it owns.
pub(crate) struct WindowRoots {
    /// Every `WindowCall` root use and the whole subtree of each frame offset.
    pub retired: BTreeSet<ExpressionUseId>,
    /// One `WindowInput` root per call argument, then per function ORDER key.
    pub inputs: Vec<ProgramRootUseBinding>,
}

/// Retire each window call occurrence and re-root its input uses on the local
/// Analytic node `local_ids` names for its Window.
pub(crate) fn window_roots(
    package: &FragmentPackage,
    local_ids: &BTreeMap<NodeId, ProgramNodeId>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<WindowRoots, FragmentCompileError> {
    let fragment = package.fragment();
    let flow = package.expression_uses().flow();
    let mut retired = BTreeSet::new();
    let mut inputs = Vec::new();
    for (site, &use_id) in package.expression_uses().bindings() {
        work.step()?;
        let ExpressionRootRole::WindowCall { call } = site.role else {
            continue;
        };
        let item = match &fragment.nodes().get(&site.node).map(|node| &node.kind) {
            Some(NodeKind::Window(spec)) => spec.expressions.get(call as usize),
            _ => None,
        }
        .ok_or(FragmentCompileError::Invalid(
            "window call root outside its Window call list",
        ))?;
        let invocation = flow
            .uses()
            .get(&use_id)
            .ok_or(FragmentCompileError::Invalid(
                "window call root has no invocation",
            ))?;
        let Some(ExprKind::WindowCall {
            args,
            function_order_by,
            frame,
            ..
        }) = fragment
            .expressions()
            .get(invocation.definition)
            .map(|definition| &definition.kind)
        else {
            return Err(FragmentCompileError::Invalid(
                "window call root is not a window call",
            ));
        };
        let channels = args
            .len()
            .checked_add(function_order_by.len())
            .ok_or(CompileControlError::ResourceExhausted)?;
        let offsets = frame.as_ref().map_or(0, |frame| {
            [&frame.start, &frame.end]
                .iter()
                .filter(|bound| {
                    matches!(bound, WindowBound::Preceding(_) | WindowBound::Following(_))
                })
                .count()
        });
        if invocation.definition != item.expression
            || channels.checked_add(offsets) != Some(invocation.arguments.len())
        {
            return Err(FragmentCompileError::Invalid(
                "window call root differs from its call channels and frame offsets",
            ));
        }
        retired.insert(use_id);
        // An offset is a frozen constant; its whole occurrence subtree leaves.
        let mut stack = invocation.arguments[channels..].to_vec();
        while let Some(offset) = stack.pop() {
            work.step()?;
            if !retired.insert(offset) {
                return Err(FragmentCompileError::Invalid(
                    "window frame offset occurrence is shared",
                ));
            }
            let offset = flow
                .uses()
                .get(&offset)
                .ok_or(FragmentCompileError::Invalid(
                    "missing window frame offset use",
                ))?;
            stack.extend(offset.arguments.iter().copied());
        }
        let local = *local_ids
            .get(&site.node)
            .ok_or(FragmentCompileError::Invalid("missing lowered Window node"))?;
        reserve_vec(&mut inputs, channels, work)?;
        for (argument, &child) in invocation.arguments[..channels].iter().enumerate() {
            inputs.push(ProgramRootUseBinding {
                site: ProgramExpressionRootSite::Node {
                    node: local,
                    role: ProgramNodeExpressionRole::WindowInput {
                        call,
                        argument: u32::try_from(argument)
                            .map_err(|_| CompileControlError::ResourceExhausted)?,
                    },
                },
                use_id: child,
            });
            work.step()?;
        }
    }
    Ok(WindowRoots { retired, inputs })
}

/// Prepare every frozen window call of the fragment through its exact
/// installed owner. `effects` holds the prepared effects of every expression
/// occurrence, so each argument root contributes its actual effects; `nodes`
/// names the local node each physical Window lowers to.
pub(crate) fn prepare_window_calls(
    package: &FragmentPackage,
    functions: &PureEngineFunctionCatalog,
    effects: &BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
    nodes: &BTreeMap<NodeId, ProgramNodeId>,
    tokens: &mut BTreeMap<ProgramCallSite, PureCallSpecialization>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ExpressionLoweringError> {
    let roots = package.expression_uses();
    let mut prepared = 0usize;
    for (site, &use_id) in roots.bindings() {
        work.step()?;
        let ExpressionRootRole::WindowCall { call } = site.role else {
            continue;
        };
        let local = *nodes
            .get(&site.node)
            .ok_or(ExpressionLoweringError::Invalid(
                "window node has no local node",
            ))?;
        let token = prepare_call(package, functions, effects, use_id, work)?;
        if tokens
            .insert(ProgramCallSite::Window { node: local, call }, token)
            .is_some()
        {
            return Err(ExpressionLoweringError::Invalid(
                "window call site is prepared twice",
            ));
        }
        prepared += 1;
        work.step()?;
    }
    // Every frozen window claim names one actual window call root.
    let mut frozen = 0usize;
    for site in package.calls().entries().keys() {
        if let PhysicalCallSite::Expression(id) = site {
            let window = roots.flow().uses().get(id).is_some_and(|invocation| {
                package
                    .fragment()
                    .expressions()
                    .get(invocation.definition)
                    .is_some_and(|definition| {
                        matches!(definition.kind, ExprKind::WindowCall { .. })
                    })
            });
            if window {
                frozen += 1;
            }
        }
        work.step()?;
    }
    if frozen != prepared {
        return Err(ExpressionLoweringError::Invalid(
            "frozen window calls differ from the actual window call roots",
        ));
    }
    Ok(())
}

fn prepare_call(
    package: &FragmentPackage,
    functions: &PureEngineFunctionCatalog,
    effects: &BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
    use_id: ExpressionUseId,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PureCallSpecialization, ExpressionLoweringError> {
    let flow = package.expression_uses().flow();
    let invocation = flow
        .uses()
        .get(&use_id)
        .ok_or(ExpressionLoweringError::Invalid(
            "window call root has no invocation",
        ))?;
    let definition = package
        .fragment()
        .expressions()
        .get(invocation.definition)
        .ok_or(ExpressionLoweringError::Invalid(
            "missing window call definition",
        ))?;
    let ExprKind::WindowCall {
        function,
        distinct,
        args,
        function_order_by,
        frame,
        ignore_nulls,
        aggregate_binding,
    } = &definition.kind
    else {
        return Err(ExpressionLoweringError::Invalid(
            "window call root is not a window call",
        ));
    };
    let site = PhysicalCallSite::Expression(use_id);
    if !function_order_by.is_empty() {
        return Err(ExpressionLoweringError::UnsupportedCall(site));
    }
    let frozen = package
        .calls()
        .entries()
        .get(&site)
        .ok_or(ExpressionLoweringError::Invalid(
            "missing frozen window call",
        ))?;
    if frozen.context != invocation.context {
        return Err(ExpressionLoweringError::Invalid(
            "frozen window call context differs from its occurrence",
        ));
    }
    let source_request = package
        .fragment()
        .call_requests()
        .get(PhysicalCallDefinition::Expression(definition.id));
    work.step()?;
    let source_request = source_request.ok_or(ExpressionLoweringError::Invalid(
        "missing original window call request",
    ))?;
    let request = crate::original_requests::materialize_call_request_observed(
        source_request,
        package.constants(),
        work,
    )?;
    work.flush()?;
    // The selection is the frozen binding itself, never a re-resolution.
    let selection = Arc::new(FunctionBindingSelection {
        overload: function.overload.clone(),
        argument_types: function.argument_types.clone(),
        result_type: FunctionResultType::Scalar(function.result_type.clone()),
        aggregate: aggregate_binding
            .as_deref()
            .map(|binding| AggregateBindingSelection {
                state_argument_contract: binding.state_argument_contract,
                intermediate_type: binding.intermediate_type.clone(),
                state_format: binding.state_format.clone(),
            }),
    });
    work.flush()?;
    functions.metadata().validate_frozen_selection(
        &function.function_id,
        function.kind,
        selection.as_ref(),
        request.request(),
        work.control(),
    )?;
    work.flush()?;
    // Each argument use is an unguarded Value occurrence of its ordered
    // argument definition; it becomes an independent WindowInput root.
    if invocation.arguments.len() < args.len() {
        return Err(ExpressionLoweringError::Invalid(
            "window call occurrence lacks its argument uses",
        ));
    }
    let mut argument_uses = Vec::new();
    reserve_vec(&mut argument_uses, args.len(), work)?;
    let mut children = ExpressionEffects::PURE_VALUE;
    for (&child, &argument) in invocation.arguments.iter().zip(args.iter()) {
        let child_invocation = flow
            .uses()
            .get(&child)
            .ok_or(ExpressionLoweringError::Invalid(
                "missing window argument occurrence",
            ))?;
        let domain = flow.domains().get(&child_invocation.context.domain);
        work.step()?;
        let exact = child_invocation.definition == argument
            && child_invocation.context.demand == EvaluationDemand::Value
            && domain.is_some_and(|domain| domain.parent.is_none() && domain.guard.is_none());
        if !exact {
            return Err(ExpressionLoweringError::Invalid(
                "window argument use differs from its actual call argument",
            ));
        }
        let summary = effects.get(&child).ok_or(ExpressionLoweringError::Invalid(
            "window argument effects were not prepared",
        ))?;
        children = children.join(summary.for_use(child_invocation.context)?);
        argument_uses.push(Some(child));
        work.step()?;
    }
    let window = WindowCallOptions::try_new(
        frozen_frame(package, frame.as_ref(), work)?,
        *ignore_nulls,
        work.control(),
    )
    .map_err(FunctionSpecializationFailure::Kernel)?;
    work.flush()?;
    let arguments = ScopedExpressionEffects::primitive(frozen.context, children);
    let options = match (function.kind, aggregate_binding.as_deref()) {
        (FunctionKind::Window, None) if !*distinct => PureCallPreparation::Window {
            arguments,
            options: window,
        },
        (FunctionKind::Aggregate, Some(binding)) if binding.phase == AggregatePhase::Single => {
            PureCallPreparation::AggregateWindow {
                arguments,
                options: AggregateWindowPreparationOptions {
                    aggregate: AggregatePreparationOptions {
                        state_interpretation: None,
                        phase: AggregateKernelPhase::Single,
                        distinct: *distinct,
                        order_keys: Arc::from([]),
                        state_input_type: None,
                    },
                    window,
                },
            }
        }
        _ => return Err(ExpressionLoweringError::UnsupportedCall(site)),
    };
    let input = CallEffectInput {
        context: frozen.context,
        argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(&argument_uses),
        function_id: &function.function_id,
        kind: function.kind,
        selected: selection.as_ref(),
        request: request.request(),
        environment: &frozen.effects.environment,
        parameters: package.parameters(),
        decimal_overflow_policy: frozen.decimal_overflow_policy,
        proof_scope: frozen.effects.proof_scope,
    };
    let token = functions.prepare_frozen(
        input,
        Arc::clone(&selection),
        &frozen.effects,
        options,
        work.control(),
    )?;
    work.flush()?;
    Ok(token)
}
