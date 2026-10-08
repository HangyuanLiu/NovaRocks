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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//! Test-only real temporal ControlIntrinsic route, sharing the checked package
//! author rather than invoking an owner through the ordinary scalar ABI.
use super::*;
use crate::exec::expr::compiled_program::{CompiledExpressionInstance, guarded_tests as host};
use novarocks_functions::{
    EngineFunctionCatalogBuilder, InstalledPureKernel, PureEngineFunctionCatalog,
};
use novarocks_local_program::{
    LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeId,
};
use novarocks_physical_plan::{
    ConstantPoolId, ConstantReference, ExprKind as PKind, FragmentBuilder, FragmentId,
    FragmentSink, LiteralValue as PLiteral, NodeId, PipelineDopDomain, ResultField, ResultPort,
    ValueDef, ValueId, ValueOrigin,
};
use novarocks_type_contract::{ArgumentControl, CompileCheckpoints, ControlShape};
use std::collections::BTreeMap;

/// The normal binding channels stay in ScalarDiffSpec.arguments. A source
/// receipt supplies the actual expression to evaluate afresh on both paths;
/// no cached normal argument array substitutes for that expression.
#[derive(Clone, Debug)]
pub(crate) struct SourceExpression {
    pub input: DiffArgument,
    pub sec_to_time: bool,
    pub cast_chain: Vec<DataType>,
}
impl SourceExpression {
    pub(super) fn wrap_legacy(
        &self,
        arena: &mut ExprArena,
        mut source: ExprId,
        policy: DecimalOverflowPolicy,
    ) -> ExprId {
        if self.sec_to_time {
            source = arena.push_typed(
                ExprNode::FunctionCall {
                    kind: lookup_function("sec_to_time").expect("actual v1 registration"),
                    args: vec![source],
                },
                DataType::Utf8,
            );
        }
        for target in &self.cast_chain {
            source = arena.push_typed(ExprNode::Cast(source, policy), target.clone());
        }
        source
    }
}
fn pure_catalogue(name: &str, source: Option<&SourceExpression>) -> PureEngineFunctionCatalog {
    let actual = builtin_engine_function_catalog();
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut manifest = Vec::new();
    let mut names = vec![name];
    if source.is_some_and(|s| s.sec_to_time) {
        names.push("sec_to_time");
    }
    for name in names {
        let definition = actual.definition(name, CatalogKind::Scalar).unwrap();
        builder.register(definition.clone()).unwrap();
        let declaration = definition.binding_declaration().unwrap();
        for overload in declaration.overloads() {
            let installed = actual
                .pure_overload_declaration_observed(
                    declaration.function_id(),
                    CatalogKind::Scalar,
                    &overload.identity,
                    &HarnessControl,
                )
                .unwrap();
            manifest.push(InstalledPureKernel {
                function: declaration.function_id().clone(),
                kind: CatalogKind::Scalar,
                implementation: installed.implementation().clone(),
                aggregate_state_format: None,
            });
        }
    }
    builder.seal_pure(manifest).unwrap()
}
fn physical_literal(value: &ConstantValue) -> Option<PLiteral> {
    match legacy_literal(value)? {
        LiteralValue::Null => Some(PLiteral::Null),
        LiteralValue::Int64(v) => Some(PLiteral::Int64(v)),
        LiteralValue::Utf8(v) => Some(PLiteral::Utf8(v.into_boxed_str())),
        LiteralValue::Date32(v) => Some(PLiteral::Date32(v)),
        _ => None,
    }
}
struct CompiledCall {
    program: Arc<LocalProgram>,
    input: RecordBatch,
}
fn compile(spec: &ScalarDiffSpec, rows: usize, result_type: &FunctionValueType) -> CompiledCall {
    let functions = pure_catalogue(&spec.name, spec.temporal_source.as_ref());
    let source_node = NodeId::new(u32::MAX);
    let input_node = NodeId::new(41);
    let output_node = NodeId::new(0);
    let fragment_id = FragmentId::new(197);
    let mut builder = FragmentBuilder::new(fragment_id);
    builder
        .add_values(source_node, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut items = Vec::new();
    let mut output_values = Vec::new();
    let mut columns = Vec::new();
    let mut arguments = Vec::new();
    let mut authors = BTreeMap::new();
    let mut pool_ids = BTreeMap::new();
    // Root request pools use the same first-seen order as original_request_sources.
    for argument in &spec.arguments {
        if let DiffArgument::Constant(value) = argument {
            let id = ConstantPoolId::new(pool_ids.len() as u32);
            pool_ids
                .entry(value.pool().backing_identity())
                .or_insert(id);
        }
    }
    let (declared_parameters, keys) = spec.semantics.parameter_table().unwrap();
    let parameters = if spec
        .temporal_source
        .as_ref()
        .is_some_and(|s| !s.cast_chain.is_empty())
    {
        declared_parameters
    } else {
        SemanticParameters::try_new([]).unwrap()
    };
    let allow_throw_exception = SemanticParameterRef {
        id: keys
            .iter()
            .find(|(key, _)| *key == SemanticParameterKey::AllowThrowException)
            .unwrap()
            .1,
        expected_key: SemanticParameterKey::AllowThrowException,
    };
    for (index, normal_argument) in spec.arguments.iter().enumerate() {
        let argument = if index == 0 {
            spec.temporal_source
                .as_ref()
                .map_or(normal_argument, |source| &source.input)
        } else {
            normal_argument
        };
        let mut expr = match argument {
            DiffArgument::Column { value_type, values } => {
                assert_eq!(values.len(), rows, "actual source column invocation length");
                let value = ValueId::new(index as u32 + 901);
                let definition = builder
                    .add_expression(
                        input_node,
                        value_type.clone(),
                        PKind::Literal(PLiteral::Null),
                    )
                    .unwrap();
                builder
                    .insert_value(ValueDef {
                        id: value,
                        ty: value_type.clone(),
                        origin: ValueOrigin::Expr {
                            node: input_node,
                            expr: definition,
                        },
                    })
                    .unwrap();
                items.push((definition, value));
                output_values.push(value);
                columns.push(values.clone());
                builder
                    .add_expression(output_node, value_type.clone(), PKind::Value(value))
                    .unwrap()
            }
            DiffArgument::Constant(value) => {
                let literal = if spec.temporal_source.is_some() && index == 0 {
                    // These source fixtures admit literal-capable exact source values.
                    physical_literal(value)
                } else if spec.legacy_constants == LegacyConstantForm::Literal {
                    physical_literal(value)
                } else {
                    None
                };
                let kind = literal.map(PKind::Literal).unwrap_or_else(|| {
                    PKind::Constant(ConstantReference {
                        pool: pool_ids[&value.pool().backing_identity()],
                        ordinal: value.ordinal(),
                    })
                });
                builder
                    .add_expression(output_node, value.value_type().clone(), kind)
                    .unwrap()
            }
        };
        if index == 0
            && let Some(source) = &spec.temporal_source
        {
            if source.sec_to_time {
                expr = host::call(
                    &mut builder,
                    &mut authors,
                    host::author(
                        &functions,
                        "sec_to_time",
                        vec![source.input.request()],
                        ControlShape::Eager,
                    ),
                    vec![expr],
                );
            }
            for target in &source.cast_chain {
                expr = builder
                    .add_expression(
                        output_node,
                        FunctionValueType::new(target.clone(), true),
                        PKind::Cast {
                            expr,
                            target: target.clone(),
                            decimal_overflow_policy: spec.semantics.decimal_overflow_policy,
                            allow_throw_exception,
                        },
                    )
                    .unwrap();
            }
        }
        arguments.push(expr);
    }
    if columns.is_empty() {
        let value = ValueId::new(899);
        let ty = FunctionValueType::new(DataType::Int8, true);
        let definition = builder
            .add_expression(input_node, ty.clone(), PKind::Literal(PLiteral::Null))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id: value,
                ty: ty.clone(),
                origin: ValueOrigin::Expr {
                    node: input_node,
                    expr: definition,
                },
            })
            .unwrap();
        items.push((definition, value));
        output_values.push(value);
        columns.push(Arc::new(Int8Array::from(vec![0; rows])));
    }
    builder
        .add_project(
            input_node,
            source_node,
            items.into_boxed_slice(),
            output_values.into_boxed_slice(),
        )
        .unwrap();
    // Temporary Eager only constructs a definition. Before any occurrence or
    // preparation, the ONE Physical author supplies its actual source shape.
    let root_expr = host::call(
        &mut builder,
        &mut authors,
        host::author(
            &functions,
            &spec.name,
            spec.arguments.iter().map(DiffArgument::request).collect(),
            ControlShape::Eager,
        ),
        arguments,
    );
    let value = builder
        .add_value(
            result_type.clone(),
            ValueOrigin::Expr {
                node: output_node,
                expr: root_expr,
            },
        )
        .unwrap();
    builder
        .add_project(
            output_node,
            input_node,
            Box::from([(root_expr, value)]),
            Box::from([value]),
        )
        .unwrap();
    let fragment = builder
        .finish_definition(
            output_node,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();
    let definition = functions
        .metadata()
        .resolve_bound_user(
            &spec.name,
            CatalogKind::Scalar,
            FunctionBindingRequest {
                arguments: &spec
                    .arguments
                    .iter()
                    .map(DiffArgument::request)
                    .collect::<Vec<_>>(),
                logical_argument_count: spec.arguments.len(),
                expected_result_type: None,
            },
            &HarnessControl,
        )
        .unwrap();
    let installed = functions
        .metadata()
        .pure_overload_declaration_observed(
            &definition.function_id,
            CatalogKind::Scalar,
            &definition.selected.overload,
            &HarnessControl,
        )
        .unwrap();
    let ArgumentControl::TemporalSource(kind) = installed.effects().argument_control else {
        panic!("actual source control declaration")
    };
    let PKind::FunctionCall { args, .. } = &fragment.expressions().get(root_expr).unwrap().kind
    else {
        unreachable!()
    };
    let mut work =
        CompileCheckpoints::try_new(&HarnessControl, CompilePhase::FunctionSpecialization).unwrap();
    let projected = novarocks_physical_plan::temporal_source_definitions_observed(
        kind,
        fragment.expressions(),
        args,
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    authors.get_mut(&root_expr).unwrap().shape =
        ControlShape::TemporalSource(projected.facts.shape());
    let result = ResultPort {
        fragment: fragment_id,
        output: fragment.nodes()[&output_node].output.clone(),
        fields: Box::from([ResultField {
            name: "temporal_result".into(),
            alias: None,
            value,
            ty: result_type.clone(),
        }]),
    };
    let program = host::compile_checked_fragment_with_parameters(
        &functions, fragment, &authors, result, parameters,
    );
    let input = RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        columns,
    )
    .unwrap();
    // The compiled input port is the real authority for field identity/order.
    CompiledCall { program, input }
}
fn evaluate(
    call: &CompiledCall,
    rows: Option<&[usize]>,
) -> Result<(PureSelection, Vec<host::TemporalInvocationDataProbe>), String> {
    let selection = rows.map_or(Ok(Selection::all(call.input.num_rows())), |rows| {
        Selection::try_sparse(call.input.num_rows(), rows).map_err(|e| e.to_string())
    })?;
    let root = ProgramExpressionRootSite::Node {
        node: ProgramNodeId::new(2),
        role: ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
    };
    let probe = host::TemporalProbeScope::enter();
    let mut instance =
        CompiledExpressionInstance::try_new(call.program.clone(), root, &HarnessControl)
            .map_err(|e| e.to_string())?;
    let output = instance
        .evaluate(&call.input, selection, &HarnessControl)
        .map_err(|e| e.to_string())?;
    let (_, values, errors) = output.into_parts();
    Ok((
        PureSelection {
            values,
            errors: errors.into_vec(),
        },
        probe.take(),
    ))
}
fn compare_executed_invocation_error(
    selected: &[usize],
    batch_error: &str,
    pure: &PureSelection,
    probes: &[host::TemporalInvocationDataProbe],
    found: &mut Vec<String>,
) -> SelectionOutcome {
    let mut outcome = SelectionOutcome {
        legacy_batch_error: true,
        ..Default::default()
    };
    // This entry is legal only after the actual phase produced InvocationData.
    for probe in probes {
        if probe.invocation_rows != selected {
            found.push(
                "this root-call differential encountered a different current invocation domain"
                    .into(),
            );
        }
        if probe.stage != 2
            || probe.shape != novarocks_type_contract::TemporalSourceShape::SecondsCastOther
            || probe.message != batch_error
        {
            found.push(format!("executed InvocationData differs from full legacy batch error: {probe:?}; {batch_error}"));
        }
        for (ordinal, row) in selected.iter().enumerate() {
            let expected = if probe.affected_rows.contains(row) {
                Some(probe.message.as_str())
            } else {
                probe
                    .prior_errors
                    .iter()
                    .find(|(prior, _)| prior == row)
                    .map(|(_, e)| e.as_str())
            };
            let error = pure.errors.iter().find(|e| e.selected_ordinal() == ordinal);
            match (expected, error) {
                (Some(expected), Some(error))
                    if *error == RowDataError::new(ordinal, expected)
                        && pure.values.is_null(ordinal) =>
                {
                    outcome.attributed_row_errors += 1
                }
                _ => found.push(format!(
                    "row {row}: actual invocation-error domain/projection differs"
                )),
            }
        }
        if probe.affected_rows.len() + probe.prior_errors.len() != probe.invocation_rows.len() {
            found.push(
                "actual invocation phase does not cover successful current domain and prior errors"
                    .into(),
            );
        }
    }
    outcome
}
pub(super) fn run(
    spec: &ScalarDiffSpec,
    bound: &ResolvedFunctionBinding,
    legacy_name: &str,
    legacy_kind: Option<ScalarLegacyImplementation>,
    rows: usize,
    result_type: &FunctionValueType,
) -> Result<ScalarDiffSummary, DifferentialFailure> {
    let catalog = builtin_engine_function_catalog();
    let declaration = catalog
        .pure_overload_declaration_observed(
            &bound.function_id,
            CatalogKind::Scalar,
            &bound.selected.overload,
            &HarnessControl,
        )
        .map_err(|e| DifferentialFailure::Specialization(e.to_string()))?;
    if !matches!(
        declaration.effects().argument_control,
        ArgumentControl::TemporalSource(_)
    ) {
        return Err(DifferentialFailure::UnsupportedPureAbi {
            overload: bound.selected.overload.clone(),
            abi: "non-temporal ControlIntrinsicV1".into(),
        });
    }
    let legacy_kind = legacy_kind.ok_or_else(|| DifferentialFailure::LegacyUnavailable {
        name: legacy_name.into(),
        reason: "no original implementation".into(),
    })?;
    let call = ScalarCall {
        spec,
        legacy_kind,
        result_type,
        rows,
    };
    let compiled = catch_unwind(AssertUnwindSafe(|| compile(spec, rows, result_type)))
        .map_err(|e| DifferentialFailure::Specialization(panic_message(&e)))?;
    let mut summary = ScalarDiffSummary {
        function: bound.function_id.clone(),
        overload: bound.selected.overload.clone(),
        legacy_name: legacy_name.into(),
        result_type: result_type.clone(),
        rows,
        selections: 0,
        legacy_batch_errors: 0,
        attributed_row_errors: 0,
        null_results: 0,
    };
    let mut selections = vec![None];
    if rows > 1 {
        let mut generator = InputGenerator::new(spec.selection_seed);
        for _ in 0..spec.sparse_selections {
            selections.push(Some(generator.selection(rows, 0.5)));
        }
    }
    let mut details = Vec::new();
    for selection in &selections {
        summary.selections += 1;
        let selected = selection.as_deref();
        let selected_rows = selected.map_or_else(|| (0..rows).collect::<Vec<_>>(), <[_]>::to_vec);
        let evaluated = catch_unwind(AssertUnwindSafe(|| evaluate(&compiled, selected)))
            .unwrap_or_else(|e| Err(panic_message(&e)));
        let (pure, probes) = match evaluated {
            Ok(value) => value,
            Err(error) => {
                details.push(format!("pure outer failure: {error}"));
                continue;
            }
        };
        let legacy = call.evaluate_legacy(Some(&selected_rows));
        let outcome = if let Err(error) = &legacy
            && !probes.is_empty()
        {
            if pure.values.len() != selected_rows.len() {
                details.push("wrong invocation error result length".into());
            }
            if !novarocks_type_contract::arrow_data_types_exact(
                pure.values.data_type(),
                &result_type.data_type,
            ) {
                details.push("invocation-error carrier differs from the frozen result".into());
            }
            compare_executed_invocation_error(&selected_rows, error, &pure, &probes, &mut details)
        } else {
            compare_selection(
                SelectionComparison {
                    result_type,
                    floats: spec.float_comparison,
                    messages: spec.error_messages,
                    rows: &selected_rows,
                },
                legacy,
                |row| call.evaluate_legacy(Some(&[row])),
                &pure,
                &mut details,
            )
        };
        summary.null_results += outcome.null_results;
        summary.attributed_row_errors += outcome.attributed_row_errors;
        summary.legacy_batch_errors += usize::from(outcome.legacy_batch_error);
    }
    if details.is_empty() {
        Ok(summary)
    } else {
        Err(DifferentialFailure::Mismatch {
            name: spec.name.clone(),
            overload: bound.selected.overload.clone(),
            details,
        })
    }
}
