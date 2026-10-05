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

//! SQL application adapter for the Functions-owned builtin catalogue.
//!
//! Syntax admission, analyzer argument extraction and the SQL catalogue trait
//! remain here. Declarations, overload selection and result type policies live
//! in `novarocks_functions::builtin`; this adapter owns no second resolver.

pub(crate) use novarocks_functions::builtin::intrinsic::{BuiltinDisposition, builtin_disposition};
pub(crate) use novarocks_functions::builtin::resolver;
#[cfg(test)]
#[path = "intrinsic.rs"]
mod intrinsic_integration_tests;
#[cfg(test)]
#[path = "registry.rs"]
mod registry_integration_tests;
#[cfg(test)]
mod selected_preparation_tests;
pub use novarocks_functions::builtin::catalogue::{
    build_builtin_engine_function_catalog, builtin_engine_function_catalog,
    contribute_builtin_functions,
};
pub(crate) use novarocks_functions::builtin::catalogue::{
    builtin_function_volatility, dynamic_scalar_data_type, resolve_bound_aggregate,
};

use std::sync::Arc;

use arrow::datatypes::DataType;
#[cfg(test)]
use novarocks_functions::{
    AggregateBindingDeclaration, AggregateOverloadMetadata, EngineFunctionCatalogBuilder,
    FunctionArgumentEvaluation, FunctionBindingDeclaration, FunctionBindingResolver,
    FunctionBindingSelection, FunctionFailureBehavior, FunctionId, FunctionOverloadDeclaration,
    FunctionOverloadId, FunctionSemantics, FunctionVisibility,
};
use novarocks_functions::{
    EngineFunctionCatalog, FunctionArgument, FunctionArgumentType, FunctionBindingError,
    FunctionBindingRequest, FunctionDefinition, FunctionKind, FunctionResolutionError,
    FunctionResultType, FunctionValueType, ResolvedAggregateSignature, ResolvedFunctionBinding,
};

#[cfg(test)]
pub(crate) use resolver::resolve_scalar_function;
pub(crate) use resolver::{ResolveError, ResolvedScalarFunction};

/// Evaluation stability of a scalar function call.
///
/// This is SQL semantic metadata, not an optimizer-local policy.  It is
/// intentionally carried by the immutable function catalog so that analysis,
/// lambda validation, CSE, predicate derivation, and aggregate pushdown make
/// the same decision.
pub(crate) use novarocks_functions::FunctionVolatility;

#[cfg(test)]
fn scalar_output_logical_type(
    binding: &ResolvedFunctionBinding,
) -> Option<novarocks_types::schema::SqlType> {
    match &binding.selected.result_type {
        FunctionResultType::Scalar(result)
            if result.logical_type == novarocks_type_contract::ValueLogicalType::Json =>
        {
            Some(novarocks_types::schema::SqlType::Json)
        }
        _ => None,
    }
}

pub(crate) fn aggregate_result_type(binding: &ResolvedFunctionBinding) -> &FunctionValueType {
    match &binding.selected.result_type {
        FunctionResultType::Scalar(result) => result,
        FunctionResultType::Relation(_) => {
            unreachable!("aggregate binding cannot produce a relation")
        }
    }
}

pub(crate) fn aggregate_selection(
    binding: &ResolvedFunctionBinding,
) -> &novarocks_functions::AggregateBindingSelection {
    binding
        .selected
        .aggregate
        .as_ref()
        .expect("aggregate binding must carry intermediate state")
}

impl crate::compiler::SqlFunctionCatalog for EngineFunctionCatalog {
    fn snapshot(&self) -> Arc<dyn crate::compiler::SqlFunctionCatalog> {
        Arc::new(self.clone())
    }

    fn select_exact_overload_observed(
        &self,
        function: &novarocks_functions::FunctionId,
        kind: novarocks_functions::FunctionKind,
        overload: &novarocks_functions::FunctionOverloadId,
        request: novarocks_functions::FunctionBindingRequest<'_>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        Arc<novarocks_functions::FunctionBindingSelection>,
        novarocks_functions::FunctionBindingError,
    > {
        EngineFunctionCatalog::select_exact_overload_observed(
            self, function, kind, overload, request, control,
        )
    }

    fn pure_overload_declaration_observed<'a>(
        &'a self,
        function_id: &novarocks_functions::FunctionId,
        kind: novarocks_functions::FunctionKind,
        overload: &novarocks_functions::FunctionOverloadId,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::PureOverloadDeclaration<'a>,
        novarocks_functions::FunctionSpecializationFailure,
    > {
        EngineFunctionCatalog::pure_overload_declaration_observed(
            self,
            function_id,
            kind,
            overload,
            control,
        )
    }

    fn prepare_fresh_selected(
        &self,
        input: novarocks_functions::CallEffectInput<'_>,
        selected: Arc<novarocks_functions::FunctionBindingSelection>,
        options: novarocks_functions::PureCallPreparation,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::PureCallSpecialization,
        novarocks_functions::FunctionSpecializationFailure,
    > {
        EngineFunctionCatalog::prepare_fresh_selected(self, input, selected, options, control)
    }

    fn resolve_scalar_signature(
        &self,
        name: &str,
        arg_types: &[DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedScalarFunction, ResolveError> {
        let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
            control,
            novarocks_type_contract::CompilePhase::FunctionSpecialization,
        )?;
        let result = (|| {
            if arg_types.len() > novarocks_functions::MAX_CALL_EFFECT_ARGUMENTS {
                return Err(novarocks_type_contract::CompileControlError::ResourceExhausted.into());
            }
            let mut arguments = Vec::with_capacity(arg_types.len());
            for data_type in arg_types {
                work.step()?;
                arguments.push(FunctionArgument::Value {
                    value_type: FunctionValueType::new(data_type.clone(), true),
                    constant: None,
                });
            }
            work.flush()?;
            let bound = self
                .resolve_bound_user(
                    name,
                    FunctionKind::Scalar,
                    FunctionBindingRequest {
                        expected_result_type: None,
                        arguments: &arguments,
                        logical_argument_count: arguments.len(),
                    },
                    control,
                )
                .map_err(|error| match error {
                    FunctionBindingError::Control(error) => ResolveError::Control(error),
                    FunctionBindingError::UnknownFunction => ResolveError::UnknownFunction,
                    FunctionBindingError::HiddenFunction => ResolveError::HiddenFunction,
                    FunctionBindingError::NoMatchingOverload => ResolveError::NoMatchingSignature {
                        candidates: self
                            .definition(name, FunctionKind::Scalar)
                            .map(|definition| definition.canonical_signatures().len())
                            .unwrap_or_default(),
                        binding_enforced: true,
                    },
                    other => ResolveError::BadSignature(other.to_string()),
                })?;
            let FunctionResultType::Scalar(result) = bound.selected.result_type else {
                return Err(ResolveError::BadSignature(
                    "scalar function selected a relation result".into(),
                ));
            };
            let argument_types = bound
                .selected
                .argument_types
                .into_vec()
                .into_iter()
                .map(|argument| {
                    work.step()?;
                    match argument {
                        FunctionArgumentType::Value(value) => Ok(value.data_type),
                        FunctionArgumentType::Lambda { .. } => Err(ResolveError::BadSignature(
                            "legacy scalar signature cannot represent a lambda argument".into(),
                        )),
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ResolvedScalarFunction {
                return_type: result.data_type,
                argument_types,
                enforce_argument_binding: true,
            })
        })();
        if matches!(result, Err(ResolveError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }

    fn resolve_scalar_binding(
        &self,
        name: &str,
        arguments: &[FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_user(
            name,
            FunctionKind::Scalar,
            FunctionBindingRequest {
                expected_result_type: None,
                arguments,
                logical_argument_count: arguments.len(),
            },
            control,
        )
    }

    fn resolve_scalar_binding_with_expected_result(
        &self,
        name: &str,
        arguments: &[FunctionArgument],
        expected: &FunctionValueType,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_user(
            name,
            FunctionKind::Scalar,
            FunctionBindingRequest {
                arguments,
                logical_argument_count: arguments.len(),
                expected_result_type: Some(expected),
            },
            control,
        )
    }

    fn resolve_value_conversion_binding(
        &self,
        argument: &FunctionArgument,
        target: &FunctionValueType,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_trusted(
            novarocks_functions::builtin::value_conversion::VALUE_CONVERSION_NAME,
            FunctionKind::Scalar,
            FunctionBindingRequest {
                arguments: std::slice::from_ref(argument),
                logical_argument_count: 1,
                expected_result_type: Some(target),
            },
            control,
        )
    }

    fn resolve_window_binding(
        &self,
        name: &str,
        arguments: &[FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_user(
            name,
            FunctionKind::Window,
            FunctionBindingRequest {
                expected_result_type: None,
                arguments,
                logical_argument_count: arguments.len(),
            },
            control,
        )
    }

    fn resolve_table_binding(
        &self,
        name: &str,
        arguments: &[FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_user(
            name,
            FunctionKind::Table,
            FunctionBindingRequest {
                expected_result_type: None,
                arguments,
                logical_argument_count: arguments.len(),
            },
            control,
        )
    }

    fn contains_aggregate(&self, name: &str) -> bool {
        self.definition(name, FunctionKind::Aggregate).is_some()
    }

    fn resolve_aggregate_binding(
        &self,
        name: &str,
        logical_argument_count: usize,
        arguments: &[FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_user(
            name,
            FunctionKind::Aggregate,
            FunctionBindingRequest {
                expected_result_type: None,
                arguments,
                logical_argument_count,
            },
            control,
        )
    }

    fn resolve_aggregate_binding_trusted(
        &self,
        name: &str,
        logical_argument_count: usize,
        arguments: &[FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_trusted(
            name,
            FunctionKind::Aggregate,
            FunctionBindingRequest {
                expected_result_type: None,
                arguments,
                logical_argument_count,
            },
            control,
        )
    }

    fn resolve_aggregate_signature(
        &self,
        name: &str,
        arg_types: &[DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        resolve_bound_aggregate(self, name, arg_types, arg_types, false, control)
    }

    fn resolve_aggregate_update_signature(
        &self,
        name: &str,
        logical_arg_types: &[DataType],
        update_arg_types: &[DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        resolve_bound_aggregate(
            self,
            name,
            logical_arg_types,
            update_arg_types,
            false,
            control,
        )
    }

    fn resolve_aggregate_trusted(
        &self,
        name: &str,
        arg_types: &[DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        resolve_bound_aggregate(self, name, arg_types, arg_types, true, control)
    }

    fn volatility(&self, name: &str) -> FunctionVolatility {
        self.definition(name, FunctionKind::Scalar)
            .or_else(|| self.definition(name, FunctionKind::Window))
            .map(FunctionDefinition::volatility)
            .unwrap_or_default()
    }
}

pub(crate) fn resolve_sql_aggregate_binding(
    catalog: &dyn crate::compiler::SqlFunctionCatalog,
    name: &str,
    args: &[crate::analysis::TypedExpr],
    order_by: &[crate::analysis::SortItem],
    trusted: bool,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
        control,
        novarocks_type_contract::CompilePhase::FunctionSpecialization,
    )?;
    let result = (|| {
        let count = args
            .len()
            .checked_add(order_by.len())
            .filter(|count| *count <= novarocks_functions::MAX_CALL_EFFECT_ARGUMENTS)
            .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
        let mut arguments = Vec::with_capacity(count);
        for argument in args.iter().chain(order_by.iter().map(|item| &item.expr)) {
            work.step()?;
            work.flush()?;
            arguments.push(crate::analysis::function_argument(
                argument,
                constant_policy,
                work.control(),
            )?);
            work.flush()?;
        }
        work.flush()?;
        if trusted {
            catalog.resolve_aggregate_binding_trusted(name, args.len(), &arguments, control)
        } else {
            catalog.resolve_aggregate_binding(name, args.len(), &arguments, control)
        }
    })();
    if matches!(result, Err(FunctionBindingError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

pub fn builtin_sql_function_catalog() -> &'static dyn crate::compiler::SqlFunctionCatalog {
    builtin_engine_function_catalog()
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn test_resolved_aggregate(
    name: &str,
    argument_types: &[DataType],
    distinct: bool,
) -> crate::binding::SqlFunctionBinding {
    let executable_name =
        novarocks_functions::aggregate_types::mangle_distinct_aggregate_name(name, distinct);
    let arguments = argument_types
        .iter()
        .cloned()
        .map(|data_type| FunctionArgument::Value {
            value_type: FunctionValueType::new(data_type, true),
            constant: None,
        })
        .collect::<Vec<_>>();
    let exact = builtin_engine_function_catalog()
        .resolve_bound_trusted(
            &executable_name,
            FunctionKind::Aggregate,
            FunctionBindingRequest {
                expected_result_type: None,
                arguments: &arguments,
                logical_argument_count: arguments.len(),
            },
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap_or_else(|error| {
            panic!("test aggregate `{executable_name}` must resolve exactly: {error}")
        });
    crate::binding::SqlFunctionBinding::new(
        exact,
        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
    )
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn test_function_catalog_snapshot() -> Arc<dyn crate::compiler::SqlFunctionCatalog> {
    builtin_sql_function_catalog().snapshot()
}

#[cfg(test)]
struct TestExactAggregateBindingResolver {
    overloads: Box<[AggregateOverloadMetadata]>,
}

#[cfg(test)]
impl TestExactAggregateBindingResolver {
    fn resolve_exact(
        &self,
        request: FunctionBindingRequest<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        if request.logical_argument_count != request.arguments.len() {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        let argument_types = request
            .arguments
            .iter()
            .map(|argument| match argument {
                FunctionArgument::Value { value_type, .. } => Ok(value_type.data_type.clone()),
                FunctionArgument::Lambda { .. } => Err(FunctionBindingError::NoMatchingOverload),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let overload = self
            .overloads
            .iter()
            .find(|overload| overload.argument_types.as_ref() == argument_types.as_slice())
            .ok_or(FunctionBindingError::NoMatchingOverload)?;
        self.selection(request, overload)
    }

    fn selection(
        &self,
        request: FunctionBindingRequest<'_>,
        overload: &AggregateOverloadMetadata,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        Ok(FunctionBindingSelection {
            overload: FunctionOverloadId::try_new(overload.identity.as_str())
                .map_err(|error| FunctionBindingError::InvalidBinding(error.to_string().into()))?,
            argument_types: request
                .arguments
                .iter()
                .map(FunctionArgument::argument_type)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            result_type: FunctionResultType::Scalar(FunctionValueType::new(
                overload.output_type.clone(),
                true,
            )),
            aggregate: Some(novarocks_functions::AggregateBindingSelection {
                state_argument_contract:
                    novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
                intermediate_type: FunctionValueType::new(overload.intermediate_type.clone(), true),
                state_format: overload.state_format.clone(),
            }),
        })
    }
}

#[cfg(test)]
impl FunctionBindingResolver for TestExactAggregateBindingResolver {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.resolve_exact(request)
    }

    fn select_at_overload_observed(
        &self,
        supplied: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result = (|| {
            let mut exact = None;
            for overload in &self.overloads {
                let matches = overload.identity.as_str() == supplied.as_str();
                work.step()?;
                if matches {
                    exact = Some(overload);
                    break;
                }
            }
            let overload = exact.ok_or(FunctionBindingError::NoMatchingOverload)?;
            let same_arity = request.logical_argument_count == request.arguments.len()
                && request.arguments.len() == overload.argument_types.len();
            work.step()?;
            if !same_arity {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            for (argument, expected) in request.arguments.iter().zip(&overload.argument_types) {
                let FunctionArgument::Value { value_type, .. } = argument else {
                    return Err(FunctionBindingError::NoMatchingOverload);
                };
                let exact = novarocks_type_contract::arrow_data_types_exact_observed(
                    &value_type.data_type,
                    expected,
                    || work.step().map_err(FunctionBindingError::from),
                )?;
                if !exact {
                    return Err(FunctionBindingError::NoMatchingOverload);
                }
            }
            work.flush()?;
            let selected = self.selection(request, overload);
            work.step()?;
            selected
        })();
        if matches!(&result, Err(FunctionBindingError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        if &self.select_at_overload_observed(&selected.overload, request, _control)? == selected {
            Ok(())
        } else {
            Err(FunctionBindingError::InvalidBinding(
                "selected test aggregate binding differs from its exact declaration".into(),
            ))
        }
    }
}

/// Build a test-only catalog whose custom aggregate has the same exact,
/// identity-bearing contract required from production contributors.
#[cfg(test)]
pub(crate) fn test_exact_aggregate_catalog(
    name: &str,
    visibility: FunctionVisibility,
    overloads: impl IntoIterator<Item = AggregateOverloadMetadata>,
) -> EngineFunctionCatalog {
    let overloads = overloads.into_iter().collect::<Vec<_>>();
    let declaration = FunctionBindingDeclaration::try_new(
        FunctionId::try_new(format!("test.aggregate/{name}/v1"))
            .expect("test aggregate function identity"),
        FunctionKind::Aggregate,
        overloads
            .iter()
            .map(|overload| FunctionOverloadDeclaration {
                effects: None,
                semantics: FunctionSemantics {
                    volatility: FunctionVolatility::Immutable,
                    argument_evaluation: FunctionArgumentEvaluation::Eager,
                    failure_behavior: FunctionFailureBehavior::Propagate,
                    intrinsic_row_error:
                        novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated,
                },
                identity: FunctionOverloadId::try_new(overload.identity.as_str())
                    .expect("test aggregate overload identity"),
                argument_pattern: format!("exact:{}:arguments", overload.identity.as_str()).into(),
                result_pattern: format!("exact:{}:output", overload.identity.as_str()).into(),
                aggregate: Some(AggregateBindingDeclaration {
                    state_argument_contract:
                        novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
                    intermediate_pattern: format!(
                        "exact:{}:intermediate",
                        overload.identity.as_str()
                    )
                    .into(),
                    state_format: overload.state_format.clone(),
                }),
            }),
    )
    .expect("test aggregate binding declaration");
    let overloads = overloads.into_boxed_slice();
    let definition = FunctionDefinition::try_new_bound_aggregate(
        name,
        visibility,
        declaration,
        Arc::new(TestExactAggregateBindingResolver {
            overloads: overloads.clone(),
        }),
        novarocks_functions::exact_aggregate_signature_contract(overloads.into_vec()),
    )
    .expect("test aggregate definition");
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(definition)
        .expect("register test aggregate");
    builder.seal_bound().expect("test aggregate catalog")
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ShadowBindingResolver(FunctionBindingSelection);
    impl FunctionBindingResolver for ShadowBindingResolver {
        fn resolve(
            &self,
            _: FunctionBindingRequest<'_>,
            _control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<FunctionBindingSelection, FunctionBindingError> {
            Ok(self.0.clone())
        }
        fn validate_selected(
            &self,
            selected: &FunctionBindingSelection,
            _: FunctionBindingRequest<'_>,
            _control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<(), FunctionBindingError> {
            if selected == &self.0 {
                Ok(())
            } else {
                Err(FunctionBindingError::NoMatchingOverload)
            }
        }
    }

    fn value_argument(
        data_type: DataType,
        nullable: bool,
        constant: Option<novarocks_functions::ConstantValue>,
    ) -> FunctionArgument {
        FunctionArgument::Value {
            value_type: FunctionValueType::new(data_type, nullable),
            constant,
        }
    }

    fn resolve_exact_scalar(
        catalog: &EngineFunctionCatalog,
        name: &str,
        arguments: &[FunctionArgument],
    ) -> ResolvedFunctionBinding {
        catalog
            .resolve_bound_user(
                name,
                FunctionKind::Scalar,
                FunctionBindingRequest {
                    expected_result_type: None,
                    arguments,
                    logical_argument_count: arguments.len(),
                },
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap_or_else(|error| panic!("{name} must bind exactly: {error}"))
    }

    fn scalar_result(binding: &ResolvedFunctionBinding) -> &FunctionValueType {
        let FunctionResultType::Scalar(result) = &binding.selected.result_type else {
            panic!("scalar binding must have a scalar result")
        };
        result
    }

    #[test]
    fn json_output_domain_uses_bound_result_not_shadowed_function_spelling() {
        let builtin = build_builtin_engine_function_catalog().unwrap();
        let args = [value_argument(DataType::Utf8, false, None)];
        let bound = resolve_exact_scalar(&builtin, "json_object", &args);
        assert_eq!(
            scalar_output_logical_type(&bound),
            Some(novarocks_types::schema::SqlType::Json)
        );

        let overload = FunctionOverloadId::try_new("test.shadow.json_object/0/v1").unwrap();
        let mut selection = bound.selected.clone();
        selection.overload = overload.clone();
        selection.result_type =
            FunctionResultType::Scalar(FunctionValueType::new(DataType::Utf8, false));
        let declaration = FunctionBindingDeclaration::try_new(
            FunctionId::try_new("test.shadow/json_object/v1").unwrap(),
            FunctionKind::Scalar,
            [FunctionOverloadDeclaration {
                effects: None,
                semantics: bound.semantics,
                identity: overload,
                argument_pattern: "(varchar...)->varchar".into(),
                result_pattern: "varchar".into(),
                aggregate: None,
            }],
        )
        .unwrap();
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder
            .register(
                FunctionDefinition::try_new_bound(
                    "json_object",
                    FunctionVisibility::Public,
                    declaration,
                    Arc::new(ShadowBindingResolver(selection)),
                )
                .unwrap(),
            )
            .unwrap();
        let shadow = builder.seal().unwrap();
        let bound = resolve_exact_scalar(&shadow, "json_object", &args);
        assert_eq!(scalar_result(&bound).data_type, DataType::Utf8);
        assert_eq!(scalar_output_logical_type(&bound), None);
    }

    #[test]
    fn sqlx1_function_builtin_snapshot_has_canonical_volatility_set() {
        let catalog = builtin_sql_function_catalog();
        for name in [
            "rand",
            "random",
            "uuid",
            "sleep",
            "now",
            "current_timestamp",
            "current_date",
            "curdate",
            "current_time",
            "curtime",
            "localtime",
            "localtimestamp",
            "utc_timestamp",
            "utc_time",
        ] {
            assert_eq!(
                catalog.volatility(name),
                FunctionVolatility::Volatile,
                "{name}"
            );
        }
        assert_eq!(catalog.volatility("lower"), FunctionVolatility::Immutable);
    }

    #[test]
    fn sqlx1_function_snapshot_resolves_registered_signature() {
        let resolved = builtin_sql_function_catalog()
            .resolve_scalar_signature(
                "lower",
                &[DataType::Utf8],
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .expect("registered function resolves through snapshot");
        assert_eq!(resolved.return_type, DataType::Utf8);
    }

    #[test]
    fn hidden_aggregate_is_rejected_by_sql_user_resolution() {
        let catalog = test_exact_aggregate_catalog(
            "$hidden_stat",
            FunctionVisibility::Hidden,
            [AggregateOverloadMetadata::try_new(
                "test/$hidden_stat/v1",
                [DataType::Int64],
                DataType::Int64,
                DataType::Int64,
                "test/$hidden_stat/state-v1",
            )
            .unwrap()],
        );
        assert!(crate::compiler::SqlFunctionCatalog::contains_aggregate(
            &catalog,
            "$hidden_stat"
        ));
        assert_eq!(
            crate::compiler::SqlFunctionCatalog::resolve_aggregate_signature(
                &catalog,
                "$hidden_stat",
                &[DataType::Int64],
                &crate::compiler::SqlCompileControl::unbounded()
            ),
            Err(FunctionResolutionError::HiddenFunction)
        );
    }
}

#[cfg(test)]
mod overload_declaration_tests;
