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

//! Each installed LEAD/LAG Window function shares its binding, effects and CPU preparation owner.

use std::sync::Arc;

use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompilePhase,
    FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior, ObservableEffects,
    PureCompileControl,
};

use super::catalogue::BuiltinScalarResolver;
use super::window_offset::{OffsetOperation, PreparedOffset};
use crate::kernel_control::{compile_failure, invalid};
use crate::{
    CallEffectInput, FunctionArgument, FunctionArgumentType, FunctionBindingDeclaration,
    FunctionBindingError, FunctionBindingRequest, FunctionBindingResolver,
    FunctionBindingSelection, FunctionCatalogError, FunctionDefinition, FunctionEffectOwner,
    FunctionEffectOwnerError, FunctionFailureBehavior, FunctionId, FunctionIntrinsicRowError,
    FunctionKind, FunctionResultType, FunctionVisibility, FunctionVolatility, KernelFailure,
    PreparedWindowKernel, PureFunctionMetadataOwner, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi, PureWindowImplementation, WindowCallContract,
    WindowCallOptions,
};

/// Only the two actual installed LEAD/LAG Window declarations have an owner.
pub(super) fn operation(name: &str) -> Option<OffsetOperation> {
    match name {
        "lead" => Some(OffsetOperation::Lead),
        "lag" => Some(OffsetOperation::Lag),
        _ => None,
    }
}

/// Complete partition input is required; no scalar row-error or NULL mask applies.
pub(super) fn effects() -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: FunctionIntrinsicRowError::NotRowEvaluated,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::CalledOnNull,
        argument_control: ArgumentControl::Window,
        instance_state: FunctionInstanceState::WindowPartition,
        observable_effects: ObservableEffects::NONE,
        environment_dependencies: Box::new([]),
    }
}

pub(super) fn definition(
    name: &str,
    declaration: FunctionBindingDeclaration,
    resolver: BuiltinScalarResolver,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    let owner = Arc::new(WindowOffsetOwner::new(name, declaration, resolver)?);
    FunctionDefinition::try_new_pure_window(name, FunctionVisibility::Public, owner).map_err(
        |error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin window_offset pure owner",
            value: error.to_string().into(),
        },
    )
}

pub(super) struct WindowOffsetOwner {
    resolver: BuiltinScalarResolver,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
    operation: OffsetOperation,
}
impl WindowOffsetOwner {
    fn new(
        name: &str,
        declaration: FunctionBindingDeclaration,
        resolver: BuiltinScalarResolver,
    ) -> Result<Self, FunctionCatalogError> {
        let operation =
            operation(name).ok_or_else(|| FunctionCatalogError::InvalidStableIdentity {
                subject: "uninstalled window_offset",
                value: name.into(),
            })?;
        let function = format!("builtin.window/{name}/v1");
        let expected = effects();
        if declaration.function_id().as_str() != function
            || declaration.kind() != FunctionKind::Window
            || declaration.overloads().len() != 3
            || declaration.overloads().iter().any(|overload| {
                overload.aggregate.is_some() || overload.effects.as_ref() != Some(&expected)
            })
        {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin window_offset pure declaration",
                value: declaration.function_id().as_str().into(),
            });
        }
        let implementation =
            PureImplementationId::try_new(format!("builtin.window/{name}/selected-v1"))?;
        let implementations = declaration
            .overloads()
            .iter()
            .map(|overload| PureImplementationDeclaration {
                overload: overload.identity.clone(),
                implementation: implementation.clone(),
                abi: PureKernelAbi::WindowV1,
            })
            .collect();
        Ok(Self {
            resolver,
            declaration,
            implementations,
            operation,
        })
    }

    fn call_effects(&self, scope: CallProofScope) -> CallEffects {
        let base = effects();
        CallEffects {
            value_stability: base.value_stability,
            own_row_error: base.own_row_error,
            failure_behavior: base.failure_behavior,
            null_behavior: base.null_behavior,
            argument_control: base.argument_control,
            instance_state: base.instance_state,
            observable_effects: base.observable_effects,
            environment: Box::new([]),
            proof_scope: scope,
        }
    }
}

impl FunctionBindingResolver for WindowOffsetOwner {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.resolver.resolve(request, control)
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        self.resolver.validate_selected(selected, request, control)
    }
}

impl PureFunctionMetadataOwner for WindowOffsetOwner {
    fn binding_declaration(&self) -> &FunctionBindingDeclaration {
        &self.declaration
    }

    fn implementation_declarations(&self) -> &[PureImplementationDeclaration] {
        &self.implementations
    }
}

impl FunctionEffectOwner for WindowOffsetOwner {
    type Error = FunctionBindingError;

    fn declaration(
        &self,
        function: &FunctionId,
        selected: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error> {
        if function != self.declaration.function_id() {
            return Err(FunctionBindingError::UnknownFunction);
        }
        self.declaration.effect_declaration(&selected.overload)
    }

    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        let result = (|| {
            let exact_header = input.function_id == self.declaration.function_id()
                && input.kind == FunctionKind::Window;
            work.step().map_err(FunctionEffectOwnerError::Control)?;
            if !exact_header {
                return Err(FunctionBindingError::UnknownFunction.into());
            }
            let exact_scope = input.environment.is_empty()
                && (matches!(input.proof_scope, CallProofScope::Unconditional)
                    || input.proof_scope == CallProofScope::Domain(input.context.domain));
            work.step().map_err(FunctionEffectOwnerError::Control)?;
            if !exact_scope {
                return Err(FunctionBindingError::InvalidBinding(
                    "window_offset has no environment dependencies and requires an exact proof scope"
                        .into(),
                )
                .into());
            }
            work.flush().map_err(FunctionEffectOwnerError::Control)?;
            self.validate_selected(input.selected, input.request, control)
                .map_err(|error| match error {
                    FunctionBindingError::Control(error) => {
                        FunctionEffectOwnerError::Control(error)
                    }
                    other => FunctionEffectOwnerError::Owner(other),
                })?;
            Ok(self.call_effects(input.proof_scope))
        })();
        if matches!(&result, Err(FunctionEffectOwnerError::Control(_))) {
            return result;
        }
        work.finish().map_err(FunctionEffectOwnerError::Control)?;
        result
    }
}

impl PureWindowImplementation for WindowOffsetOwner {
    fn prepare_window(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<WindowCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedWindowKernel>, KernelFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        let result: Result<Arc<dyn PreparedWindowKernel>, KernelFailure> = (|| {
            let exact_call = input.function_id == self.declaration.function_id()
                && input.kind == FunctionKind::Window
                && contract.call().function_id() == input.function_id
                && std::ptr::eq(contract.call().selected(), input.selected)
                && contract.call().context() == input.context
                && contract.call().decimal_overflow_policy() == input.decimal_overflow_policy
                && contract.call().effects() == &self.call_effects(input.proof_scope)
                && input.environment.is_empty();
            work.step().map_err(compile_failure)?;
            if !exact_call {
                return Err(invalid(
                    "window_offset preparation differs from its exact checked call",
                ));
            }
            work.flush().map_err(compile_failure)?;
            self.validate_selected(input.selected, input.request, control)
                .map_err(|error| match error {
                    FunctionBindingError::Control(error) => compile_failure(error),
                    _ => invalid("window_offset preparation has a stale selected binding"),
                })?;
            let offset = canonical_call(input, *contract.options(), &mut work)?;
            work.flush().map_err(compile_failure)?;
            // The prepared object retains the same canonical contract. Its body is
            // a static pure implementation and needs no live resolver or authority.
            let prepared = Arc::new(PreparedOffset {
                contract,
                operation: self.operation,
                offset,
            }) as Arc<dyn PreparedWindowKernel>;
            work.flush().map_err(compile_failure)?;
            Ok(prepared)
        })();
        if matches!(
            &result,
            Err(KernelFailure::Cancelled
                | KernelFailure::DeadlineExceeded
                | KernelFailure::ResourceExhausted)
        ) {
            return result;
        }
        work.finish().map_err(compile_failure)?;
        result
    }
}

fn canonical_call(
    input: CallEffectInput<'_>,
    options: WindowCallOptions,
    work: &mut CompileCheckpoints<'_>,
) -> Result<i64, KernelFailure> {
    let count = input.request.arguments.len();
    let shape = (1..=2).contains(&count)
        && input.request.logical_argument_count == count
        && input.selected.argument_types.len() == count;
    work.step().map_err(compile_failure)?;
    if !shape {
        // The third independent D domain requires the original default cast and
        // combined zip resource author. It is not a same-type rebinding.
        return Err(invalid(
            "LEAD/LAG default conversion and zip are not implemented",
        ));
    }
    for (actual, selected) in input
        .request
        .arguments
        .iter()
        .zip(&input.selected.argument_types)
    {
        let (
            FunctionArgument::Value {
                value_type: actual, ..
            },
            FunctionArgumentType::Value(selected),
        ) = (actual, selected)
        else {
            return Err(invalid("LEAD/LAG requires canonical value channels"));
        };
        if !actual.exactly_equals_observed::<KernelFailure>(selected, || {
            work.step().map_err(compile_failure)
        })? {
            return Err(invalid(
                "LEAD/LAG request differs from its complete selected source type",
            ));
        }
    }
    let FunctionArgumentType::Value(source) = &input.selected.argument_types[0] else {
        return Err(invalid("LEAD/LAG requires its canonical source value"));
    };
    let FunctionResultType::Scalar(result) = &input.selected.result_type else {
        return Err(invalid("LEAD/LAG requires a scalar result"));
    };
    let exact = result.nullable && result.logical_type == source.logical_type;
    work.step().map_err(compile_failure)?;
    if !exact
        || !novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
            &source.data_type,
            &result.data_type,
            || work.step().map_err(compile_failure),
        )?
    {
        return Err(invalid(
            "LEAD/LAG result differs from its complete nullable source domain",
        ));
    }
    let supported = options.frame().is_none_or(|frame| {
        frame.exclusion == novarocks_type_contract::WindowFrameExclusion::NoOthers
    });
    work.step().map_err(compile_failure)?;
    if !supported {
        return Err(invalid(
            "LEAD/LAG requires the existing NO OTHERS geometry ABI",
        ));
    }
    if count == 1 {
        return Ok(1);
    }
    let FunctionArgument::Value { value_type, .. } = &input.request.arguments[1] else {
        return Err(invalid(
            "LEAD/LAG offset requires a canonical value channel",
        ));
    };
    let canonical = value_type.data_type == arrow_schema::DataType::Int64
        && value_type.logical_type == novarocks_type_contract::ValueLogicalType::Physical;
    work.step().map_err(compile_failure)?;
    if !canonical {
        return Err(invalid(
            "LEAD/LAG offset differs from its exact I64 constant domain",
        ));
    }
    // Reuse the original complete CV source/type and observed selected-row
    // authors. Runtime Scalar shape never promotes a dynamic input to a literal.
    let binding_failure = |error| match error {
        FunctionBindingError::Control(cause) => compile_failure(cause),
        _ => invalid("LEAD/LAG offset constant differs from its checked source"),
    };
    work.flush().map_err(compile_failure)?;
    let value = super::catalogue::constant_source(input.request.arguments.get(1), work)
        .map_err(binding_failure)?
        .ok_or_else(|| invalid("LEAD/LAG offset must be a checked integer constant"))?;
    work.flush().map_err(compile_failure)?;
    let offset = value
        .int64_observed(CompilePhase::FunctionSpecialization, work.control())
        .map_err(FunctionBindingError::from)
        .map_err(binding_failure)?
        .ok_or_else(|| invalid("LEAD/LAG offset must be a non-NULL integer constant"))?;
    let nonnegative = offset >= 0;
    work.step().map_err(compile_failure)?;
    if !nonnegative {
        return Err(invalid("LEAD/LAG offset must be non-negative"));
    }
    Ok(offset)
}
