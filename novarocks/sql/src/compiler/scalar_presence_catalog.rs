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
//! Candidate-request scalar implementation presence; this is not full support preparation.
use super::SqlFunctionCatalog;
use novarocks_functions::{
    FunctionBindingError, FunctionKind, FunctionSpecializationFailure, ResolvedFunctionBinding,
};
use novarocks_type_contract::PureCompileControl;
use std::sync::Arc;

#[derive(Clone, Debug)]
struct ScalarPresenceCatalog {
    original: Arc<dyn SqlFunctionCatalog>,
}

pub(super) fn scope(original: Arc<dyn SqlFunctionCatalog>) -> Arc<dyn SqlFunctionCatalog> {
    Arc::new(ScalarPresenceCatalog { original })
}
impl ScalarPresenceCatalog {
    fn admit_identity(
        &self,
        function: &novarocks_functions::FunctionId,
        kind: FunctionKind,
        overload: &novarocks_functions::FunctionOverloadId,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        // Table/operator, window and aggregate lifecycle admission have separate authors.
        if kind != FunctionKind::Scalar {
            return Ok(());
        }
        match self
            .original
            .pure_overload_declaration_observed(function, kind, overload, control)
        {
            Ok(_loan) => Ok(()),
            Err(FunctionSpecializationFailure::Control(cause)) => {
                Err(FunctionBindingError::Control(cause))
            }
            Err(FunctionSpecializationFailure::MissingPureImplementation(overload)) => {
                Err(FunctionBindingError::UnavailableImplementation(overload))
            }
            Err(FunctionSpecializationFailure::Binding(error)) => Err(error),
            Err(error) => Err(FunctionBindingError::InvalidBinding(
                error.to_string().into(),
            )),
        }
    }
    fn admit(
        &self,
        binding: ResolvedFunctionBinding,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.admit_identity(
            &binding.function_id,
            binding.kind,
            &binding.selected.overload,
            control,
        )?;
        Ok(binding)
    }
}
impl SqlFunctionCatalog for ScalarPresenceCatalog {
    fn admit_native_bitnot_source_observed(
        &self,
        source: &novarocks_functions::FunctionValueType,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        novarocks_functions::PreparedNativeBitNotRecipe::try_new(source, source, control)
            .map(|_| ())
            .map_err(|error| match error.control_error() {
                Some(cause) => FunctionBindingError::Control(cause),
                None => FunctionBindingError::InvalidBinding(error.to_string().into()),
            })
    }
    fn snapshot(&self) -> Arc<dyn SqlFunctionCatalog> {
        Arc::new(self.clone())
    }
    fn select_exact_overload_observed(
        &self,
        _function: &novarocks_functions::FunctionId,
        _kind: novarocks_functions::FunctionKind,
        _overload: &novarocks_functions::FunctionOverloadId,
        _request: novarocks_functions::FunctionBindingRequest<'_>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        Arc<novarocks_functions::FunctionBindingSelection>,
        novarocks_functions::FunctionBindingError,
    > {
        let selected = self
            .original
            .select_exact_overload_observed(_function, _kind, _overload, _request, control)?;
        self.admit_identity(_function, _kind, _overload, control)?;
        Ok(selected)
    }
    fn pure_overload_declaration_observed<'a>(
        &'a self,
        _function_id: &novarocks_functions::FunctionId,
        _kind: novarocks_functions::FunctionKind,
        _overload: &novarocks_functions::FunctionOverloadId,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::PureOverloadDeclaration<'a>,
        novarocks_functions::FunctionSpecializationFailure,
    > {
        self.original
            .pure_overload_declaration_observed(_function_id, _kind, _overload, control)
    }
    fn prepare_fresh_selected(
        &self,
        _input: novarocks_functions::CallEffectInput<'_>,
        _selected: Arc<novarocks_functions::FunctionBindingSelection>,
        _options: novarocks_functions::PureCallPreparation,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::PureCallSpecialization,
        novarocks_functions::FunctionSpecializationFailure,
    > {
        self.original
            .prepare_fresh_selected(_input, _selected, _options, control)
    }
    fn resolve_scalar_signature(
        &self,
        name: &str,
        arg_types: &[arrow::datatypes::DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        self.original
            .resolve_scalar_signature(name, arg_types, control)
    }
    fn resolve_scalar_binding(
        &self,
        _name: &str,
        _arguments: &[novarocks_functions::FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        let binding = self
            .original
            .resolve_scalar_binding(_name, _arguments, control)?;
        self.admit(binding, control)
    }
    fn resolve_scalar_binding_with_expected_result(
        &self,
        _name: &str,
        _arguments: &[novarocks_functions::FunctionArgument],
        _expected: &novarocks_functions::FunctionValueType,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        let binding = self
            .original
            .resolve_scalar_binding_with_expected_result(_name, _arguments, _expected, control)?;
        self.admit(binding, control)
    }
    fn resolve_value_conversion_binding(
        &self,
        _argument: &novarocks_functions::FunctionArgument,
        _target: &novarocks_functions::FunctionValueType,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        let binding = self
            .original
            .resolve_value_conversion_binding(_argument, _target, control)?;
        self.admit(binding, control)
    }
    fn resolve_window_binding(
        &self,
        _name: &str,
        _arguments: &[novarocks_functions::FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        self.original
            .resolve_window_binding(_name, _arguments, control)
    }
    fn resolve_table_binding(
        &self,
        _name: &str,
        _arguments: &[novarocks_functions::FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        self.original
            .resolve_table_binding(_name, _arguments, control)
    }
    fn contains_aggregate(&self, name: &str) -> bool {
        self.original.contains_aggregate(name)
    }
    fn resolve_aggregate_binding(
        &self,
        _name: &str,
        _logical_argument_count: usize,
        _arguments: &[novarocks_functions::FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        self.original
            .resolve_aggregate_binding(_name, _logical_argument_count, _arguments, control)
    }
    fn resolve_aggregate_binding_trusted(
        &self,
        name: &str,
        logical_argument_count: usize,
        arguments: &[novarocks_functions::FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        self.original.resolve_aggregate_binding_trusted(
            name,
            logical_argument_count,
            arguments,
            control,
        )
    }
    fn resolve_aggregate_signature(
        &self,
        name: &str,
        arg_types: &[arrow::datatypes::DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedAggregateSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        self.original
            .resolve_aggregate_signature(name, arg_types, control)
    }
    fn resolve_aggregate_update_signature(
        &self,
        name: &str,
        logical_arg_types: &[arrow::datatypes::DataType],
        update_arg_types: &[arrow::datatypes::DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedAggregateSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        self.original.resolve_aggregate_update_signature(
            name,
            logical_arg_types,
            update_arg_types,
            control,
        )
    }
    fn resolve_aggregate_trusted(
        &self,
        name: &str,
        arg_types: &[arrow::datatypes::DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedAggregateSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        self.original
            .resolve_aggregate_trusted(name, arg_types, control)
    }
    fn volatility(&self, name: &str) -> novarocks_functions::FunctionVolatility {
        self.original.volatility(name)
    }
    fn snapshot_for_scalar_presence(&self) -> Arc<dyn SqlFunctionCatalog> {
        Arc::new(self.clone())
    }
}
