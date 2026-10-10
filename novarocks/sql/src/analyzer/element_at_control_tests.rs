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

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use arrow::datatypes::DataType;
use novarocks_functions::{
    FunctionArgument, FunctionArgumentType, FunctionBindingError, FunctionResolutionError,
    FunctionResultType, FunctionValueType, FunctionVolatility, ResolvedAggregateSignature,
    ResolvedFunctionBinding, ResolvedFunctionSignature,
};
use novarocks_parser::ast;
use novarocks_type_contract::{CompileControlError, PureCompileControl};

use crate::analysis::{ExprKind, QueryBody, ResolvedQuery};
use crate::catalog::PlannerTableProvider;
use crate::compiler::SqlFunctionCatalog;

#[derive(Debug)]
struct EmptyCatalog;
impl PlannerTableProvider for EmptyCatalog {
    fn resolve_table_for_analysis(
        &self,
        _: Option<&str>,
        _: &str,
        table: &str,
    ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
        Err(format!("unexpected table: {table}"))
    }
}

#[derive(Clone, Debug)]
struct OneShotArrayControl {
    cause: CompileControlError,
    array_bindings: Arc<AtomicUsize>,
    refused: Arc<AtomicBool>,
    after_refusal: Arc<AtomicUsize>,
}
impl OneShotArrayControl {
    fn new(cause: CompileControlError) -> Self {
        Self {
            cause,
            array_bindings: Arc::new(AtomicUsize::new(0)),
            refused: Arc::new(AtomicBool::new(false)),
            after_refusal: Arc::new(AtomicUsize::new(0)),
        }
    }
    fn observe_delegate(&self) {
        if self.refused.load(Ordering::SeqCst) {
            self.after_refusal.fetch_add(1, Ordering::SeqCst);
        }
    }
}
impl SqlFunctionCatalog for OneShotArrayControl {
    fn snapshot(&self) -> Arc<dyn SqlFunctionCatalog> {
        self.observe_delegate();
        Arc::new(self.clone())
    }
    fn resolve_scalar_signature(
        &self,
        name: &str,
        args: &[DataType],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionSignature, FunctionResolutionError> {
        self.observe_delegate();
        crate::functions::builtin_sql_function_catalog()
            .resolve_scalar_signature(name, args, control)
    }
    fn resolve_scalar_binding(
        &self,
        name: &str,
        args: &[FunctionArgument],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.observe_delegate();
        if name == "__array_literal" && self.array_bindings.fetch_add(1, Ordering::SeqCst) == 0 {
            self.refused.store(true, Ordering::SeqCst);
            return Err(FunctionBindingError::Control(self.cause));
        }
        // A retry deliberately succeeds, so the fixture exposes a swallowed
        // first refusal instead of merely failing again for the same reason.
        crate::functions::builtin_sql_function_catalog().resolve_scalar_binding(name, args, control)
    }
    fn resolve_scalar_binding_with_expected_result(
        &self,
        name: &str,
        args: &[FunctionArgument],
        expected: &FunctionValueType,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.observe_delegate();
        crate::functions::builtin_sql_function_catalog()
            .resolve_scalar_binding_with_expected_result(name, args, expected, control)
    }
    fn resolve_value_conversion_binding(
        &self,
        arg: &FunctionArgument,
        target: &FunctionValueType,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.observe_delegate();
        crate::functions::builtin_sql_function_catalog()
            .resolve_value_conversion_binding(arg, target, control)
    }
    fn contains_aggregate(&self, name: &str) -> bool {
        self.observe_delegate();
        crate::functions::builtin_sql_function_catalog().contains_aggregate(name)
    }
    fn resolve_aggregate_signature(
        &self,
        name: &str,
        args: &[DataType],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        self.observe_delegate();
        crate::functions::builtin_sql_function_catalog()
            .resolve_aggregate_signature(name, args, control)
    }
    fn resolve_aggregate_trusted(
        &self,
        name: &str,
        args: &[DataType],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        self.observe_delegate();
        crate::functions::builtin_sql_function_catalog()
            .resolve_aggregate_trusted(name, args, control)
    }
    fn volatility(&self, name: &str) -> FunctionVolatility {
        self.observe_delegate();
        crate::functions::builtin_sql_function_catalog().volatility(name)
    }
}

fn analyze(
    sql: &str,
    functions: &dyn SqlFunctionCatalog,
) -> Result<ResolvedQuery, crate::analyze_error::AnalyzeError> {
    let statements = novarocks_parser::parse(sql).unwrap();
    let [ast::Statement::Query(query)] = statements.as_slice() else {
        panic!("query");
    };
    super::analyze_with_function_catalog(
        query,
        &EmptyCatalog,
        "default",
        functions,
        crate::constant::test_constant_policy(),
        &crate::compiler::SqlCompileControl::unbounded(),
    )
    .map(|result| result.0)
}

#[test]
fn element_at_type_probe_preserves_first_catalog_control_without_rebinding() {
    // This is the real parser/analyzer/catalog binding path; the test does
    // not install a CPU implementation or generalize analyzer control APIs.
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let functions = OneShotArrayControl::new(cause);
        let error = analyze("SELECT element_at([1,2],1)", &functions).unwrap_err();
        assert_eq!(error.control_error(), Some(cause));
        assert_eq!(error.span(), None);
        assert_eq!(functions.array_bindings.load(Ordering::SeqCst), 1);
        assert_eq!(functions.after_refusal.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn element_at_type_probe_routes_actual_array_and_map_sources_without_overload_guessing() {
    let query = analyze(
        "SELECT element_at([1,2],1), [1,2][1], element_at(map{1:2},1), map{1:2}[1]",
        crate::functions::builtin_sql_function_catalog(),
    )
    .unwrap();
    let QueryBody::Select(select) = query.body else {
        panic!("select");
    };
    let expected = [
        ("__array_element_at", DataType::Int8, DataType::Int64),
        ("__array_element_at", DataType::Int8, DataType::Int32),
        ("__map_element_at", DataType::Int64, DataType::Int64),
        ("__map_element_at", DataType::Int64, DataType::Int64),
    ];
    assert_eq!(select.projection.len(), expected.len());
    for (project, (name, result, index)) in select.projection.iter().zip(expected) {
        let ExprKind::FunctionCall {
            name: actual,
            binding,
            args,
            ..
        } = &project.expr.kind
        else {
            panic!("bound subscript");
        };
        assert_eq!(actual, name);
        assert_eq!(
            binding.function_id.as_str(),
            format!("builtin.scalar/{name}/v1")
        );
        assert_eq!(args.len(), 2);
        assert_eq!(binding.selected.argument_types.len(), 2);
        for (argument, selected) in args.iter().zip(binding.selected.argument_types.iter()) {
            assert_eq!(
                selected,
                &FunctionArgumentType::Value(argument.value_type.clone())
            );
        }
        let result = FunctionValueType::new(result, true);
        assert_eq!(project.expr.value_type, result);
        assert_eq!(
            binding.selected.result_type,
            FunctionResultType::Scalar(result)
        );
        assert_eq!(args[1].value_type, FunctionValueType::new(index, false));
        assert!(matches!(args[0].kind, ExprKind::FunctionCall { .. }));
    }
}
