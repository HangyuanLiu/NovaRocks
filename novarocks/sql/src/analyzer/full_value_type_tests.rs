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

use std::cell::RefCell;
use std::num::{NonZeroU32, NonZeroU64};
use std::rc::Rc;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields};
use novarocks_parser::ast;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl, ValueLogicalType,
};
use novarocks_types::schema::{ColumnDef, SqlType};

use super::{helpers, scope::AnalyzerScope};
use crate::analysis::{ExprKind, QueryBody};
use crate::catalog::PlannerTableProvider;
use crate::column_id::ColumnRefFactory;

fn column(name: &str, carrier: DataType, logical: Option<SqlType>) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        data_type: carrier,
        nullable: false,
        write_default: None,
        logical_type: logical,
    }
}

struct SourceCatalog;
impl PlannerTableProvider for SourceCatalog {
    fn resolve_table_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
        use crate::binding::{SqlTableBindingId, SqlTableBindingScopeId};
        use crate::planner::table::{
            ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector,
            TableDef,
        };
        let columns = vec![
            column("j", DataType::Utf8, Some(SqlType::Json)),
            column("h", DataType::Binary, Some(SqlType::Hll)),
            column("b", DataType::Binary, Some(SqlType::Bitmap)),
            column("v", DataType::LargeBinary, Some(SqlType::Variant)),
            column("i", DataType::FixedSizeBinary(16), Some(SqlType::LargeInt)),
            column("raw", DataType::FixedSizeBinary(16), None),
            column(
                "ja",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                Some(SqlType::Array(Box::new(SqlType::Json))),
            ),
            column(
                "ja_meta",
                DataType::List(Arc::new(
                    Field::new("source_item", DataType::Utf8, false).with_metadata(
                        [("provider".to_string(), "retained-source".to_string())].into(),
                    ),
                )),
                Some(SqlType::Array(Box::new(SqlType::Json))),
            ),
        ];
        let planner = TableDef {
            name: table.into(),
            columns,
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::Sql(SqlScanSource::new(
                SqlTableBindingId::new(
                    SqlTableBindingScopeId::new(NonZeroU64::new(811).unwrap()),
                    NonZeroU32::new(1).unwrap(),
                ),
                SqlTableIdentity {
                    catalog: catalog.unwrap_or("default_catalog").into(),
                    namespace: database.into(),
                    table: table.into(),
                },
                SqlScanKind::Data {
                    version: SqlTableVersionSelector::Current,
                },
            )),
        };
        Ok(crate::catalog::ResolvedAnalyzerTable::from_planner(
            catalog, database, planner,
        ))
    }
}

fn analyze(sql: &str) -> crate::analysis::ResolvedQuery {
    let statements = novarocks_parser::parse(sql).unwrap();
    let [ast::Statement::Query(query)] = statements.as_slice() else {
        panic!("expected query");
    };
    super::analyze(query, &SourceCatalog, "default").unwrap().0
}

#[test]
fn catalog_root_identity_survives_projection_alias_derived_cte_and_outer_padding() {
    let expected = [
        ValueLogicalType::Json,
        ValueLogicalType::Hll,
        ValueLogicalType::Bitmap,
        ValueLogicalType::Variant,
        ValueLogicalType::LargeInt,
        ValueLogicalType::Physical,
    ];
    for sql in [
        "SELECT j,h,b,v,i,raw FROM source",
        "SELECT q.j,q.h,q.b,q.v,q.i,q.raw FROM (SELECT j,h,b,v,i,raw FROM source) q",
        "WITH q AS (SELECT j,h,b,v,i,raw FROM source) SELECT j,h,b,v,i,raw FROM q",
        "SELECT r.j,r.h,r.b,r.v,r.i,r.raw FROM source l LEFT JOIN source r ON 1=1",
    ] {
        let resolved = analyze(sql);
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select");
        };
        for ((output, project), logical) in resolved
            .output_columns
            .iter()
            .zip(&select.projection)
            .zip(expected)
        {
            assert_eq!(output.value_type.logical_type, logical, "{sql}");
            assert_eq!(project.expr.value_type, output.value_type, "{sql}");
            if sql.contains("LEFT JOIN") {
                assert!(output.value_type.nullable);
            }
        }
    }
}

#[test]
fn actual_field_domain_reaches_lambda_parameter_body_and_bound_result() {
    let resolved = analyze("SELECT array_map(x -> x, ja) FROM source");
    let QueryBody::Select(select) = resolved.body else {
        panic!("expected select");
    };
    let expression = &select.projection[0].expr;
    let ExprKind::FunctionCall { args, binding, .. } = &expression.kind else {
        panic!("expected actual call");
    };
    let ExprKind::LambdaFunction { params, body } = &args[0].kind else {
        panic!("expected lambda");
    };
    assert_eq!(params[0].value_type.logical_type, ValueLogicalType::Json);
    assert_eq!(body.value_type, params[0].value_type);
    let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
    else {
        panic!("expected scalar");
    };
    assert_eq!(&expression.value_type, result);
    let DataType::List(item) = &result.data_type else {
        panic!("expected list");
    };
    assert_eq!(
        novarocks_type_contract::field_logical_type(item),
        Ok(ValueLogicalType::Json)
    );
}

#[test]
fn unnest_uses_complete_selected_relation_domain() {
    let resolved = analyze("SELECT u.* FROM source, UNNEST(ja) u");
    assert_eq!(
        resolved.output_columns[0].value_type.logical_type,
        ValueLogicalType::Json
    );
}

#[test]
fn explicit_largeint_and_typed_null_are_authored_by_syntax_not_carrier() {
    for sql in [
        "SELECT 9223372036854775808",
        "SELECT CAST(NULL AS LARGEINT)",
    ] {
        let resolved = analyze(sql);
        assert_eq!(
            resolved.output_columns[0].value_type.logical_type,
            ValueLogicalType::LargeInt
        );
    }
    assert_eq!(
        analyze("SELECT raw FROM source").output_columns[0]
            .value_type
            .logical_type,
        ValueLogicalType::Physical
    );
    assert_eq!(
        analyze("SELECT CAST(NULL AS JSON)").output_columns[0]
            .value_type
            .logical_type,
        ValueLogicalType::Json
    );
    assert_eq!(
        analyze("SELECT CAST('not JSON' AS JSON)").output_columns[0]
            .value_type
            .logical_type,
        ValueLogicalType::Physical
    );
}

#[test]
fn empty_array_declares_expected_result_only_to_its_exact_owner() {
    let resolved = analyze("SELECT ARRAY<JSON>[]");
    let QueryBody::Select(select) = resolved.body else {
        panic!("expected select");
    };
    let expression = &select.projection[0].expr;
    assert!(!expression.value_type.nullable);
    let ExprKind::FunctionCall { args, binding, .. } = &expression.kind else {
        panic!("expected direct bound literal owner");
    };
    assert!(args.is_empty());
    let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
    else {
        panic!("expected scalar");
    };
    assert_eq!(&expression.value_type, result);
    let DataType::List(item) = &result.data_type else {
        panic!("expected list");
    };
    assert_eq!(
        novarocks_type_contract::field_logical_type(item),
        Ok(ValueLogicalType::Json)
    );
    assert_eq!(item.data_type(), &DataType::Utf8);
}

struct StopAt {
    error: CompileControlError,
    at_entry: bool,
}
impl PureCompileControl for StopAt {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        if self.at_entry || units == 256 {
            Err(self.error)
        } else {
            Ok(())
        }
    }
}

#[test]
fn declared_source_validation_preserves_typed_control_at_entry_and_inside_fields() {
    let source = column(
        "wide",
        DataType::Struct(Fields::from(
            (0..320)
                .map(|i| Field::new(format!("f{i}"), DataType::Utf8, true))
                .collect::<Vec<_>>(),
        )),
        None,
    );
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at_entry in [true, false] {
            let failure =
                helpers::column_value_type(&source, &StopAt { error, at_entry }).unwrap_err();
            assert_eq!(failure.control_error(), Some(error));
        }
    }
    let validated =
        helpers::column_value_type(&source, crate::optimizer::test_optimizer_control()).unwrap();
    assert_eq!(validated.data_type, source.data_type);
}

#[test]
fn conflicting_catalog_tag_is_rejected_without_carrier_guess() {
    let source = column("j", DataType::FixedSizeBinary(16), Some(SqlType::Json));
    assert!(
        helpers::column_value_type(&source, crate::optimizer::test_optimizer_control()).is_err()
    );
    let source = column("plain", DataType::FixedSizeBinary(16), None);
    assert_eq!(
        helpers::column_value_type(&source, crate::optimizer::test_optimizer_control())
            .unwrap()
            .logical_type,
        ValueLogicalType::Physical
    );
}

#[test]
fn scope_padding_preserves_source_and_parameter_domains_without_retagging_factory() {
    let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
    let mut scope = AnalyzerScope::new(factory.clone());
    let ty =
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap();
    let id = scope.add_column(Some("t"), "j", ty.clone());
    scope.mark_all_nullable();
    assert_eq!(
        scope.resolve_value_type(Some("t"), "j").unwrap().1,
        helpers::with_nullability(ty.clone(), true)
    );
    assert_eq!(factory.borrow().value_type(id), Some(&ty));
}

#[test]
fn assignment_common_owner_preserves_complete_same_domain_and_null_sources() {
    let control = crate::optimizer::test_optimizer_control();
    for (carrier, logical) in [
        (DataType::Utf8, ValueLogicalType::Json),
        (DataType::Binary, ValueLogicalType::Hll),
        (DataType::Binary, ValueLogicalType::Bitmap),
        (DataType::LargeBinary, ValueLogicalType::Variant),
        (DataType::FixedSizeBinary(16), ValueLogicalType::LargeInt),
        (DataType::FixedSizeBinary(16), ValueLogicalType::Physical),
    ] {
        let source = FunctionValueType::try_with_logical_type(carrier, false, logical).unwrap();
        let nil = FunctionValueType::new(DataType::Null, true);
        assert_eq!(
            helpers::assignment_common_value_type(&source, &nil, control).unwrap(),
            helpers::with_nullability(source.clone(), true)
        );
        assert_eq!(
            helpers::assignment_common_value_type(&nil, &source, control).unwrap(),
            helpers::with_nullability(source.clone(), true)
        );
        assert_eq!(
            helpers::assignment_common_value_type(&source, &source, control).unwrap(),
            source
        );
    }
    let left = column(
        "nested",
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
        Some(SqlType::Array(Box::new(SqlType::Json))),
    );
    let right = column(
        "nested",
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        Some(SqlType::Array(Box::new(SqlType::Json))),
    );
    let left = helpers::column_value_type(&left, control).unwrap();
    let right = helpers::column_value_type(&right, control).unwrap();
    let common = helpers::assignment_common_value_type(&left, &right, control).unwrap();
    let DataType::List(item) = &common.data_type else {
        panic!("expected list");
    };
    assert!(item.is_nullable());
    assert_eq!(
        novarocks_type_contract::field_logical_type(item),
        Ok(ValueLogicalType::Json)
    );
}

#[test]
fn case_union_and_values_use_the_same_authored_source_domains() {
    for sql in [
        "SELECT CASE WHEN TRUE THEN j ELSE NULL END FROM source",
        "SELECT j FROM source UNION ALL SELECT NULL",
        "VALUES (parse_json('{}')),(NULL)",
    ] {
        assert_eq!(
            analyze(sql).output_columns[0].value_type.logical_type,
            ValueLogicalType::Json,
            "{sql}"
        );
    }
}

#[test]
fn scalar_aggregate_and_window_outputs_are_complete_selected_owner_facts() {
    for sql in [
        "SELECT parse_json('{}')",
        "SELECT array_agg(j) FROM source",
        "SELECT first_value(j) OVER () FROM source",
    ] {
        let query = analyze(sql);
        let QueryBody::Select(select) = query.body else {
            panic!("expected select");
        };
        let expression = &select.projection[0].expr;
        let binding = match &expression.kind {
            ExprKind::FunctionCall { binding, .. } | ExprKind::WindowCall { binding, .. } => {
                binding
            }
            ExprKind::AggregateCall { resolved, .. } => resolved,
            _ => panic!("expected exact bound call"),
        };
        let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
        else {
            panic!("expected scalar");
        };
        assert_eq!(&expression.value_type, result, "{sql}");
        assert_eq!(&query.output_columns[0].value_type, result, "{sql}");
    }
}

fn projection(sql: &str) -> crate::analysis::TypedExpr {
    let QueryBody::Select(mut select) = analyze(sql).body else {
        panic!("expected select");
    };
    select.projection.remove(0).expr
}

fn assert_selected_conversion(expression: &crate::analysis::TypedExpr, suffix: &str) {
    let ExprKind::FunctionCall { binding, args, .. } = &expression.kind else {
        panic!(
            "expected selected value conversion, got {:?}",
            expression.kind
        );
    };
    assert_eq!(
        binding.function_id.as_str(),
        novarocks_functions::builtin::value_conversion::VALUE_CONVERSION_FUNCTION_ID
    );
    assert!(binding.selected.overload.as_str().ends_with(suffix));
    let [novarocks_functions::FunctionArgumentType::Value(source)] =
        binding.selected.argument_types.as_ref()
    else {
        panic!("expected exact value argument");
    };
    assert_eq!(source, &args[0].value_type);
    let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
    else {
        panic!("expected scalar result");
    };
    assert_eq!(result, &expression.value_type);
}

#[test]
fn explicit_logical_casts_use_exact_selected_conversion_bindings() {
    for (sql, suffix, logical) in [
        (
            "SELECT CAST(j AS VARCHAR) FROM source",
            "json_text_same_structure/v1",
            ValueLogicalType::Physical,
        ),
        (
            "SELECT CAST(i AS BIGINT) FROM source",
            "largeint_to_signed_null_overflow/v1",
            ValueLogicalType::Physical,
        ),
        (
            "SELECT CAST(i AS DOUBLE) FROM source",
            "largeint_to_float_round/v1",
            ValueLogicalType::Physical,
        ),
        (
            "SELECT CAST(1 AS LARGEINT)",
            "signed_to_largeint/v1",
            ValueLogicalType::LargeInt,
        ),
    ] {
        let expression = projection(sql);
        assert_selected_conversion(&expression, suffix);
        assert_eq!(expression.value_type.logical_type, logical);
    }
    assert!(matches!(
        projection("SELECT CAST(j AS JSON) FROM source").kind,
        ExprKind::Cast { .. }
    ));
    assert!(matches!(
        projection("SELECT CAST(1 AS BIGINT)").kind,
        ExprKind::Cast { .. }
    ));
}

#[test]
fn mixed_case_and_typed_array_convert_actual_json_sources() {
    let expression = projection("SELECT CASE WHEN TRUE THEN j ELSE 'text' END FROM source");
    let ExprKind::Case { when_then, .. } = expression.kind else {
        panic!("expected CASE");
    };
    assert_selected_conversion(&when_then[0].1, "json_text_same_structure/v1");
    let expression = projection("SELECT ARRAY<VARCHAR>[j] FROM source");
    let ExprKind::FunctionCall { args, .. } = expression.kind else {
        panic!("expected array literal owner");
    };
    assert_selected_conversion(&args[0], "json_text_same_structure/v1");
}

#[test]
fn only_literal_null_is_declared_without_running_its_source() {
    let literal = projection("SELECT CAST(NULL AS JSON)");
    assert!(matches!(
        literal.kind,
        ExprKind::Literal(crate::analysis::LiteralValue::Null)
    ));
    assert_eq!(literal.value_type.logical_type, ValueLogicalType::Json);
    assert!(literal.value_type.nullable);
    let expression = projection("SELECT CAST(element_at([], 1) AS JSON)");
    assert_selected_conversion(&expression, "null_to_typed_nullable/v1");
    let ExprKind::FunctionCall { args, .. } = expression.kind else {
        panic!("expected evaluated NULL lift");
    };
    assert!(matches!(args[0].kind, ExprKind::FunctionCall { .. }));
    assert_eq!(args[0].value_type.data_type, DataType::Null);
}

#[test]
fn hidden_conversion_cannot_be_selected_as_a_user_function() {
    let statements =
        novarocks_parser::parse("SELECT __value_domain_conversion(j) FROM source").unwrap();
    let [ast::Statement::Query(query)] = statements.as_slice() else {
        panic!("expected query");
    };
    assert!(super::analyze(query, &SourceCatalog, "default").is_err());
}

#[derive(Debug)]
struct MissingConversionPort;
impl crate::compiler::SqlFunctionCatalog for MissingConversionPort {
    fn snapshot(&self) -> Arc<dyn crate::compiler::SqlFunctionCatalog> {
        Arc::new(Self)
    }
    fn resolve_scalar_signature(
        &self,
        name: &str,
        args: &[DataType],
    ) -> Result<
        novarocks_functions::ResolvedFunctionSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        crate::functions::builtin_sql_function_catalog().resolve_scalar_signature(name, args)
    }
    fn contains_aggregate(&self, name: &str) -> bool {
        crate::functions::builtin_sql_function_catalog().contains_aggregate(name)
    }
    fn resolve_aggregate_signature(
        &self,
        name: &str,
        args: &[DataType],
    ) -> Result<
        novarocks_functions::ResolvedAggregateSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        crate::functions::builtin_sql_function_catalog().resolve_aggregate_signature(name, args)
    }
    fn resolve_aggregate_trusted(
        &self,
        name: &str,
        args: &[DataType],
    ) -> Result<
        novarocks_functions::ResolvedAggregateSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        crate::functions::builtin_sql_function_catalog().resolve_aggregate_trusted(name, args)
    }
    fn volatility(&self, name: &str) -> novarocks_functions::FunctionVolatility {
        crate::functions::builtin_sql_function_catalog().volatility(name)
    }
}

#[test]
fn actual_request_catalog_missing_conversion_port_has_no_fallback() {
    let statements = novarocks_parser::parse("SELECT CAST(j AS VARCHAR) FROM source").unwrap();
    let [ast::Statement::Query(query)] = statements.as_slice() else {
        panic!("expected query");
    };
    let error = super::analyze_with_function_catalog(
        query,
        &SourceCatalog,
        "default",
        &MissingConversionPort,
        &crate::compiler::SqlCompileControl::unbounded(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("binding declaration"), "{error}");
}

#[test]
fn selected_scalar_text_argument_converts_source_json_before_rebinding() {
    for sql in [
        "SELECT parse_json(j) FROM source",
        "SELECT length(j) FROM source",
    ] {
        let expression = projection(sql);
        let ExprKind::FunctionCall { binding, args, .. } = expression.kind else {
            panic!("expected scalar owner");
        };
        assert_ne!(
            binding.function_id.as_str(),
            novarocks_functions::builtin::value_conversion::VALUE_CONVERSION_FUNCTION_ID
        );
        assert_selected_conversion(&args[0], "json_text_same_structure/v1");
        assert_eq!(args[0].value_type.logical_type, ValueLogicalType::Physical);
    }
}

#[test]
fn nested_json_conversion_preserves_source_facts_before_same_domain_cast() {
    let expression = projection(
        "SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ CAST(ja_meta AS ARRAY<VARCHAR>) FROM source",
    );
    let ExprKind::Cast {
        expr,
        decimal_overflow_policy,
        ..
    } = &expression.kind
    else {
        panic!("expected final carrier cast");
    };
    assert_eq!(
        *decimal_overflow_policy,
        novarocks_type_contract::DecimalOverflowPolicy::ReportError
    );
    assert_selected_conversion(expr, "json_text_same_structure/v1");
    let DataType::List(item) = &expr.value_type.data_type else {
        panic!("expected exact intermediate list");
    };
    assert_eq!(item.name(), "source_item");
    assert!(!item.is_nullable());
    assert_eq!(
        item.metadata().get("provider").map(String::as_str),
        Some("retained-source")
    );
    assert_eq!(
        novarocks_type_contract::field_logical_type(item),
        Ok(ValueLogicalType::Physical)
    );
    let DataType::List(target) = &expression.value_type.data_type else {
        panic!("expected final list");
    };
    assert!(target.is_nullable());
    assert_eq!(
        novarocks_type_contract::field_logical_type(target),
        Ok(ValueLogicalType::Physical)
    );
}
