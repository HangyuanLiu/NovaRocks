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

//! Actual FE casts that change an authored logical value domain.
//!
//! The request's function catalog owns conversion selection. This module does
//! not infer identity from carriers or install execution implementations.

use novarocks_parser::Span;
use novarocks_type_contract::{FunctionValueType, preserves_nested_logical_identity};

use crate::analysis::{ExprKind, LiteralValue, TypedExpr, function_argument};
use crate::analyze_error::AnalyzeError;

impl super::AnalyzerContext<'_> {
    /// Preserve ordinary carrier CASTs, but select the exact conversion owner
    /// whenever the root or a nested logical identity changes. Literal NULL
    /// alone can use the existing typed-NULL declaration without evaluation.
    pub(super) fn cast_to_value_type(
        &self,
        source: TypedExpr,
        target: FunctionValueType,
        span: Span,
    ) -> Result<TypedExpr, AnalyzeError> {
        super::helpers::validate_value_type(&source.value_type, self.control)?;
        super::helpers::validate_value_type(&target, self.control)?;
        self.check_control()?;
        if source.value_type == target {
            return Ok(source);
        }
        // The shared identity walker is bounded by the validated value types;
        // its internal traversal remains opaque to this request control.
        if matches!(source.kind, ExprKind::Literal(LiteralValue::Null)) {
            let mut value_type = target;
            value_type.nullable = true;
            return Ok(TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Null),
                value_type,
            });
        }
        let carrier_cast = source.value_type.logical_type == target.logical_type
            && preserves_nested_logical_identity(&source.value_type.data_type, &target.data_type);
        self.check_control()?;
        if carrier_cast {
            return Ok(TypedExpr {
                kind: ExprKind::Cast {
                    target: target.data_type.clone(),
                    expr: Box::new(source),
                    decimal_overflow_policy: self
                        .sql_semantics
                        .sql_mode()
                        .decimal_overflow_policy(),
                },
                value_type: target,
            });
        }
        self.check_control()?;
        // Resolver internals are a bounded owner operation, not a claim of
        // cooperative traversal or runtime resource authorization.
        let result = convert_value_domain_with_catalog(
            self.function_catalog,
            source,
            target,
            self.sql_semantics.sql_mode().decimal_overflow_policy(),
            self.control,
        )
        .map_err(|error| error.at_type_mismatch(span))?;
        self.check_control()?;
        Ok(result)
    }
}

/// A narrow existing binder adapter: the caller owns request observation and
/// the supplied catalog remains the sole selected conversion authority.
pub(super) fn convert_value_domain_with_catalog(
    catalog: &dyn crate::compiler::SqlFunctionCatalog,
    source: TypedExpr,
    target: FunctionValueType,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<TypedExpr, AnalyzeError> {
    control
        .checkpoint(novarocks_type_contract::CompilePhase::Validate, 0)
        .map_err(AnalyzeError::control)?;
    if matches!(source.kind, ExprKind::Literal(LiteralValue::Null)) {
        let mut value_type = target;
        value_type.nullable = true;
        return Ok(TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Null),
            value_type,
        });
    }
    let intermediate =
        novarocks_functions::builtin::value_conversion::conversion_intermediate_type(
            &source.value_type,
            &target,
        )
        .map_err(|error| AnalyzeError::internal(error.to_string()))?
        .ok_or_else(|| {
            AnalyzeError::internal(
                "a logical-domain conversion requires an exact admitted implementation",
            )
        })?;
    let argument = function_argument(&source);
    let binding = catalog
        .resolve_value_conversion_binding(&argument, &intermediate, control)
        .map_err(AnalyzeError::function_binding)?;
    let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
    else {
        return Err(AnalyzeError::internal(
            "value conversion owner must produce a scalar result",
        ));
    };
    let value_type = result.clone();
    let converted = TypedExpr {
        value_type,
        kind: ExprKind::FunctionCall {
            name: novarocks_functions::builtin::value_conversion::VALUE_CONVERSION_NAME.to_string(),
            volatility: binding.semantics.volatility,
            binding: crate::binding::SqlFunctionBinding::new(binding, decimal_overflow_policy),
            args: vec![source],
            distinct: false,
        },
    };
    if converted.value_type.logical_type == target.logical_type
        && converted.value_type.nullable == target.nullable
        && novarocks_type_contract::arrow_data_types_exact(
            &converted.value_type.data_type,
            &target.data_type,
        )
    {
        Ok(converted)
    } else {
        Ok(TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(converted),
                target: target.data_type.clone(),
                decimal_overflow_policy,
            },
            value_type: target,
        })
    }
}
