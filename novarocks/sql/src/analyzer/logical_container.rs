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

//! Closed owner-bound wrapper projections. An output adapter adds semantic
//! field markers without modifying the executable selected overload.

use super::*;
use crate::analysis::LiteralValue;
use novarocks_functions::{FunctionKind, ResolvedFunctionBinding};
use novarocks_types::logical::NR_LOGICAL_TYPE_KEY;
use std::sync::Arc;

pub(super) fn argument_source(source: Option<&ast::Expr>, index: usize) -> Option<&ast::Expr> {
    match source? {
        ast::Expr::FunctionCall(call) => call.arguments.get(index),
        ast::Expr::Array(array) => array.elements.get(index),
        ast::Expr::Map(map) => map.entries.get(index / 2).map(|entry| {
            if index.is_multiple_of(2) {
                &entry.key
            } else {
                &entry.value
            }
        }),
        ast::Expr::Nested(nested) => argument_source(Some(&nested.expression), index),
        ast::Expr::Access(access) if index == 0 => Some(&access.expr),
        _ => None,
    }
}

fn call(expression: &TypedExpr) -> Option<(&ResolvedFunctionBinding, &[TypedExpr])> {
    match &expression.kind {
        ExprKind::FunctionCall { binding, args, .. } if binding.kind == FunctionKind::Scalar => {
            Some((binding, args))
        }
        ExprKind::AggregateCall { resolved, args, .. }
            if resolved.kind == FunctionKind::Aggregate =>
        {
            Some((resolved, args))
        }
        ExprKind::WindowCall {
            aggregate_binding: Some(binding),
            args,
            ..
        } if binding.kind == FunctionKind::Aggregate => Some((binding, args)),
        ExprKind::Cast { expr, .. } => call(expr),
        _ => None,
    }
}

fn supported(id: &str) -> bool {
    matches!(
        id,
        "builtin.scalar/__array_literal/v1"
            | "builtin.aggregate/array_agg/v1"
            | "builtin.aggregate/array_agg_distinct/v1"
            | "builtin.aggregate/map_agg/v1"
            | "builtin.scalar/map/v1"
            | "builtin.scalar/map_from_arrays/v1"
            | "builtin.scalar/row/v1"
            | "builtin.scalar/struct/v1"
            | "builtin.scalar/named_struct/v1"
            | "builtin.scalar/__struct_subfield/v1"
            | "builtin.scalar/__array_element_at/v1"
            | "builtin.scalar/__map_element_at/v1"
            | "builtin.scalar/array_min/v1"
            | "builtin.scalar/array_max/v1"
            | "builtin.scalar/map_keys/v1"
            | "builtin.scalar/map_values/v1"
            | "builtin.scalar/array_sortby/v1"
            | "builtin.scalar/array_sort/v1"
            | "builtin.scalar/array_distinct/v1"
            | "builtin.scalar/array_slice/v1"
    )
}

impl AnalyzerContext<'_> {
    fn wrapper_input_type(
        &self,
        source: Option<&ast::Expr>,
        value: &TypedExpr,
        scope: &AnalyzerScope,
    ) -> Option<SqlType> {
        self.logical_output_type(source, value, scope)
            .or_else(|| ordinary_type(&value.data_type, false))
    }

    pub(super) fn container_output_type(
        &self,
        source: Option<&ast::Expr>,
        expression: &TypedExpr,
        scope: &AnalyzerScope,
    ) -> Option<SqlType> {
        let (binding, args) = call(expression)?;
        let id = binding.function_id.as_str();
        if !supported(id) {
            return None;
        }
        // An explicit literal item type owns the output's semantic choice.
        // In particular an explicit VARCHAR literal cannot regain Json merely
        // because its original values came from JSON producers. JSON's separate
        // source witness preserves the empty/all-null literal case as well.
        if id == "builtin.scalar/__array_literal/v1"
            && let Some(ast::Expr::Array(array)) = source
            && array.element_type.is_some()
        {
            return if self.json_list_provenance(source, expression, scope) {
                Some(SqlType::Array(Box::new(SqlType::Json)))
            } else {
                ordinary_type(&expression.data_type, false)
            };
        }
        let input =
            |i: usize| self.wrapper_input_type(argument_source(source, i), args.get(i)?, scope);
        let item = |i: usize| match input(i)? {
            SqlType::Array(item) => Some(*item),
            _ => None,
        };
        let map = |i: usize| match input(i)? {
            SqlType::Map(key, value) => Some((*key, *value)),
            _ => None,
        };
        let merged = |indices: &[usize], fallback: &DataType| {
            let mut domain = None;
            for &i in indices {
                let value = args.get(i)?;
                if is_null_value(argument_source(source, i), value) {
                    continue;
                }
                let current = input(i)?;
                if domain.as_ref().is_some_and(|previous| *previous != current) {
                    return ordinary_type(fallback, false);
                }
                domain = Some(current);
            }
            domain.or_else(|| ordinary_type(fallback, false))
        };
        match id {
            "builtin.scalar/__array_literal/v1"
            | "builtin.aggregate/array_agg/v1"
            | "builtin.aggregate/array_agg_distinct/v1" => {
                let DataType::List(field) = &expression.data_type else {
                    return None;
                };
                let indices = if id == "builtin.scalar/__array_literal/v1" {
                    (0..args.len()).collect::<Vec<_>>()
                } else {
                    vec![0]
                };
                merged(&indices, field.data_type()).map(|item| SqlType::Array(Box::new(item)))
            }
            "builtin.scalar/map/v1" | "builtin.aggregate/map_agg/v1" => {
                let DataType::Map(entries, _) = &expression.data_type else {
                    return None;
                };
                let DataType::Struct(fields) = entries.data_type() else {
                    return None;
                };
                if fields.len() != 2 {
                    return None;
                }
                let keys = (0..args.len()).step_by(2).collect::<Vec<_>>();
                let values = (1..args.len()).step_by(2).collect::<Vec<_>>();
                Some(SqlType::Map(
                    Box::new(merged(&keys, fields[0].data_type())?),
                    Box::new(merged(&values, fields[1].data_type())?),
                ))
            }
            "builtin.scalar/map_from_arrays/v1" => {
                Some(SqlType::Map(Box::new(item(0)?), Box::new(item(1)?)))
            }
            "builtin.scalar/row/v1"
            | "builtin.scalar/struct/v1"
            | "builtin.scalar/named_struct/v1" => {
                let DataType::Struct(fields) = &expression.data_type else {
                    return None;
                };
                let named = id == "builtin.scalar/named_struct/v1";
                if fields.len() != if named { args.len() / 2 } else { args.len() } {
                    return None;
                }
                let fields = fields
                    .iter()
                    .enumerate()
                    .map(|(i, field)| {
                        let arg = if named { 2 * i + 1 } else { i };
                        Some((field.name().clone(), input(arg)?))
                    })
                    .collect::<Option<Vec<_>>>()?;
                Some(SqlType::Struct(fields))
            }
            "builtin.scalar/__struct_subfield/v1" => {
                let ExprKind::Literal(LiteralValue::String(name)) = &args.get(1)?.kind else {
                    return None;
                };
                if let Some(SqlType::Struct(fields)) = input(0) {
                    fields
                        .into_iter()
                        .find(|(field, _)| field.eq_ignore_ascii_case(name))
                        .map(|(_, ty)| ty)
                } else {
                    let DataType::Struct(fields) = &args.first()?.data_type else {
                        return None;
                    };
                    fields
                        .iter()
                        .find(|field| field.name().eq_ignore_ascii_case(name))
                        .and_then(|field| field_type(field))
                }
            }
            "builtin.scalar/__array_element_at/v1"
            | "builtin.scalar/array_min/v1"
            | "builtin.scalar/array_max/v1" => item(0).or_else(|| {
                let DataType::List(field) = &args.first()?.data_type else {
                    return None;
                };
                field_type(field)
            }),
            "builtin.scalar/__map_element_at/v1"
            | "builtin.scalar/map_keys/v1"
            | "builtin.scalar/map_values/v1" => {
                let index = usize::from(id != "builtin.scalar/map_keys/v1");
                let logical = map(0)
                    .map(|(key, value)| if index == 0 { key } else { value })
                    .or_else(|| {
                        let DataType::Map(entries, _) = &args.first()?.data_type else {
                            return None;
                        };
                        let DataType::Struct(fields) = entries.data_type() else {
                            return None;
                        };
                        field_type(fields.get(index)?)
                    })?;
                Some(if id == "builtin.scalar/__map_element_at/v1" {
                    logical
                } else {
                    SqlType::Array(Box::new(logical))
                })
            }
            "builtin.scalar/array_sortby/v1"
            | "builtin.scalar/array_sort/v1"
            | "builtin.scalar/array_distinct/v1"
            | "builtin.scalar/array_slice/v1" => input(0),
            _ => None,
        }
        .filter(|logical| logical_carrier_matches(logical, &expression.data_type))
    }

    pub(in crate::analyzer) fn adapt_bound_output_domains(
        &self,
        expression: TypedExpr,
        source: Option<&ast::Expr>,
        scope: &AnalyzerScope,
        span: Span,
    ) -> Result<TypedExpr, AnalyzeError> {
        // Public CAST semantics are owned by their existing analyzer path.
        if matches!(source, Some(ast::Expr::Cast(_))) {
            return Ok(expression);
        }
        let Some((binding, args)) = call(&expression) else {
            return Ok(expression);
        };
        if !supported(binding.function_id.as_str()) {
            return Ok(expression);
        }
        // The complete input/output walk precedes allocation of the adapted
        // schema. An adapter cannot wash an unknown marker or a conflicting
        // trusted catalog declaration into an ordinary type.
        for (i, value) in args.iter().enumerate() {
            validate_markers(&value.data_type)
                .map_err(|message| AnalyzeError::type_mismatch(message, span))?;
            let logical = self.logical_output_type(argument_source(source, i), value, scope);
            if value.data_type == DataType::LargeBinary && logical.is_none() {
                return Err(AnalyzeError::type_mismatch(
                    "wrapper input LargeBinary requires its original logical identity",
                    span,
                ));
            }
            if let Some(logical) = logical {
                let json_list_witness = matches!(&logical, SqlType::Array(item) if item.as_ref() == &SqlType::Json)
                    && self.json_list_provenance(argument_source(source, i), value, scope);
                if json_list_witness && !witnessed_json_list_source(value) {
                    return Err(AnalyzeError::type_mismatch(
                        "wrapper input logical identity differs from its declared nested fields",
                        span,
                    ));
                }
                let mut candidate = value;
                loop {
                    if nested_source_matches(&logical, &candidate.data_type)
                        || (json_list_witness && witnessed_json_list_fields(&candidate.data_type))
                    {
                        break;
                    }
                    match &candidate.kind {
                        ExprKind::Cast { expr, .. } => candidate = expr,
                        _ => {
                            return Err(AnalyzeError::type_mismatch(
                                "wrapper input logical identity differs from its declared nested fields",
                                span,
                            ));
                        }
                    }
                }
            }
        }
        validate_markers(&expression.data_type)
            .map_err(|message| AnalyzeError::type_mismatch(message, span))?;
        let target = if let Some(logical) = self.logical_output_type(source, &expression, scope) {
            if nested_source_matches(&logical, &expression.data_type) {
                return Ok(expression);
            }
            project_type(&expression.data_type, &logical)
        } else if let Some(target) = self.partial_wrapper_type(source, &expression, scope) {
            target
        } else {
            return Ok(expression);
        };
        validate_markers(&target).map_err(|message| AnalyzeError::type_mismatch(message, span))?;
        if target == expression.data_type {
            return Ok(expression);
        }
        let nullable = expression.nullable;
        Ok(TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(expression),
                target: target.clone(),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            data_type: target,
            nullable,
        })
    }

    // SqlType has no Null leaf. A partial wrapper still preserves independent
    // proven siblings in the actual fields; it never invents a Null SqlType or
    // loses an opaque value just because another field is a Null constant.
    fn partial_wrapper_type(
        &self,
        source: Option<&ast::Expr>,
        expression: &TypedExpr,
        scope: &AnalyzerScope,
    ) -> Option<DataType> {
        let (binding, args) = call(expression)?;
        let id = binding.function_id.as_str();
        let project = |field: &Field, i: usize| match args
            .get(i)
            .and_then(|value| self.wrapper_input_type(argument_source(source, i), value, scope))
        {
            Some(logical) => project_field(field, &logical),
            None => Arc::new(field.clone()),
        };
        let merged = |field: &Field, indices: Vec<usize>| {
            let mut domain = None;
            let mut conflict = false;
            for i in indices {
                let value = &args[i];
                if is_null_value(argument_source(source, i), value) {
                    continue;
                }
                let current = self.wrapper_input_type(argument_source(source, i), value, scope);
                match (domain.as_ref(), current) {
                    (_, None) => conflict = true,
                    (Some(previous), Some(current)) if *previous != current => conflict = true,
                    (None, Some(current)) => domain = Some(current),
                    _ => {}
                }
            }
            if conflict && field.data_type() == &DataType::LargeBinary {
                // A freshly synthesized unmarked field cannot reuse Native's
                // existing nested Variant rule after erasing opaque origins.
                return Arc::new(field.clone().with_metadata(
                    [(NR_LOGICAL_TYPE_KEY.to_owned(), "invalid".to_owned())].into(),
                ));
            }
            if !conflict && let Some(logical) = domain {
                project_field(field, &logical)
            } else {
                Arc::new(field.clone())
            }
        };
        match (id, &expression.data_type) {
            (
                "builtin.scalar/__array_literal/v1"
                | "builtin.aggregate/array_agg/v1"
                | "builtin.aggregate/array_agg_distinct/v1",
                DataType::List(field),
            ) => {
                let indices = if id == "builtin.scalar/__array_literal/v1" {
                    (0..args.len()).collect()
                } else {
                    vec![0]
                };
                Some(DataType::List(merged(field, indices)))
            }
            (
                "builtin.scalar/row/v1"
                | "builtin.scalar/struct/v1"
                | "builtin.scalar/named_struct/v1",
                DataType::Struct(fields),
            ) => {
                let named = id == "builtin.scalar/named_struct/v1";
                Some(DataType::Struct(
                    fields
                        .iter()
                        .enumerate()
                        .map(|(i, field)| project(field, if named { 2 * i + 1 } else { i }))
                        .collect::<Vec<_>>()
                        .into(),
                ))
            }
            (
                "builtin.aggregate/map_agg/v1" | "builtin.scalar/map/v1",
                DataType::Map(entries, sorted),
            ) => {
                let DataType::Struct(fields) = entries.data_type() else {
                    return None;
                };
                if fields.len() != 2 {
                    return None;
                }
                let keys = merged(&fields[0], (0..args.len()).step_by(2).collect());
                let values = merged(&fields[1], (1..args.len()).step_by(2).collect());
                Some(DataType::Map(
                    Arc::new(Field::new(
                        entries.name(),
                        DataType::Struct(vec![keys, values].into()),
                        entries.is_nullable(),
                    )),
                    *sorted,
                ))
            }
            ("builtin.scalar/map_from_arrays/v1", DataType::Map(entries, sorted))
                if args.len() == 2 =>
            {
                let DataType::Struct(fields) = entries.data_type() else {
                    return None;
                };
                if fields.len() != 2 {
                    return None;
                }
                let mut projected = Vec::with_capacity(2);
                for (field, arg) in fields.iter().zip(args) {
                    let DataType::List(item) = &arg.data_type else {
                        return None;
                    };
                    projected.push(match field_type(item) {
                        Some(logical) => project_field(field, &logical),
                        None => Arc::new(field.as_ref().clone()),
                    });
                }
                Some(DataType::Map(
                    Arc::new(Field::new(
                        entries.name(),
                        DataType::Struct(projected.into()),
                        entries.is_nullable(),
                    )),
                    *sorted,
                ))
            }
            _ => None,
        }
    }
}

fn borrowed_marker(field: &Field) -> Option<LogicalType> {
    let value = field.metadata().get(NR_LOGICAL_TYPE_KEY)?;
    let value = value.trim();
    [
        ("json", LogicalType::Json),
        ("hll", LogicalType::Hll),
        ("bitmap", LogicalType::Bitmap),
        ("object", LogicalType::Object),
        ("percentile", LogicalType::Percentile),
    ]
    .into_iter()
    .find(|(name, _)| value.eq_ignore_ascii_case(name))
    .map(|(_, logical)| logical)
}

fn leaf_marker(logical: &SqlType) -> Option<LogicalType> {
    match logical {
        SqlType::Json => Some(LogicalType::Json),
        SqlType::Hll => Some(LogicalType::Hll),
        SqlType::Bitmap => Some(LogicalType::Bitmap),
        SqlType::Object => Some(LogicalType::Object),
        SqlType::Percentile => Some(LogicalType::Percentile),
        _ => None,
    }
}

fn field_type(field: &Field) -> Option<SqlType> {
    if field.metadata().contains_key(NR_LOGICAL_TYPE_KEY) {
        let ty = match borrowed_marker(field)? {
            LogicalType::Json => SqlType::Json,
            LogicalType::Hll => SqlType::Hll,
            LogicalType::Bitmap => SqlType::Bitmap,
            LogicalType::Object => SqlType::Object,
            LogicalType::Percentile => SqlType::Percentile,
        };
        return logical_carrier_matches(&ty, field.data_type()).then_some(ty);
    }
    ordinary_type(field.data_type(), true)
}

// Plain companion fields are the actual ordinary storage declared by a bound
// overload. Ambiguous domains never originate here. Only an existing nested
// unmarked LargeBinary field uses Native's closed Variant carrier rule.
fn ordinary_type(data_type: &DataType, nested: bool) -> Option<SqlType> {
    use arrow::datatypes::TimeUnit;
    Some(match data_type {
        DataType::Int8 => SqlType::TinyInt,
        DataType::Int16 => SqlType::SmallInt,
        DataType::Int32 => SqlType::Int,
        DataType::Int64 => SqlType::BigInt,
        DataType::FixedSizeBinary(16) => SqlType::LargeInt,
        DataType::Float32 => SqlType::Float,
        DataType::Float64 => SqlType::Double,
        DataType::Utf8 | DataType::LargeUtf8 => SqlType::String,
        DataType::Binary => SqlType::Binary,
        DataType::LargeBinary if nested => SqlType::Variant,
        DataType::Boolean => SqlType::Boolean,
        DataType::Date32 => SqlType::Date,
        DataType::Time64(TimeUnit::Microsecond) => SqlType::Time,
        DataType::Timestamp(TimeUnit::Microsecond, _) => SqlType::DateTime,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => SqlType::DateTimeNs,
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            SqlType::Decimal {
                precision: *precision,
                scale: *scale,
            }
        }
        DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
            SqlType::Array(Box::new(field_type(item)?))
        }
        DataType::Struct(fields) => SqlType::Struct(
            fields
                .iter()
                .map(|field| Some((field.name().clone(), field_type(field)?)))
                .collect::<Option<Vec<_>>>()?,
        ),
        DataType::Map(entries, _) => {
            if entries.metadata().contains_key(NR_LOGICAL_TYPE_KEY) {
                return None;
            }
            let DataType::Struct(fields) = entries.data_type() else {
                return None;
            };
            if fields.len() != 2 {
                return None;
            }
            SqlType::Map(
                Box::new(field_type(&fields[0])?),
                Box::new(field_type(&fields[1])?),
            )
        }
        _ => return None,
    })
}

// This is compatibility with an independently established JSON-list witness,
// not provenance inferred from storage. Binding coercions and trusted catalog
// declarations may omit the item's marker; a present different marker refuses.
fn witnessed_json_list_source(value: &TypedExpr) -> bool {
    let compatible = witnessed_json_list_fields(&value.data_type)
        || matches!((&value.kind, &value.data_type),
            (ExprKind::FunctionCall { binding, args, .. }, DataType::List(item))
                if binding.function_id.as_str() == "builtin.scalar/__array_literal/v1"
                    && args.is_empty() && item.data_type() == &DataType::Null
                    && !item.metadata().contains_key(NR_LOGICAL_TYPE_KEY));
    if !compatible {
        return false;
    }
    match &value.kind {
        ExprKind::Cast { expr, .. } | ExprKind::Nested(expr) => witnessed_json_list_source(expr),
        _ => true,
    }
}

fn witnessed_json_list_fields(data_type: &DataType) -> bool {
    let DataType::List(item) = data_type else {
        return false;
    };
    item.data_type() == &DataType::Utf8
        && (!item.metadata().contains_key(NR_LOGICAL_TYPE_KEY)
            || borrowed_marker(item) == Some(LogicalType::Json))
}

fn nested_source_matches(logical: &SqlType, data_type: &DataType) -> bool {
    let child = |logical: &SqlType, field: &Field| {
        let marker = borrowed_marker(field);
        (!field.metadata().contains_key(NR_LOGICAL_TYPE_KEY) || marker.is_some())
            && marker == leaf_marker(logical)
            && nested_source_matches(logical, field.data_type())
    };
    if !logical_carrier_matches(logical, data_type) {
        return false;
    }
    match (logical, data_type) {
        (
            SqlType::Array(item),
            DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _),
        ) => child(item, field),
        (SqlType::Struct(expected), DataType::Struct(fields)) => expected
            .iter()
            .zip(fields)
            .all(|((_, logical), field)| child(logical, field)),
        (SqlType::Map(key, value), DataType::Map(entries, _)) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return false;
            };
            !entries.metadata().contains_key(NR_LOGICAL_TYPE_KEY)
                && fields.len() == 2
                && child(key, &fields[0])
                && child(value, &fields[1])
        }
        _ => true,
    }
}

fn validate_markers(data_type: &DataType) -> Result<(), &'static str> {
    let child = |field: &Field| {
        if field.metadata().contains_key(NR_LOGICAL_TYPE_KEY) && field_type(field).is_none() {
            return Err("wrapper input contains an unknown or incompatible logical marker");
        }
        validate_markers(field.data_type())
    };
    match data_type {
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            child(field)
        }
        DataType::Struct(fields) => {
            for field in fields {
                child(field)?;
            }
            Ok(())
        }
        DataType::Map(entries, _) => {
            if entries.metadata().contains_key(NR_LOGICAL_TYPE_KEY) {
                return Err("wrapper map entries cannot carry a scalar logical marker");
            }
            let DataType::Struct(fields) = entries.data_type() else {
                return Err("wrapper map entries must be a struct");
            };
            if fields.len() != 2 {
                return Err("wrapper map entries must contain key and value");
            }
            child(&fields[0])?;
            child(&fields[1])
        }
        _ => Ok(()),
    }
}

fn project_field(field: &Field, logical: &SqlType) -> Arc<Field> {
    let projected = Field::new(
        field.name(),
        project_type(field.data_type(), logical),
        field.is_nullable(),
    );
    Arc::new(match leaf_marker(logical) {
        Some(marker) => field_with_logical_type(projected, marker),
        None => projected,
    })
}

fn project_type(data_type: &DataType, logical: &SqlType) -> DataType {
    match (data_type, logical) {
        (DataType::List(field), SqlType::Array(item)) => DataType::List(project_field(field, item)),
        (DataType::LargeList(field), SqlType::Array(item)) => {
            DataType::LargeList(project_field(field, item))
        }
        (DataType::FixedSizeList(field, len), SqlType::Array(item)) => {
            DataType::FixedSizeList(project_field(field, item), *len)
        }
        (DataType::Struct(fields), SqlType::Struct(logical)) => DataType::Struct(
            fields
                .iter()
                .zip(logical)
                .map(|(field, (_, logical))| project_field(field, logical))
                .collect::<Vec<_>>()
                .into(),
        ),
        (DataType::Map(entries, sorted), SqlType::Map(key, value)) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return data_type.clone();
            };
            DataType::Map(
                Arc::new(Field::new(
                    entries.name(),
                    DataType::Struct(
                        vec![
                            project_field(&fields[0], key),
                            project_field(&fields[1], value),
                        ]
                        .into(),
                    ),
                    entries.is_nullable(),
                )),
                *sorted,
            )
        }
        _ => data_type.clone(),
    }
}
