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

fn string_value(expression: &TypedExpr) -> Option<&str> {
    match &expression.kind {
        ExprKind::Literal(LiteralValue::String(value)) => Some(value),
        ExprKind::Constant(value) => value.try_utf8().ok().flatten(),
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
        ExprKind::WindowCall {
            binding,
            aggregate_binding: None,
            args,
            ..
        } if window_value_binding(binding, expression, args) => Some((binding, args)),
        ExprKind::Cast { expr, .. } => call(expr),
        _ => None,
    }
}

fn supported(binding: &ResolvedFunctionBinding) -> bool {
    let id = binding.function_id.as_str();
    if id.starts_with("builtin.window/") && binding.kind != FunctionKind::Window {
        return false;
    }
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
            | "builtin.scalar/array_flatten/v1"
            | "builtin.scalar/array_repeat/v1"
            | "builtin.scalar/arrays_zip/v1"
            | "builtin.scalar/__array_struct_subfield/v1"
            | "builtin.window/first_value/v1"
            | "builtin.window/last_value/v1"
            | "builtin.window/lead/v1"
            | "builtin.window/lag/v1"
    )
}

// These projections belong to the installed selected call, not its display
// name. An internal marker-only Cast never rewrites the selected carrier.
fn is_container_transform(id: &str) -> bool {
    matches!(
        id,
        "builtin.scalar/array_flatten/v1"
            | "builtin.scalar/array_repeat/v1"
            | "builtin.scalar/arrays_zip/v1"
            | "builtin.scalar/__array_struct_subfield/v1"
    )
}

fn transform_binding_matches(expression: &TypedExpr) -> bool {
    let mut original = expression;
    while let ExprKind::Cast { expr, .. } = &original.kind {
        original = expr;
    }
    let ExprKind::FunctionCall { binding, args, .. } = &original.kind else {
        return false;
    };
    let id = binding.function_id.as_str();
    if binding.kind != FunctionKind::Scalar
        || !is_container_transform(id)
        || binding.logical_argument_count != args.len()
        || binding.selected.argument_types.len() != args.len()
    {
        return false;
    }
    let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
    else {
        return false;
    };
    if result != &original.value_type
        || !args
            .iter()
            .zip(&binding.selected.argument_types)
            .all(|(arg, selected)| {
                matches!(selected, novarocks_functions::FunctionArgumentType::Value(value)
                if value == &arg.value_type)
            })
    {
        return false;
    }
    let DataType::List(output) = &original.value_type.data_type else {
        return false;
    };
    match id {
        "builtin.scalar/array_flatten/v1" => {
            args.len() == 1
                && matches!(&args[0].value_type.data_type, DataType::List(outer) if matches!(outer.data_type(), DataType::List(_)))
        }
        "builtin.scalar/array_repeat/v1" => args.len() == 2,
        "builtin.scalar/arrays_zip/v1" => {
            !args.is_empty()
                && args.iter().all(|arg| {
                    matches!(arg.value_type.data_type, DataType::List(_) | DataType::Null)
                })
                && matches!(output.data_type(), DataType::Struct(fields) if fields.len() == args.len())
        }
        "builtin.scalar/__array_struct_subfield/v1" => {
            args.len() == 2
                && string_value(&args[1]).is_some()
                && matches!(&args[0].value_type.data_type, DataType::List(item) if matches!(item.data_type(), DataType::Struct(_)))
        }
        _ => false,
    }
}

fn copy_transform_field(output: &Field, source: &Field) -> Option<Arc<Field>> {
    let ty = copy_window_markers(output.data_type(), source.data_type())?;
    let marker = borrowed_marker(source);
    if let Some(marker) = marker {
        let logical = match marker {
            LogicalType::Json => SqlType::Json,
            LogicalType::Hll => SqlType::Hll,
            LogicalType::Bitmap => SqlType::Bitmap,
            LogicalType::Object => SqlType::Object,
            LogicalType::Percentile => SqlType::Percentile,
        };
        if !logical_carrier_matches(&logical, output.data_type()) {
            return None;
        }
    }
    let mut metadata = output.metadata().clone();
    metadata.remove(NR_LOGICAL_TYPE_KEY);
    if let Some(marker) = marker {
        metadata.insert(
            NR_LOGICAL_TYPE_KEY.to_owned(),
            marker.metadata_value().to_owned(),
        );
    }
    Some(Arc::new(
        output.clone().with_data_type(ty).with_metadata(metadata),
    ))
}

fn window_value_indices(id: &str, count: usize) -> [Option<usize>; 2] {
    [
        Some(0),
        (count > 2 && matches!(id, "builtin.window/lead/v1" | "builtin.window/lag/v1"))
            .then_some(2),
    ]
}

fn copy_window_markers(output: &DataType, source: &DataType) -> Option<DataType> {
    window_marker_shape(output, source, None, false)
}

fn merge_window_markers(output: &DataType, left: &DataType, right: &DataType) -> Option<DataType> {
    window_marker_shape(output, left, Some(right), false)
}

// The selected output owns names, nullability, List widths and Map flags. The
// value suppliers contribute only marker facts, already independently checked
// before this projection. Null siblings remain actual Null fields.
fn window_marker_shape(
    output: &DataType,
    left: &DataType,
    right: Option<&DataType>,
    complete_missing: bool,
) -> Option<DataType> {
    fn item(data_type: &DataType) -> Option<&Field> {
        match data_type {
            DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
                Some(item)
            }
            _ => None,
        }
    }
    fn field(
        output: &Field,
        left: &Field,
        right: Option<&Field>,
        complete_missing: bool,
    ) -> Option<Arc<Field>> {
        let ty = window_marker_shape(
            output.data_type(),
            left.data_type(),
            right.map(Field::data_type),
            complete_missing,
        )?;
        let left_marker = borrowed_marker(left);
        // Missing metadata on an implicit coercion is not a plain-value
        // contribution. Recover only a compatible independently known origin.
        let origin_marker = right.and_then(borrowed_marker).filter(|marker| {
            let logical = match marker {
                LogicalType::Json => SqlType::Json,
                LogicalType::Hll => SqlType::Hll,
                LogicalType::Bitmap => SqlType::Bitmap,
                LogicalType::Object => SqlType::Object,
                LogicalType::Percentile => SqlType::Percentile,
            };
            logical_carrier_matches(&logical, output.data_type())
        });
        let marker = match right {
            Some(_) if complete_missing => left_marker.or(origin_marker),
            None => left_marker,
            Some(right) if left.data_type() == &DataType::Null => borrowed_marker(right),
            Some(right) if right.data_type() == &DataType::Null => left_marker,
            Some(right) if left_marker == borrowed_marker(right) => left_marker,
            Some(_) => None,
        };
        let conflict = right.is_some_and(|right| {
            left.data_type() != &DataType::Null
                && right.data_type() != &DataType::Null
                && if complete_missing {
                    left_marker.is_some() && origin_marker.is_some() && left_marker != origin_marker
                } else {
                    left_marker != borrowed_marker(right)
                }
        });
        let unadmitted = (left.metadata().contains_key(NR_LOGICAL_TYPE_KEY)
            && left_marker.is_none())
            || right.is_some_and(|right| {
                right.metadata().contains_key(NR_LOGICAL_TYPE_KEY)
                    && borrowed_marker(right).is_none()
            });
        let mut metadata = output.metadata().clone();
        metadata.remove(NR_LOGICAL_TYPE_KEY);
        if unadmitted
            || (conflict && (complete_missing || output.data_type() == &DataType::LargeBinary))
        {
            // Preserve the existing refusal witness instead of laundering an
            // erased opaque origin into Native's nested unmarked Variant rule.
            metadata.insert(NR_LOGICAL_TYPE_KEY.to_owned(), "invalid".to_owned());
        }
        let projected = Field::new(output.name(), ty, output.is_nullable()).with_metadata(metadata);
        Some(Arc::new(match marker {
            Some(marker) if !conflict && !unadmitted => field_with_logical_type(projected, marker),
            _ => projected,
        }))
    }
    Some(match output {
        DataType::List(target) => DataType::List(field(
            target,
            item(left)?,
            match right {
                Some(right) => Some(item(right)?),
                None => None,
            },
            complete_missing,
        )?),
        DataType::LargeList(target) => DataType::LargeList(field(
            target,
            item(left)?,
            match right {
                Some(right) => Some(item(right)?),
                None => None,
            },
            complete_missing,
        )?),
        DataType::FixedSizeList(target, size) => DataType::FixedSizeList(
            field(
                target,
                item(left)?,
                match right {
                    Some(right) => Some(item(right)?),
                    None => None,
                },
                complete_missing,
            )?,
            *size,
        ),
        DataType::Struct(target) => {
            let DataType::Struct(left) = left else {
                return None;
            };
            let right = match right {
                Some(DataType::Struct(fields)) => Some(fields),
                None => None,
                _ => return None,
            };
            if left.len() != target.len()
                || right.is_some_and(|fields| fields.len() != target.len())
            {
                return None;
            }
            DataType::Struct(
                target
                    .iter()
                    .enumerate()
                    .map(|(i, target)| {
                        field(
                            target,
                            &left[i],
                            right.map(|fields| fields[i].as_ref()),
                            complete_missing,
                        )
                    })
                    .collect::<Option<Vec<_>>>()?
                    .into(),
            )
        }
        DataType::Map(target, sorted) => {
            let DataType::Map(left, _) = left else {
                return None;
            };
            let right = match right {
                Some(DataType::Map(entries, _)) => Some(entries.as_ref()),
                None => None,
                _ => return None,
            };
            DataType::Map(field(target, left, right, complete_missing)?, *sorted)
        }
        _ => output.clone(),
    })
}

// These facts belong to four selected builtin Window bindings. A spelling,
// an aggregate window, or a result carrier detached from the selected overload
// cannot establish value provenance.
fn window_value_binding(
    binding: &ResolvedFunctionBinding,
    expression: &TypedExpr,
    args: &[TypedExpr],
) -> bool {
    binding.kind == FunctionKind::Window
        && matches!(
            binding.function_id.as_str(),
            "builtin.window/first_value/v1"
                | "builtin.window/last_value/v1"
                | "builtin.window/lead/v1"
                | "builtin.window/lag/v1"
        )
        && !args.is_empty()
        && binding.logical_argument_count == args.len()
        && binding.selected.argument_types.len() == args.len()
        && matches!(&binding.selected.result_type,
            novarocks_functions::FunctionResultType::Scalar(result)
                if result.data_type == expression.value_type.data_type)
}

// Equal leaves retain their original identity. Different value suppliers are
// merged recursively against the actual selected output shape; the fallback
// is an ordinary leaf, never a domain guessed from Binary/Utf8 storage.
fn merge_window_logical(left: SqlType, right: SqlType, output: &DataType) -> Option<SqlType> {
    if left == right {
        return logical_carrier_matches(&left, output).then_some(left);
    }
    match (left, right, output) {
        (
            SqlType::Array(left),
            SqlType::Array(right),
            DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _),
        ) => Some(SqlType::Array(Box::new(merge_window_logical(
            *left,
            *right,
            item.data_type(),
        )?))),
        (SqlType::Struct(left), SqlType::Struct(right), DataType::Struct(fields))
            if left.len() == fields.len() && right.len() == fields.len() =>
        {
            let mut merged = Vec::with_capacity(fields.len());
            for (((left_name, left), (right_name, right)), field) in
                left.into_iter().zip(right).zip(fields)
            {
                if left_name != *field.name() || right_name != *field.name() {
                    return None;
                }
                merged.push((
                    field.name().clone(),
                    merge_window_logical(left, right, field.data_type())?,
                ));
            }
            Some(SqlType::Struct(merged))
        }
        (
            SqlType::Map(left_key, left_value),
            SqlType::Map(right_key, right_value),
            DataType::Map(entries, _),
        ) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return None;
            };
            if fields.len() != 2 {
                return None;
            }
            Some(SqlType::Map(
                Box::new(merge_window_logical(
                    *left_key,
                    *right_key,
                    fields[0].data_type(),
                )?),
                Box::new(merge_window_logical(
                    *left_value,
                    *right_value,
                    fields[1].data_type(),
                )?),
            ))
        }
        // No top-level LargeBinary fallback is admitted here. Erasing a known
        // opaque domain must not invent Variant through its physical carrier.
        (
            _,
            _,
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Struct(_)
            | DataType::Map(_, _),
        ) => None,
        (_, _, output) => ordinary_type(output, false),
    }
}

impl AnalyzerContext<'_> {
    fn wrapper_input_type(
        &self,
        source: Option<&ast::Expr>,
        value: &TypedExpr,
        scope: &AnalyzerScope,
    ) -> Option<SqlType> {
        self.logical_output_type(source, value, scope)
            .or_else(|| ordinary_type(&value.value_type.data_type, false))
    }

    pub(super) fn window_value_output_type(
        &self,
        source: Option<&ast::Expr>,
        expression: &TypedExpr,
        scope: &AnalyzerScope,
    ) -> Option<SqlType> {
        let ExprKind::WindowCall {
            binding,
            args,
            aggregate_binding: None,
            ..
        } = &expression.kind
        else {
            return None;
        };
        if !window_value_binding(binding, expression, args) {
            return None;
        }
        let values = window_value_indices(binding.function_id.as_str(), args.len());
        let mut logical = None;
        for i in values.into_iter().flatten() {
            let value = &args[i];
            let source = argument_source(source, i);
            if is_null_value(source, value) {
                continue;
            }
            let current = self.wrapper_input_type(source, value, scope)?;
            logical = Some(match logical {
                Some(previous) => {
                    merge_window_logical(previous, current, &expression.value_type.data_type)?
                }
                None => current,
            });
        }
        logical.filter(|logical| logical_carrier_matches(logical, &expression.value_type.data_type))
    }

    // SqlType intentionally has no Null leaf. For a value such as row(NULL,
    // bitmap), preserve the proven sibling in its actual nested fields without
    // manufacturing a complete SqlType. Both possible lead/lag suppliers still
    // contribute to every non-null leaf.
    fn partial_window_value_type(
        &self,
        source: Option<&ast::Expr>,
        expression: &TypedExpr,
        scope: &AnalyzerScope,
    ) -> Option<DataType> {
        let (binding, args) = call(expression)?;
        if binding.kind != FunctionKind::Window {
            return None;
        }
        let mut target = None;
        for i in window_value_indices(binding.function_id.as_str(), args.len())
            .into_iter()
            .flatten()
        {
            let value = &args[i];
            let source = argument_source(source, i);
            if is_null_value(source, value) {
                continue;
            }
            let current = match self.wrapper_input_type(source, value, scope) {
                Some(logical) => project_type(&expression.value_type.data_type, &logical),
                None => {
                    let mut projected = copy_window_markers(
                        &expression.value_type.data_type,
                        &value.value_type.data_type,
                    )?;
                    if !matches!(source, Some(ast::Expr::Cast(_))) {
                        let mut candidate = value;
                        while let ExprKind::Cast { expr, .. } | ExprKind::Nested(expr) =
                            &candidate.kind
                        {
                            candidate = expr;
                            projected = window_marker_shape(
                                &expression.value_type.data_type,
                                &projected,
                                Some(&candidate.value_type.data_type),
                                true,
                            )?;
                        }
                    }
                    projected
                }
            };
            target = Some(match target {
                None => current,
                Some(previous) => {
                    merge_window_markers(&expression.value_type.data_type, &previous, &current)?
                }
            });
        }
        target
    }

    pub(super) fn container_output_type(
        &self,
        source: Option<&ast::Expr>,
        expression: &TypedExpr,
        scope: &AnalyzerScope,
    ) -> Option<SqlType> {
        let (binding, args) = call(expression)?;
        let id = binding.function_id.as_str();
        if !supported(binding)
            || (is_container_transform(id) && !transform_binding_matches(expression))
        {
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
                ordinary_type(&expression.value_type.data_type, false)
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
                let DataType::List(field) = &expression.value_type.data_type else {
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
                let DataType::Map(entries, _) = &expression.value_type.data_type else {
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
                let DataType::Struct(fields) = &expression.value_type.data_type else {
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
                let Some(name) = string_value(args.get(1)?) else {
                    return None;
                };
                if let Some(SqlType::Struct(fields)) = input(0) {
                    fields
                        .into_iter()
                        .find(|(field, _)| field.eq_ignore_ascii_case(name))
                        .map(|(_, ty)| ty)
                } else {
                    let DataType::Struct(fields) = &args.first()?.value_type.data_type else {
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
                let DataType::List(field) = &args.first()?.value_type.data_type else {
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
                        let DataType::Map(entries, _) = &args.first()?.value_type.data_type else {
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
            "builtin.scalar/array_flatten/v1" => match item(0)? {
                SqlType::Array(inner) => Some(SqlType::Array(inner)),
                _ => None,
            },
            "builtin.scalar/array_repeat/v1" => Some(SqlType::Array(Box::new(input(0)?))),
            "builtin.scalar/arrays_zip/v1" => {
                let DataType::List(output) = &expression.value_type.data_type else {
                    return None;
                };
                let DataType::Struct(fields) = output.data_type() else {
                    return None;
                };
                let fields = fields
                    .iter()
                    .enumerate()
                    .map(|(i, field)| Some((field.name().clone(), item(i)?)))
                    .collect::<Option<Vec<_>>>()?;
                Some(SqlType::Array(Box::new(SqlType::Struct(fields))))
            }
            "builtin.scalar/__array_struct_subfield/v1" => {
                let Some(name) = string_value(args.get(1)?) else {
                    return None;
                };
                let selected = match item(0) {
                    Some(SqlType::Struct(fields)) => fields
                        .into_iter()
                        .find(|(field, _)| field.eq_ignore_ascii_case(name))
                        .map(|(_, ty)| ty),
                    _ => {
                        let DataType::List(item) = &args[0].value_type.data_type else {
                            return None;
                        };
                        let DataType::Struct(fields) = item.data_type() else {
                            return None;
                        };
                        fields
                            .iter()
                            .find(|field| field.name().eq_ignore_ascii_case(name))
                            .and_then(|field| field_type(field))
                    }
                }?;
                Some(SqlType::Array(Box::new(selected)))
            }
            "builtin.scalar/array_sortby/v1"
            | "builtin.scalar/array_sort/v1"
            | "builtin.scalar/array_distinct/v1"
            | "builtin.scalar/array_slice/v1" => input(0),
            _ => None,
        }
        .filter(|logical| logical_carrier_matches(logical, &expression.value_type.data_type))
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
        if !supported(binding)
            || (is_container_transform(binding.function_id.as_str())
                && !transform_binding_matches(&expression))
        {
            return Ok(expression);
        }
        // The complete input/output walk precedes allocation of the adapted
        // schema. An adapter cannot wash an unknown marker or a conflicting
        // trusted catalog declaration into an ordinary type.
        for (i, value) in args.iter().enumerate() {
            if binding.kind == FunctionKind::Window
                || is_container_transform(binding.function_id.as_str())
            {
                let mut original = value;
                loop {
                    validate_markers(&original.value_type.data_type)
                        .map_err(|message| AnalyzeError::type_mismatch(message, span))?;
                    match &original.kind {
                        ExprKind::Cast { expr, .. } | ExprKind::Nested(expr) => original = expr,
                        _ => break,
                    }
                }
            }
            validate_markers(&value.value_type.data_type)
                .map_err(|message| AnalyzeError::type_mismatch(message, span))?;
            let logical = self.logical_output_type(argument_source(source, i), value, scope);
            // A returned NULL literal (possibly materialized as a typed
            // CAST) supplies no LargeBinary payload or identity. This exception
            // is confined to value/default contributors of the four exact
            // Window bindings and the four exact container transforms;
            // original marker validation still ran above.
            let neutral_null_supplier = ((binding.kind == FunctionKind::Window
                && window_value_indices(binding.function_id.as_str(), args.len())
                    .contains(&Some(i)))
                || (is_container_transform(binding.function_id.as_str())
                    && (binding.function_id.as_str() == "builtin.scalar/arrays_zip/v1" || i == 0)))
                && is_null_value(argument_source(source, i), value);
            if value.value_type.data_type == DataType::LargeBinary
                && logical.is_none()
                && !neutral_null_supplier
            {
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
                    if nested_source_matches(&logical, &candidate.value_type.data_type)
                        || (json_list_witness
                            && witnessed_json_list_fields(&candidate.value_type.data_type))
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
        validate_markers(&expression.value_type.data_type)
            .map_err(|message| AnalyzeError::type_mismatch(message, span))?;
        let target = if let Some(logical) = self.logical_output_type(source, &expression, scope) {
            if nested_source_matches(&logical, &expression.value_type.data_type) {
                return Ok(expression);
            }
            project_type(&expression.value_type.data_type, &logical)
        } else if let Some(target) = self.partial_wrapper_type(source, &expression, scope) {
            target
        } else {
            return Ok(expression);
        };
        validate_markers(&target).map_err(|message| AnalyzeError::type_mismatch(message, span))?;
        if target == expression.value_type.data_type {
            return Ok(expression);
        }
        let mut value_type = expression.value_type.clone();
        value_type.data_type = target.clone();
        Ok(TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(expression),
                target: target.clone(),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            value_type,
        })
    }

    // The selected List/Struct owns its shape. The source contributes only
    // proven item markers, including siblings of actual Null fields.
    fn partial_transform_type(
        &self,
        source: Option<&ast::Expr>,
        expression: &TypedExpr,
        scope: &AnalyzerScope,
    ) -> Option<DataType> {
        if !transform_binding_matches(expression) {
            return None;
        }
        let (binding, args) = call(expression)?;
        let DataType::List(output) = &expression.value_type.data_type else {
            return None;
        };
        let input_shape = |i: usize| {
            let value = args.get(i)?;
            let source = argument_source(source, i);
            if let Some(logical) = self.wrapper_input_type(source, value, scope) {
                return Some(project_type(&value.value_type.data_type, &logical));
            }
            let mut shape = value.value_type.data_type.clone();
            if !matches!(source, Some(ast::Expr::Cast(_))) {
                let mut original = value;
                while let ExprKind::Cast { expr, .. } | ExprKind::Nested(expr) = &original.kind {
                    original = expr;
                    shape = window_marker_shape(
                        &value.value_type.data_type,
                        &shape,
                        Some(&original.value_type.data_type),
                        true,
                    )?;
                }
            }
            Some(shape)
        };
        let item = match binding.function_id.as_str() {
            "builtin.scalar/array_flatten/v1" => {
                let DataType::List(outer) = input_shape(0)? else {
                    return None;
                };
                let DataType::List(inner) = outer.data_type() else {
                    return None;
                };
                copy_transform_field(output, inner)?
            }
            "builtin.scalar/array_repeat/v1" => {
                match self.wrapper_input_type(argument_source(source, 0), &args[0], scope) {
                    Some(logical) => project_field(output, &logical),
                    None => Arc::new(output.as_ref().clone().with_data_type(copy_window_markers(
                        output.data_type(),
                        &input_shape(0)?,
                    )?)),
                }
            }
            "builtin.scalar/arrays_zip/v1" => {
                let DataType::Struct(fields) = output.data_type() else {
                    return None;
                };
                let mut projected = Vec::with_capacity(fields.len());
                for (i, field) in fields.iter().enumerate() {
                    projected.push(match input_shape(i)? {
                        DataType::List(item) => copy_transform_field(field, &item)?,
                        DataType::Null if field.data_type() == &DataType::Null => field.clone(),
                        _ => return None,
                    });
                }
                Arc::new(
                    output
                        .as_ref()
                        .clone()
                        .with_data_type(DataType::Struct(projected.into())),
                )
            }
            "builtin.scalar/__array_struct_subfield/v1" => {
                let Some(name) = string_value(&args[1]) else {
                    return None;
                };
                let DataType::List(item) = input_shape(0)? else {
                    return None;
                };
                let DataType::Struct(fields) = item.data_type() else {
                    return None;
                };
                let source = fields
                    .iter()
                    .find(|field| field.name().eq_ignore_ascii_case(name))?;
                copy_transform_field(output, source)?
            }
            _ => return None,
        };
        Some(DataType::List(item))
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
        if binding.kind == FunctionKind::Window {
            return self.partial_window_value_type(source, expression, scope);
        }
        if is_container_transform(id) {
            return self.partial_transform_type(source, expression, scope);
        }
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
        match (id, &expression.value_type.data_type) {
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
                    let DataType::List(item) = &arg.value_type.data_type else {
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
    let compatible = witnessed_json_list_fields(&value.value_type.data_type)
        || matches!((&value.kind, &value.value_type.data_type),
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
