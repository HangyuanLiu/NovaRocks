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

use arrow::array::{Array, ArrayRef, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray};
use arrow::datatypes::DataType;

use crate::runtime::query_result::QueryResult;
use crate::sql::parser::ast::Literal;

/// Convert a scalar query result into SQL that can be substituted for a user variable.
pub fn query_result_to_user_variable_literal(result: &QueryResult) -> Result<String, String> {
    if result.columns.len() != 1 {
        return Err(format!(
            "user variable assignment expected 1 column, got {}",
            result.columns.len()
        ));
    }
    let row_count = result.row_count();
    if row_count == 0 {
        return Ok("null".to_string());
    }
    if row_count > 1 {
        return Err("Subquery returns more than 1 row".to_string());
    }
    for chunk in &result.chunks {
        if chunk.len() == 0 {
            continue;
        }
        let column = chunk
            .columns()
            .first()
            .ok_or_else(|| "empty query chunk".to_string())?;
        let declared = result
            .columns
            .first()
            .ok_or_else(|| "user variable assignment missing column metadata".to_string())?;
        return query_result_cell_to_user_variable_sql(column, &declared.data_type, 0);
    }
    Ok("null".to_string())
}

fn query_result_cell_to_user_variable_sql(
    column: &ArrayRef,
    declared_type: &DataType,
    row_idx: usize,
) -> Result<String, String> {
    if column.is_null(row_idx) {
        return Ok("NULL".to_string());
    }
    if let Some(text) = arrow_text_cell(column, row_idx) {
        return user_variable_text_to_sql(&text?, declared_type);
    }
    let literal = crate::sql::literal::literal_from_batch(column, row_idx)?;
    user_variable_literal_to_sql(&literal)
}

fn arrow_text_cell(column: &ArrayRef, row_idx: usize) -> Option<Result<String, String>> {
    match column.data_type() {
        DataType::Utf8 => Some(
            column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "failed to downcast user variable value to StringArray".to_string())
                .map(|arr| arr.value(row_idx).to_string()),
        ),
        DataType::LargeUtf8 => Some(
            column
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    "failed to downcast user variable value to LargeStringArray".to_string()
                })
                .map(|arr| arr.value(row_idx).to_string()),
        ),
        DataType::Binary => Some(
            column
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "failed to downcast user variable value to BinaryArray".to_string())
                .map(|arr| String::from_utf8_lossy(arr.value(row_idx)).into_owned()),
        ),
        DataType::LargeBinary => Some(
            column
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    "failed to downcast user variable value to LargeBinaryArray".to_string()
                })
                .map(|arr| String::from_utf8_lossy(arr.value(row_idx)).into_owned()),
        ),
        _ => None,
    }
}

fn user_variable_text_to_sql(text: &str, declared_type: &DataType) -> Result<String, String> {
    Ok(match declared_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => text.to_string(),
        DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) | DataType::Struct(_) => {
            text.to_string()
        }
        DataType::Null => "NULL".to_string(),
        _ => single_quoted_user_variable_sql(text),
    })
}

pub(crate) fn user_variable_literal_to_sql(literal: &Literal) -> Result<String, String> {
    Ok(match literal {
        Literal::Null => "NULL".to_string(),
        Literal::Bool(value) => if *value { "TRUE" } else { "FALSE" }.to_string(),
        Literal::Int(value) => value.to_string(),
        Literal::Float(value) => {
            if !value.is_finite() {
                return Err(format!(
                    "non-finite floating literal is not supported: {value}"
                ));
            }
            value.to_string()
        }
        Literal::String(value) | Literal::Date(value) => single_quoted_user_variable_sql(value),
        Literal::Array(items) => format!(
            "[{}]",
            items
                .iter()
                .map(user_variable_literal_to_sql)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        ),
        Literal::Map(entries) => {
            let mut args = Vec::with_capacity(entries.len() * 2);
            for (key, value) in entries {
                args.push(user_variable_literal_to_sql(key)?);
                args.push(user_variable_literal_to_sql(value)?);
            }
            format!("map({})", args.join(", "))
        }
        Literal::Struct(values) => format!(
            "row({})",
            values
                .iter()
                .map(user_variable_literal_to_sql)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        ),
    })
}

fn single_quoted_user_variable_sql(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    for ch in value.chars() {
        match ch {
            '\'' => escaped.push_str("''"),
            '\\' => escaped.push_str(r"\\"),
            _ => escaped.push(ch),
        }
    }
    format!("'{escaped}'")
}
