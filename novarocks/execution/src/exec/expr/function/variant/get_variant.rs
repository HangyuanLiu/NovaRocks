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
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanBuilder, Date32Builder, Float64Builder, Int32Builder, Int64Builder,
    LargeBinaryArray, LargeBinaryBuilder, StringArray, StringBuilder, TimestampMicrosecondBuilder,
};
use chrono::{Local, Offset};
use serde_json::Value as JsonValue;

use crate::exec::chunk::Chunk;
use crate::exec::expr::ExprId;
use crate::exec::expr::expression_contract;
use crate::exec::expr::function::pattern_memo::PatternMemo;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use novarocks_types::value::variant::{
    VariantPath, VariantValue, is_variant_null, parse_variant_path_nullable, try_variant_to_bool,
    try_variant_to_date_days, try_variant_to_datetime_micros, try_variant_to_f64,
    try_variant_to_i64, try_variant_to_json, try_variant_to_string, try_variant_to_time_micros,
    variant_query_nullable,
};

/// Parsed path arguments of one evaluation, keyed by path text.
type PathMemo<'a> = PatternMemo<'a, Option<VariantPath>, ExecutionFailure>;

fn memo_path<'memo, 'data>(
    paths: &'memo mut PathMemo<'data>,
    text: &'data str,
) -> ExecutionResult<Option<&'memo VariantPath>> {
    paths
        .get_or_compile(text, |text| {
            parse_variant_path_nullable(text).map_err(ExecutionFailure::from)
        })
        .map(|path| path.as_ref())
        .map_err(Clone::clone)
}

fn eval_variant_path_args<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    args: &[ExprId],
    chunk: &Chunk,
    fn_name: &str,
) -> ExecutionResult<(ArrayRef, ArrayRef)> {
    if args.len() != 2 {
        return Err(expression_contract(format_args!(
            "{} expects 2 arguments, got {}",
            fn_name,
            args.len()
        )));
    }
    let variant_array = arena.eval(args[0], chunk)?;
    let path_array = arena.eval(args[1], chunk)?;
    Ok((variant_array, path_array))
}

fn downcast_variant_path_args<'a>(
    variant_array: &'a ArrayRef,
    path_array: &'a ArrayRef,
    fn_name: &str,
) -> ExecutionResult<(&'a LargeBinaryArray, &'a StringArray)> {
    let variant_arr = variant_array
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| {
            expression_contract(format_args!(
                "{} expects LargeBinary for variant argument",
                fn_name
            ))
        })?;
    let path_arr = path_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            expression_contract(format_args!("{} expects Utf8 for path argument", fn_name))
        })?;
    Ok((variant_arr, path_arr))
}

fn resolve_variant_value<'a>(
    variant_arr: &LargeBinaryArray,
    path_arr: &'a StringArray,
    row: usize,
    paths: &mut PathMemo<'a>,
) -> ExecutionResult<Option<VariantValue>> {
    if variant_arr.is_null(row) || path_arr.is_null(row) {
        return Ok(None);
    }
    let Some(path) = memo_path(paths, path_arr.value(row))? else {
        return Ok(None);
    };
    let value = VariantValue::from_serialized(variant_arr.value(row))?;
    Ok(variant_query_nullable(&value, path)?)
}

fn query_json_by_path<'a>(
    json: &'a JsonValue,
    path: &[novarocks_types::value::variant::VariantPathSegment],
) -> Option<&'a JsonValue> {
    let mut current = json;
    for segment in path {
        current = match segment {
            novarocks_types::value::variant::VariantPathSegment::ObjectKey(key) => {
                current.get(key)?
            }
            novarocks_types::value::variant::VariantPathSegment::ArrayIndex(idx) => {
                current.get(*idx as usize)?
            }
        };
    }
    Some(current)
}

fn json_value_to_output_string(value: &JsonValue) -> ExecutionResult<Option<String>> {
    if value.is_null() {
        return Ok(None);
    }
    if let Some(s) = value.as_str() {
        Ok(Some(s.to_string()))
    } else {
        json_value_to_query_string(value).map(Some)
    }
}

fn json_value_to_i64(value: &JsonValue) -> Option<i64> {
    if value.is_null() {
        return None;
    }
    if let Some(v) = value.as_bool() {
        return Some(if v { 1 } else { 0 });
    }
    if let Some(v) = value.as_i64() {
        return Some(v);
    }
    if let Some(v) = value.as_u64() {
        return i64::try_from(v).ok();
    }
    if let Some(v) = value.as_f64() {
        if v.is_finite() {
            return Some(v as i64);
        }
        return None;
    }
    if let Some(v) = value.as_str() {
        return v.trim().parse::<i64>().ok();
    }
    None
}

fn json_value_to_bool(value: &JsonValue) -> Option<bool> {
    if value.is_null() {
        return None;
    }
    if let Some(v) = value.as_bool() {
        return Some(v);
    }
    if let Some(v) = value.as_i64() {
        return Some(v != 0);
    }
    if let Some(v) = value.as_u64() {
        return Some(v != 0);
    }
    if let Some(v) = value.as_f64() {
        return Some(v != 0.0);
    }
    if let Some(v) = value.as_str() {
        return parse_bool_like_string(v);
    }
    None
}

fn json_value_to_f64(value: &JsonValue) -> Option<f64> {
    if value.is_null() {
        return None;
    }
    if let Some(v) = value.as_bool() {
        return Some(if v { 1.0 } else { 0.0 });
    }
    if let Some(v) = value.as_f64() {
        return Some(v);
    }
    if let Some(v) = value.as_i64() {
        return Some(v as f64);
    }
    if let Some(v) = value.as_u64() {
        return Some(v as f64);
    }
    if let Some(v) = value.as_str() {
        return v.trim().parse::<f64>().ok();
    }
    None
}

fn parse_bool_like_string(value: &str) -> Option<bool> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(parsed) = trimmed.parse::<i32>() {
        return Some(parsed != 0);
    }
    if trimmed.eq_ignore_ascii_case("true") {
        return Some(true);
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return Some(false);
    }
    None
}

fn format_json_number(value: &serde_json::Number) -> String {
    if let Some(v) = value.as_i64() {
        return v.to_string();
    }
    if let Some(v) = value.as_u64() {
        return v.to_string();
    }
    if let Some(v) = value.as_f64() {
        if v.is_finite() && v.fract() == 0.0 {
            let as_i128 = v as i128;
            if as_i128 >= i64::MIN as i128 && as_i128 <= i64::MAX as i128 {
                return (as_i128 as i64).to_string();
            }
            if as_i128 >= 0 && as_i128 <= u64::MAX as i128 {
                return (as_i128 as u64).to_string();
            }
        }
        return v.to_string();
    }
    value.to_string()
}

fn format_json_query_value(value: &JsonValue, out: &mut String) -> ExecutionResult<()> {
    match value {
        JsonValue::Null => out.push_str("null"),
        JsonValue::Bool(v) => out.push_str(if *v { "true" } else { "false" }),
        JsonValue::Number(v) => out.push_str(&format_json_number(v)),
        JsonValue::String(v) => {
            let escaped =
                serde_json::to_string(v).map_err(crate::exec::variant_failure::json_encoding)?;
            out.push_str(&escaped);
        }
        JsonValue::Array(items) => {
            out.push('[');
            for (idx, item) in items.iter().enumerate() {
                if idx > 0 {
                    out.push_str(", ");
                }
                format_json_query_value(item, out)?;
            }
            out.push(']');
        }
        JsonValue::Object(map) => {
            out.push('{');
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            for (idx, key) in keys.iter().enumerate() {
                if idx > 0 {
                    out.push_str(", ");
                }
                let escaped_key = serde_json::to_string(key)
                    .map_err(crate::exec::variant_failure::json_encoding)?;
                out.push_str(&escaped_key);
                out.push_str(": ");
                let child = map
                    .get(*key)
                    .ok_or_else(|| expression_contract("json_query missing object key"))?;
                format_json_query_value(child, out)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

fn json_value_to_query_string(value: &JsonValue) -> ExecutionResult<String> {
    let mut out = String::new();
    format_json_query_value(value, &mut out)?;
    Ok(out)
}

fn variant_value_to_query_string(value: &VariantValue) -> ExecutionResult<Option<String>> {
    if is_variant_null(value)? {
        return Ok(Some("null".to_string()));
    }
    let Some(text) = try_variant_to_json(value)? else {
        return Ok(None);
    };
    let json = serde_json::from_str::<JsonValue>(&text)
        .map_err(crate::exec::variant_failure::json_encoding)?;
    json_value_to_query_string(&json).map(Some)
}

pub fn eval_json_query<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let (variant_array, path_array) = eval_variant_path_args(arena, args, chunk, "json_query")?;
    let path_arr = path_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| expression_contract("json_query expects Utf8 for path argument"))?;

    let mut builder = StringBuilder::new();
    let mut paths = PathMemo::new();
    if let Some(variant_arr) = variant_array.as_any().downcast_ref::<LargeBinaryArray>() {
        for row in 0..chunk.len() {
            let value = match resolve_variant_value(variant_arr, path_arr, row, &mut paths)? {
                Some(v) => v,
                None => {
                    builder.append_null();
                    continue;
                }
            };
            match variant_value_to_query_string(&value)? {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        }
        return Ok(Arc::new(builder.finish()) as ArrayRef);
    }

    let json_arr = variant_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            expression_contract("json_query expects VARIANT or JSON/VARCHAR as first argument")
        })?;
    for row in 0..chunk.len() {
        if json_arr.is_null(row) || path_arr.is_null(row) {
            builder.append_null();
            continue;
        }
        let path = match memo_path(&mut paths, path_arr.value(row))? {
            Some(path) => path,
            None => {
                builder.append_null();
                continue;
            }
        };
        let json = match serde_json::from_str::<JsonValue>(json_arr.value(row)) {
            Ok(v) => v,
            Err(_) => {
                builder.append_null();
                continue;
            }
        };
        let output = query_json_by_path(&json, &path.segments)
            .map(json_value_to_query_string)
            .transpose()?;
        match output {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_variant_query<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let (variant_array, path_array) = eval_variant_path_args(arena, args, chunk, "variant_query")?;
    let (variant_arr, path_arr) =
        downcast_variant_path_args(&variant_array, &path_array, "variant_query")?;

    let mut builder = LargeBinaryBuilder::new();
    let mut paths = PathMemo::new();
    for row in 0..chunk.len() {
        let value = match resolve_variant_value(variant_arr, path_arr, row, &mut paths)? {
            Some(v) => v,
            None => {
                builder.append_null();
                continue;
            }
        };
        let serialized = value.serialize();
        builder.append_value(serialized.as_slice());
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_get_variant_bool<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let (variant_array, path_array) =
        eval_variant_path_args(arena, args, chunk, "get_variant_bool")?;
    let path_arr = path_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| expression_contract("get_variant_bool expects Utf8 for path argument"))?;

    let mut builder = BooleanBuilder::new();
    let mut paths = PathMemo::new();
    if let Some(variant_arr) = variant_array.as_any().downcast_ref::<LargeBinaryArray>() {
        for row in 0..chunk.len() {
            let value = match resolve_variant_value(variant_arr, path_arr, row, &mut paths)? {
                Some(v) => v,
                None => {
                    builder.append_null();
                    continue;
                }
            };
            if is_variant_null(&value)? {
                builder.append_null();
                continue;
            }
            match try_variant_to_bool(&value)? {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        }
        return Ok(Arc::new(builder.finish()) as ArrayRef);
    }

    let json_arr = variant_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            expression_contract(
                "get_variant_bool expects VARIANT or JSON/VARCHAR as first argument",
            )
        })?;
    for row in 0..chunk.len() {
        if json_arr.is_null(row) || path_arr.is_null(row) {
            builder.append_null();
            continue;
        }
        let path = match memo_path(&mut paths, path_arr.value(row))? {
            Some(path) => path,
            None => {
                builder.append_null();
                continue;
            }
        };
        let json = match serde_json::from_str::<JsonValue>(json_arr.value(row)) {
            Ok(v) => v,
            Err(_) => {
                builder.append_null();
                continue;
            }
        };
        match query_json_by_path(&json, &path.segments).and_then(json_value_to_bool) {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_get_variant_int<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let (variant_array, path_array) =
        eval_variant_path_args(arena, args, chunk, "get_variant_int")?;
    let path_arr = path_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| expression_contract("get_variant_int expects Utf8 for path argument"))?;

    let mut builder = Int64Builder::new();
    let mut paths = PathMemo::new();
    if let Some(variant_arr) = variant_array.as_any().downcast_ref::<LargeBinaryArray>() {
        for row in 0..chunk.len() {
            let value = match resolve_variant_value(variant_arr, path_arr, row, &mut paths)? {
                Some(v) => v,
                None => {
                    builder.append_null();
                    continue;
                }
            };
            if is_variant_null(&value)? {
                builder.append_null();
                continue;
            }
            match try_variant_to_i64(&value)? {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        }
        return Ok(Arc::new(builder.finish()) as ArrayRef);
    }

    let json_arr = variant_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            expression_contract("get_variant_int expects VARIANT or JSON/VARCHAR as first argument")
        })?;
    for row in 0..chunk.len() {
        if json_arr.is_null(row) || path_arr.is_null(row) {
            builder.append_null();
            continue;
        }
        let path = match memo_path(&mut paths, path_arr.value(row))? {
            Some(path) => path,
            None => {
                builder.append_null();
                continue;
            }
        };
        let json = match serde_json::from_str::<JsonValue>(json_arr.value(row)) {
            Ok(v) => v,
            Err(_) => {
                builder.append_null();
                continue;
            }
        };
        match query_json_by_path(&json, &path.segments).and_then(json_value_to_i64) {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_get_variant_double<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let (variant_array, path_array) =
        eval_variant_path_args(arena, args, chunk, "get_variant_double")?;
    let path_arr = path_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| expression_contract("get_variant_double expects Utf8 for path argument"))?;

    let mut builder = Float64Builder::new();
    let mut paths = PathMemo::new();
    if let Some(variant_arr) = variant_array.as_any().downcast_ref::<LargeBinaryArray>() {
        for row in 0..chunk.len() {
            let value = match resolve_variant_value(variant_arr, path_arr, row, &mut paths)? {
                Some(v) => v,
                None => {
                    builder.append_null();
                    continue;
                }
            };
            if is_variant_null(&value)? {
                builder.append_null();
                continue;
            }
            match try_variant_to_f64(&value)? {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        }
        return Ok(Arc::new(builder.finish()) as ArrayRef);
    }

    let json_arr = variant_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            expression_contract(
                "get_variant_double expects VARIANT or JSON/VARCHAR as first argument",
            )
        })?;
    for row in 0..chunk.len() {
        if json_arr.is_null(row) || path_arr.is_null(row) {
            builder.append_null();
            continue;
        }
        let path = match memo_path(&mut paths, path_arr.value(row))? {
            Some(path) => path,
            None => {
                builder.append_null();
                continue;
            }
        };
        let json = match serde_json::from_str::<JsonValue>(json_arr.value(row)) {
            Ok(v) => v,
            Err(_) => {
                builder.append_null();
                continue;
            }
        };
        match query_json_by_path(&json, &path.segments).and_then(json_value_to_f64) {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_get_variant_string<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let (variant_array, path_array) =
        eval_variant_path_args(arena, args, chunk, "get_variant_string")?;
    let path_arr = path_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| expression_contract("get_variant_string expects Utf8 for path argument"))?;

    let mut builder = StringBuilder::new();
    let mut paths = PathMemo::new();
    if let Some(variant_arr) = variant_array.as_any().downcast_ref::<LargeBinaryArray>() {
        for row in 0..chunk.len() {
            let value = match resolve_variant_value(variant_arr, path_arr, row, &mut paths)? {
                Some(v) => v,
                None => {
                    builder.append_null();
                    continue;
                }
            };
            if is_variant_null(&value)? {
                builder.append_null();
                continue;
            }
            match try_variant_to_string(&value)? {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        }
        return Ok(Arc::new(builder.finish()) as ArrayRef);
    }

    let json_arr = variant_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            expression_contract(
                "get_variant_string expects VARIANT or JSON/VARCHAR as first argument",
            )
        })?;
    for row in 0..chunk.len() {
        if json_arr.is_null(row) || path_arr.is_null(row) {
            builder.append_null();
            continue;
        }
        let path = match memo_path(&mut paths, path_arr.value(row))? {
            Some(path) => path,
            None => {
                builder.append_null();
                continue;
            }
        };
        let json = match serde_json::from_str::<JsonValue>(json_arr.value(row)) {
            Ok(v) => v,
            Err(_) => {
                builder.append_null();
                continue;
            }
        };
        let output = query_json_by_path(&json, &path.segments)
            .map(json_value_to_output_string)
            .transpose()?
            .flatten();
        match output {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn json_length_of_value(value: &JsonValue) -> i32 {
    match value {
        JsonValue::Array(items) => i32::try_from(items.len()).unwrap_or(i32::MAX),
        JsonValue::Object(map) => i32::try_from(map.len()).unwrap_or(i32::MAX),
        _ => 1,
    }
}

pub fn eval_json_exists<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let (variant_array, path_array) = eval_variant_path_args(arena, args, chunk, "json_exists")?;
    let path_arr = path_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| expression_contract("json_exists expects Utf8 for path argument"))?;

    let mut builder = BooleanBuilder::new();
    let mut paths = PathMemo::new();
    if let Some(variant_arr) = variant_array.as_any().downcast_ref::<LargeBinaryArray>() {
        for row in 0..chunk.len() {
            if variant_arr.is_null(row) || path_arr.is_null(row) {
                builder.append_null();
                continue;
            }
            let path = match memo_path(&mut paths, path_arr.value(row))? {
                Some(path) => path,
                None => {
                    builder.append_value(false);
                    continue;
                }
            };
            let value = VariantValue::from_serialized(variant_arr.value(row))?;
            builder.append_value(variant_query_nullable(&value, path)?.is_some());
        }
        return Ok(Arc::new(builder.finish()) as ArrayRef);
    }

    let json_arr = variant_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            expression_contract("json_exists expects VARIANT or JSON/VARCHAR as first argument")
        })?;
    for row in 0..chunk.len() {
        if json_arr.is_null(row) || path_arr.is_null(row) {
            builder.append_null();
            continue;
        }
        let path = match memo_path(&mut paths, path_arr.value(row))? {
            Some(path) => path,
            None => {
                builder.append_value(false);
                continue;
            }
        };
        let json = match serde_json::from_str::<JsonValue>(json_arr.value(row)) {
            Ok(value) => value,
            Err(_) => {
                builder.append_value(false);
                continue;
            }
        };
        builder.append_value(query_json_by_path(&json, &path.segments).is_some());
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_json_length<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let (variant_array, path_array) = eval_variant_path_args(arena, args, chunk, "json_length")?;
    let path_arr = path_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| expression_contract("json_length expects Utf8 for path argument"))?;

    let mut builder = Int32Builder::new();
    let mut paths = PathMemo::new();
    if let Some(variant_arr) = variant_array.as_any().downcast_ref::<LargeBinaryArray>() {
        for row in 0..chunk.len() {
            if variant_arr.is_null(row) || path_arr.is_null(row) {
                builder.append_null();
                continue;
            }
            let path = match memo_path(&mut paths, path_arr.value(row))? {
                Some(path) => path,
                None => {
                    builder.append_value(0);
                    continue;
                }
            };
            let value = VariantValue::from_serialized(variant_arr.value(row))?;
            let queried = match variant_query_nullable(&value, path)? {
                Some(value) => value,
                None => {
                    builder.append_value(0);
                    continue;
                }
            };
            let Some(text) = try_variant_to_json(&queried)? else {
                builder.append_value(0);
                continue;
            };
            let json = serde_json::from_str::<JsonValue>(&text)
                .map_err(crate::exec::variant_failure::json_encoding)?;
            builder.append_value(json_length_of_value(&json));
        }
        return Ok(Arc::new(builder.finish()) as ArrayRef);
    }

    let json_arr = variant_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            expression_contract("json_length expects VARIANT or JSON/VARCHAR as first argument")
        })?;
    for row in 0..chunk.len() {
        if json_arr.is_null(row) || path_arr.is_null(row) {
            builder.append_null();
            continue;
        }
        let path = match memo_path(&mut paths, path_arr.value(row))? {
            Some(path) => path,
            None => {
                builder.append_value(0);
                continue;
            }
        };
        let json = match serde_json::from_str::<JsonValue>(json_arr.value(row)) {
            Ok(value) => value,
            Err(_) => {
                builder.append_value(0);
                continue;
            }
        };
        let length = query_json_by_path(&json, &path.segments)
            .map(json_length_of_value)
            .unwrap_or(0);
        builder.append_value(length);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_get_variant_date<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let (variant_array, path_array) =
        eval_variant_path_args(arena, args, chunk, "get_variant_date")?;
    let (variant_arr, path_arr) =
        downcast_variant_path_args(&variant_array, &path_array, "get_variant_date")?;

    let tz = Local::now().offset().fix();
    let mut builder = Date32Builder::new();
    let mut paths = PathMemo::new();
    for row in 0..chunk.len() {
        let value = match resolve_variant_value(variant_arr, path_arr, row, &mut paths)? {
            Some(v) => v,
            None => {
                builder.append_null();
                continue;
            }
        };
        if is_variant_null(&value)? {
            builder.append_null();
            continue;
        }
        match try_variant_to_date_days(&value, tz)? {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_get_variant_datetime<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let (variant_array, path_array) =
        eval_variant_path_args(arena, args, chunk, "get_variant_datetime")?;
    let (variant_arr, path_arr) =
        downcast_variant_path_args(&variant_array, &path_array, "get_variant_datetime")?;

    let tz = Local::now().offset().fix();
    let mut builder = TimestampMicrosecondBuilder::new();
    let mut paths = PathMemo::new();
    for row in 0..chunk.len() {
        let value = match resolve_variant_value(variant_arr, path_arr, row, &mut paths)? {
            Some(v) => v,
            None => {
                builder.append_null();
                continue;
            }
        };
        if is_variant_null(&value)? {
            builder.append_null();
            continue;
        }
        match try_variant_to_datetime_micros(&value, tz)? {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_get_variant_time<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let (variant_array, path_array) =
        eval_variant_path_args(arena, args, chunk, "get_variant_time")?;
    let (variant_arr, path_arr) =
        downcast_variant_path_args(&variant_array, &path_array, "get_variant_time")?;

    let tz = Local::now().offset().fix();
    let mut builder = TimestampMicrosecondBuilder::new();
    let mut paths = PathMemo::new();
    for row in 0..chunk.len() {
        let value = match resolve_variant_value(variant_arr, path_arr, row, &mut paths)? {
            Some(v) => v,
            None => {
                builder.append_null();
                continue;
            }
        };
        if is_variant_null(&value)? {
            builder.append_null();
            continue;
        }
        match try_variant_to_time_micros(&value, tz)? {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[cfg(test)]
mod nullable_failure_properties {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use novarocks_types::value::variant_encode::encode_json_text_to_variant_bytes;

    #[test]
    fn query_absence_remains_nullable_but_damaged_input_and_extent_failure_escape() {
        let bytes = encode_json_text_to_variant_bytes(r#"{"a":[1]}"#).unwrap();
        let input = LargeBinaryArray::from(vec![Some(bytes.as_slice())]);
        for path in ["$.missing", "$.a[8]", "$.a[0].wrong", "bad"] {
            let paths = StringArray::from(vec![path]);
            assert!(
                resolve_variant_value(&input, &paths, 0, &mut PathMemo::new())
                    .unwrap()
                    .is_none()
            );
        }
        let paths = StringArray::from(vec!["$"]);
        let damaged = LargeBinaryArray::from(vec![Some([].as_slice())]);
        let failure = resolve_variant_value(&damaged, &paths, 0, &mut PathMemo::new()).unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Data);
        assert_eq!(failure.failure_fact().reason().code(), 60);
        let oversize = (16_u32 * 1024 * 1024 + 1).to_le_bytes();
        let input = LargeBinaryArray::from(vec![Some(oversize.as_slice())]);
        let failure = resolve_variant_value(&input, &paths, 0, &mut PathMemo::new()).unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Representation);
        assert_eq!(
            failure
                .failure_fact()
                .representation()
                .unwrap()
                .representable_prefix,
            None
        );
    }

    #[test]
    fn memo_observations_preserve_one_unmaskable_occurrence() {
        let failure = ExecutionFailure::from(novarocks_functions::KernelFailure::DeadlineExceeded);
        let mut memo = PathMemo::new();
        assert!(memo.get_or_compile("$", |_| Err(failure.clone())).is_err());
        let first = memo_path(&mut memo, "$").unwrap_err();
        let second = memo_path(&mut memo, "$").unwrap_err();
        assert!(failure.same_occurrence(&first));
        assert!(first.same_occurrence(&second));
        assert_eq!(first.failure_fact().class(), FailureClass::Control);
    }
}
