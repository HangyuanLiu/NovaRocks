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

//! Spark-aligned `variant_get` / `try_variant_get`, evaluated through the
//! upstream parquet-variant-compute kernel so expression-layer results are
//! row-identical with the scan-layer shredded fast path (IV3-6 decision B).

use super::common::binary_value_at_any;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanBufferBuilder, LargeBinaryArray, LargeBinaryBuilder, StringArray,
    StructArray, new_empty_array,
};
use arrow::buffer::NullBuffer;
use arrow::compute::CastOptions;
use arrow::datatypes::{DataType, Field};
use parquet::variant::{
    GetOptions, VariantArray, json_to_variant, unshred_variant, variant_get as kernel_variant_get,
};

use crate::exec::chunk::Chunk;
use crate::exec::expr::expression_contract;
use crate::exec::expr::{ExprId, ExprNode, LiteralValue};
use crate::exec::variant_failure::{SdkStage, input_row, sdk};
use crate::runtime::fragment::ExecutionResult;
use novarocks_functions::aggregate_scalar::BinaryResultBuilder;
use novarocks_types::value::variant::{
    VariantMetadata, VariantPathSegment, VariantValue, parse_variant_path, split_serialized,
    variant_get_target_type,
};

/// Read the original required string literal or its checked frozen value.
/// SQL admission remains literal-only; this never evaluates a foldable tree.
fn literal_utf8_arg<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    id: ExprId,
    what: &str,
    fn_name: &str,
) -> ExecutionResult<String> {
    match arena.node(id) {
        Some(ExprNode::Literal(LiteralValue::Utf8(s))) => Ok(s.clone()),
        Some(ExprNode::Constant(value))
            if value.value_type().logical_type()
                == novarocks_type_contract::ValueLogicalType::Physical
                && value.value_type().data_type() == &DataType::Utf8 =>
        {
            value
                .try_utf8()
                .map_err(crate::runtime::fragment::ExecutionFailure::from_domain)?
                .map(str::to_owned)
                .ok_or_else(|| {
                    expression_contract(format_args!(
                        "{fn_name} requires a constant string literal for the {what} argument"
                    ))
                })
        }
        _ => Err(expression_contract(format_args!(
            "{fn_name} requires a constant string literal for the {what} argument"
        ))),
    }
}

fn kernel_path(path_str: &str) -> ExecutionResult<parquet::variant::VariantPath<'static>> {
    let parsed = parse_variant_path(path_str)?;
    let elems: Vec<parquet::variant::VariantPathElement<'static>> = parsed
        .segments
        .iter()
        .map(|seg| match seg {
            VariantPathSegment::ObjectKey(k) => {
                parquet::variant::VariantPathElement::field(k.clone())
            }
            VariantPathSegment::ArrayIndex(i) => {
                parquet::variant::VariantPathElement::index(*i as usize)
            }
        })
        .collect();
    Ok(parquet::variant::VariantPath::from(elems))
}

/// Engine LargeBinary `[size|metadata|value]` rows -> upstream VariantArray.
fn large_binary_to_variant_array(arr: &LargeBinaryArray) -> ExecutionResult<VariantArray> {
    let mut metadata_builder = BinaryResultBuilder::new();
    let mut value_builder = BinaryResultBuilder::new();
    let mut validity = BooleanBufferBuilder::new(arr.len());
    // Compute once; the struct-level validity bit makes null-row child bytes
    // unread, but use a valid empty variant metadata rather than empty bytes
    // so we never depend on the kernel ignoring null-row content.
    let empty_meta = VariantMetadata::empty();
    let empty_meta_raw = empty_meta.raw();
    for row in 0..arr.len() {
        if arr.is_null(row) {
            metadata_builder.append_value(empty_meta_raw)?;
            value_builder.append_value([])?;
            validity.append(false);
            continue;
        }
        let (m, v) = split_serialized(arr.value(row))
            .map_err(|source| input_row("variant_get", row, source))?;
        metadata_builder.append_value(m)?;
        value_builder.append_value(v)?;
        validity.append(true);
    }
    let fields = arrow::datatypes::Fields::from(vec![
        Field::new("metadata", DataType::Binary, false),
        Field::new("value", DataType::Binary, true),
    ]);
    let struct_arr = StructArray::try_new(
        fields,
        vec![
            Arc::new(metadata_builder.finish()?) as ArrayRef,
            Arc::new(value_builder.finish()?) as ArrayRef,
        ],
        Some(NullBuffer::new(validity.finish())),
    )
    .map_err(|source| sdk(SdkStage::AssembleInput, source))?;
    VariantArray::try_new(&struct_arr).map_err(|source| sdk(SdkStage::AssembleInput, source))
}

/// Upstream VariantArray -> engine LargeBinary rows (2-arg return form).
fn variant_array_to_large_binary(va: VariantArray) -> ExecutionResult<ArrayRef> {
    let va = if va.typed_value_field().is_some() {
        unshred_variant(&va).map_err(|source| sdk(SdkStage::Unshred, source))?
    } else {
        va
    };
    let metadata = va.metadata_field().clone();
    let value = va
        .value_field()
        .cloned()
        .ok_or_else(|| expression_contract("variant_get: kernel result missing value column"))?;
    let len = metadata.len();
    let mut builder = LargeBinaryBuilder::new();
    for row in 0..len {
        if va.is_null(row) {
            builder.append_null();
            continue;
        }
        let (m, v) = match (
            binary_value_at_any(&metadata, row, "variant_get")?,
            binary_value_at_any(&value, row, "variant_get")?,
        ) {
            (Some(m), Some(v)) => (m, v),
            _ => {
                builder.append_null();
                continue;
            }
        };
        let serialized = VariantValue::create(m, v)
            .map_err(|source| input_row("variant_get", row, source))?
            .serialize();
        builder.append_value(serialized.as_slice());
    }
    Ok(Arc::new(builder.finish()))
}

fn eval_variant_get_impl<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    args: &[ExprId],
    chunk: &Chunk,
    strict: bool,
    fn_name: &str,
) -> ExecutionResult<ArrayRef> {
    if !(2..=3).contains(&args.len()) {
        return Err(expression_contract(format_args!(
            "{fn_name} expects 2 or 3 arguments, got {}",
            args.len()
        )));
    }
    let path_str = literal_utf8_arg(arena, args[1], "path", fn_name)?;
    let target_type = if args.len() == 3 {
        Some(variant_get_target_type(&literal_utf8_arg(
            arena, args[2], "type", fn_name,
        )?)?)
    } else {
        None
    };
    let result_type = target_type.clone().unwrap_or(DataType::LargeBinary);
    if chunk.is_empty() {
        return Ok(new_empty_array(&result_type));
    }

    let input = arena.eval(args[0], chunk)?;
    let variant_array = if let Some(bin) = input.as_any().downcast_ref::<LargeBinaryArray>() {
        large_binary_to_variant_array(bin)?
    } else if let Some(json) = input.as_any().downcast_ref::<StringArray>() {
        // JSON-string input mode (e.g. variant_get(parse_json(s), ...)).
        // Malformed JSON is a query error in both strict and try modes —
        // try_ only relaxes the *cast*, mirroring Spark.
        let json_ref: ArrayRef = Arc::new(json.clone());
        json_to_variant(&json_ref).map_err(|source| sdk(SdkStage::ParseJson, source))?
    } else {
        return Err(expression_contract(format_args!(
            "{fn_name} expects VARIANT or JSON/VARCHAR as first argument, got {:?}",
            input.data_type()
        )));
    };

    let mut opts =
        GetOptions::new_with_path(kernel_path(&path_str)?).with_cast_options(CastOptions {
            safe: !strict,
            format_options: Default::default(),
        });
    if let Some(dt) = &target_type {
        opts = opts.with_as_type(Some(Arc::new(Field::new("", dt.clone(), true))));
    }

    let input_ref: ArrayRef = Arc::new(variant_array.into_inner());
    let result =
        kernel_variant_get(&input_ref, opts).map_err(|source| sdk(SdkStage::Extract, source))?;

    match target_type {
        Some(_) => Ok(result),
        None => {
            let va = VariantArray::try_new(result.as_ref())
                .map_err(|source| sdk(SdkStage::InspectResult, source))?;
            variant_array_to_large_binary(va)
        }
    }
}

pub fn eval_variant_get<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    eval_variant_get_impl(arena, args, chunk, true, "variant_get")
}

pub fn eval_try_variant_get<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    eval_variant_get_impl(arena, args, chunk, false, "try_variant_get")
}

#[cfg(test)]
mod kernel_failure_properties {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::error::Error;

    #[test]
    fn real_upstream_cast_keeps_strict_and_try_behavior() {
        let input: ArrayRef = Arc::new(StringArray::from(vec![r#"{"a":"not-an-integer"}"#]));
        let variant: ArrayRef = Arc::new(json_to_variant(&input).unwrap().into_inner());
        for safe in [false, true] {
            let options = GetOptions::new_with_path(kernel_path("$.a").unwrap())
                .with_as_type(Some(Arc::new(Field::new("", DataType::Int64, true))))
                .with_cast_options(CastOptions {
                    safe,
                    format_options: Default::default(),
                });
            let result = kernel_variant_get(&variant, options)
                .map_err(|source| sdk(SdkStage::Extract, source));
            if safe {
                assert!(result.unwrap().is_null(0));
            } else {
                let failure = result.unwrap_err();
                assert_eq!(failure.failure_fact().class(), FailureClass::Data);
                assert_eq!(failure.failure_fact().reason().code(), 502);
                assert!(
                    failure
                        .source()
                        .unwrap()
                        .source()
                        .unwrap()
                        .source()
                        .unwrap()
                        .is::<arrow::error::ArrowError>()
                );
            }
        }
    }

    #[test]
    fn actual_malformed_json_is_not_a_try_cast_refusal_and_keeps_original_sdk_error() {
        let input: ArrayRef = Arc::new(StringArray::from(vec!["{broken"]));
        let weak = Arc::downgrade(&input);
        let source = json_to_variant(&input).unwrap_err();
        let failure = sdk(SdkStage::ParseJson, source);
        drop(input);
        assert!(weak.upgrade().is_none());
        assert_eq!(failure.failure_fact().class(), FailureClass::Data);
        let sdk_source = failure.source().unwrap().source().unwrap();
        assert!(
            sdk_source
                .source()
                .unwrap()
                .is::<arrow::error::ArrowError>()
        );
    }

    #[test]
    fn assembling_real_corrupted_binary_retains_row_and_original_domain_fact_only() {
        let input = Arc::new(LargeBinaryArray::from(vec![Some([].as_slice())]));
        let weak = Arc::downgrade(&input);
        let failure = large_binary_to_variant_array(&input).unwrap_err();
        drop(input);
        assert!(weak.upgrade().is_none());
        assert_eq!(failure.failure_fact().reason().code(), 60);
        assert_eq!(failure.failure_fact().class(), FailureClass::Data);
        assert!(failure.to_string().contains("row 0"));
        assert!(
            failure
                .source()
                .unwrap()
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<novarocks_types::value::variant::VariantFailure>()
        );
    }
}
