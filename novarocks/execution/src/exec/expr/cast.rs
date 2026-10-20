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
use crate::exec::chunk::{Chunk, ChunkFieldSchema};
use crate::exec::expr::function::{parse_date_value, parse_datetime_value};
use crate::exec::expr::{ExprArena, ExprId};
use crate::runtime::fragment::ExecutionFailure;
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Date32Array, Date32Builder, Decimal128Array,
    Decimal256Array, Float32Array, Float32Builder, Float64Array, Float64Builder, Int8Array,
    Int8Builder, Int16Array, Int16Builder, Int32Array, Int32Builder, Int64Array, Int64Builder,
    LargeBinaryArray, ListArray, MapArray, StringArray, StringBuilder, StructArray,
    Time64MicrosecondArray, TimestampMicrosecondArray, TimestampMicrosecondBuilder,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array, make_array, new_null_array,
};
use arrow::compute::{cast, take};
use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer, i256};
use chrono::{DateTime, Datelike, Local, NaiveDate, NaiveDateTime, NaiveTime, Offset, Timelike};
use novarocks_functions::calendar_numeric::numeric_datetime_literal_to_naive as datetime_literal_to_naive_datetime;
use novarocks_type_contract::{
    DecimalOverflowPolicy, decimal_error_policy_cast_supported, is_checked_decimal_numeric_cast,
};
use novarocks_types::largeint;

use serde_json::Value as JsonValue;
use std::sync::Arc;

use novarocks_types::value::variant::{
    VariantValue, is_variant_null, variant_to_bool, variant_to_date_days,
    variant_to_datetime_micros, variant_to_f64, variant_to_i64, variant_to_string,
    variant_to_time_micros,
};

impl From<novarocks_types::arrow_cast::ScalarCastFailure> for ExecutionFailure {
    fn from(cause: novarocks_types::arrow_cast::ScalarCastFailure) -> Self {
        Self::from_domain(cause)
    }
}
impl From<novarocks_functions::decimal128_rescale::DecimalCastFailure> for ExecutionFailure {
    fn from(cause: novarocks_functions::decimal128_rescale::DecimalCastFailure) -> Self {
        Self::from_domain(cause)
    }
}
impl From<novarocks_functions::largeint::LargeIntCarrierFailure> for ExecutionFailure {
    fn from(cause: novarocks_functions::largeint::LargeIntCarrierFailure) -> Self {
        Self::from_domain(cause)
    }
}
/// The CAST expression owns source/target context; the value domain retains
/// its original cause and decides the neutral fact. Labels are cold, bounded
/// diagnostics and cannot add SQL position or change the failure category.
#[derive(Debug)]
struct CastEvaluationFailure {
    source: ExecutionFailure,
    from: novarocks_failure_contract::BoundedDetail,
    to: novarocks_failure_contract::BoundedDetail,
    composite: bool,
}
impl std::fmt::Display for CastEvaluationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.composite {
            // Field/layout facts must remain visible before potentially wide
            // type labels consume the bounded diagnostic budget.
            write!(
                f,
                "{}; CAST failed: from {} to {}",
                self.source.detail(),
                self.from.as_str(),
                self.to.as_str()
            )
        } else {
            write!(
                f,
                "CAST failed: from {} to {}: {}",
                self.from.as_str(),
                self.to.as_str(),
                self.source.detail()
            )
        }
    }
}
impl std::error::Error for CastEvaluationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for CastEvaluationFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&self.source)
    }
}
fn cast_evaluation_failure(
    source: ExecutionFailure,
    from: &DataType,
    to: &DataType,
) -> ExecutionFailure {
    let composite = [from, to].into_iter().any(|ty| {
        matches!(
            ty,
            DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(_, _)
                | DataType::Struct(_)
                | DataType::Map(_, _)
        )
    });
    ExecutionFailure::from_domain(CastEvaluationFailure {
        source,
        composite,
        from: novarocks_failure_contract::BoundedDetail::truncating_display(format_args!(
            "{from:?}"
        )),
        to: novarocks_failure_contract::BoundedDetail::truncating_display(format_args!("{to:?}")),
    })
}

#[derive(Debug)]
struct CastNumericFailure(novarocks_failure_contract::BoundedDetail);
impl std::fmt::Display for CastNumericFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}
impl std::error::Error for CastNumericFailure {}
impl novarocks_failure_contract::FailureProjection for CastNumericFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact()
    }
}
fn cast_numeric_failure(detail: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(CastNumericFailure(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}
#[derive(Debug)]
struct CastConstructionFailure(arrow::error::ArrowError);
impl std::fmt::Display for CastConstructionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CAST carrier construction: {}", self.0)
    }
}
impl std::error::Error for CastConstructionFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for CastConstructionFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact};
        let (class, reason) = match self.0 {
            arrow::error::ArrowError::MemoryError(_) => (
                FailureClass::Resource,
                crate::runtime::fragment::ExecutionReason::ArrayCastAllocation,
            ),
            _ => (
                FailureClass::Invariant,
                crate::runtime::fragment::ExecutionReason::ArrayCastContract,
            ),
        };
        FailureFact::new(class, reason.reason())
    }
}
pub(super) fn cast_construct_failure(cause: arrow::error::ArrowError) -> ExecutionFailure {
    ExecutionFailure::from_domain(CastConstructionFailure(cause))
}
#[derive(Debug)]
struct CastJsonFailure {
    source: serde_json::Error,
    decode: bool,
}
impl std::fmt::Display for CastJsonFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CAST JSON conversion: {}", self.source)
    }
}
impl std::error::Error for CastJsonFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for CastJsonFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact};
        if self.decode {
            return novarocks_functions::DataFailureReason::InvalidEncoding.failure_fact();
        }
        FailureFact::new(
            FailureClass::Invariant,
            crate::runtime::fragment::ExecutionReason::ArrayCastJsonEncode.reason(),
        )
    }
}
fn cast_json_encode_failure(source: serde_json::Error) -> ExecutionFailure {
    ExecutionFailure::from_domain(CastJsonFailure {
        source,
        decode: false,
    })
}
fn cast_json_decode_failure(source: serde_json::Error) -> ExecutionFailure {
    ExecutionFailure::from_domain(CastJsonFailure {
        source,
        decode: true,
    })
}
#[derive(Debug)]
struct CastLargeIntWidth(std::array::TryFromSliceError);
impl std::fmt::Display for CastLargeIntWidth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CAST LARGEINT value requires 16 bytes: {}", self.0)
    }
}
impl std::error::Error for CastLargeIntWidth {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for CastLargeIntWidth {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            crate::runtime::fragment::ExecutionReason::ArrayCastLargeIntWidth.reason(),
        )
    }
}
fn cast_largeint_view<'a>(
    array: &'a ArrayRef,
    context: &str,
) -> Result<&'a arrow::array::FixedSizeBinaryArray, ExecutionFailure> {
    let array = array
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .ok_or_else(|| {
            super::expression_contract(format_args!("{context}: expected LARGEINT carrier"))
        })?;
    if array.value_length() != 16 {
        return Err(super::expression_contract(format_args!(
            "{context}: expected 16 bytes, got {}",
            array.value_length()
        )));
    }
    Ok(array)
}
fn cast_largeint_value(
    array: &arrow::array::FixedSizeBinaryArray,
    row: usize,
) -> Result<i128, ExecutionFailure> {
    let bytes: [u8; 16] = array
        .value(row)
        .try_into()
        .map_err(|cause| ExecutionFailure::from_domain(CastLargeIntWidth(cause)))?;
    Ok(i128::from_be_bytes(bytes))
}
fn cast_pow10_i256(exp: usize) -> Result<i256, ExecutionFailure> {
    let mut out = i256::ONE;
    for _ in 0..exp {
        out = out.checked_mul(i256::from_i128(10)).ok_or_else(|| {
            super::expression_contract("decimal scale factor exceeds its frozen i256 carrier")
        })?;
    }
    Ok(out)
}

impl From<novarocks_functions::list_cast_core::ListCastFailure> for ExecutionFailure {
    fn from(cause: novarocks_functions::list_cast_core::ListCastFailure) -> Self {
        Self::from_domain(cause)
    }
}
#[derive(Debug)]
enum CastContainerExtent {
    Offsets { required: u128, minimum_unit: bool },
    Backing { required: u128 },
}
impl std::fmt::Display for CastContainerExtent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Offsets {
                required,
                minimum_unit,
            } => write!(
                f,
                "CAST {} requires {required} child positions, i32 offset limit {}",
                if *minimum_unit { "one row" } else { "output" },
                i32::MAX
            ),
            Self::Backing { required } => write!(
                f,
                "CAST temporary backing requires {required} bytes, layout limit {}",
                isize::MAX
            ),
        }
    }
}
impl std::error::Error for CastContainerExtent {}
impl novarocks_failure_contract::FailureProjection for CastContainerExtent {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::fragment::ExecutionReason;
        use novarocks_failure_contract::{
            FailureClass, FailureFact, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, ResourceDimension, ResourceFacts, ResourceScope, domain,
        };
        match self {
            Self::Offsets {
                required,
                minimum_unit,
            } => FailureFact::new(
                FailureClass::Representation,
                ExecutionReason::ArrayCastContainerOffset.reason(),
            )
            .with_representation(RepresentationFacts {
                boundary: if *minimum_unit {
                    RepresentationBoundary::MinimumUnit
                } else {
                    RepresentationBoundary::SingleOutput
                },
                layout: RepresentationLayout {
                    domain: domain::EXECUTION,
                    code: std::num::NonZeroU16::new(32).unwrap(),
                },
                required: u64::try_from(*required).ok(),
                extent_limit: Some(i32::MAX as u64),
                representable_prefix: None,
            }),
            Self::Backing { required } => {
                novarocks_functions::allocation_failure::rust_array_backing_failure_fact(
                    ExecutionReason::ArrayCastContainerBacking.reason(),
                    *required,
                    RepresentationBoundary::SingleOutput,
                )
            }
        }
    }
}
// The caller sizes the actual demanded rows before cloning/flattening them.
// These layout bounds report representation facts; they do not grant memory.
fn preflight_cast_container(
    rows: usize,
    total: u128,
    largest: usize,
    item_width: usize,
) -> Result<(), ExecutionFailure> {
    if total > i32::MAX as u128 {
        let minimum_unit = largest as u128 > i32::MAX as u128;
        return Err(ExecutionFailure::from_domain(
            CastContainerExtent::Offsets {
                required: if minimum_unit { largest as u128 } else { total },
                minimum_unit,
            },
        ));
    }
    for required in [
        (rows as u128 + 1) * std::mem::size_of::<i32>() as u128,
        total * item_width as u128,
    ] {
        if required > isize::MAX as u128 {
            return Err(ExecutionFailure::from_domain(
                CastContainerExtent::Backing { required },
            ));
        }
    }
    Ok(())
}

impl From<novarocks_functions::date_float_cast::DateFloatFailure> for ExecutionFailure {
    fn from(cause: novarocks_functions::date_float_cast::DateFloatFailure) -> Self {
        Self::from_domain(cause)
    }
}
impl From<novarocks_functions::float_date_cast::FloatDateFailure> for ExecutionFailure {
    fn from(cause: novarocks_functions::float_date_cast::FloatDateFailure) -> Self {
        Self::from_domain(cause)
    }
}
impl From<novarocks_functions::temporal_carrier::TemporalCastFailure> for ExecutionFailure {
    fn from(cause: novarocks_functions::temporal_carrier::TemporalCastFailure) -> Self {
        Self::from_domain(cause)
    }
}

const UNIX_EPOCH_DAY_OFFSET: i32 = 719163;
fn pow10_i128(scale: u32) -> Option<i128> {
    novarocks_functions::legacy_decimal::checked_pow10_i128(scale as usize)
}

fn format_decimal_with_scale(unscaled: i128, scale: i8) -> String {
    novarocks_functions::builtin::string_extended::format_decimal_with_scale(unscaled, scale)
}

fn format_decimal256_with_scale(unscaled: i256, scale: i8) -> String {
    novarocks_functions::builtin::string_extended::format_decimal256_with_scale(unscaled, scale)
}

fn cast_date32_to_float64(arr: &Date32Array) -> Result<ArrayRef, ExecutionFailure> {
    let mut builder = Float64Builder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        builder.append_value(novarocks_functions::date_float_cast::value_f64(
            arr.value(i),
        )?);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn cast_date32_to_float32(arr: &Date32Array) -> Result<ArrayRef, ExecutionFailure> {
    let mut builder = Float32Builder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        builder.append_value(novarocks_functions::date_float_cast::value_f32(
            arr.value(i),
        )?);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn cast_float64_to_date32(arr: &Float64Array) -> Result<ArrayRef, ExecutionFailure> {
    let mut builder = Date32Builder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        builder.append_value(novarocks_functions::float_date_cast::value_f64(
            arr.value(i),
        )?);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn cast_float32_to_date32(arr: &Float32Array) -> Result<ArrayRef, ExecutionFailure> {
    let mut builder = Date32Builder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        builder.append_value(novarocks_functions::float_date_cast::value_f32(
            arr.value(i),
        )?);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn sanitize_non_finite_float64(arr: &Float64Array) -> ArrayRef {
    let mut builder = Float64Builder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        let value = arr.value(i);
        if !value.is_finite() {
            builder.append_null();
        } else {
            builder.append_value(value);
        }
    }
    Arc::new(builder.finish()) as ArrayRef
}

fn sanitize_non_finite_float32(arr: &Float32Array) -> ArrayRef {
    let mut builder = Float32Builder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        let value = arr.value(i);
        match novarocks_functions::decimal_float_cast::finite_f32_value(value) {
            None => builder.append_null(),
            Some(value) => builder.append_value(value),
        }
    }
    Arc::new(builder.finish()) as ArrayRef
}

fn cast_float64_to_float32_preserve_non_finite(arr: &Float64Array) -> ArrayRef {
    let mut builder = Float32Builder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(arr.value(i) as f32);
        }
    }
    Arc::new(builder.finish()) as ArrayRef
}

fn cast_float32_to_float64_preserve_non_finite(arr: &Float32Array) -> ArrayRef {
    let mut builder = Float64Builder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(arr.value(i) as f64);
        }
    }
    Arc::new(builder.finish()) as ArrayRef
}

fn sanitize_non_finite_cast_result(
    array: ArrayRef,
    target_type: &DataType,
) -> Result<ArrayRef, ExecutionFailure> {
    match target_type {
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| super::expression_contract("failed to downcast to Float64Array"))?;
            Ok(sanitize_non_finite_float64(arr))
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| super::expression_contract("failed to downcast to Float32Array"))?;
            Ok(sanitize_non_finite_float32(arr))
        }
        _ => Ok(array),
    }
}

fn is_integral_target_type(target_type: &DataType) -> bool {
    matches!(
        target_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

fn integral_target_name(target_type: &DataType) -> &'static str {
    match target_type {
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INT",
        DataType::Int64 => "BIGINT",
        DataType::UInt8 => "TINYINT UNSIGNED",
        DataType::UInt16 => "SMALLINT UNSIGNED",
        DataType::UInt32 => "INT UNSIGNED",
        DataType::UInt64 => "BIGINT UNSIGNED",
        _ => "INTEGER",
    }
}

fn first_float_to_int_overflow_value(
    source: &ArrayRef,
    casted: &ArrayRef,
    target_type: &DataType,
) -> Result<Option<f64>, ExecutionFailure> {
    if source.len() != casted.len() || !is_integral_target_type(target_type) {
        return Ok(None);
    }

    match source.data_type() {
        DataType::Float32 => {
            let arr = source
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| super::expression_contract("failed to downcast to Float32Array"))?;
            for i in 0..arr.len() {
                if !arr.is_null(i) && casted.is_null(i) {
                    return Ok(Some(arr.value(i) as f64));
                }
            }
            Ok(None)
        }
        DataType::Float64 => {
            let arr = source
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| super::expression_contract("failed to downcast to Float64Array"))?;
            for i in 0..arr.len() {
                if !arr.is_null(i) && casted.is_null(i) {
                    return Ok(Some(arr.value(i)));
                }
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

fn has_decimal_to_decimal_overflow(source: &ArrayRef, casted: &ArrayRef) -> bool {
    if source.len() != casted.len() {
        return false;
    }
    let source_is_decimal = matches!(
        source.data_type(),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
    );
    let target_is_decimal = matches!(
        casted.data_type(),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
    );
    if !source_is_decimal || !target_is_decimal {
        return false;
    }
    for row in 0..source.len() {
        if !source.is_null(row) && casted.is_null(row) {
            return true;
        }
    }
    false
}

fn parse_time_string_to_seconds(raw: &str) -> Option<i64> {
    novarocks_functions::time_text_cast::parse_time_string_to_seconds(raw)
}

fn parse_datetime_string_to_seconds(raw: &str) -> Option<i64> {
    novarocks_functions::time_text_cast::parse_datetime_string_to_seconds(raw)
}

fn parse_time_integer_to_seconds(value: i64) -> Option<i64> {
    if value < 0 {
        return None;
    }
    if value < 100 {
        return Some(value);
    }
    let hour = value / 10000;
    let minute = (value / 100) % 100;
    let second = value % 100;
    if minute >= 60 || second >= 60 {
        return None;
    }
    hour.checked_mul(3600)?
        .checked_add(minute.checked_mul(60)?)?
        .checked_add(second)
}

fn parse_time_float_to_seconds(value: f64) -> Option<i64> {
    if !value.is_finite() {
        return None;
    }
    parse_time_integer_to_seconds(value.trunc() as i64)
}

fn seconds_to_time64_micro_array(seconds: Vec<Option<i64>>) -> Result<ArrayRef, ExecutionFailure> {
    Ok(novarocks_functions::time_text_cast::seconds_to_time64_micro_array(seconds))
}

fn parse_varchar_to_boolean_starrocks(value: &str) -> Option<bool> {
    // StarRocks BE first parses VARCHAR as int32; when that succeeds, non-zero is true.
    // If integer parsing fails, it falls back to strict boolean text parsing.
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(v) = trimmed.parse::<i32>() {
        return Some(v != 0);
    }
    if trimmed.eq_ignore_ascii_case("true") {
        return Some(true);
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return Some(false);
    }
    None
}

fn cast_utf8_to_boolean_array(arr: &StringArray) -> ArrayRef {
    let mut builder = BooleanBuilder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        match parse_varchar_to_boolean_starrocks(arr.value(i)) {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish()) as ArrayRef
}

fn format_float64_for_varchar(value: f64) -> String {
    novarocks_functions::builtin::string_extended::format_float64_for_varchar(value)
}

fn format_float32_for_varchar(value: f32) -> String {
    novarocks_functions::builtin::string_extended::format_float32_for_varchar(value)
}

fn decimal_precision_limit(precision: u8) -> Option<i128> {
    if precision == 0 {
        return Some(1);
    }
    pow10_i128(precision as u32)
}

fn decimal_value_within_precision(value: i128, precision: u8) -> bool {
    let Some(limit) = decimal_precision_limit(precision) else {
        return false;
    };
    value > -limit && value < limit
}

fn decimal256_precision_limit(precision: u8) -> Option<i256> {
    if precision == 0 {
        return Some(i256::ONE);
    }
    cast_pow10_i256(precision as usize).ok()
}

fn decimal256_value_within_precision(value: i256, precision: u8) -> bool {
    let Some(limit) = decimal256_precision_limit(precision) else {
        return false;
    };
    let Some(neg_limit) = limit.checked_neg() else {
        return false;
    };
    value > neg_limit && value < limit
}

pub fn format_timestamp_for_varchar(unit: &TimeUnit, value: i64, tz: Option<&str>) -> String {
    novarocks_functions::builtin::string_extended::format_timestamp_for_varchar(unit, value, tz)
}

fn field_has_json_semantics(field_schema: Option<&novarocks_type_contract::ValueTypeRef>) -> bool {
    field_schema.is_some_and(|schema| {
        schema.domain() == Some(novarocks_type_contract::ValueLogicalType::Json)
    })
}

fn format_json_value_for_cast(value: &JsonValue, out: &mut String) -> Result<(), ExecutionFailure> {
    match value {
        JsonValue::Null => out.push_str("null"),
        JsonValue::Bool(v) => out.push_str(if *v { "true" } else { "false" }),
        JsonValue::Number(v) => out.push_str(&v.to_string()),
        JsonValue::String(v) => {
            let escaped = serde_json::to_string(v).map_err(cast_json_encode_failure)?;
            out.push_str(&escaped);
        }
        JsonValue::Array(items) => {
            out.push('[');
            for (idx, item) in items.iter().enumerate() {
                if idx > 0 {
                    out.push_str(", ");
                }
                format_json_value_for_cast(item, out)?;
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
                let escaped_key = serde_json::to_string(key).map_err(cast_json_encode_failure)?;
                out.push_str(&escaped_key);
                out.push_str(": ");
                let child = map.get(*key).ok_or_else(|| {
                    super::expression_contract("json object key missing while cast")
                })?;
                format_json_value_for_cast(child, out)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

fn json_value_to_normalized_text(value: &JsonValue) -> Result<String, ExecutionFailure> {
    let mut out = String::new();
    format_json_value_for_cast(value, &mut out)?;
    Ok(out)
}

fn json_value_to_varchar(value: &JsonValue) -> Result<Option<String>, ExecutionFailure> {
    match value {
        JsonValue::Null => Ok(None),
        JsonValue::String(v) => Ok(Some(v.clone())),
        _ => Ok(Some(json_value_to_normalized_text(value)?)),
    }
}

fn parse_utf8_json_rows(arr: &StringArray) -> Vec<Option<JsonValue>> {
    let mut out = Vec::with_capacity(arr.len());
    for row in 0..arr.len() {
        if arr.is_null(row) {
            out.push(None);
            continue;
        }
        out.push(serde_json::from_str::<JsonValue>(arr.value(row)).ok());
    }
    out
}

fn cast_optional_utf8_to_target(
    values: Vec<Option<String>>,
    target_type: &DataType,
) -> Result<ArrayRef, ExecutionFailure> {
    let source = Arc::new(StringArray::from(values)) as ArrayRef;
    if matches!(target_type, DataType::Utf8) {
        return Ok(source);
    }
    cast_with_special_rules(&source, target_type)
}

fn is_integral_or_largeint_target_type(target_type: &DataType) -> bool {
    is_integral_target_type(target_type)
        || matches!(
            target_type,
            DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH
        )
}

fn json_number_to_integral_string(value: &serde_json::Number) -> Option<String> {
    if let Some(v) = value.as_i64() {
        return Some(v.to_string());
    }
    if let Some(v) = value.as_u64() {
        return Some(v.to_string());
    }
    let v = value.as_f64()?;
    if !v.is_finite() || v > i128::MAX as f64 || v < i128::MIN as f64 {
        return None;
    }
    Some((v.trunc() as i128).to_string())
}

fn cast_json_values_to_list(
    values: &[Option<JsonValue>],
    target_field: Arc<Field>,
    target_field_schema: Option<&novarocks_type_contract::ValueTypeRef>,
) -> Result<ArrayRef, ExecutionFailure> {
    let (total, largest) = values
        .iter()
        .filter_map(|value| match value {
            Some(JsonValue::Array(items)) => Some(items.len()),
            _ => None,
        })
        .fold((0_u128, 0_usize), |(total, largest), len| {
            (total + len as u128, largest.max(len))
        });
    preflight_cast_container(
        values.len(),
        total,
        largest,
        std::mem::size_of::<Option<JsonValue>>(),
    )?;
    let mut flat_values = Vec::<Option<JsonValue>>::new();
    let mut offsets = Vec::with_capacity(values.len() + 1);
    offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(values.len());

    for value in values {
        match value {
            Some(JsonValue::Array(items)) => {
                current += items.len() as i64;
                for item in items {
                    flat_values.push(Some(item.clone()));
                }
                null_builder.append_non_null();
                offsets.push(current as i32);
            }
            _ => {
                null_builder.append_null();
                offsets.push(current as i32);
            }
        }
    }

    let cast_values = cast_json_values_to_target(
        &flat_values,
        target_field.data_type(),
        target_field_schema.and_then(|schema| schema.child_type_at(0)),
    )?;
    let out = ListArray::try_new(
        target_field,
        OffsetBuffer::new(offsets.into()),
        cast_values,
        null_builder.finish(),
    )
    .map_err(cast_construct_failure)?;
    Ok(Arc::new(out) as ArrayRef)
}

fn cast_json_values_to_struct(
    values: &[Option<JsonValue>],
    target_fields: Fields,
    target_field_schema: Option<&novarocks_type_contract::ValueTypeRef>,
) -> Result<ArrayRef, ExecutionFailure> {
    let field_count = target_fields.len();
    let mut field_values = vec![Vec::<Option<JsonValue>>::with_capacity(values.len()); field_count];
    let mut struct_nulls = NullBufferBuilder::new(values.len());

    for value in values {
        match value {
            Some(JsonValue::Array(items)) => {
                for (idx, field_buf) in field_values.iter_mut().enumerate() {
                    field_buf.push(items.get(idx).cloned());
                }
                struct_nulls.append_non_null();
            }
            Some(JsonValue::Object(map)) => {
                for (idx, field) in target_fields.iter().enumerate() {
                    field_values[idx].push(map.get(field.name()).cloned());
                }
                struct_nulls.append_non_null();
            }
            _ => {
                for field_buf in &mut field_values {
                    field_buf.push(None);
                }
                struct_nulls.append_null();
            }
        }
    }

    let mut casted_fields = Vec::with_capacity(field_count);
    for (idx, field) in target_fields.iter().enumerate() {
        casted_fields.push(cast_json_values_to_target(
            &field_values[idx],
            field.data_type(),
            target_field_schema.and_then(|schema| schema.child_type_at(idx)),
        )?);
    }
    let out = StructArray::try_new(target_fields, casted_fields, struct_nulls.finish())
        .map_err(cast_construct_failure)?;
    Ok(Arc::new(out) as ArrayRef)
}

fn cast_json_values_to_map(
    values: &[Option<JsonValue>],
    entries_field: Arc<Field>,
    ordered: bool,
    target_field_schema: Option<&novarocks_type_contract::ValueTypeRef>,
) -> Result<ArrayRef, ExecutionFailure> {
    let DataType::Struct(entry_fields) = entries_field.data_type() else {
        return Err(super::expression_contract(
            "CAST MAP target entries field must be STRUCT",
        ));
    };
    if entry_fields.len() != 2 {
        return Err(super::expression_contract(
            "CAST MAP target entries struct must have 2 fields",
        ));
    }
    let key_field = entry_fields[0].clone();
    let value_field = entry_fields[1].clone();

    let (total, largest) = values
        .iter()
        .filter_map(|value| match value {
            Some(JsonValue::Object(items)) => Some(items.len()),
            _ => None,
        })
        .fold((0_u128, 0_usize), |(total, largest), len| {
            (total + len as u128, largest.max(len))
        });
    preflight_cast_container(
        values.len(),
        total,
        largest,
        std::mem::size_of::<Option<JsonValue>>().max(std::mem::size_of::<Option<String>>()),
    )?;
    let mut key_values = Vec::<Option<String>>::new();
    let mut value_values = Vec::<Option<JsonValue>>::new();
    let mut offsets = Vec::with_capacity(values.len() + 1);
    offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut map_nulls = NullBufferBuilder::new(values.len());

    for value in values {
        match value {
            Some(JsonValue::Object(map)) => {
                for (key, child) in map.iter() {
                    key_values.push(Some(key.clone()));
                    value_values.push(Some(child.clone()));
                }
                current += map.len() as i64;
                map_nulls.append_non_null();
                offsets.push(current as i32);
            }
            _ => {
                map_nulls.append_null();
                offsets.push(current as i32);
            }
        }
    }

    let casted_keys = cast_optional_utf8_to_target(key_values, key_field.data_type())?;
    let casted_values = cast_json_values_to_target(
        &value_values,
        value_field.data_type(),
        target_field_schema.and_then(|schema| schema.child_type_at(1)),
    )?;
    let entries =
        StructArray::try_new(entry_fields.clone(), vec![casted_keys, casted_values], None)
            .map_err(cast_construct_failure)?;
    let map = MapArray::try_new(
        entries_field,
        OffsetBuffer::new(offsets.into()),
        entries,
        map_nulls.finish(),
        ordered,
    )
    .map_err(cast_construct_failure)?;
    Ok(Arc::new(map) as ArrayRef)
}

fn cast_json_values_to_target(
    values: &[Option<JsonValue>],
    target_type: &DataType,
    target_field_schema: Option<&novarocks_type_contract::ValueTypeRef>,
) -> Result<ArrayRef, ExecutionFailure> {
    match target_type {
        DataType::Utf8 => {
            let json_semantic = field_has_json_semantics(target_field_schema);
            let mut builder = StringBuilder::new();
            for value in values {
                match value {
                    None => builder.append_null(),
                    Some(v) if json_semantic => {
                        builder.append_value(json_value_to_normalized_text(v)?);
                    }
                    Some(v) => match json_value_to_varchar(v)? {
                        Some(text) => builder.append_value(text),
                        None => builder.append_null(),
                    },
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::List(target_field) => {
            cast_json_values_to_list(values, target_field.clone(), target_field_schema)
        }
        DataType::Struct(target_fields) => {
            cast_json_values_to_struct(values, target_fields.clone(), target_field_schema)
        }
        DataType::Map(entries_field, ordered) => {
            cast_json_values_to_map(values, entries_field.clone(), *ordered, target_field_schema)
        }
        _ => {
            let mut scalar_strings = Vec::with_capacity(values.len());
            for value in values {
                let text = match value {
                    None => None,
                    Some(v) if v.is_null() => None,
                    Some(JsonValue::Bool(v))
                        if is_integral_or_largeint_target_type(target_type) =>
                    {
                        Some(if *v { "1" } else { "0" }.to_string())
                    }
                    Some(JsonValue::Number(v))
                        if is_integral_or_largeint_target_type(target_type) =>
                    {
                        json_number_to_integral_string(v)
                    }
                    Some(v) if v.is_string() => v.as_str().map(|s| s.to_string()),
                    Some(v) => Some(json_value_to_normalized_text(v)?),
                };
                scalar_strings.push(text);
            }
            cast_optional_utf8_to_target(scalar_strings, target_type)
        }
    }
}

fn cast_utf8_json_to_target(
    array: &ArrayRef,
    target_type: &DataType,
    target_field_schema: Option<&novarocks_type_contract::ValueTypeRef>,
) -> Result<ArrayRef, ExecutionFailure> {
    let arr = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| super::expression_contract("failed to downcast to StringArray"))?;
    let parsed = parse_utf8_json_rows(arr);
    cast_json_values_to_target(&parsed, target_type, target_field_schema)
}

fn cast_struct_to_struct(
    array: &ArrayRef,
    target_fields: &Fields,
) -> Result<ArrayRef, ExecutionFailure> {
    let source = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| super::expression_contract("failed to downcast to StructArray"))?;
    let source_fields = match array.data_type() {
        DataType::Struct(fields) => fields,
        other => {
            return Err(super::expression_contract(format_args!(
                "CAST failed: source is not STRUCT, got {:?}",
                other
            )));
        }
    };
    if source_fields.len() != target_fields.len() {
        return Err(super::expression_contract(format_args!(
            "CAST STRUCT field count mismatch: source={} target={}",
            source_fields.len(),
            target_fields.len()
        )));
    }

    // Build a name-to-index map for source fields to support by-name matching
    // when field names differ in order but all target names exist in source.
    let source_name_to_idx: std::collections::HashMap<&str, usize> = source_fields
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().as_str(), i))
        .collect();
    let use_by_name = target_fields
        .iter()
        .all(|f| source_name_to_idx.contains_key(f.name().as_str()));

    let mut casted_columns = Vec::with_capacity(target_fields.len());
    let mut out_fields = target_fields.iter().cloned().collect::<Vec<_>>();
    for (idx, target_field) in target_fields.iter().enumerate() {
        let source_idx = if use_by_name {
            *source_name_to_idx
                .get(target_field.name().as_str())
                .unwrap()
        } else {
            idx
        };
        let source_col = source.column(source_idx).clone();
        let casted = if source_col.data_type() == target_field.data_type() {
            source_col
        } else if source_col.null_count() == source_col.len() {
            new_null_array(target_field.data_type(), source_col.len())
        } else {
            cast_with_special_rules(&source_col, target_field.data_type())?
        };
        if casted.data_type() != target_field.data_type() {
            out_fields[idx] = Arc::new(Field::new(
                target_field.name(),
                casted.data_type().clone(),
                target_field.is_nullable(),
            ));
        }
        casted_columns.push(casted);
    }

    let out = StructArray::try_new(
        Fields::from(out_fields),
        casted_columns,
        source.nulls().cloned(),
    )
    .map_err(cast_construct_failure)?;
    Ok(Arc::new(out) as ArrayRef)
}

fn cast_map_to_utf8_array(array: &ArrayRef) -> Result<ArrayRef, ExecutionFailure> {
    let source = array
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| super::expression_contract("failed to downcast to MapArray"))?;
    let (entries_field, ordered) = match source.data_type() {
        DataType::Map(entries_field, ordered) => (entries_field.clone(), *ordered),
        other => {
            return Err(super::expression_contract(format_args!(
                "CAST failed: source is not MAP, got {:?}",
                other
            )));
        }
    };
    let DataType::Struct(entry_fields) = entries_field.data_type() else {
        return Err(super::expression_contract(
            "CAST MAP source entries field must be STRUCT",
        ));
    };
    if entry_fields.len() != 2 {
        return Err(super::expression_contract(
            "CAST MAP source entries struct must have 2 fields",
        ));
    }
    let (sorted_offsets, sorted_indices) =
        crate::exec::expr::function::map::sorted_map_offsets_and_indices(source)?;
    let sorted_index_array = UInt32Array::from(sorted_indices);
    let sorted_keys =
        take(source.keys().as_ref(), &sorted_index_array, None).map_err(cast_arrow_failure)?;
    let sorted_values =
        take(source.values().as_ref(), &sorted_index_array, None).map_err(cast_arrow_failure)?;
    let entries =
        StructArray::try_new(entry_fields.clone(), vec![sorted_keys, sorted_values], None)
            .map_err(cast_construct_failure)?;
    let sorted_map = MapArray::try_new(
        entries_field,
        sorted_offsets,
        entries,
        source.nulls().cloned(),
        ordered,
    )
    .map_err(cast_construct_failure)?;
    cast(&sorted_map, &DataType::Utf8).map_err(cast_arrow_failure)
}

fn cast_map_to_map(
    array: &ArrayRef,
    target_entries: &Arc<Field>,
    ordered: bool,
) -> Result<ArrayRef, ExecutionFailure> {
    let source = array
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| super::expression_contract("failed to downcast to MapArray"))?;
    let DataType::Struct(target_entry_fields) = target_entries.data_type() else {
        return Err(super::expression_contract(
            "CAST MAP target entries type must be STRUCT",
        ));
    };
    if target_entry_fields.len() != 2 {
        return Err(super::expression_contract(
            "CAST MAP target entries struct must have 2 fields",
        ));
    }
    let target_key_field = target_entry_fields[0].clone();
    let target_value_field = target_entry_fields[1].clone();

    let source_entries = source.entries();
    let source_keys = source_entries.column(0).clone();
    let source_values = source_entries.column(1).clone();

    let casted_keys = if source_keys.data_type() == target_key_field.data_type() {
        source_keys
    } else if source_keys.null_count() == source_keys.len() {
        new_null_array(target_key_field.data_type(), source_keys.len())
    } else {
        cast_with_special_rules(&source_keys, target_key_field.data_type())?
    };
    let casted_values = if source_values.data_type() == target_value_field.data_type() {
        source_values
    } else if source_values.null_count() == source_values.len() {
        new_null_array(target_value_field.data_type(), source_values.len())
    } else {
        cast_with_special_rules(&source_values, target_value_field.data_type())?
    };

    let mut out_entry_fields = target_entry_fields.clone();
    if casted_keys.null_count() > 0 && !target_key_field.is_nullable() {
        let mut adjusted = out_entry_fields.iter().cloned().collect::<Vec<_>>();
        adjusted[0] = Arc::new(Field::new(
            target_key_field.name(),
            target_key_field.data_type().clone(),
            true,
        ));
        out_entry_fields = Fields::from(adjusted);
    }
    if casted_values.data_type() != target_value_field.data_type() {
        let mut adjusted = out_entry_fields.iter().cloned().collect::<Vec<_>>();
        adjusted[1] = Arc::new(Field::new(
            target_value_field.name(),
            casted_values.data_type().clone(),
            target_value_field.is_nullable(),
        ));
        out_entry_fields = Fields::from(adjusted);
    }

    let out_entries = StructArray::try_new(
        out_entry_fields.clone(),
        vec![casted_keys, casted_values],
        None,
    )
    .map_err(cast_construct_failure)?;
    let out_entries_field = Arc::new(Field::new(
        target_entries.name(),
        DataType::Struct(out_entry_fields),
        target_entries.is_nullable(),
    ));
    let out = MapArray::try_new(
        out_entries_field,
        OffsetBuffer::new(source.value_offsets().to_vec().into()),
        out_entries,
        source.nulls().cloned(),
        ordered,
    )
    .map_err(cast_construct_failure)?;
    Ok(Arc::new(out) as ArrayRef)
}

fn cast_list_to_map(
    array: &ArrayRef,
    target_entries: &Arc<Field>,
    ordered: bool,
) -> Result<ArrayRef, ExecutionFailure> {
    let source = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| super::expression_contract("failed to downcast to ListArray"))?;
    let values = if source.values().data_type() == target_entries.data_type() {
        source.values().clone()
    } else {
        cast_with_special_rules(source.values(), target_entries.data_type())?
    };
    let entries = values
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| super::expression_contract("CAST LIST to MAP requires STRUCT list values"))?
        .clone();
    let out = MapArray::try_new(
        target_entries.clone(),
        OffsetBuffer::new(source.value_offsets().to_vec().into()),
        entries,
        source.nulls().cloned(),
        ordered,
    )
    .map_err(cast_construct_failure)?;
    Ok(Arc::new(out) as ArrayRef)
}

#[derive(Debug)]
struct ArrayCastArrowFailure(arrow::error::ArrowError);
impl std::fmt::Display for ArrayCastArrowFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "array CAST: {}", self.0)
    }
}
impl std::error::Error for ArrayCastArrowFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for ArrayCastArrowFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::fragment::ExecutionReason;
        use arrow::error::ArrowError;
        use novarocks_failure_contract::{
            FailureClass, FailureFact, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        if matches!(self.0, ArrowError::ArithmeticOverflow(_)) {
            return novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact();
        }
        let (class, reason) = match self.0 {
            ArrowError::MemoryError(_) => {
                (FailureClass::Resource, ExecutionReason::ArrayCastAllocation)
            }
            ArrowError::OffsetOverflowError(_) => (
                FailureClass::Representation,
                ExecutionReason::ArrayCastOffset,
            ),
            ArrowError::CastError(_) => (FailureClass::Data, ExecutionReason::ArrayCastData),
            ArrowError::IoError(..) => (
                FailureClass::Unexpected,
                ExecutionReason::ArrayCastUnexpected,
            ),
            _ => (FailureClass::Invariant, ExecutionReason::ArrayCastContract),
        };
        let fact = FailureFact::new(class, reason.reason());
        if matches!(self.0, ArrowError::OffsetOverflowError(_)) {
            fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: RepresentationLayout {
                    domain: domain::EXECUTION,
                    code: std::num::NonZeroU16::new(31).unwrap(),
                },
                required: None,
                extent_limit: None,
                representable_prefix: None,
            })
        } else {
            fact
        }
    }
}
pub(super) fn cast_arrow_failure(
    source: arrow::error::ArrowError,
) -> crate::runtime::fragment::ExecutionFailure {
    crate::runtime::fragment::ExecutionFailure::from_domain(ArrayCastArrowFailure(source))
}
pub fn cast_with_special_rules(
    array: &ArrayRef,
    target_type: &DataType,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    cast_with_special_rules_with_field_schema(array, target_type, None)
}

fn cast_with_special_rules_with_field_schema(
    array: &ArrayRef,
    target_type: &DataType,
    target_field_schema: Option<&novarocks_type_contract::ValueTypeRef>,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    if array.data_type() == target_type {
        return Ok(array.clone());
    }
    // Cast-to-Null: produce an all-NULL array. Arrow's stock kernel rejects
    // most `<T> → Null` casts even though it is semantically well-defined —
    // the Null type only has the NULL value, so every input row collapses to
    // NULL. This shows up in queries like `map{}['a']`, where the empty map
    // literal is typed `MAP<Null, Null>` and the key argument must be cast to
    // the Null key type before lookup (the lookup itself trivially returns
    // NULL).
    if target_type == &DataType::Null {
        return Ok(new_null_array(&DataType::Null, array.len()));
    }
    if array.data_type() == &DataType::Null {
        return Ok(novarocks_functions::list_cast_core::null_source(
            array.len(),
            target_type,
        ));
    }
    if !matches!(
        array.data_type(),
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Struct(_)
            | DataType::Map(_, _)
    ) && !matches!(
        target_type,
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Struct(_)
            | DataType::Map(_, _)
    ) {
        return novarocks_types::arrow_cast::cast_scalar_with_special_rules(array, target_type)
            .map_err(crate::runtime::fragment::ExecutionFailure::from_domain);
    }
    match (array.data_type(), target_type) {
        (DataType::Map(_, _), DataType::Utf8) => cast_map_to_utf8_array(array),
        (DataType::Utf8, DataType::List(target_field))
            if novarocks_type_contract::default_json_structure_carrier_cast_supported(
                array.data_type(),
                target_type,
            ) =>
        {
            if let Some(out) = try_cast_utf8_json_array_to_list(
                array,
                target_field.clone(),
                target_field_schema.and_then(|schema| schema.child_type_at(0)),
            )? {
                Ok(out)
            } else {
                cast(array.as_ref(), target_type).map_err(cast_arrow_failure)
            }
        }
        (DataType::Utf8, DataType::Struct(_)) | (DataType::Utf8, DataType::Map(_, _))
            if novarocks_type_contract::default_json_structure_carrier_cast_supported(
                array.data_type(),
                target_type,
            ) =>
        {
            cast_utf8_json_to_target(array, target_type, target_field_schema)
        }
        (DataType::List(source_field), DataType::Map(target_entries, ordered))
            if matches!(source_field.data_type(), DataType::Struct(_)) =>
        {
            cast_list_to_map(array, target_entries, *ordered)
        }
        (DataType::List(_), DataType::List(target_field)) => {
            novarocks_functions::list_cast_core::cast(array, target_field, &mut |child, target| {
                cast_with_special_rules_with_field_schema(
                    child,
                    target,
                    target_field_schema.and_then(|schema| schema.child_type_at(0)),
                )
            })
        }
        (DataType::Struct(_), DataType::Struct(target_fields)) => {
            cast_struct_to_struct(array, target_fields)
        }
        (DataType::Map(_, _), DataType::Map(target_entries, ordered)) => {
            cast_map_to_map(array, target_entries, *ordered)
        }
        _ => cast(array.as_ref(), target_type).map_err(cast_arrow_failure),
    }
}

fn cast_utf8_to_decimal_with_empty_as_null(
    child_array: &ArrayRef,
    target_type: &DataType,
) -> Result<ArrayRef, ExecutionFailure> {
    let arr = child_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| super::expression_contract("failed to downcast to StringArray"))?;

    let mut builder = StringBuilder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        let value = arr.value(i);
        if value.trim().is_empty() {
            builder.append_null();
        } else {
            builder.append_value(value);
        }
    }

    let normalized = Arc::new(builder.finish()) as ArrayRef;
    cast(normalized.as_ref(), target_type).map_err(cast_arrow_failure)
}

fn try_cast_utf8_json_array_to_list(
    child_array: &ArrayRef,
    target_field: Arc<Field>,
    target_field_schema: Option<&novarocks_type_contract::ValueTypeRef>,
) -> Result<Option<ArrayRef>, ExecutionFailure> {
    let arr = child_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| super::expression_contract("failed to downcast to StringArray"))?;

    let mut parsed_rows = Vec::<Option<Vec<Option<String>>>>::with_capacity(arr.len());
    for row in 0..arr.len() {
        if arr.is_null(row) {
            parsed_rows.push(None);
            continue;
        }
        let value: JsonValue = match serde_json::from_str(arr.value(row)) {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };
        match value {
            JsonValue::Null => {
                parsed_rows.push(None);
            }
            JsonValue::Array(items) => {
                let mut row_values = Vec::with_capacity(items.len());
                for item in items {
                    if item.is_null() {
                        row_values.push(None);
                    } else {
                        let normalized = if let Some(text) = item.as_str() {
                            text.to_string()
                        } else {
                            item.to_string()
                        };
                        row_values.push(Some(normalized));
                    }
                }
                parsed_rows.push(Some(row_values));
            }
            _ => return Ok(None),
        }
    }

    let (total, largest) = parsed_rows
        .iter()
        .filter_map(|row| row.as_ref().map(Vec::len))
        .fold((0_u128, 0_usize), |(total, largest), len| {
            (total + len as u128, largest.max(len))
        });
    preflight_cast_container(
        arr.len(),
        total,
        largest,
        std::mem::size_of::<Option<String>>(),
    )?;
    let mut flat_values = Vec::<Option<String>>::new();
    let mut offsets = Vec::with_capacity(arr.len() + 1);
    offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(arr.len());

    for parsed in parsed_rows {
        match parsed {
            None => {
                null_builder.append_null();
                offsets.push(current as i32);
            }
            Some(values) => {
                current += values.len() as i64;
                flat_values.extend(values);
                null_builder.append_non_null();
                offsets.push(current as i32);
            }
        }
    }

    let string_values = Arc::new(StringArray::from(flat_values)) as ArrayRef;
    let cast_values = if string_values.data_type() == target_field.data_type() {
        string_values
    } else {
        cast_with_special_rules_with_field_schema(
            &string_values,
            target_field.data_type(),
            target_field_schema,
        )?
    };
    let out = ListArray::try_new(
        target_field.clone(),
        OffsetBuffer::new(offsets.into()),
        cast_values,
        null_builder.finish(),
    )
    .map_err(cast_construct_failure)?;
    Ok(Some(Arc::new(out) as ArrayRef))
}

fn cast_largeint_binary_to_decimal(
    child_array: &ArrayRef,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef, ExecutionFailure> {
    let arr = cast_largeint_view(child_array, "cast LARGEINT to DECIMAL")?;
    let mut values = Vec::with_capacity(arr.len());
    let multiplier = if scale >= 0 {
        Some(10_i128.checked_pow(scale as u32).ok_or_else(|| {
            super::expression_contract(format_args!("decimal scale overflow: {scale}"))
        })?)
    } else {
        None
    };
    let divisor = if scale < 0 {
        Some(10_i128.checked_pow((-scale) as u32).ok_or_else(|| {
            super::expression_contract(format_args!("decimal scale overflow: {scale}"))
        })?)
    } else {
        None
    };

    for row in 0..arr.len() {
        if arr.is_null(row) {
            values.push(None);
            continue;
        }
        let mut value = cast_largeint_value(arr, row)?;
        if let Some(m) = multiplier {
            let Some(scaled) = value.checked_mul(m) else {
                values.push(None);
                continue;
            };
            value = scaled;
        } else if let Some(d) = divisor {
            value /= d;
        }
        let enforce_precision = precision <= 18;
        if enforce_precision && !decimal_value_within_precision(value, precision) {
            values.push(None);
            continue;
        }
        values.push(Some(value));
    }

    let out = Decimal128Array::from(values)
        .with_precision_and_scale(precision, scale)
        .map_err(cast_construct_failure)?;
    Ok(Arc::new(out) as ArrayRef)
}

fn cast_largeint_binary_to_decimal256(
    child_array: &ArrayRef,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef, ExecutionFailure> {
    let arr = cast_largeint_view(child_array, "cast LARGEINT to DECIMAL256")?;
    let mut values = Vec::with_capacity(arr.len());
    let multiplier = if scale >= 0 {
        Some(cast_pow10_i256(scale as usize)?)
    } else {
        None
    };
    let divisor = if scale < 0 {
        Some(cast_pow10_i256((-scale) as usize)?)
    } else {
        None
    };

    for row in 0..arr.len() {
        if arr.is_null(row) {
            values.push(None);
            continue;
        }
        let mut value = i256::from_i128(cast_largeint_value(arr, row)?);
        if let Some(m) = multiplier {
            let Some(scaled) = value.checked_mul(m) else {
                values.push(None);
                continue;
            };
            value = scaled;
        } else if let Some(d) = divisor {
            let Some(scaled) = value.checked_div(d) else {
                values.push(None);
                continue;
            };
            value = scaled;
        }
        if !decimal256_value_within_precision(value, precision) {
            values.push(None);
            continue;
        }
        values.push(Some(value));
    }

    let out = Decimal256Array::from(values)
        .with_precision_and_scale(precision, scale)
        .map_err(cast_construct_failure)?;
    Ok(Arc::new(out) as ArrayRef)
}

fn decimal256_to_f64(value: i256, scale: i8) -> f64 {
    novarocks_functions::decimal_float_cast::decimal256_to_f64(value, scale)
}

fn cast_decimal256_to_float64(
    child_array: &ArrayRef,
    scale: i8,
) -> Result<ArrayRef, ExecutionFailure> {
    let arr = child_array
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .ok_or_else(|| {
            super::expression_contract("failed to downcast to Decimal256Array for float64 cast")
        })?;
    let mut values: Vec<Option<f64>> = Vec::with_capacity(arr.len());
    for row in 0..arr.len() {
        if arr.is_null(row) {
            values.push(None);
        } else {
            values.push(Some(decimal256_to_f64(arr.value(row), scale)));
        }
    }
    Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
}

fn cast_decimal256_to_float32(
    child_array: &ArrayRef,
    scale: i8,
) -> Result<ArrayRef, ExecutionFailure> {
    let arr = child_array
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .ok_or_else(|| {
            super::expression_contract("failed to downcast to Decimal256Array for float32 cast")
        })?;
    let mut values: Vec<Option<f32>> = Vec::with_capacity(arr.len());
    for row in 0..arr.len() {
        if arr.is_null(row) {
            values.push(None);
        } else {
            // Convert to f64 first for precision, then narrow to f32.
            // f64->f32 narrowing preserves +inf/-inf for out-of-range values.
            values.push(Some(
                novarocks_functions::decimal_float_cast::decimal256_to_f32(arr.value(row), scale),
            ));
        }
    }
    Ok(Arc::new(Float32Array::from(values)) as ArrayRef)
}

fn cast_largeint_binary_to_float64(child_array: &ArrayRef) -> Result<ArrayRef, ExecutionFailure> {
    let arr = cast_largeint_view(child_array, "cast LARGEINT to DOUBLE")?;
    let mut out = Float64Builder::new();
    for row in 0..arr.len() {
        if arr.is_null(row) {
            out.append_null();
            continue;
        }
        out.append_value(cast_largeint_value(arr, row)? as f64);
    }
    Ok(Arc::new(out.finish()) as ArrayRef)
}

fn cast_largeint_binary_to_float32(child_array: &ArrayRef) -> Result<ArrayRef, ExecutionFailure> {
    let arr = cast_largeint_view(child_array, "cast LARGEINT to FLOAT")?;
    let mut out = Float32Builder::new();
    for row in 0..arr.len() {
        if arr.is_null(row) {
            out.append_null();
            continue;
        }
        out.append_value(cast_largeint_value(arr, row)? as f32);
    }
    Ok(Arc::new(out.finish()) as ArrayRef)
}

fn is_decimal_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Decimal128(..) | DataType::Decimal256(..)
    )
}

fn checked_numeric_cast_has_overflow(
    source: &ArrayRef,
    casted: &ArrayRef,
) -> Result<bool, ExecutionFailure> {
    novarocks_functions::decimal128_rescale::checked_numeric_cast_has_overflow_legacy(
        source, casted,
    )
    .map_err(Into::into)
}

/// Enforce a declared decimal target even on same-scale/retag conversion paths.
/// Limits are frozen metadata and computed once, before iterating values.
fn enforce_declared_decimal_precision(array: ArrayRef) -> Result<ArrayRef, ExecutionFailure> {
    match array.data_type() {
        DataType::Decimal128(..) => {
            novarocks_functions::decimal128_rescale::enforce_precision_legacy(array)
                .map_err(Into::into)
        }
        DataType::Decimal256(precision, scale) => {
            let (precision, scale) = (*precision, *scale);
            let limit = cast_pow10_i256(usize::from(precision))?;
            let source = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| {
                    super::expression_contract("checked CAST Decimal256 downcast failed")
                })?;
            let fits = |value: i256| value > -limit && value < limit;
            if (0..source.len()).all(|row| source.is_null(row) || fits(source.value(row))) {
                return Ok(array);
            }
            let values = (0..source.len())
                .map(|row| {
                    if source.is_null(row) || !fits(source.value(row)) {
                        None
                    } else {
                        Some(source.value(row))
                    }
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(
                Decimal256Array::from(values)
                    .with_precision_and_scale(precision, scale)
                    .map_err(cast_construct_failure)?,
            ))
        }
        _ => Ok(array),
    }
}

#[cfg(test)]
#[path = "cast_numeric_oracle_tests.rs"]
mod cast_numeric_oracle_tests;

pub fn eval<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    cast_expr: ExprId,
    child: ExprId,
    decimal_overflow_policy: DecimalOverflowPolicy,
    chunk: &Chunk,
) -> Result<ArrayRef, ExecutionFailure> {
    let target_type = arena
        .data_type(cast_expr)
        .ok_or_else(|| super::expression_contract("CAST missing target data type"))?
        .clone();
    let target_field_schema = arena
        .field_schema(cast_expr)
        .map(ChunkFieldSchema::value_type);
    let source_type = arena
        .data_type(child)
        .ok_or_else(|| super::expression_contract("CAST missing source data type"))?;
    if !decimal_error_policy_cast_supported(source_type, &target_type, decimal_overflow_policy) {
        return Err(super::expression_contract(
            "ERROR_IF_OVERFLOW is unsupported for nested Decimal numeric CAST",
        ));
    }

    let child_array = arena.eval(child, chunk)?;
    if let (DataType::Decimal128(..), DataType::Decimal128(precision, scale)) =
        (child_array.data_type(), &target_type)
    {
        return novarocks_functions::decimal128_rescale::evaluate_legacy(
            &child_array,
            *precision,
            *scale,
            decimal_overflow_policy,
            arena.allow_throw_exception(),
        )
        .map_err(Into::into);
    }
    if matches!(
        child_array.data_type(),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    ) && let DataType::Decimal128(precision, scale) = target_type
    {
        return novarocks_functions::integral_decimal128::evaluate_legacy(
            &child_array,
            precision,
            scale,
            decimal_overflow_policy,
            arena.allow_throw_exception(),
        )
        .map_err(Into::into);
    }
    if is_checked_decimal_numeric_cast(child_array.data_type(), &target_type) {
        let casted = if child_array.data_type() == &target_type {
            child_array.clone()
        } else {
            cast_with_special_rules_with_field_schema(
                &child_array,
                &target_type,
                target_field_schema,
            )
            .map_err(|source| {
                cast_evaluation_failure(source, child_array.data_type(), &target_type)
            })?
        };
        let casted = enforce_declared_decimal_precision(casted)?;
        let overflow = checked_numeric_cast_has_overflow(&child_array, &casted)?;
        // ALLOW_THROW retains its existing Decimal-to-Decimal contract; the new
        // per-expression policy independently governs all checked numeric Decimal casts.
        if overflow
            && (decimal_overflow_policy == DecimalOverflowPolicy::ReportError
                || (arena.allow_throw_exception()
                    && is_decimal_type(child_array.data_type())
                    && is_decimal_type(&target_type)))
        {
            return Err(cast_numeric_failure(
                "Expr evaluate meet error: The numeric type cast involving decimal overflows",
            ));
        }
        return sanitize_non_finite_cast_result(casted, &target_type);
    }
    if child_array.data_type() == &target_type {
        return Ok(child_array);
    }

    if let DataType::LargeBinary = child_array.data_type() {
        let variant_arr = child_array
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| super::expression_contract("failed to downcast to LargeBinaryArray"))?;
        let casted = cast_variant_array(variant_arr, &target_type);
        return casted.and_then(|array| sanitize_non_finite_cast_result(array, &target_type));
    }

    match (child_array.data_type(), &target_type) {
        (DataType::Utf8, DataType::Boolean) => {
            let arr = child_array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| super::expression_contract("failed to downcast to StringArray"))?;
            return Ok(cast_utf8_to_boolean_array(arr));
        }
        (DataType::Date32, DataType::Float64) => {
            let arr = child_array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| super::expression_contract("failed to downcast to Date32Array"))?;
            let casted = cast_date32_to_float64(arr).map_err(|source| {
                cast_evaluation_failure(source, child_array.data_type(), &target_type)
            });
            return casted.and_then(|array| sanitize_non_finite_cast_result(array, &target_type));
        }
        (DataType::Date32, DataType::Float32) => {
            let arr = child_array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| super::expression_contract("failed to downcast to Date32Array"))?;
            let casted = cast_date32_to_float32(arr).map_err(|source| {
                cast_evaluation_failure(source, child_array.data_type(), &target_type)
            });
            return casted.and_then(|array| sanitize_non_finite_cast_result(array, &target_type));
        }
        (DataType::Float64, DataType::Float32) => {
            let arr = child_array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| super::expression_contract("failed to downcast to Float64Array"))?;
            return Ok(cast_float64_to_float32_preserve_non_finite(arr));
        }
        (DataType::Float32, DataType::Float64) => {
            let arr = child_array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| super::expression_contract("failed to downcast to Float32Array"))?;
            return Ok(cast_float32_to_float64_preserve_non_finite(arr));
        }
        (DataType::Float64, DataType::Date32) => {
            let arr = child_array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| super::expression_contract("failed to downcast to Float64Array"))?;
            let casted = cast_float64_to_date32(arr).map_err(|source| {
                cast_evaluation_failure(source, child_array.data_type(), &target_type)
            });
            return casted.and_then(|array| sanitize_non_finite_cast_result(array, &target_type));
        }
        (DataType::Float32, DataType::Date32) => {
            let arr = child_array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| super::expression_contract("failed to downcast to Float32Array"))?;
            let casted = cast_float32_to_date32(arr).map_err(|source| {
                cast_evaluation_failure(source, child_array.data_type(), &target_type)
            });
            return casted.and_then(|array| sanitize_non_finite_cast_result(array, &target_type));
        }
        (DataType::FixedSizeBinary(width), DataType::Decimal128(precision, scale))
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            let casted = cast_largeint_binary_to_decimal(&child_array, *precision, *scale)?;
            return sanitize_non_finite_cast_result(casted, &target_type);
        }
        (DataType::FixedSizeBinary(width), DataType::Decimal256(precision, scale))
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            let casted = cast_largeint_binary_to_decimal256(&child_array, *precision, *scale)?;
            return sanitize_non_finite_cast_result(casted, &target_type);
        }
        (DataType::FixedSizeBinary(width), DataType::Float64)
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            let casted = cast_largeint_binary_to_float64(&child_array)?;
            return sanitize_non_finite_cast_result(casted, &target_type);
        }
        (DataType::FixedSizeBinary(width), DataType::Float32)
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            let casted = cast_largeint_binary_to_float32(&child_array)?;
            return sanitize_non_finite_cast_result(casted, &target_type);
        }
        // Decimal256 → Float32/Float64: use our custom conversion that preserves +/-inf,
        // and return early to skip the sanitize_non_finite_cast_result step below which
        // would convert infinity to null.
        (DataType::Decimal256(_, source_scale), DataType::Float32) => {
            return cast_decimal256_to_float32(&child_array, *source_scale);
        }
        (DataType::Decimal256(_, source_scale), DataType::Float64) => {
            return cast_decimal256_to_float64(&child_array, *source_scale);
        }
        _ => {}
    }

    if matches!(child_array.data_type(), DataType::Utf8)
        && matches!(target_type, DataType::Decimal128(_, _))
    {
        let casted = cast_utf8_to_decimal_with_empty_as_null(&child_array, &target_type)?;
        return sanitize_non_finite_cast_result(casted, &target_type);
    }

    let casted =
        cast_with_special_rules_with_field_schema(&child_array, &target_type, target_field_schema)
            .map_err(|source| {
                cast_evaluation_failure(source, child_array.data_type(), &target_type)
            })?;
    if arena.allow_throw_exception() {
        if let Some(value) = first_float_to_int_overflow_value(&child_array, &casted, &target_type)?
        {
            return Err(cast_numeric_failure(format_args!(
                "Expr evaluate meet error: CAST failed: from {:?} to {:?}: {} conflict with range of {}",
                child_array.data_type(),
                target_type,
                value,
                integral_target_name(&target_type)
            )));
        }
        if has_decimal_to_decimal_overflow(&child_array, &casted) {
            return Err(cast_numeric_failure(
                "Expr evaluate meet error: The type cast from decimal to decimal overflows",
            ));
        }
    }
    sanitize_non_finite_cast_result(casted, &target_type)
}

fn eval_time_internal<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    cast_expr: ExprId,
    child: ExprId,
    chunk: &Chunk,
    source_is_datetime: bool,
) -> Result<ArrayRef, ExecutionFailure> {
    let target_type = arena
        .data_type(cast_expr)
        .ok_or_else(|| super::expression_contract("CAST missing target data type"))?
        .clone();
    if !matches!(target_type, DataType::Time64(TimeUnit::Microsecond)) {
        return Err(super::expression_contract(format_args!(
            "CAST failed: TIME target must be Time64(Microsecond), got {:?}",
            target_type
        )));
    }

    let child_array = arena.eval(child, chunk)?;
    if matches!(
        arena.node(child),
        Some(
            crate::exec::expr::ExprNode::CastTime(_, _)
                | crate::exec::expr::ExprNode::CastTimeFromDatetime(_, _)
        )
    ) {
        if child_array.data_type() == &target_type {
            return Ok(child_array);
        }
        return cast(&child_array, &target_type).map_err(cast_arrow_failure);
    }

    if matches!(
        child_array.data_type(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        let mode = if source_is_datetime {
            novarocks_functions::time_text_cast::TimeTextParseMode::Datetime
        } else {
            novarocks_functions::time_text_cast::TimeTextParseMode::Duration
        };
        return novarocks_functions::time_text_cast::evaluate_arrays(
            &child_array,
            &target_type,
            mode,
        )
        .map_err(Into::into);
    }
    if matches!(
        child_array.data_type(),
        DataType::Date32 | DataType::Timestamp(_, _)
    ) {
        return novarocks_functions::time_calendar_cast::evaluate_arrays(
            &child_array,
            &target_type,
        )
        .map_err(Into::into);
    }
    let mut seconds = Vec::with_capacity(child_array.len());
    match child_array.data_type() {
        DataType::Boolean => {
            let arr = child_array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| super::expression_contract("failed to downcast to BooleanArray"))?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    seconds.push(None);
                } else {
                    seconds.push(parse_time_integer_to_seconds(if arr.value(i) {
                        1
                    } else {
                        0
                    }));
                }
            }
        }
        DataType::Int8 => {
            let arr = child_array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| super::expression_contract("failed to downcast to Int8Array"))?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    seconds.push(None);
                } else {
                    seconds.push(parse_time_integer_to_seconds(arr.value(i) as i64));
                }
            }
        }
        DataType::Int16 => {
            let arr = child_array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| super::expression_contract("failed to downcast to Int16Array"))?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    seconds.push(None);
                } else {
                    seconds.push(parse_time_integer_to_seconds(arr.value(i) as i64));
                }
            }
        }
        DataType::Int32 => {
            let arr = child_array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| super::expression_contract("failed to downcast to Int32Array"))?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    seconds.push(None);
                } else {
                    seconds.push(parse_time_integer_to_seconds(arr.value(i) as i64));
                }
            }
        }
        DataType::Int64 => {
            let arr = child_array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| super::expression_contract("failed to downcast to Int64Array"))?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    seconds.push(None);
                } else {
                    seconds.push(parse_time_integer_to_seconds(arr.value(i)));
                }
            }
        }
        DataType::Float32 => {
            let arr = child_array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| super::expression_contract("failed to downcast to Float32Array"))?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    seconds.push(None);
                } else {
                    seconds.push(parse_time_float_to_seconds(arr.value(i) as f64));
                }
            }
        }
        DataType::Float64 => {
            let arr = child_array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| super::expression_contract("failed to downcast to Float64Array"))?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    seconds.push(None);
                } else {
                    seconds.push(parse_time_float_to_seconds(arr.value(i)));
                }
            }
        }
        DataType::Decimal128(_, scale) => {
            let arr = child_array
                .as_any()
                .downcast_ref::<arrow::array::Decimal128Array>()
                .ok_or_else(|| {
                    super::expression_contract("failed to downcast to Decimal128Array")
                })?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    seconds.push(None);
                    continue;
                }
                let mut value = arr.value(i);
                if *scale > 0 {
                    let factor = 10_i128.pow(*scale as u32);
                    value /= factor;
                }
                seconds.push(parse_time_integer_to_seconds(value as i64));
            }
        }
        _ => {
            let casted = cast(child_array.as_ref(), &target_type).map_err(cast_arrow_failure)?;
            return sanitize_non_finite_cast_result(casted, &target_type);
        }
    }

    Ok((seconds_to_time64_micro_array(seconds))?)
}

pub fn eval_time<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    cast_expr: ExprId,
    child: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, ExecutionFailure> {
    eval_time_internal(arena, cast_expr, child, chunk, false)
}

pub fn eval_time_from_datetime<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    cast_expr: ExprId,
    child: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, ExecutionFailure> {
    eval_time_internal(arena, cast_expr, child, chunk, true)
}

fn cast_variant_array(
    variant_arr: &LargeBinaryArray,
    target_type: &DataType,
) -> Result<ArrayRef, ExecutionFailure> {
    let tz = Local::now().offset().fix();
    match target_type {
        DataType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for row in 0..variant_arr.len() {
                let value = match parse_variant(variant_arr, row) {
                    Some(v) => v,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };
                if is_variant_null(&value).unwrap_or(true) {
                    builder.append_null();
                    continue;
                }
                match variant_to_bool(&value) {
                    Ok(v) => builder.append_value(v),
                    Err(_) => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Int8 => {
            let mut builder = Int8Builder::new();
            for row in 0..variant_arr.len() {
                let value = match parse_variant(variant_arr, row) {
                    Some(v) => v,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };
                if is_variant_null(&value).unwrap_or(true) {
                    builder.append_null();
                    continue;
                }
                match variant_to_i64(&value) {
                    Ok(v) => builder.append_value(v as i8),
                    Err(_) => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Int16 => {
            let mut builder = Int16Builder::new();
            for row in 0..variant_arr.len() {
                let value = match parse_variant(variant_arr, row) {
                    Some(v) => v,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };
                if is_variant_null(&value).unwrap_or(true) {
                    builder.append_null();
                    continue;
                }
                match variant_to_i64(&value) {
                    Ok(v) => builder.append_value(v as i16),
                    Err(_) => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::new();
            for row in 0..variant_arr.len() {
                let value = match parse_variant(variant_arr, row) {
                    Some(v) => v,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };
                if is_variant_null(&value).unwrap_or(true) {
                    builder.append_null();
                    continue;
                }
                match variant_to_i64(&value) {
                    Ok(v) => builder.append_value(v as i32),
                    Err(_) => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::new();
            for row in 0..variant_arr.len() {
                let value = match parse_variant(variant_arr, row) {
                    Some(v) => v,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };
                if is_variant_null(&value).unwrap_or(true) {
                    builder.append_null();
                    continue;
                }
                match variant_to_i64(&value) {
                    Ok(v) => builder.append_value(v),
                    Err(_) => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Float32 => {
            let mut builder = Float32Builder::new();
            for row in 0..variant_arr.len() {
                let value = match parse_variant(variant_arr, row) {
                    Some(v) => v,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };
                if is_variant_null(&value).unwrap_or(true) {
                    builder.append_null();
                    continue;
                }
                match variant_to_f64(&value) {
                    Ok(v) if v.is_finite() => builder.append_value(v as f32),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::new();
            for row in 0..variant_arr.len() {
                let value = match parse_variant(variant_arr, row) {
                    Some(v) => v,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };
                if is_variant_null(&value).unwrap_or(true) {
                    builder.append_null();
                    continue;
                }
                match variant_to_f64(&value) {
                    Ok(v) if v.is_finite() => builder.append_value(v),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for row in 0..variant_arr.len() {
                let value = match parse_variant(variant_arr, row) {
                    Some(v) => v,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };
                if is_variant_null(&value).unwrap_or(true) {
                    builder.append_null();
                    continue;
                }
                match variant_to_string(&value) {
                    Ok(v) => builder.append_value(v),
                    Err(_) => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Date32 => {
            let mut builder = Date32Builder::new();
            for row in 0..variant_arr.len() {
                let value = match parse_variant(variant_arr, row) {
                    Some(v) => v,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };
                if is_variant_null(&value).unwrap_or(true) {
                    builder.append_null();
                    continue;
                }
                match variant_to_date_days(&value, tz) {
                    Ok(v) => builder.append_value(v),
                    Err(_) => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let mut builder = TimestampMicrosecondBuilder::new();
            for row in 0..variant_arr.len() {
                let value = match parse_variant(variant_arr, row) {
                    Some(v) => v,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };
                if is_variant_null(&value).unwrap_or(true) {
                    builder.append_null();
                    continue;
                }
                let micros = match variant_to_datetime_micros(&value, tz)
                    .or_else(|_| variant_to_time_micros(&value, tz))
                {
                    Ok(v) => v,
                    Err(_) => {
                        builder.append_null();
                        continue;
                    }
                };
                builder.append_value(micros);
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _) => {
            let mut values = Vec::with_capacity(variant_arr.len());
            for row in 0..variant_arr.len() {
                let value = match parse_variant(variant_arr, row) {
                    Some(v) => v,
                    None => {
                        values.push(None);
                        continue;
                    }
                };
                if is_variant_null(&value).unwrap_or(true) {
                    values.push(None);
                    continue;
                }
                let json_text = value.to_json_local()?;
                let json = serde_json::from_str::<JsonValue>(&json_text)
                    .map_err(cast_json_decode_failure)?;
                values.push(Some(json));
            }
            cast_json_values_to_target(&values, target_type, None)
        }
        _ => Err(super::expression_contract(
            "CAST from VARIANT is not supported",
        )),
    }
}

fn parse_variant(variant_arr: &LargeBinaryArray, row: usize) -> Option<VariantValue> {
    if variant_arr.is_null(row) {
        return None;
    }
    VariantValue::from_serialized(variant_arr.value(row)).ok()
}

#[cfg(test)]
mod tests {
    use novarocks_failure_contract::FailureProjection;
    // Exercise layout limits without constructing multi-gigabyte input values.
    #[test]
    fn container_extent_uses_exact_unit_or_output_facts_before_backing() {
        use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary};
        preflight_cast_container(2, i32::MAX as u128, i32::MAX as usize, 1).unwrap();
        let batch = preflight_cast_container(2, i32::MAX as u128 + 1, 1, 1).unwrap_err();
        assert_eq!(batch.failure_fact().class(), FailureClass::Representation);
        let facts = batch.failure_fact().representation().unwrap();
        assert_eq!(facts.boundary, RepresentationBoundary::SingleOutput);
        assert_eq!(facts.required, Some(i32::MAX as u64 + 1));
        let single = preflight_cast_container(1, i32::MAX as u128 + 1, i32::MAX as usize + 1, 1)
            .unwrap_err();
        assert_eq!(
            single.failure_fact().representation().unwrap().boundary,
            RepresentationBoundary::MinimumUnit
        );
        let backing = preflight_cast_container(usize::MAX, 0, 0, 1).unwrap_err();
        assert_eq!(backing.failure_fact().class(), FailureClass::Representation);
        assert!(backing.failure_fact().resource().is_none());
        let extent = backing.failure_fact().representation().unwrap();
        assert_eq!(extent.required, None);
        assert_eq!(extent.layout.code.get(), 80);
        assert_eq!(extent.extent_limit, Some(isize::MAX as u64));
        assert_eq!(extent.boundary, RepresentationBoundary::SingleOutput);
    }

    use super::*;
    use crate::exec::chunk::{Chunk, ChunkFieldSchema, ChunkSchema, ChunkSlotSchema};
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use arrow::array::{
        ArrayRef, BinaryArray, Decimal128Array, Decimal256Array, FixedSizeBinaryArray, Int8Array,
        Int32Array, Int64Array, LargeBinaryArray, StringArray, StructArray,
    };
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_functions::calendar_numeric::{
        numeric_datetime_literal_to_naive as datetime_literal_to_naive_datetime,
        standardize_numeric_datetime_literal as standardize_date_literal,
    };
    use novarocks_types::SlotId;
    use novarocks_types::largeint;

    use novarocks_types::logical::{LogicalType, field_with_logical_type};
    use std::sync::Arc;

    fn chunk_len_1() -> Chunk {
        let array = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "dummy",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        }
    }

    fn json_struct_field_schema() -> ChunkFieldSchema {
        let json_child =
            field_with_logical_type(Field::new("a", DataType::Utf8, true), LogicalType::Json);
        let field = Field::new("out", DataType::Struct(vec![json_child].into()), true);
        ChunkFieldSchema::from_field(&field).expect("json struct field schema")
    }

    fn days_since_epoch(year: i32, month: u32, day: u32) -> i32 {
        let date = NaiveDate::from_ymd_opt(year, month, day).unwrap();
        date.num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET
    }

    #[test]
    fn test_date32_to_float64_literal() {
        let days = days_since_epoch(1999, 1, 2);
        let arr = Date32Array::from(vec![Some(days)]);
        let out = cast_date32_to_float64(&arr).unwrap();
        let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(out.value(0), 19990102.0);
    }

    #[test]
    fn test_float64_to_date32_literal() {
        let arr = Float64Array::from(vec![Some(19990102.9)]);
        let out = cast_float64_to_date32(&arr).unwrap();
        let out = out.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(out.value(0), days_since_epoch(1999, 1, 2));
    }

    #[test]
    fn test_sanitize_non_finite_float64_to_null() {
        let input = Arc::new(Float64Array::from(vec![
            Some(1.5),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(f64::NAN),
            None,
        ])) as ArrayRef;
        let out = sanitize_non_finite_cast_result(input, &DataType::Float64).unwrap();
        let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(out.value(0), 1.5);
        assert!(out.is_null(1));
        assert!(out.is_null(2));
        assert!(out.is_null(3));
        assert!(out.is_null(4));
    }

    #[test]
    fn test_cast_binary_to_largebinary_preserves_variant_payload_bytes() {
        let input = Arc::new(BinaryArray::from(vec![Some(&[0xab, 0x01][..]), None])) as ArrayRef;

        let out = cast_with_special_rules(&input, &DataType::LargeBinary).expect("cast");
        let out = out
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .expect("LargeBinaryArray");

        assert_eq!(out.value(0), &[0xab, 0x01]);
        assert!(out.is_null(1));
    }

    #[test]
    fn test_cast_json_to_struct_uses_expr_field_schema_for_nested_json() {
        let schema = Arc::new(Schema::new(vec![Field::new("src", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![Some("{\"a\":\"x\"}")])) as ArrayRef],
        )
        .expect("batch");
        let chunk = Chunk::new_with_chunk_schema(
            batch,
            Arc::new(
                ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                    SlotId::new(1),
                    Field::new("src", DataType::Utf8, true),
                    None,
                    None,
                )])
                .expect("chunk schema"),
            ),
        );

        let mut arena = ExprArena::default();
        let child = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let target_type = DataType::Struct(vec![Field::new("a", DataType::Utf8, true)].into());
        let cast_expr = arena.push_typed(
            ExprNode::Cast(child, DecimalOverflowPolicy::OutputNull),
            target_type,
        );
        arena.set_field_schema(cast_expr, json_struct_field_schema());

        let out = eval(
            &arena,
            cast_expr,
            child,
            DecimalOverflowPolicy::OutputNull,
            &chunk,
        )
        .expect("cast");
        let out = out.as_any().downcast_ref::<StructArray>().expect("struct");
        let values = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string");
        assert_eq!(values.value(0), "\"x\"");
    }

    #[test]
    fn test_sanitize_non_finite_float32_to_null() {
        let input = Arc::new(Float32Array::from(vec![
            Some(2.5),
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
            Some(f32::NAN),
            None,
        ])) as ArrayRef;
        let out = sanitize_non_finite_cast_result(input, &DataType::Float32).unwrap();
        let out = out.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(out.value(0), 2.5);
        assert!(out.is_null(1));
        assert!(out.is_null(2));
        assert!(out.is_null(3));
        assert!(out.is_null(4));
    }

    #[test]
    fn test_cast_utf8_to_date32_uses_lenient_datetime_parser() {
        let input = Arc::new(StringArray::from(vec![Some("2023-08-17 08:00:006")])) as ArrayRef;
        let out = cast_with_special_rules(&input, &DataType::Date32).expect("cast to date");
        let out = out.as_any().downcast_ref::<Date32Array>().expect("date32");
        assert!(!out.is_null(0));
        assert_eq!(out.value(0), days_since_epoch(2023, 8, 17));
    }

    #[test]
    fn test_cast_utf8_to_timestamp_uses_lenient_datetime_parser() {
        let input = Arc::new(StringArray::from(vec![Some("2023-08-17 08:00:006")])) as ArrayRef;
        let out =
            cast_with_special_rules(&input, &DataType::Timestamp(TimeUnit::Microsecond, None))
                .expect("cast to timestamp");
        let out = out
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp");
        assert!(!out.is_null(0));
        assert_eq!(
            out.value(0),
            NaiveDate::from_ymd_opt(2023, 8, 17)
                .unwrap()
                .and_hms_opt(8, 0, 6)
                .unwrap()
                .and_utc()
                .timestamp_micros()
        );
    }

    #[test]
    fn test_cast_time_string_parsing_rules() {
        assert_eq!(parse_time_string_to_seconds("00:00:00"), Some(0));
        assert_eq!(parse_time_string_to_seconds("25:00:00"), Some(90_000));
        assert_eq!(parse_time_string_to_seconds("1970-01-01 01:01:01"), None);
        assert_eq!(parse_time_string_to_seconds("+01:02:03"), None);
        assert_eq!(parse_time_string_to_seconds("-01:02:03"), None);
    }

    #[test]
    fn test_cast_time_from_datetime_source_uses_datetime_parser() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();

        let literal = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("1970-01-01 01:01:01".to_string())),
            DataType::Utf8,
        );
        let cast_time = arena.push_typed(
            ExprNode::CastTime(literal, DecimalOverflowPolicy::OutputNull),
            DataType::Time64(TimeUnit::Microsecond),
        );
        let direct = arena.eval(cast_time, &chunk).expect("cast time eval");
        let direct = direct
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .expect("time64 output");
        assert!(direct.is_null(0));

        let cast_time_from_dt = arena.push_typed(
            ExprNode::CastTimeFromDatetime(literal, DecimalOverflowPolicy::OutputNull),
            DataType::Time64(TimeUnit::Microsecond),
        );
        let from_dt = arena
            .eval(cast_time_from_dt, &chunk)
            .expect("cast time from datetime eval");
        let from_dt = from_dt
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .expect("time64 output");
        assert!(!from_dt.is_null(0));
        assert_eq!(from_dt.value(0), 3_661_000_000);
    }

    #[test]
    fn test_cast_time_allows_extended_hour_literals() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();

        let literal = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("25:00:00".to_string())),
            DataType::Utf8,
        );
        let cast_time = arena.push_typed(
            ExprNode::CastTime(literal, DecimalOverflowPolicy::OutputNull),
            DataType::Time64(TimeUnit::Microsecond),
        );
        let out = arena.eval(cast_time, &chunk).expect("cast time eval");
        let out = out
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .expect("time64 output");
        assert_eq!(out.value(0), 90_000_000_000);
    }

    #[test]
    fn test_parse_varchar_to_boolean_starrocks_semantics() {
        assert_eq!(parse_varchar_to_boolean_starrocks("2"), Some(true));
        assert_eq!(parse_varchar_to_boolean_starrocks("-1"), Some(true));
        assert_eq!(parse_varchar_to_boolean_starrocks("+0"), Some(false));
        assert_eq!(parse_varchar_to_boolean_starrocks(" 2"), Some(true));
        assert_eq!(parse_varchar_to_boolean_starrocks("2147483648"), None);
        assert_eq!(parse_varchar_to_boolean_starrocks("true"), Some(true));
        assert_eq!(parse_varchar_to_boolean_starrocks("FALSE"), Some(false));
        assert_eq!(parse_varchar_to_boolean_starrocks("t"), None);
        assert_eq!(parse_varchar_to_boolean_starrocks("f"), None);
        assert_eq!(parse_varchar_to_boolean_starrocks(""), None);
        assert_eq!(parse_varchar_to_boolean_starrocks("   "), None);
    }

    #[test]
    fn test_cast_utf8_to_boolean_array_starrocks_semantics() {
        let input = StringArray::from(vec![
            Some("2"),
            Some("-1"),
            Some("+0"),
            Some("false"),
            Some("TRUE"),
            Some("t"),
            Some("2147483648"),
            Some(""),
            None,
        ]);
        let out = cast_utf8_to_boolean_array(&input);
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert!(out.value(0));
        assert!(out.value(1));
        assert!(!out.value(2));
        assert!(!out.value(3));
        assert!(out.value(4));
        assert!(out.is_null(5));
        assert!(out.is_null(6));
        assert!(out.is_null(7));
        assert!(out.is_null(8));
    }

    #[test]
    fn test_cast_decimal_utf8_empty_or_blank_returns_null() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();

        let empty = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("".to_string())),
            DataType::Utf8,
        );
        let blank = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("   ".to_string())),
            DataType::Utf8,
        );
        let zero = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("0".to_string())),
            DataType::Utf8,
        );

        let cast_empty = arena.push_typed(
            ExprNode::Cast(empty, DecimalOverflowPolicy::OutputNull),
            DataType::Decimal128(10, 2),
        );
        let cast_blank = arena.push_typed(
            ExprNode::Cast(blank, DecimalOverflowPolicy::OutputNull),
            DataType::Decimal128(10, 2),
        );
        let cast_zero = arena.push_typed(
            ExprNode::Cast(zero, DecimalOverflowPolicy::OutputNull),
            DataType::Decimal128(10, 2),
        );

        let empty_out = arena.eval(cast_empty, &chunk).unwrap();
        let empty_out = empty_out
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert!(empty_out.is_null(0));

        let blank_out = arena.eval(cast_blank, &chunk).unwrap();
        let blank_out = blank_out
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert!(blank_out.is_null(0));

        let zero_out = arena.eval(cast_zero, &chunk).unwrap();
        let zero_out = zero_out.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert!(!zero_out.is_null(0));
        assert_eq!(zero_out.value(0), 0);
    }

    #[test]
    fn test_cast_decimal_to_utf8_keeps_full_unscaled_value() {
        let wide = Decimal128Array::from(vec![
            Some(10_000_i128),
            Some(20_100_i128),
            Some(100_000_i128),
            Some(-10_000_i128),
            Some(1_100_i128),
            None,
        ])
        .with_precision_and_scale(38, 3)
        .unwrap();
        let data = wide
            .to_data()
            .into_builder()
            .data_type(DataType::Decimal128(4, 3))
            .build()
            .unwrap();
        let arr = make_array(data);

        let out = cast_with_special_rules(&arr, &DataType::Utf8).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "10.000");
        assert_eq!(out.value(1), "20.100");
        assert_eq!(out.value(2), "100.000");
        assert_eq!(out.value(3), "-10.000");
        assert_eq!(out.value(4), "1.100");
        assert!(out.is_null(5));
    }

    #[test]
    fn test_cast_decimal256_to_utf8_preserves_unscaled_digits() {
        let arr = Arc::new(
            Decimal256Array::from(vec![
                Some(i256::from_i128(12_340)),
                Some(i256::from_i128(-500)),
                None,
            ])
            .with_precision_and_scale(76, 2)
            .unwrap(),
        ) as ArrayRef;
        let out = cast_with_special_rules(&arr, &DataType::Utf8).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "123.40");
        assert_eq!(out.value(1), "-5.00");
        assert!(out.is_null(2));
    }

    #[test]
    fn test_cast_decimal256_to_largeint_binary_truncates_and_nulls_overflow() {
        let huge = i256::from_string("123456789012345678901234567890123456789012").unwrap();
        let arr = Arc::new(
            Decimal256Array::from(vec![
                Some(i256::from_i128(12_345)), // 123.45 -> 123
                Some(i256::from_i128(-5)),     // -0.05 -> 0
                Some(huge),                    // overflow after scale adjustment
                None,
            ])
            .with_precision_and_scale(50, 2)
            .unwrap(),
        ) as ArrayRef;
        let out = cast_with_special_rules(&arr, &DataType::FixedSizeBinary(16)).unwrap();
        let out = out.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(largeint::value_at(out, 0).unwrap(), 123_i128);
        assert_eq!(largeint::value_at(out, 1).unwrap(), 0_i128);
        assert!(out.is_null(2));
        assert!(out.is_null(3));
    }

    #[test]
    fn test_cast_decimal256_to_boolean_and_int64_semantics() {
        let arr = Arc::new(
            Decimal256Array::from(vec![
                Some(i256::from_i128(1)),     // 0.001
                Some(i256::ZERO),             // 0.000
                Some(i256::from_i128(-1)),    // -0.001
                Some(i256::from_i128(1_500)), // 1.500
                None,
            ])
            .with_precision_and_scale(20, 3)
            .unwrap(),
        ) as ArrayRef;

        let bool_out = cast_with_special_rules(&arr, &DataType::Boolean).unwrap();
        let bool_out = bool_out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bool_out.value(0));
        assert!(!bool_out.value(1));
        assert!(bool_out.value(2));
        assert!(bool_out.value(3));
        assert!(bool_out.is_null(4));

        let int_out = cast_with_special_rules(&arr, &DataType::Int64).unwrap();
        let int_out = int_out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int_out.value(0), 0_i64);
        assert_eq!(int_out.value(1), 0_i64);
        assert_eq!(int_out.value(2), 0_i64);
        assert_eq!(int_out.value(3), 1_i64);
        assert!(int_out.is_null(4));
    }

    #[test]
    fn test_cast_decimal_to_decimal_downscale_rounds_half_up() {
        let arr = Arc::new(
            Decimal128Array::from(vec![
                Some(3_185_i128),  // 0.3185 -> 0.32
                Some(-3_185_i128), // -0.3185 -> -0.32
                Some(3_149_i128),  // 0.3149 -> 0.31
                Some(-3_149_i128), // -0.3149 -> -0.31
            ])
            .with_precision_and_scale(10, 4)
            .unwrap(),
        ) as ArrayRef;
        let out = cast_with_special_rules(&arr, &DataType::Decimal128(10, 2)).unwrap();
        let out = out.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(out.value(0), 32);
        assert_eq!(out.value(1), -32);
        assert_eq!(out.value(2), 31);
        assert_eq!(out.value(3), -31);
    }

    #[test]
    fn test_cast_largeint_binary_to_decimal128() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();

        let lit = arena.push_typed(
            ExprNode::Literal(LiteralValue::LargeInt(9_223_372_036_854_775_808_i128)),
            DataType::FixedSizeBinary(16),
        );
        let cast_expr = arena.push_typed(
            ExprNode::Cast(lit, DecimalOverflowPolicy::OutputNull),
            DataType::Decimal128(38, 0),
        );

        let out = arena.eval(cast_expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(out.value(0), 9_223_372_036_854_775_808_i128);
    }

    #[test]
    fn list_json_metadata_cast_preserves_sliced_values_offsets_and_validity_buffers() {
        use novarocks_types::logical::{
            LogicalType, field_with_logical_type, logical_type_of_field,
        };
        for values in [
            Arc::new(StringArray::from(vec![
                Some("{\"a\":1}"),
                None,
                Some("{\"b\":2}"),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![None::<&str>, None, None])) as ArrayRef,
        ] {
            let source = ListArray::new(
                Arc::new(Field::new("item", DataType::Utf8, true)),
                OffsetBuffer::new(vec![0_i32, 1, 1, 3, 3].into()),
                values,
                Some(arrow_buffer::NullBuffer::from(vec![
                    true, true, true, false,
                ])),
            )
            .slice(1, 3);
            let target = DataType::List(Arc::new(field_with_logical_type(
                Field::new("item", DataType::Utf8, true),
                LogicalType::Json,
            )));
            let source = Arc::new(source) as ArrayRef;
            let output = cast_with_special_rules(&source, &target).unwrap();
            let source_list = source.as_any().downcast_ref::<ListArray>().unwrap();
            let output_list = output.as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(output_list.data_type(), &target);
            assert_eq!(source_list.value_offsets(), output_list.value_offsets());
            assert_eq!(
                source_list.to_data().buffers()[0].as_ptr(),
                output_list.to_data().buffers()[0].as_ptr()
            );
            assert!(Arc::ptr_eq(source_list.values(), output_list.values()));
            assert_eq!(source_list.nulls(), output_list.nulls());
            assert_eq!(
                source_list.nulls().unwrap().buffer().as_ptr(),
                output_list.nulls().unwrap().buffer().as_ptr()
            );
            assert!(output_list.is_null(2));
            assert!(output_list.value(0).is_empty());
            assert_eq!(
                output_list.value(1).to_data(),
                source_list.value(1).to_data()
            );
            let DataType::List(item) = output_list.data_type() else {
                unreachable!();
            };
            assert_eq!(logical_type_of_field(item), Some(LogicalType::Json));
            let reversed = cast_with_special_rules(&output, source.data_type()).unwrap();
            let reversed = reversed.as_any().downcast_ref::<ListArray>().unwrap();
            assert!(Arc::ptr_eq(source_list.values(), reversed.values()));
            assert_eq!(reversed.data_type(), source.data_type());
        }
    }

    #[test]
    fn test_cast_list_largeint_binary_to_decimal128() {
        let item_field = Arc::new(Field::new("item", DataType::FixedSizeBinary(16), true));
        let values =
            largeint::array_from_i128(&[Some(9_223_372_036_854_775_808_i128), None, Some(-5_i128)])
                .unwrap();
        let source = Arc::new(ListArray::new(
            item_field,
            OffsetBuffer::new(vec![0_i32, 2, 3].into()),
            values,
            None,
        )) as ArrayRef;
        let target_type = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Decimal128(38, 0),
            true,
        )));

        let out = cast_with_special_rules(&source, &target_type).unwrap();

        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(out.data_type(), &target_type);
        let values = out
            .values()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(values.value(0), 9_223_372_036_854_775_808_i128);
        assert!(values.is_null(1));
        assert_eq!(values.value(2), -5_i128);
    }

    #[test]
    fn test_cast_int16_to_largeint_binary() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();

        let lit = arena.push_typed(ExprNode::Literal(LiteralValue::Int16(7)), DataType::Int16);
        let cast_expr = arena.push_typed(
            ExprNode::Cast(lit, DecimalOverflowPolicy::OutputNull),
            DataType::FixedSizeBinary(16),
        );

        let out = arena.eval(cast_expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(largeint::value_at(out, 0).unwrap(), 7_i128);
    }

    #[test]
    fn test_cast_utf8_to_largeint_binary() {
        let arr = Arc::new(StringArray::from(vec![
            Some("42"),
            Some("-17"),
            Some("  9000 "),
            Some("oops"),
            Some(""),
            None,
        ])) as ArrayRef;
        let out = cast_with_special_rules(&arr, &DataType::FixedSizeBinary(16)).unwrap();
        let out = out.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(largeint::value_at(out, 0).unwrap(), 42_i128);
        assert_eq!(largeint::value_at(out, 1).unwrap(), -17_i128);
        assert_eq!(largeint::value_at(out, 2).unwrap(), 9000_i128);
        assert!(out.is_null(3));
        assert!(out.is_null(4));
        assert!(out.is_null(5));
    }

    #[test]
    fn test_cast_float_to_decimal_rounds_like_starrocks() {
        let arr = Arc::new(Float64Array::from(vec![
            Some(0.31847),
            Some(0.315),
            Some(-0.315),
            Some(-0.314),
            None,
        ])) as ArrayRef;
        let out = cast_with_special_rules(&arr, &DataType::Decimal128(10, 2)).unwrap();
        let out = out.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(out.value(0), 32);
        assert_eq!(out.value(1), 32);
        assert_eq!(out.value(2), -32);
        assert_eq!(out.value(3), -31);
        assert!(out.is_null(4));
    }

    #[test]
    fn test_cast_float32_to_utf8_uses_compact_representation() {
        let arr = Float32Array::from(vec![Some(0.369_004_f32), Some(1.5_f32), Some(-0.0)]);
        let input = Arc::new(arr) as ArrayRef;
        let out = cast_with_special_rules(&input, &DataType::Utf8).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "0.369004");
        assert_eq!(out.value(1), "1.5");
        assert_eq!(out.value(2), "0");
    }

    #[test]
    fn test_cast_float_overflow_respects_allow_throw_exception() {
        let chunk = chunk_len_1();

        let mut arena_default = ExprArena::default();
        let lit_default = arena_default.push_typed(
            ExprNode::Literal(LiteralValue::Float64(2_147_483_648.0)),
            DataType::Float64,
        );
        let cast_default = arena_default.push_typed(
            ExprNode::Cast(lit_default, DecimalOverflowPolicy::OutputNull),
            DataType::Int32,
        );
        let out = arena_default.eval(cast_default, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(out.is_null(0));

        let mut arena_strict = ExprArena::default();
        arena_strict.set_allow_throw_exception(true);
        let lit_strict = arena_strict.push_typed(
            ExprNode::Literal(LiteralValue::Float64(2_147_483_648.0)),
            DataType::Float64,
        );
        let cast_strict = arena_strict.push_typed(
            ExprNode::Cast(lit_strict, DecimalOverflowPolicy::OutputNull),
            DataType::Int32,
        );
        let err = arena_strict.eval(cast_strict, &chunk).unwrap_err();
        assert_eq!(
            err.failure_fact(),
            novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact()
        );
        assert!(err.detail().contains("conflict with range of INT"));
    }

    #[test]
    fn test_cast_struct_decimal_to_int64_fields() {
        let first = Arc::new(
            Decimal128Array::from(vec![
                Some(9_223_372_036_854_775_807_i128),
                Some(-9_223_372_036_854_775_808_i128),
            ])
            .with_precision_and_scale(38, 0)
            .unwrap(),
        ) as ArrayRef;
        let second = Arc::new(Int8Array::from(vec![Some(1_i8), Some(-1_i8)])) as ArrayRef;
        let source_fields = Fields::from(vec![
            Field::new("col1", DataType::Decimal128(38, 0), true),
            Field::new("col2", DataType::Int8, true),
        ]);
        let source =
            Arc::new(StructArray::new(source_fields, vec![first, second], None)) as ArrayRef;

        let target = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let out = cast_with_special_rules(&source, &target).expect("cast struct");
        let out = out.as_any().downcast_ref::<StructArray>().expect("struct");
        let a = out
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("a int64");
        let b = out
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("b int64");
        assert_eq!(a.value(0), 9_223_372_036_854_775_807_i64);
        assert_eq!(a.value(1), -9_223_372_036_854_775_808_i64);
        assert_eq!(b.value(0), 1_i64);
        assert_eq!(b.value(1), -1_i64);
    }

    #[test]
    fn test_cast_largeint_binary_to_int64_overflow_to_null() {
        let source = largeint::array_from_i128(&[
            Some(i64::MAX as i128),
            Some(i64::MIN as i128),
            Some(i64::MAX as i128 + 1),
            None,
        ])
        .expect("largeint source");
        let out = cast_with_special_rules(&source, &DataType::Int64).expect("cast int64");
        let out = out.as_any().downcast_ref::<Int64Array>().expect("int64");
        assert_eq!(out.value(0), i64::MAX);
        assert_eq!(out.value(1), i64::MIN);
        assert!(out.is_null(2));
        assert!(out.is_null(3));
    }

    #[test]
    fn test_cast_struct_largeint_binary_to_int64_fields() {
        let first = largeint::array_from_i128(&[Some(9_i128), Some(-9_i128)]).expect("largeint");
        let second = Arc::new(Int8Array::from(vec![Some(1_i8), Some(-1_i8)])) as ArrayRef;
        let source_fields = Fields::from(vec![
            Field::new(
                "col1",
                DataType::FixedSizeBinary(largeint::LARGEINT_BYTE_WIDTH),
                true,
            ),
            Field::new("col2", DataType::Int8, true),
        ]);
        let source =
            Arc::new(StructArray::new(source_fields, vec![first, second], None)) as ArrayRef;

        let target = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let out = cast_with_special_rules(&source, &target).expect("cast struct");
        let out = out.as_any().downcast_ref::<StructArray>().expect("struct");
        let a = out
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("a int64");
        let b = out
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("b int64");
        assert_eq!(a.value(0), 9_i64);
        assert_eq!(a.value(1), -9_i64);
        assert_eq!(b.value(0), 1_i64);
        assert_eq!(b.value(1), -1_i64);
    }

    #[test]
    fn cast_struct_field_count_mismatch_keeps_specific_error_first() {
        let source_fields = Fields::from(vec![
            Field::new("col1", DataType::Int64, true),
            Field::new("col2", DataType::Utf8, true),
            Field::new("col3", DataType::Null, true),
            Field::new("col4", DataType::Null, true),
        ]);
        let source = Arc::new(StructArray::new(
            source_fields.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(3)])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("")])) as ArrayRef,
                arrow::array::new_null_array(&DataType::Null, 1),
                arrow::array::new_null_array(&DataType::Null, 1),
            ],
            None,
        )) as ArrayRef;
        let source_type = DataType::Struct(source_fields);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            source_type.clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![source]).expect("batch");
        let chunk = Chunk::new_with_chunk_schema(
            batch,
            Arc::new(
                ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                    SlotId::new(1),
                    Field::new("s", source_type.clone(), true),
                    None,
                    None,
                )])
                .expect("chunk schema"),
            ),
        );
        let target_type = DataType::Struct(
            vec![
                Field::new("c0", DataType::Int32, true),
                Field::new("c1", DataType::Utf8, true),
            ]
            .into(),
        );
        let mut arena = ExprArena::default();
        let child = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), source_type);
        let cast_expr = arena.push_typed(
            ExprNode::Cast(child, DecimalOverflowPolicy::OutputNull),
            target_type,
        );

        let err = arena
            .eval(cast_expr, &chunk)
            .expect_err("field count mismatch");

        assert!(
            err.detail().starts_with("CAST STRUCT field count mismatch"),
            "got: {err}"
        );
    }

    // IV3-7 Task 12: nanosecond timestamp cast semantics

    #[test]
    fn cast_seconds_and_milliseconds_to_micros_keep_checked_overflow_nulls() {
        for (source, factor) in [
            (TimeUnit::Second, 1_000_000_i64),
            (TimeUnit::Millisecond, 1_000),
        ] {
            for zone in [None, Some(Arc::<str>::from("UTC"))] {
                let boundary = i64::MAX / factor;
                let values = vec![
                    Some(1),
                    Some(-1),
                    Some(boundary),
                    Some(boundary + 1),
                    Some(i64::MAX),
                    Some(i64::MIN),
                    None,
                ];
                let array: ArrayRef = match source {
                    TimeUnit::Second => {
                        Arc::new(TimestampSecondArray::from(values).with_timezone_opt(zone))
                    }
                    TimeUnit::Millisecond => {
                        Arc::new(TimestampMillisecondArray::from(values).with_timezone_opt(zone))
                    }
                    _ => unreachable!(),
                };
                let out = cast_with_special_rules(
                    &array,
                    &DataType::Timestamp(TimeUnit::Microsecond, None),
                )
                .unwrap();
                let out = out
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                assert_eq!(
                    out.iter().collect::<Vec<_>>(),
                    [
                        Some(factor),
                        Some(-factor),
                        Some(boundary * factor),
                        None,
                        None,
                        None,
                        None
                    ]
                );
            }
        }
    }

    #[test]
    fn cast_same_unit_timestamp_retags_timezone_metadata() {
        let src = Arc::new(TimestampMicrosecondArray::from(vec![Some(1_000_i64)])) as ArrayRef;
        let target = DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()));
        let out = cast_with_special_rules(&src, &target).unwrap();
        assert_eq!(out.data_type(), &target);
        let a = out
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(a.value(0), 1_000);
    }

    #[test]
    fn cast_nanos_to_micros_truncates() {
        // 1_000_000_789 ns = 1_000_789 us + 789 ns sub-microsecond remainder.
        // Truncation toward zero: 1_000_000_789 / 1000 = 1_000_000 us (integer division).
        let src = Arc::new(TimestampNanosecondArray::from(vec![Some(
            1_000_000_789_i64,
        )])) as ArrayRef;
        let out = cast_with_special_rules(&src, &DataType::Timestamp(TimeUnit::Microsecond, None))
            .unwrap();
        let a = out
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(a.value(0), 1_000_000);
    }

    #[test]
    fn cast_micros_to_nanos_widens_in_range() {
        let src = Arc::new(TimestampMicrosecondArray::from(vec![Some(1_000_i64)])) as ArrayRef;
        let out = cast_with_special_rules(&src, &DataType::Timestamp(TimeUnit::Nanosecond, None))
            .unwrap();
        let a = out
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(a.value(0), 1_000_000);
    }

    #[test]
    fn cast_micros_to_nanos_overflow_errors() {
        // i64::MAX / 2 microseconds * 1000 overflows i64.
        let src = Arc::new(TimestampMicrosecondArray::from(vec![Some(i64::MAX / 2)])) as ArrayRef;
        let r = cast_with_special_rules(&src, &DataType::Timestamp(TimeUnit::Nanosecond, None));
        assert!(r.is_err(), "expected overflow error but got {:?}", r);
    }

    #[test]
    fn cast_utf8_to_nanos_keeps_nanoseconds() {
        // The string has 9 fractional digits; sub-microsecond digits must survive.
        let src = Arc::new(StringArray::from(vec![Some(
            "2024-01-02 03:04:05.123456789",
        )])) as ArrayRef;
        let out = cast_with_special_rules(&src, &DataType::Timestamp(TimeUnit::Nanosecond, None))
            .unwrap();
        let a = out
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        // The last 3 digits (789) are the sub-microsecond part.
        assert_eq!(a.value(0) % 1_000, 789);
    }
}

#[cfg(test)]
mod overflow_policy_prepared_tests {
    use super::*;
    use crate::exec::expr::ExprNode;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    fn prepare(source: ArrayRef, target: DataType) -> (ExprArena, Chunk, ExprId, ExprId) {
        let source_type = source.data_type().clone();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "source",
            source_type.clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![source]).unwrap();
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), source_type);
        let nullable = arena.push_typed(
            ExprNode::Cast(source, DecimalOverflowPolicy::OutputNull),
            target.clone(),
        );
        let throwing = arena.push_typed(
            ExprNode::Cast(source, DecimalOverflowPolicy::ReportError),
            target,
        );
        (arena, chunk, nullable, throwing)
    }

    #[test]
    fn prepared_numeric_casts_respect_declared_precision_and_freeze_distinct_policies() {
        let cases = vec![
            (
                Arc::new(Int64Array::from(vec![Some(1), None, Some(1_000_000_000)])) as ArrayRef,
                DataType::Decimal128(9, 0),
                1,
            ),
            (
                Arc::new(
                    Decimal128Array::from(vec![Some(12_345), None, Some(999_995)])
                        .with_precision_and_scale(6, 3)
                        .unwrap(),
                ) as ArrayRef,
                DataType::Decimal128(5, 2),
                1_235,
            ),
            (
                Arc::new(
                    Decimal128Array::from(vec![Some(12), None, Some(1_000)])
                        .with_precision_and_scale(4, 0)
                        .unwrap(),
                ) as ArrayRef,
                DataType::Decimal128(3, 0),
                12,
            ),
            (
                Arc::new(Float64Array::from(vec![
                    Some(12.345),
                    None,
                    Some(1_000_000_000.0),
                ])) as ArrayRef,
                DataType::Decimal128(5, 2),
                1_235,
            ),
            (
                largeint::array_from_i128(&[Some(1), None, Some(1_000_000_000)]).unwrap(),
                DataType::Decimal128(9, 0),
                1,
            ),
            (
                Arc::new(
                    Decimal256Array::from(vec![
                        Some(i256::from_i128(12)),
                        None,
                        Some(i256::from_i128(1000)),
                    ])
                    .with_precision_and_scale(4, 0)
                    .unwrap(),
                ) as ArrayRef,
                DataType::Decimal256(3, 0),
                12,
            ),
        ];
        for (source, target, first) in cases {
            let (arena, chunk, nullable, throwing) = prepare(source, target.clone());
            let result = arena.eval(nullable, &chunk).unwrap();
            assert_eq!(result.data_type(), &target);
            assert_eq!(result.null_count(), 2);
            if let Some(array) = result.as_any().downcast_ref::<Decimal128Array>() {
                assert_eq!(array.value(0), first);
            } else {
                assert_eq!(
                    result
                        .as_any()
                        .downcast_ref::<Decimal256Array>()
                        .unwrap()
                        .value(0),
                    i256::from_i128(first)
                );
            }
            assert!(
                arena
                    .eval(throwing, &chunk)
                    .unwrap_err()
                    .detail()
                    .contains("numeric type cast involving decimal overflows")
            );
            assert_eq!(arena.eval(nullable, &chunk).unwrap().null_count(), 2);
        }
        let source = Arc::new(
            Decimal128Array::from(vec![Some(12), None, Some(128)])
                .with_precision_and_scale(3, 0)
                .unwrap(),
        ) as ArrayRef;
        let (arena, chunk, nullable, throwing) = prepare(source, DataType::Int8);
        let result = arena.eval(nullable, &chunk).unwrap();
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(0),
            12
        );
        assert_eq!(result.null_count(), 2);
        assert!(arena.eval(throwing, &chunk).is_err());
    }

    #[test]
    fn prepared_invalid_parse_nonfinite_and_null_do_not_become_numeric_overflow() {
        let sources = vec![
            Arc::new(StringArray::from(vec![
                Some("not-a-number"),
                Some(""),
                None,
            ])) as ArrayRef,
            Arc::new(Float64Array::from(vec![
                Some(f64::NAN),
                Some(f64::INFINITY),
                None,
            ])) as ArrayRef,
        ];
        for source in sources {
            let (arena, chunk, nullable, throwing) = prepare(source, DataType::Decimal128(5, 2));
            assert_eq!(arena.eval(nullable, &chunk).unwrap().null_count(), 3);
            assert_eq!(arena.eval(throwing, &chunk).unwrap().null_count(), 3);
        }
    }

    #[test]
    fn prepared_signed_rounding_and_allow_throw_contract_remain_independent() {
        let source = Arc::new(
            Decimal128Array::from(vec![12_345, -12_345])
                .with_precision_and_scale(5, 3)
                .unwrap(),
        ) as ArrayRef;
        let (arena, chunk, nullable, throwing) = prepare(source, DataType::Decimal128(4, 2));
        for id in [nullable, throwing] {
            let result = arena.eval(id, &chunk).unwrap();
            let array = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
            assert_eq!(array.values().as_ref(), &[1235, -1235]);
        }
        let source = Arc::new(Int64Array::from(vec![1_000_000_000])) as ArrayRef;
        let (mut arena, chunk, nullable, throwing) = prepare(source, DataType::Decimal128(9, 0));
        arena.set_allow_throw_exception(true);
        // Existing ALLOW governs Decimal-to-Decimal casts, not this integer input.
        assert_eq!(arena.eval(nullable, &chunk).unwrap().null_count(), 1);
        assert!(arena.eval(throwing, &chunk).is_err());
        let source = Arc::new(
            Decimal128Array::from(vec![1000])
                .with_precision_and_scale(4, 0)
                .unwrap(),
        ) as ArrayRef;
        let (mut arena, chunk, nullable, _) = prepare(source, DataType::Decimal128(3, 0));
        arena.set_allow_throw_exception(true);
        assert!(arena.eval(nullable, &chunk).is_err());
    }
    #[test]
    fn prepared_nested_numeric_cast_rejects_report_before_recursive_execution() {
        let field = Arc::new(Field::new("item", DataType::Decimal128(4, 0), true));
        let source = Arc::new(ListArray::new(
            field.clone(),
            OffsetBuffer::new(vec![0_i32, 1].into()),
            Arc::new(
                Decimal128Array::from(vec![12])
                    .with_precision_and_scale(4, 0)
                    .unwrap(),
            ),
            None,
        )) as ArrayRef;
        let target = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Decimal128(3, 0),
            true,
        )));
        let (arena, chunk, nullable, throwing) = prepare(source.clone(), target);
        assert_eq!(
            arena.eval(throwing, &chunk).unwrap_err().detail(),
            "ERROR_IF_OVERFLOW is unsupported for nested Decimal numeric CAST"
        );
        let result = arena.eval(nullable, &chunk).unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(
            list.values()
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            12
        );
        let (arena, chunk, _, same) = prepare(source, DataType::List(field));
        assert_eq!(arena.eval(same, &chunk).unwrap().len(), 1);
    }
}

#[cfg(test)]
mod intrinsic_cast_row_contract_tests {
    use super::*;
    use crate::exec::expr::ExprNode;
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    fn prepare(
        source: ArrayRef,
        target: DataType,
        allow_throw: bool,
        policy: DecimalOverflowPolicy,
    ) -> (ExprArena, Chunk, ExprId) {
        let ty = source.data_type().clone();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("source", ty.clone(), true)])),
            vec![source],
        )
        .unwrap();
        let layout = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, layout);
        let mut arena = ExprArena::default();
        arena.set_allow_throw_exception(allow_throw);
        let child = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), ty);
        let cast = arena.push_typed(ExprNode::Cast(child, policy), target);
        (arena, chunk, cast)
    }

    #[test]
    fn typed_arena_temporal_rows_raise_own_errors_under_both_decimal_policies() {
        // 200 million days fits Date32/i32 and the CE-day addition, but exceeds
        // chrono's representable calendar range. No malformed carrier is used.
        for target in [DataType::Utf8, DataType::Float32, DataType::Float64] {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                let (arena, chunk, cast) = prepare(
                    Arc::new(Date32Array::from(vec![Some(200_000_000), None])),
                    target.clone(),
                    false,
                    policy,
                );
                assert!(arena.eval(cast, &chunk).is_err());
            }
        }
        let source = Arc::new(TimestampMicrosecondArray::from(vec![
            Some(i64::MAX / 2),
            None,
        ])) as ArrayRef;
        let (arena, chunk, cast) = prepare(
            source,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
            DecimalOverflowPolicy::OutputNull,
        );
        assert!(arena.eval(cast, &chunk).is_err());
    }

    #[test]
    fn typed_arena_recursive_date_format_error_is_not_caught_by_parent_cast_policy() {
        let item = Arc::new(Field::new("item", DataType::Date32, true));
        let list = Arc::new(ListArray::new(
            item,
            OffsetBuffer::new(vec![0_i32, 1, 2].into()),
            Arc::new(Date32Array::from(vec![Some(200_000_000), None])),
            None,
        )) as ArrayRef;
        let target = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let (arena, chunk, cast) = prepare(list, target, false, DecimalOverflowPolicy::OutputNull);
        assert!(arena.eval(cast, &chunk).is_err());
    }

    #[test]
    fn typed_arena_struct_and_map_recursive_format_errors_propagate() {
        let source_fields: Fields = vec![
            Field::new("d", DataType::Date32, true),
            Field::new("i", DataType::Int64, true),
        ]
        .into();
        let source = Arc::new(StructArray::new(
            source_fields,
            vec![
                Arc::new(Date32Array::from(vec![Some(200_000_000), None])),
                Arc::new(Int64Array::from(vec![Some(1), None])),
            ],
            None,
        )) as ArrayRef;
        let target = DataType::Struct(
            vec![
                Field::new("i", DataType::Int64, true),
                Field::new("d", DataType::Utf8, true),
            ]
            .into(),
        );
        let (arena, chunk, cast) =
            prepare(source, target, false, DecimalOverflowPolicy::OutputNull);
        assert!(arena.eval(cast, &chunk).is_err());

        let fields: Fields = vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::Date32, true),
        ]
        .into();
        let entries = StructArray::new(
            fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![0])),
                Arc::new(Date32Array::from(vec![Some(200_000_000)])),
            ],
            None,
        );
        let source = Arc::new(MapArray::new(
            Arc::new(Field::new("entries", DataType::Struct(fields), false)),
            OffsetBuffer::new(vec![0_i32, 1].into()),
            entries,
            None,
            false,
        )) as ArrayRef;
        let fields: Fields = vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true),
        ]
        .into();
        let target = DataType::Map(
            Arc::new(Field::new("entries", DataType::Struct(fields), false)),
            false,
        );
        let (arena, chunk, cast) =
            prepare(source, target, false, DecimalOverflowPolicy::OutputNull);
        assert!(arena.eval(cast, &chunk).is_err());
    }

    #[test]
    fn legacy_float_integer_mode_and_caught_invalid_utf8_remain_distinct() {
        let source = Arc::new(Float64Array::from(vec![Some(127.0), Some(128.0), None])) as ArrayRef;
        let (arena, chunk, cast) = prepare(
            source.clone(),
            DataType::Int8,
            false,
            DecimalOverflowPolicy::OutputNull,
        );
        let result = arena.eval(cast, &chunk).unwrap();
        let result = result.as_any().downcast_ref::<Int8Array>().unwrap();
        assert_eq!(
            result.iter().collect::<Vec<_>>(),
            vec![Some(127), None, None]
        );
        let (arena, chunk, cast) = prepare(
            source,
            DataType::Int8,
            true,
            DecimalOverflowPolicy::OutputNull,
        );
        assert!(arena.eval(cast, &chunk).is_err());
        let source = Arc::new(arrow::array::BinaryArray::from(vec![
            Some(&[0xff][..]),
            None,
        ])) as ArrayRef;
        let (arena, chunk, cast) = prepare(
            source,
            DataType::Utf8,
            true,
            DecimalOverflowPolicy::OutputNull,
        );
        let result = arena.eval(cast, &chunk).unwrap();
        assert_eq!(result.null_count(), 2);
    }
}

#[cfg(test)]
#[path = "cast_float_numeric_oracle_tests.rs"]
mod cast_float_numeric_oracle_tests;

#[cfg(test)]
#[path = "cast_float_identity_oracle_tests.rs"]
mod cast_float_identity_oracle_tests;

#[cfg(test)]
#[path = "cast_value_conversion_oracle_tests.rs"]
mod cast_value_conversion_oracle_tests;

#[cfg(test)]
#[path = "cast_bool_oracle_tests.rs"]
mod cast_bool_oracle_tests;

#[cfg(test)]
#[path = "cast_unsigned_oracle_tests.rs"]
mod cast_unsigned_oracle_tests;

#[cfg(test)]
mod cast_context_properties {
    use super::*;
    use novarocks_failure_contract::FailureProjection;
    use std::error::Error;

    // An actual value-domain refusal must survive CAST context and driver
    // location unchanged. Differential CAST suites cover the public routes.
    #[test]
    fn specific_structure_failure_precedes_wide_context_labels() {
        let fields = Fields::from(vec![
            Field::new("field_".repeat(100), DataType::Int32, true),
            Field::new("other", DataType::Int32, true),
        ]);
        let input: ArrayRef = Arc::new(StructArray::new(
            fields,
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![7])) as ArrayRef,
                Arc::new(arrow::array::Int32Array::from(vec![8])) as ArrayRef,
            ],
            None,
        ));
        let target = DataType::Struct(Fields::from(vec![Field::new("one", DataType::Int32, true)]));
        let original = cast_with_special_rules(&input, &target).unwrap_err();
        let failure = cast_evaluation_failure(original.clone(), input.data_type(), &target);
        assert_eq!(failure.failure_fact(), original.failure_fact());
        assert!(
            failure
                .detail()
                .starts_with("CAST STRUCT field count mismatch: source=2 target=1")
        );
        assert!(failure.detail().len() <= 512);
    }

    #[test]
    fn original_value_cause_survives_cast_context() {
        let original = cast_date32_to_float64(&Date32Array::from(vec![i32::MIN])).unwrap_err();
        let expected = original.failure_fact();
        let failure =
            cast_evaluation_failure(original.clone(), &DataType::Date32, &DataType::Float64)
                .at_operator(3, crate::runtime::fragment::PipelineOperation::Pull);
        assert_eq!(failure.failure_fact(), expected);
        assert_eq!(
            failure.detail(),
            "CAST failed: from Date32 to Float64: invalid Date32 value -2147483648"
        );
        let mut current: &(dyn Error + 'static) = &failure;
        loop {
            if let Some(context) = current.downcast_ref::<CastEvaluationFailure>() {
                assert!(context.source.same_occurrence(&original));
                break;
            }
            current = current
                .source()
                .expect("original CAST context must remain in source chain");
        }
    }
}
