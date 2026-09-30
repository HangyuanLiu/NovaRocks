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

//! Exact SQL literal source values in the shared immutable constant owner.
//!
//! The caller supplies the complete source field/type and admission policy.
//! A target coercion belongs to the surrounding CAST, not this conversion.
//! Logical identities are neither inferred nor established here. Construction
//! uses standard Arrow arrays after the constant owner's finite preflight;
//! this does not authorize the host's first allocation or supply a MEM grant.

use std::{fmt, sync::Arc};

use arrow::{
    array::{
        Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
        FixedSizeBinaryArray, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        StringArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    },
    datatypes::{DataType, Field, TimeUnit, i256},
};
use novarocks_constant_contract::{
    ConstantError, ConstantPolicy, ConstantValue, preflight_scalar_construction,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
    ValueLogicalType,
};

use crate::common::LiteralValue;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlLiteralConstantError {
    Constant(ConstantError),
    InvalidLiteral {
        kind: &'static str,
        detail: &'static str,
    },
}

impl From<ConstantError> for SqlLiteralConstantError {
    fn from(error: ConstantError) -> Self {
        Self::Constant(error)
    }
}
impl From<CompileControlError> for SqlLiteralConstantError {
    fn from(error: CompileControlError) -> Self {
        Self::Constant(ConstantError::Control(error))
    }
}
impl fmt::Display for SqlLiteralConstantError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Constant(error) => error.fmt(f),
            Self::InvalidLiteral { kind, detail } => write!(f, "invalid {kind} literal: {detail}"),
        }
    }
}
impl std::error::Error for SqlLiteralConstantError {}

fn invalid(kind: &'static str, detail: &'static str) -> SqlLiteralConstantError {
    SqlLiteralConstantError::InvalidLiteral { kind, detail }
}

/// Convert a literal's accurate source carrier. The caller must preserve a
/// separate source-to-target CAST when the context requires another type.
pub(crate) fn sql_literal_constant(
    literal: &LiteralValue,
    field: Arc<Field>,
    source_type: FunctionValueType,
    policy: ConstantPolicy,
    phase: CompilePhase,
    control: &dyn PureCompileControl,
) -> Result<ConstantValue, SqlLiteralConstantError> {
    control.checkpoint(phase, 0)?;
    let native_source = match literal {
        LiteralValue::Null => source_type.nullable,
        LiteralValue::Bool(_) => matches!(source_type.data_type, DataType::Boolean),
        LiteralValue::Int(_) => matches!(
            source_type.data_type,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Date32
                | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
                | DataType::Timestamp(_, _)
        ),
        LiteralValue::LargeInt(_) => matches!(source_type.data_type, DataType::FixedSizeBinary(16)),
        LiteralValue::Float(_) => matches!(source_type.data_type, DataType::Float64),
        LiteralValue::String(_) => matches!(source_type.data_type, DataType::Utf8),
        LiteralValue::Binary(_) => matches!(source_type.data_type, DataType::Binary),
        LiteralValue::Decimal(_) => matches!(
            source_type.data_type,
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
        ),
    };
    if !native_source {
        return Err(invalid(
            "source",
            "literal kind differs from its exact source carrier or nullability",
        ));
    }
    if !matches!(literal, LiteralValue::Null) {
        let intrinsic = if matches!(literal, LiteralValue::LargeInt(_)) {
            ValueLogicalType::LargeInt
        } else {
            ValueLogicalType::Physical
        };
        if source_type.logical_type != intrinsic {
            return Err(invalid(
                "source",
                "logical identity differs from the SQL literal's intrinsic domain",
            ));
        }
    }
    let payload = match literal {
        LiteralValue::String(value) => value.len() as u64,
        LiteralValue::Binary(value) => value.len() as u64,
        _ => 0,
    };
    let null = matches!(literal, LiteralValue::Null);
    preflight_scalar_construction(&field, &source_type, payload, null, policy, phase, control)?;
    if null {
        return ConstantValue::null(field, source_type, policy, phase, control).map_err(Into::into);
    }
    let data = match (literal, &source_type.data_type) {
        (LiteralValue::Bool(value), DataType::Boolean) => {
            BooleanArray::from(vec![*value]).to_data()
        }
        (LiteralValue::Int(value), DataType::Int8) => Int8Array::from(vec![
            i8::try_from(*value)
                .map_err(|_| invalid("Int8", "value is outside the source range"))?,
        ])
        .to_data(),
        (LiteralValue::Int(value), DataType::Int16) => Int16Array::from(vec![
            i16::try_from(*value)
                .map_err(|_| invalid("Int16", "value is outside the source range"))?,
        ])
        .to_data(),
        (LiteralValue::Int(value), DataType::Int32) => Int32Array::from(vec![
            i32::try_from(*value)
                .map_err(|_| invalid("Int32", "value is outside the source range"))?,
        ])
        .to_data(),
        (LiteralValue::Int(value), DataType::Int64) => Int64Array::from(vec![*value]).to_data(),
        (LiteralValue::Int(value), DataType::Date32) => {
            Date32Array::from(vec![i32::try_from(*value).map_err(|_| {
                invalid("Date32", "epoch days are outside the source range")
            })?])
            .to_data()
        }
        (LiteralValue::Int(value), DataType::Time64(TimeUnit::Microsecond)) => {
            Time64MicrosecondArray::from(vec![*value]).to_data()
        }
        (LiteralValue::Int(value), DataType::Time64(TimeUnit::Nanosecond)) => {
            Time64NanosecondArray::from(vec![*value]).to_data()
        }
        (LiteralValue::Int(value), DataType::Timestamp(unit, timezone)) => match unit {
            TimeUnit::Second => TimestampSecondArray::from(vec![*value])
                .with_timezone_opt(timezone.clone())
                .to_data(),
            TimeUnit::Millisecond => TimestampMillisecondArray::from(vec![*value])
                .with_timezone_opt(timezone.clone())
                .to_data(),
            TimeUnit::Microsecond => TimestampMicrosecondArray::from(vec![*value])
                .with_timezone_opt(timezone.clone())
                .to_data(),
            TimeUnit::Nanosecond => TimestampNanosecondArray::from(vec![*value])
                .with_timezone_opt(timezone.clone())
                .to_data(),
        },
        (LiteralValue::LargeInt(value), DataType::FixedSizeBinary(16)) => {
            let bytes = value.to_be_bytes();
            FixedSizeBinaryArray::try_from_iter([bytes.as_slice()].into_iter())
                .map_err(|error| ConstantError::Arrow(error.to_string()))?
                .to_data()
        }
        (LiteralValue::Float(value), DataType::Float64) => {
            Float64Array::from(vec![f64::from_bits(value.to_bits())]).to_data()
        }
        (LiteralValue::String(value), DataType::Utf8) => {
            StringArray::from(vec![value.as_str()]).to_data()
        }
        (LiteralValue::Binary(value), DataType::Binary) => {
            BinaryArray::from(vec![value.as_slice()]).to_data()
        }
        (LiteralValue::Decimal(value), DataType::Decimal128(precision, scale)) => {
            let unscaled =
                parse_decimal::<i128>(value, *scale, *precision, policy, phase, control)?;
            Decimal128Array::from(vec![unscaled])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|error| ConstantError::Arrow(error.to_string()))?
                .to_data()
        }
        (LiteralValue::Decimal(value), DataType::Decimal256(precision, scale)) => {
            let unscaled =
                parse_decimal::<i256>(value, *scale, *precision, policy, phase, control)?;
            Decimal256Array::from(vec![unscaled])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|error| ConstantError::Arrow(error.to_string()))?
                .to_data()
        }
        (LiteralValue::Null, _) => unreachable!("NULL was handled before source dispatch"),
        _ => {
            return Err(invalid(
                "source",
                "literal kind differs from its exact source carrier",
            ));
        }
    };
    control.checkpoint(phase, 0)?;
    ConstantValue::from_scalar_array(field, source_type, data, policy, phase, control)
        .map_err(Into::into)
}

// One decimal parser serves the two actual SQL carriers. All native operations
// are fixed-width and checked. It performs no input-sized allocation, library
// string parse, diagnostic copy or unchecked truncation.
trait DecimalInteger: Copy + Eq {
    const ZERO: Self;
    fn small(value: u8) -> Self;
    fn checked_add(self, other: Self) -> Option<Self>;
    fn checked_sub(self, other: Self) -> Option<Self>;
    fn checked_mul(self, other: Self) -> Option<Self>;
    fn checked_div(self, other: Self) -> Option<Self>;
    fn checked_rem(self, other: Self) -> Option<Self>;
}

macro_rules! decimal_integer {
    ($ty:ty, $zero:expr, $small:expr) => {
        impl DecimalInteger for $ty {
            const ZERO: Self = $zero;
            fn small(value: u8) -> Self {
                ($small)(value)
            }
            fn checked_add(self, other: Self) -> Option<Self> {
                self.checked_add(other)
            }
            fn checked_sub(self, other: Self) -> Option<Self> {
                self.checked_sub(other)
            }
            fn checked_mul(self, other: Self) -> Option<Self> {
                self.checked_mul(other)
            }
            fn checked_div(self, other: Self) -> Option<Self> {
                self.checked_div(other)
            }
            fn checked_rem(self, other: Self) -> Option<Self> {
                self.checked_rem(other)
            }
        }
    };
}
decimal_integer!(i128, 0, i128::from);
decimal_integer!(i256, i256::ZERO, |value| i256::from_i128(i128::from(value)));

fn parse_decimal<T: DecimalInteger>(
    text: &str,
    scale: i8,
    precision: u8,
    policy: ConstantPolicy,
    phase: CompilePhase,
    control: &dyn PureCompileControl,
) -> Result<T, SqlLiteralConstantError> {
    let mut work = CompileCheckpoints::try_new(control, phase)?;
    // The existing explicit work policy bounds even long runs of leading zero
    // digits. It is not a new parser cap. No source-sized temporary is allocated.
    let upper_work = (text.len() as u64)
        .checked_mul(2)
        .and_then(|units| units.checked_add(u64::from(scale.unsigned_abs())))
        .and_then(|units| units.checked_add(192))
        .ok_or(ConstantError::Limit(
            "SQL decimal parse work arithmetic overflow",
        ))?;
    if upper_work > policy.max_library_validation_work {
        return Err(ConstantError::Limit("SQL decimal parse work policy exceeded").into());
    }
    let result = parse_decimal_digits::<T>(text, scale, precision, &mut work);
    work.finish()?;
    result
}

fn parse_decimal_digits<T: DecimalInteger>(
    text: &str,
    scale: i8,
    precision: u8,
    work: &mut CompileCheckpoints<'_>,
) -> Result<T, SqlLiteralConstantError> {
    let negative = text.starts_with('-');
    let unsigned = text
        .strip_prefix('-')
        .or_else(|| text.strip_prefix('+'))
        .unwrap_or(text);
    let mut value = T::ZERO;
    let mut seen_dot = false;
    let mut digits = 0u64;
    let mut fractional = 0u64;
    let ten = T::small(10);
    for byte in unsigned.bytes() {
        work.step()?;
        if byte == b'.' && !seen_dot {
            seen_dot = true;
            continue;
        }
        if !byte.is_ascii_digit() {
            return Err(invalid("Decimal", "value is not a plain decimal literal"));
        }
        digits += 1;
        fractional += u64::from(seen_dot);
        value = value
            .checked_mul(ten)
            .and_then(|value| {
                if negative {
                    value.checked_sub(T::small(byte - b'0'))
                } else {
                    value.checked_add(T::small(byte - b'0'))
                }
            })
            .ok_or_else(|| invalid("Decimal", "unscaled digits exceed the source carrier"))?;
    }
    if digits == 0 {
        return Err(invalid("Decimal", "value has no digits"));
    }
    let fractional = i64::try_from(fractional)
        .map_err(|_| invalid("Decimal", "fractional digit count is outside its domain"))?;
    let adjustment = i64::from(scale)
        .checked_sub(fractional)
        .ok_or_else(|| invalid("Decimal", "scale adjustment is outside its domain"))?;
    let mut power = T::small(1);
    for _ in 0..adjustment.unsigned_abs() {
        work.step()?;
        power = power
            .checked_mul(ten)
            .ok_or_else(|| invalid("Decimal", "scale adjustment exceeds the source carrier"))?;
    }
    value = if adjustment >= 0 {
        value
            .checked_mul(power)
            .ok_or_else(|| invalid("Decimal", "value exceeds its declared scale"))?
    } else {
        if value.checked_rem(power) != Some(T::ZERO) {
            return Err(invalid(
                "Decimal",
                "value loses nonzero digits at the declared scale",
            ));
        }
        value.checked_div(power).ok_or_else(|| {
            invalid(
                "Decimal",
                "value cannot be represented at the declared scale",
            )
        })?
    };
    let mut remaining = value;
    let mut required_precision = 1u8;
    while let Some(next) = remaining.checked_div(ten) {
        work.step()?;
        if next == T::ZERO {
            break;
        }
        required_precision += 1;
        remaining = next;
    }
    if required_precision > precision {
        return Err(invalid(
            "Decimal",
            "unscaled value exceeds the declared precision",
        ));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_type_contract::NR_LOGICAL_TYPE_KEY;
    use std::sync::Mutex;

    struct Control {
        failure: Option<CompileControlError>,
        at_256: bool,
        calls: Mutex<Vec<(CompilePhase, u32)>>,
    }
    impl Control {
        fn good() -> Self {
            Self {
                failure: None,
                at_256: false,
                calls: Mutex::default(),
            }
        }
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            self.calls.lock().unwrap().push((phase, units));
            if let Some(error) = self.failure
                && (!self.at_256 || units == 256)
            {
                return Err(error);
            }
            Ok(())
        }
    }
    fn policy() -> ConstantPolicy {
        ConstantPolicy {
            max_rows: 100,
            max_array_nodes: 4096,
            max_logical_elements: 1_000_000,
            max_retained_buffer_bytes: 1_000_000,
            max_type_depth: 64,
            max_type_nodes: 4096,
            max_dictionary_depth: 16,
            max_metadata_bytes: 1_000_000,
            max_library_validation_work: 10_000_000,
            max_library_validation_bytes: 10_000_000,
        }
    }
    fn field(ty: &FunctionValueType) -> Arc<Field> {
        let mut field = Field::new("literal", ty.data_type.clone(), ty.nullable);
        if let Some(logical) = ty.logical_type.metadata_value() {
            field = field.with_metadata([(NR_LOGICAL_TYPE_KEY.into(), logical.into())].into());
        }
        Arc::new(field)
    }
    fn value(literal: LiteralValue, data_type: DataType) -> ConstantValue {
        let logical_type = if matches!(literal, LiteralValue::LargeInt(_)) {
            ValueLogicalType::LargeInt
        } else {
            ValueLogicalType::Physical
        };
        let ty = FunctionValueType::try_with_logical_type(
            data_type,
            matches!(literal, LiteralValue::Null),
            logical_type,
        )
        .unwrap();
        sql_literal_constant(
            &literal,
            field(&ty),
            ty,
            policy(),
            CompilePhase::LowerProgram,
            &Control::good(),
        )
        .unwrap()
    }

    #[test]
    fn all_eight_sql_source_variants_keep_actual_values() {
        assert_eq!(
            value(LiteralValue::Bool(true), DataType::Boolean)
                .try_boolean()
                .unwrap(),
            Some(true)
        );
        assert_eq!(
            value(LiteralValue::Int(-7), DataType::Int64)
                .try_i64()
                .unwrap(),
            Some(-7)
        );
        let large = value(
            LiteralValue::LargeInt(i128::MIN),
            DataType::FixedSizeBinary(16),
        );
        assert_eq!(
            large
                .pool()
                .array()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(0),
            i128::MIN.to_be_bytes().as_slice()
        );
        assert_eq!(
            value(LiteralValue::Float(1.25), DataType::Float64)
                .try_f64_bits()
                .unwrap(),
            Some(1.25f64.to_bits())
        );
        assert_eq!(
            value(
                LiteralValue::Decimal("12.34".into()),
                DataType::Decimal128(4, 2)
            )
            .try_decimal128()
            .unwrap(),
            Some(1234)
        );
        assert_eq!(
            value(LiteralValue::String("e\\f\0中国".into()), DataType::Utf8)
                .try_utf8()
                .unwrap(),
            Some("e\\f\0中国")
        );
        assert_eq!(
            value(LiteralValue::Binary(vec![0, 255, 1]), DataType::Binary)
                .try_binary()
                .unwrap(),
            Some([0, 255, 1].as_slice())
        );
        let null = value(LiteralValue::Null, DataType::Int16);
        assert_eq!(null.try_i64().unwrap(), None);
        assert!(
            null.is_null_observed(CompilePhase::Validate, &Control::good())
                .unwrap()
        );
    }

    #[test]
    fn signed_widths_and_native_temporal_sources_are_exact() {
        for ty in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ] {
            assert_eq!(
                value(LiteralValue::Int(-12), ty).try_i64().unwrap(),
                Some(-12)
            );
        }
        let date = value(LiteralValue::Int(19_732), DataType::Date32);
        assert_eq!(
            date.pool()
                .array()
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value(0),
            19_732
        );
        let micros = value(
            LiteralValue::Int(123_456),
            DataType::Time64(TimeUnit::Microsecond),
        );
        assert_eq!(
            micros
                .pool()
                .array()
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .unwrap()
                .value(0),
            123_456
        );
        let nanos = value(
            LiteralValue::Int(123_456),
            DataType::Time64(TimeUnit::Nanosecond),
        );
        assert_eq!(
            nanos
                .pool()
                .array()
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .unwrap()
                .value(0),
            123_456
        );
        for unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            let ty = DataType::Timestamp(unit, Some("Asia/Shanghai".into()));
            let constant = value(LiteralValue::Int(-123_456), ty.clone());
            assert_eq!(constant.value_type().data_type, ty);
            assert_eq!(
                constant.pool().array().to_data().buffers()[0].typed_data::<i64>(),
                &[-123_456]
            );
        }
    }

    #[test]
    fn decimal256_and_negative_scales_preserve_exact_unscaled_values() {
        let text = "123456789012345678901234567890123456789012345678901234567890.00";
        let expected =
            i256::from_string("12345678901234567890123456789012345678901234567890123456789000")
                .unwrap();
        assert_eq!(
            value(
                LiteralValue::Decimal(text.into()),
                DataType::Decimal256(65, 2)
            )
            .try_decimal256()
            .unwrap(),
            Some(expected)
        );
        assert_eq!(
            value(
                LiteralValue::Decimal("-1200".into()),
                DataType::Decimal128(2, -2)
            )
            .try_decimal128()
            .unwrap(),
            Some(-12)
        );
        assert_eq!(
            value(
                LiteralValue::Decimal("1200".into()),
                DataType::Decimal256(2, -2)
            )
            .try_decimal256()
            .unwrap(),
            Some(i256::from_i128(12))
        );
        assert_eq!(
            value(
                LiteralValue::Decimal("+.50".into()),
                DataType::Decimal128(2, 2)
            )
            .try_decimal128()
            .unwrap(),
            Some(50)
        );
    }

    #[test]
    fn exact_declared_precision_truncation_and_plain_grammar_are_checked() {
        for (text, ty) in [
            ("12.34", DataType::Decimal128(3, 2)),
            ("12.34", DataType::Decimal256(3, 2)),
            ("1201", DataType::Decimal128(2, -2)),
            ("1201", DataType::Decimal256(2, -2)),
            ("1.01", DataType::Decimal128(2, 0)),
            ("1.01", DataType::Decimal256(2, 0)),
            ("1e2", DataType::Decimal128(3, 0)),
            ("1.2.3", DataType::Decimal256(3, 0)),
            (".", DataType::Decimal128(3, 0)),
            ("+", DataType::Decimal256(3, 0)),
        ] {
            let source = FunctionValueType::new(ty, false);
            assert!(matches!(
                sql_literal_constant(
                    &LiteralValue::Decimal(text.into()),
                    field(&source),
                    source,
                    policy(),
                    CompilePhase::Validate,
                    &Control::good()
                ),
                Err(SqlLiteralConstantError::InvalidLiteral { .. })
            ));
        }
    }

    #[test]
    fn nullable_logical_and_nested_null_sources_are_not_inferred() {
        let source =
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap();
        let constant = sql_literal_constant(
            &LiteralValue::Null,
            field(&source),
            source.clone(),
            policy(),
            CompilePhase::Validate,
            &Control::good(),
        )
        .unwrap();
        assert_eq!(constant.value_type(), &source);
        assert!(
            constant
                .is_null_observed(CompilePhase::Validate, &Control::good())
                .unwrap()
        );
        let nested = DataType::List(field(&source));
        let constant = value(LiteralValue::Null, nested.clone());
        assert_eq!(constant.value_type().data_type, nested);
        let physical = value(LiteralValue::String("{}".into()), DataType::Utf8);
        assert_eq!(
            physical.value_type().logical_type,
            ValueLogicalType::Physical
        );
        // The mandatory FVT owns the root domain; absent duplicate Field
        // metadata is permitted, while an explicit conflicting label is not.
        let bare_field = Arc::new(Field::new("literal", DataType::Utf8, true));
        let bare = sql_literal_constant(
            &LiteralValue::Null,
            bare_field,
            source.clone(),
            policy(),
            CompilePhase::Validate,
            &Control::good(),
        )
        .unwrap();
        assert_eq!(bare.value_type(), &source);
        let wrong_field = field(&source);
        let physical_source = FunctionValueType::new(DataType::Utf8, true);
        assert!(matches!(
            sql_literal_constant(
                &LiteralValue::Null,
                wrong_field,
                physical_source,
                policy(),
                CompilePhase::Validate,
                &Control::good()
            ),
            Err(SqlLiteralConstantError::Constant(ConstantError::Invalid(
                "constant root logical label differs from exact value identity"
            )))
        ));
    }

    #[test]
    fn nan_payloads_and_both_zero_signs_survive() {
        for bits in [0, 1u64 << 63, 0x7ff8_0000_0000_0042, 0xfff8_0000_0000_0123] {
            assert_eq!(
                value(LiteralValue::Float(f64::from_bits(bits)), DataType::Float64)
                    .try_f64_bits()
                    .unwrap(),
                Some(bits)
            );
        }
    }

    #[test]
    fn caller_metadata_cannot_retag_nonnull_sql_source_domains() {
        for (literal, data_type, logical) in [
            (
                LiteralValue::String("{}".into()),
                DataType::Utf8,
                ValueLogicalType::Json,
            ),
            (
                LiteralValue::Binary(vec![0]),
                DataType::Binary,
                ValueLogicalType::Hll,
            ),
            (
                LiteralValue::Binary(vec![0]),
                DataType::LargeBinary,
                ValueLogicalType::Variant,
            ),
            (
                LiteralValue::LargeInt(1),
                DataType::FixedSizeBinary(16),
                ValueLogicalType::Uuid,
            ),
            (
                LiteralValue::LargeInt(1),
                DataType::FixedSizeBinary(16),
                ValueLogicalType::Physical,
            ),
        ] {
            let source =
                FunctionValueType::try_with_logical_type(data_type, false, logical).unwrap();
            assert!(matches!(
                sql_literal_constant(
                    &literal,
                    field(&source),
                    source,
                    policy(),
                    CompilePhase::Validate,
                    &Control::good()
                ),
                Err(SqlLiteralConstantError::InvalidLiteral { .. })
            ));
        }
        for (data_type, logical) in [
            (DataType::Utf8, ValueLogicalType::Json),
            (DataType::LargeBinary, ValueLogicalType::Variant),
            (DataType::FixedSizeBinary(16), ValueLogicalType::Uuid),
            (DataType::FixedSizeBinary(16), ValueLogicalType::LargeInt),
        ] {
            let source =
                FunctionValueType::try_with_logical_type(data_type, true, logical).unwrap();
            let constant = sql_literal_constant(
                &LiteralValue::Null,
                field(&source),
                source.clone(),
                policy(),
                CompilePhase::Validate,
                &Control::good(),
            )
            .unwrap();
            assert_eq!(constant.value_type(), &source);
            assert!(
                constant
                    .is_null_observed(CompilePhase::Validate, &Control::good())
                    .unwrap()
            );
        }
    }

    #[test]
    fn context_targets_are_not_reinterpreted_as_literal_sources() {
        for (literal, ty) in [
            (LiteralValue::Int(128), DataType::Int8),
            (LiteralValue::Int(i64::MAX), DataType::Int32),
            (LiteralValue::Int(i64::MAX), DataType::Date32),
            (LiteralValue::Int(1), DataType::UInt64),
            (LiteralValue::Int(1), DataType::Date64),
            (LiteralValue::Int(1), DataType::Time32(TimeUnit::Second)),
            (LiteralValue::Float(1.0), DataType::Float32),
            (LiteralValue::String("2024-01-01".into()), DataType::Date32),
            (
                LiteralValue::String("INTERVAL 1 DAY".into()),
                DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano),
            ),
            (
                LiteralValue::String("[]".into()),
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            ),
            (
                LiteralValue::Binary(vec![0; 16]),
                DataType::FixedSizeBinary(16),
            ),
            (LiteralValue::LargeInt(1), DataType::Binary),
            (LiteralValue::Bool(true), DataType::Int8),
            (LiteralValue::Decimal("1".into()), DataType::Decimal32(2, 0)),
            (LiteralValue::Decimal("1".into()), DataType::Decimal64(2, 0)),
            (LiteralValue::Null, DataType::Utf8),
        ] {
            let source = FunctionValueType::new(ty, false);
            assert!(matches!(
                sql_literal_constant(
                    &literal,
                    field(&source),
                    source,
                    policy(),
                    CompilePhase::Validate,
                    &Control::good()
                ),
                Err(SqlLiteralConstantError::InvalidLiteral { .. })
            ));
        }
    }

    #[test]
    fn caller_policy_preflights_payload_and_decimal_work() {
        let source = FunctionValueType::new(DataType::Utf8, false);
        let mut limited = policy();
        limited.max_retained_buffer_bytes = 128;
        assert!(matches!(
            sql_literal_constant(
                &LiteralValue::String("x".repeat(1024)),
                field(&source),
                source,
                limited,
                CompilePhase::Validate,
                &Control::good()
            ),
            Err(SqlLiteralConstantError::Constant(ConstantError::Limit(_)))
        ));
        let mut limited = policy();
        limited.max_library_validation_work = 1000;
        assert!(matches!(
            parse_decimal::<i128>(
                &"0".repeat(1024),
                0,
                1,
                limited,
                CompilePhase::Validate,
                &Control::good()
            ),
            Err(SqlLiteralConstantError::Constant(ConstantError::Limit(_)))
        ));
    }

    #[test]
    fn typed_control_is_preserved_at_entry_and_each_256_digits() {
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at_256 in [false, true] {
                let control = Control {
                    failure: Some(error),
                    at_256,
                    calls: Mutex::default(),
                };
                let source = FunctionValueType::new(DataType::Decimal256(2, 2), false);
                let literal = LiteralValue::Decimal(format!("{}.01", "0".repeat(1024)));
                assert!(
                    matches!(sql_literal_constant(&literal, field(&source), source, policy(), CompilePhase::LowerProgram, &control), Err(SqlLiteralConstantError::Constant(ConstantError::Control(actual))) if actual == error)
                );
                let calls = control.calls.lock().unwrap();
                assert_eq!(calls.first(), Some(&(CompilePhase::LowerProgram, 0)));
                assert!(calls.iter().all(|(phase, units)| *phase == CompilePhase::LowerProgram && *units <= 256));
                if at_256 {
                    assert_eq!(calls.last(), Some(&(CompilePhase::LowerProgram, 256)));
                } else {
                    assert_eq!(calls.len(), 1);
                }
            }
        }
    }

    #[test]
    fn parsing_accounts_success_and_invalid_final_partial_work() {
        let control = Control::good();
        let text = format!("{}.01", "0".repeat(1100));
        assert_eq!(
            parse_decimal::<i128>(&text, 2, 2, policy(), CompilePhase::Validate, &control).unwrap(),
            1
        );
        let calls = control.calls.lock().unwrap();
        assert!(calls.iter().all(|(_, units)| *units <= 256));
        assert_eq!(
            calls
                .iter()
                .map(|(_, units)| u64::from(*units))
                .sum::<u64>(),
            text.len() as u64 + 1
        );
        drop(calls);
        let control = Control::good();
        assert!(matches!(
            parse_decimal::<i128>("00x", 0, 1, policy(), CompilePhase::Validate, &control),
            Err(SqlLiteralConstantError::InvalidLiteral { .. })
        ));
        assert_eq!(
            *control.calls.lock().unwrap(),
            [(CompilePhase::Validate, 0), (CompilePhase::Validate, 3)]
        );
    }
}
