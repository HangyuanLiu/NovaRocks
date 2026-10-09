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

//! The original percentile input readers; the enum selects diagnostics only.
use crate::largeint;
use arrow_array::*;
use arrow_schema::DataType;
#[derive(Clone, Copy, Debug)]
pub enum PercentileInputDiagnostic<'a> {
    ExactUpdate,
    ExactMerge,
    LegacyLabel(&'a str),
}
impl std::fmt::Display for PercentileInputDiagnostic<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ExactUpdate => "percentile_disc_cont_update",
            Self::ExactMerge => "percentile_disc_cont_merge",
            Self::LegacyLabel(label) => label,
        })
    }
}
pub fn numeric_value_at(
    array: &ArrayRef,
    row: usize,
    context: PercentileInputDiagnostic<'_>,
) -> Result<Option<f64>, String> {
    match array.data_type() {
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Int8Array"))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row) as f64))
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Int16Array"))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row) as f64))
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Int32Array"))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row) as f64))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Int64Array"))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row) as f64))
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Float32Array"))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row) as f64))
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Float64Array"))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row)))
        }
        DataType::Decimal128(_, scale) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Decimal128Array"))?;
            let divisor = 10_f64.powi(*scale as i32);
            Ok((!arr.is_null(row)).then_some(arr.value(row) as f64 / divisor))
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| format!("{context}: failed to downcast FixedSizeBinaryArray"))?;
            Ok((!arr.is_null(row)).then_some(largeint::value_at(arr, row)? as f64))
        }
        other => Err(format!(
            "{context}: unsupported numeric input type {:?}",
            other
        )),
    }
}

pub enum PercentilePayloadFailure<'a> {
    Downcast(&'static str),
    Unsupported(&'a DataType),
}
impl PercentilePayloadFailure<'_> {
    pub fn message<'a>(
        &'a self,
        context: PercentileInputDiagnostic<'a>,
    ) -> PercentilePayloadMessage<'a> {
        PercentilePayloadMessage {
            failure: self,
            context,
        }
    }
}
pub struct PercentilePayloadMessage<'a> {
    failure: &'a PercentilePayloadFailure<'a>,
    context: PercentileInputDiagnostic<'a>,
}
impl std::fmt::Display for PercentilePayloadMessage<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.failure {
            PercentilePayloadFailure::Downcast(kind) => {
                write!(f, "{}: failed to downcast {}", self.context, kind)
            }
            PercentilePayloadFailure::Unsupported(ty) => write!(
                f,
                "{}: unsupported percentile payload type {:?}",
                self.context, ty
            ),
        }
    }
}
pub fn payload_bytes_at<'a>(
    array: &'a ArrayRef,
    row: usize,
    context: PercentileInputDiagnostic<'_>,
) -> Result<Option<&'a [u8]>, String> {
    payload_bytes_at_with_failure(array, row, &mut |failure| {
        failure.message(context).to_string()
    })
}
pub fn payload_bytes_at_with_failure<'a, E>(
    array: &'a ArrayRef,
    row: usize,
    fail: &mut impl FnMut(PercentilePayloadFailure<'_>) -> E,
) -> Result<Option<&'a [u8]>, E> {
    match array.data_type() {
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| fail(PercentilePayloadFailure::Downcast("BinaryArray")))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row)))
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| fail(PercentilePayloadFailure::Downcast("StringArray")))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row).as_bytes()))
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| fail(PercentilePayloadFailure::Downcast("LargeBinaryArray")))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row)))
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| fail(PercentilePayloadFailure::Downcast("LargeStringArray")))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row).as_bytes()))
        }
        other => Err(fail(PercentilePayloadFailure::Unsupported(other))),
    }
}
