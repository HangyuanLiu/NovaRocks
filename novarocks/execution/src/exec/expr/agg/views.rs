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
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{DateTime, NaiveDate};
use novarocks_functions::AggregateInputBatch;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AggregateViewFailure {
    InputContract(novarocks_failure_contract::BoundedDetail),
    PhysicalWrapper {
        expected: &'static str,
    },
    Unsupported {
        view: &'static str,
        actual: novarocks_failure_contract::BoundedDetail,
    },
}
impl std::fmt::Display for AggregateViewFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InputContract(detail) => detail.fmt(f),
            Self::PhysicalWrapper { expected } => write!(f, "failed to downcast to {expected}"),
            Self::Unsupported { view, actual } => {
                write!(f, "unsupported {view} input type: {actual}")
            }
        }
    }
}
impl std::error::Error for AggregateViewFailure {}
impl novarocks_failure_contract::FailureProjection for AggregateViewFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        let reason = match self {
            Self::InputContract(_) => {
                crate::runtime::fragment::ExecutionReason::AggregateInputContract
            }
            Self::PhysicalWrapper { .. } => {
                crate::runtime::fragment::ExecutionReason::AggregateViewWrapper
            }
            Self::Unsupported { .. } => {
                crate::runtime::fragment::ExecutionReason::AggregateViewType
            }
        };
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            reason.reason(),
        )
    }
}
impl From<AggregateViewFailure> for crate::runtime::fragment::ExecutionFailure {
    fn from(source: AggregateViewFailure) -> Self {
        Self::from_domain(source)
    }
}

pub(crate) fn aggregate_input_contract(
    detail: impl std::fmt::Display,
) -> crate::runtime::fragment::ExecutionFailure {
    crate::runtime::fragment::ExecutionFailure::from_domain(AggregateViewFailure::InputContract(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}

pub enum AggInputView<'a> {
    None,
    Any(&'a ArrayRef),
    Int(IntArrayView<'a>),
    Float(FloatArrayView<'a>),
    Bool(&'a BooleanArray),
    Utf8(Utf8ArrayView<'a>),
    Binary(&'a BinaryArray),
    AvgState(AvgStateView<'a>),
    AvgDecimalState(AvgDecimalStateView<'a>),
}

pub struct AvgStateView<'a> {
    pub sums: FloatArrayView<'a>,
    pub counts: IntArrayView<'a>,
}

impl<'a> AvgStateView<'a> {
    pub fn value_at(&self, row: usize) -> Option<(f64, i64)> {
        let sum = self.sums.value_at(row)?;
        let count = self.counts.value_at(row)?;
        Some((sum, count))
    }
}

pub struct AvgDecimalStateView<'a> {
    pub sums: &'a Decimal128Array,
    pub counts: IntArrayView<'a>,
}

impl<'a> AvgDecimalStateView<'a> {
    pub fn value_at(&self, row: usize) -> Option<(i128, i64)> {
        if self.sums.is_null(row) {
            return None;
        }
        let sum = self.sums.value(row);
        let count = self.counts.value_at(row)?;
        Some((sum, count))
    }
}

pub fn build_agg_input_batches_with_row_count<'a>(
    arrays: &'a [Option<ArrayRef>],
    row_count: usize,
) -> Result<Vec<AggregateInputBatch<'a>>, novarocks_functions::AggregateInputBatchError> {
    let mut batches = Vec::with_capacity(arrays.len());
    for array in arrays {
        batches.push(AggregateInputBatch::try_new(array.as_ref(), row_count)?);
    }
    Ok(batches)
}

#[derive(Clone, Debug)]
pub enum IntArrayView<'a> {
    Int64(&'a Int64Array),
    Int32(&'a Int32Array),
    Int16(&'a Int16Array),
    Int8(&'a Int8Array),
}

impl<'a> IntArrayView<'a> {
    pub fn new(array: &'a ArrayRef) -> Result<Self, AggregateViewFailure> {
        match array.data_type() {
            DataType::Int64 => array
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(Self::Int64)
                .ok_or_else(|| AggregateViewFailure::PhysicalWrapper {
                    expected: "Int64Array",
                }),
            DataType::Int32 => array
                .as_any()
                .downcast_ref::<Int32Array>()
                .map(Self::Int32)
                .ok_or_else(|| AggregateViewFailure::PhysicalWrapper {
                    expected: "Int32Array",
                }),
            DataType::Int16 => array
                .as_any()
                .downcast_ref::<Int16Array>()
                .map(Self::Int16)
                .ok_or_else(|| AggregateViewFailure::PhysicalWrapper {
                    expected: "Int16Array",
                }),
            DataType::Int8 => array
                .as_any()
                .downcast_ref::<Int8Array>()
                .map(Self::Int8)
                .ok_or_else(|| AggregateViewFailure::PhysicalWrapper {
                    expected: "Int8Array",
                }),
            other => Err(AggregateViewFailure::Unsupported {
                view: "int",
                actual: novarocks_failure_contract::BoundedDetail::truncating_display(
                    format_args!("{other:?}"),
                ),
            }),
        }
    }

    pub fn value_at(&self, row: usize) -> Option<i64> {
        match self {
            IntArrayView::Int64(arr) => (!arr.is_null(row)).then(|| arr.value(row)),
            IntArrayView::Int32(arr) => (!arr.is_null(row)).then(|| arr.value(row) as i64),
            IntArrayView::Int16(arr) => (!arr.is_null(row)).then(|| arr.value(row) as i64),
            IntArrayView::Int8(arr) => (!arr.is_null(row)).then(|| arr.value(row) as i64),
        }
    }
}

#[derive(Clone, Debug)]
pub enum FloatArrayView<'a> {
    Float64(&'a Float64Array),
    Float32(&'a Float32Array),
}

impl<'a> FloatArrayView<'a> {
    pub fn new(array: &'a ArrayRef) -> Result<Self, AggregateViewFailure> {
        match array.data_type() {
            DataType::Float64 => array
                .as_any()
                .downcast_ref::<Float64Array>()
                .map(Self::Float64)
                .ok_or_else(|| AggregateViewFailure::PhysicalWrapper {
                    expected: "Float64Array",
                }),
            DataType::Float32 => array
                .as_any()
                .downcast_ref::<Float32Array>()
                .map(Self::Float32)
                .ok_or_else(|| AggregateViewFailure::PhysicalWrapper {
                    expected: "Float32Array",
                }),
            other => Err(AggregateViewFailure::Unsupported {
                view: "float",
                actual: novarocks_failure_contract::BoundedDetail::truncating_display(
                    format_args!("{other:?}"),
                ),
            }),
        }
    }

    pub fn value_at(&self, row: usize) -> Option<f64> {
        match self {
            FloatArrayView::Float64(arr) => (!arr.is_null(row)).then(|| arr.value(row)),
            FloatArrayView::Float32(arr) => (!arr.is_null(row)).then(|| arr.value(row) as f64),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Utf8ArrayView<'a> {
    Utf8(&'a StringArray),
    Date32(&'a Date32Array),
    TimestampSecond(&'a TimestampSecondArray, Option<String>),
    TimestampMillisecond(&'a TimestampMillisecondArray, Option<String>),
    TimestampMicrosecond(&'a TimestampMicrosecondArray, Option<String>),
    TimestampNanosecond(&'a TimestampNanosecondArray, Option<String>),
    Decimal128(&'a Decimal128Array, i8),
}

impl<'a> Utf8ArrayView<'a> {
    pub fn len(&self) -> usize {
        match self {
            Utf8ArrayView::Utf8(arr) => arr.len(),
            Utf8ArrayView::Date32(arr) => arr.len(),
            Utf8ArrayView::TimestampSecond(arr, _) => arr.len(),
            Utf8ArrayView::TimestampMillisecond(arr, _) => arr.len(),
            Utf8ArrayView::TimestampMicrosecond(arr, _) => arr.len(),
            Utf8ArrayView::TimestampNanosecond(arr, _) => arr.len(),
            Utf8ArrayView::Decimal128(arr, _) => arr.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn new(array: &'a ArrayRef) -> Result<Self, AggregateViewFailure> {
        match array.data_type() {
            DataType::Utf8 => array
                .as_any()
                .downcast_ref::<StringArray>()
                .map(Self::Utf8)
                .ok_or_else(|| AggregateViewFailure::PhysicalWrapper {
                    expected: "StringArray",
                }),
            DataType::Date32 => array
                .as_any()
                .downcast_ref::<Date32Array>()
                .map(Self::Date32)
                .ok_or_else(|| AggregateViewFailure::PhysicalWrapper {
                    expected: "Date32Array",
                }),
            DataType::Timestamp(unit, tz) => {
                let tz_string = tz.as_deref().map(|s| s.to_string());
                match unit {
                    TimeUnit::Second => array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .map(|arr| Self::TimestampSecond(arr, tz_string))
                        .ok_or_else(|| AggregateViewFailure::PhysicalWrapper {
                            expected: "TimestampSecondArray",
                        }),
                    TimeUnit::Millisecond => array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .map(|arr| Self::TimestampMillisecond(arr, tz_string))
                        .ok_or_else(|| AggregateViewFailure::PhysicalWrapper {
                            expected: "TimestampMillisecondArray",
                        }),
                    TimeUnit::Microsecond => array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .map(|arr| Self::TimestampMicrosecond(arr, tz_string))
                        .ok_or_else(|| AggregateViewFailure::PhysicalWrapper {
                            expected: "TimestampMicrosecondArray",
                        }),
                    TimeUnit::Nanosecond => array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .map(|arr| Self::TimestampNanosecond(arr, tz_string))
                        .ok_or_else(|| AggregateViewFailure::PhysicalWrapper {
                            expected: "TimestampNanosecondArray",
                        }),
                }
            }
            DataType::Decimal128(_, scale) => array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .map(|arr| Self::Decimal128(arr, *scale))
                .ok_or_else(|| AggregateViewFailure::PhysicalWrapper {
                    expected: "Decimal128Array",
                }),
            other => Err(AggregateViewFailure::Unsupported {
                view: "utf8",
                actual: novarocks_failure_contract::BoundedDetail::truncating_display(
                    format_args!("{other:?}"),
                ),
            }),
        }
    }

    pub fn value_at(&self, row: usize) -> Option<String> {
        match self {
            Utf8ArrayView::Utf8(arr) => (!arr.is_null(row)).then(|| arr.value(row).to_string()),
            Utf8ArrayView::Date32(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    let days = arr.value(row);
                    Some(format_date32(days))
                }
            }
            Utf8ArrayView::TimestampSecond(arr, tz) => {
                if arr.is_null(row) {
                    None
                } else {
                    let seconds = arr.value(row);
                    Some(format_timestamp(TimeUnit::Second, seconds, tz.as_deref()))
                }
            }
            Utf8ArrayView::TimestampMillisecond(arr, tz) => {
                if arr.is_null(row) {
                    None
                } else {
                    let millis = arr.value(row);
                    Some(format_timestamp(
                        TimeUnit::Millisecond,
                        millis,
                        tz.as_deref(),
                    ))
                }
            }
            Utf8ArrayView::TimestampMicrosecond(arr, tz) => {
                if arr.is_null(row) {
                    None
                } else {
                    let micros = arr.value(row);
                    Some(format_timestamp(
                        TimeUnit::Microsecond,
                        micros,
                        tz.as_deref(),
                    ))
                }
            }
            Utf8ArrayView::TimestampNanosecond(arr, tz) => {
                if arr.is_null(row) {
                    None
                } else {
                    let nanos = arr.value(row);
                    Some(format_timestamp(TimeUnit::Nanosecond, nanos, tz.as_deref()))
                }
            }
            Utf8ArrayView::Decimal128(arr, scale) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(format_decimal(arr.value(row), *scale))
                }
            }
        }
    }
}

fn format_date32(days_since_epoch: i32) -> String {
    let date = NaiveDate::from_num_days_from_ce_opt(719163 + days_since_epoch)
        .unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
    date.format("%Y-%m-%d").to_string()
}

fn format_timestamp(unit: TimeUnit, value: i64, tz: Option<&str>) -> String {
    let timestamp_str = match unit {
        TimeUnit::Second => {
            let dt = DateTime::from_timestamp(value, 0)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
        }
        TimeUnit::Millisecond => {
            let seconds = value / 1000;
            let nanos = ((value % 1000) * 1_000_000) as u32;
            let dt = DateTime::from_timestamp(seconds, nanos)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
        }
        TimeUnit::Microsecond => {
            let seconds = value / 1_000_000;
            let nanos = ((value % 1_000_000) * 1000) as u32;
            let dt = DateTime::from_timestamp(seconds, nanos)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string()
        }
        TimeUnit::Nanosecond => {
            let seconds = value / 1_000_000_000;
            let nanos = (value % 1_000_000_000) as u32;
            let dt = DateTime::from_timestamp(seconds, nanos)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.9f").to_string()
        }
    };
    if let Some(tz) = tz {
        format!("{} {}", timestamp_str, tz)
    } else {
        timestamp_str
    }
}

fn format_decimal(unscaled: i128, scale: i8) -> String {
    novarocks_functions::decimal_text::format_decimal_with_scale(unscaled, scale)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Fields};

    use super::build_agg_input_batches_with_row_count;

    #[test]
    fn aggregate_input_batches_keep_zero_argument_row_count() {
        let batches = build_agg_input_batches_with_row_count(&[None], 7).unwrap();
        assert_eq!(batches[0].row_count(), 7);
        assert!(batches[0].values().is_none());
    }

    #[test]
    fn aggregate_input_batches_preserve_packed_struct_owner() {
        let left: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let right: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        let packed: ArrayRef = Arc::new(StructArray::new(
            Fields::from(vec![
                Field::new("f0", DataType::Int64, true),
                Field::new("f1", DataType::Utf8, true),
            ]),
            vec![left, right],
            None,
        ));
        let arrays = [Some(packed)];

        let batches = build_agg_input_batches_with_row_count(&arrays, 2).unwrap();
        assert_eq!(batches[0].row_count(), 2);
        assert!(matches!(
            batches[0].values().map(|array| array.data_type()),
            Some(DataType::Struct(fields)) if fields.len() == 2
        ));
    }
}
