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

//! Arrow result batches to MySQL scalar-row adaptation.

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, StringArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{Duration, NaiveDate, NaiveDateTime, Utc};
use novarocks_query_application::api::ResultField;
use novarocks_types::{
    FieldRenderSchema, format_mysql_container_value_with_schema, is_zero_date_sentinel_date32,
};

use crate::MysqlResultValue;

/// Converts one Arrow batch row into MySQL scalar values using the frozen Query
/// Application result schema and Arrow field metadata.
pub fn build_mysql_row(
    batch: &RecordBatch,
    columns: &[ResultField],
    row_idx: usize,
) -> Result<Vec<MysqlResultValue>, String> {
    if batch.num_columns() != columns.len() {
        return Err(format!(
            "query result column count mismatch: metadata has {}, batch has {}",
            columns.len(),
            batch.num_columns()
        ));
    }
    if batch.schema().fields().len() != columns.len() {
        return Err(format!(
            "query result field count mismatch: schema has {}, metadata has {}",
            batch.schema().fields().len(),
            columns.len()
        ));
    }
    batch
        .columns()
        .iter()
        .zip(batch.schema().fields().iter())
        .zip(columns.iter())
        .map(|((column, field), declared)| {
            let field_schema = FieldRenderSchema::from_field(field.as_ref());
            array_value_to_mysql_value(column, declared, row_idx, Some(&field_schema))
        })
        .collect()
}

/// Converts one Arrow scalar into its MySQL wire value.
pub fn array_value_to_mysql_value(
    column: &ArrayRef,
    declared: &ResultField,
    row_idx: usize,
    field_schema: Option<&FieldRenderSchema>,
) -> Result<MysqlResultValue, String> {
    if column.is_null(row_idx) {
        return Ok(MysqlResultValue::Null);
    }

    if let Some(novarocks_types::schema::SqlType::Decimal { scale, .. }) = declared.logical_type() {
        return decimal_to_mysql_value(column, row_idx, *scale);
    }

    if matches!(declared.data_type(), DataType::Date32)
        && matches!(column.data_type(), DataType::Timestamp(_, _))
    {
        return timestamp_to_date_mysql_value(column, timestamp_unit(column.data_type())?, row_idx);
    }
    if matches!(
        declared.data_type(),
        DataType::Time32(_) | DataType::Time64(_)
    ) && matches!(column.data_type(), DataType::Timestamp(_, _))
    {
        return timestamp_to_time_mysql_value(column, timestamp_unit(column.data_type())?, row_idx);
    }

    if field_schema.is_some_and(FieldRenderSchema::renders_opaque_binary) {
        return Ok(MysqlResultValue::Null);
    }

    let name_lower = declared.name().to_lowercase();
    if matches!(column.data_type(), DataType::Binary | DataType::LargeBinary)
        && (name_lower.starts_with("bitmap_agg(")
            || name_lower.starts_with("bitmap_union(")
            || name_lower.starts_with("hll_union(")
            || name_lower.starts_with("hll_raw_agg("))
    {
        return Ok(MysqlResultValue::Null);
    }

    match column.data_type() {
        DataType::Boolean => downcast_array::<BooleanArray>(column, "BooleanArray")
            .map(|arr| MysqlResultValue::Int(if arr.value(row_idx) { 1 } else { 0 })),
        DataType::Int8 => downcast_array::<Int8Array>(column, "Int8Array")
            .map(|arr| MysqlResultValue::Int(i64::from(arr.value(row_idx)))),
        DataType::Int16 => downcast_array::<Int16Array>(column, "Int16Array")
            .map(|arr| MysqlResultValue::Int(i64::from(arr.value(row_idx)))),
        DataType::Int32 => downcast_array::<Int32Array>(column, "Int32Array")
            .map(|arr| MysqlResultValue::Int(i64::from(arr.value(row_idx)))),
        DataType::Int64 => downcast_array::<Int64Array>(column, "Int64Array")
            .map(|arr| MysqlResultValue::Int(arr.value(row_idx))),
        DataType::UInt8 => downcast_array::<UInt8Array>(column, "UInt8Array")
            .map(|arr| MysqlResultValue::UInt(u64::from(arr.value(row_idx)))),
        DataType::UInt16 => downcast_array::<UInt16Array>(column, "UInt16Array")
            .map(|arr| MysqlResultValue::UInt(u64::from(arr.value(row_idx)))),
        DataType::UInt32 => downcast_array::<UInt32Array>(column, "UInt32Array")
            .map(|arr| MysqlResultValue::UInt(u64::from(arr.value(row_idx)))),
        DataType::UInt64 => downcast_array::<UInt64Array>(column, "UInt64Array")
            .map(|arr| MysqlResultValue::UInt(arr.value(row_idx))),
        DataType::Float32 => downcast_array::<Float32Array>(column, "Float32Array")
            .map(|arr| MysqlResultValue::Float(arr.value(row_idx))),
        DataType::Float64 => downcast_array::<Float64Array>(column, "Float64Array")
            .map(|arr| MysqlResultValue::Double(arr.value(row_idx))),
        DataType::FixedSizeBinary(width)
            if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
        {
            let arr = downcast_array::<FixedSizeBinaryArray>(column, "FixedSizeBinaryArray")?;
            let value = novarocks_types::largeint::i128_from_be_bytes(arr.value(row_idx))?;
            Ok(MysqlResultValue::Bytes(value.to_string().into_bytes()))
        }
        DataType::Utf8 => downcast_array::<StringArray>(column, "StringArray")
            .map(|arr| MysqlResultValue::Bytes(arr.value(row_idx).as_bytes().to_vec())),
        DataType::LargeUtf8 => downcast_array::<LargeStringArray>(column, "LargeStringArray")
            .map(|arr| MysqlResultValue::Bytes(arr.value(row_idx).as_bytes().to_vec())),
        DataType::Binary => downcast_array::<BinaryArray>(column, "BinaryArray")
            .map(|arr| MysqlResultValue::Bytes(arr.value(row_idx).to_vec())),
        DataType::LargeBinary => downcast_array::<LargeBinaryArray>(column, "LargeBinaryArray")
            .map(|arr| MysqlResultValue::Bytes(arr.value(row_idx).to_vec())),
        DataType::Date32 => {
            let arr = downcast_array::<Date32Array>(column, "Date32Array")?;
            date32_to_mysql_value(arr.value(row_idx))
        }
        DataType::Decimal128(_, scale) => decimal128_to_mysql_value(column, row_idx, *scale),
        DataType::Time32(unit) => time_to_mysql_value(column, *unit, row_idx),
        DataType::Time64(unit) => time_to_mysql_value(column, *unit, row_idx),
        DataType::Timestamp(unit, _) => timestamp_to_mysql_value(column, *unit, row_idx),
        DataType::Null => Ok(MysqlResultValue::Null),
        DataType::List(_) | DataType::Map(_, _) | DataType::Struct(_) => {
            Ok(MysqlResultValue::Bytes(
                format_mysql_container_value_with_schema(column, row_idx, field_schema)?
                    .into_bytes(),
            ))
        }
        other => Err(format!(
            "standalone mysql server does not support output column type {other:?}"
        )),
    }
}

fn decimal128_to_mysql_value(
    column: &ArrayRef,
    row_idx: usize,
    scale: i8,
) -> Result<MysqlResultValue, String> {
    let arr = downcast_array::<Decimal128Array>(column, "Decimal128Array")?;
    Ok(MysqlResultValue::Bytes(
        format_decimal128_string(arr.value(row_idx), scale)?.into_bytes(),
    ))
}

fn format_decimal128_string(value: i128, scale: i8) -> Result<String, String> {
    if scale < 0 {
        return Err(format!("unsupported decimal scale: {scale}"));
    }
    let scale = u32::try_from(scale).map_err(|_| format!("unsupported decimal scale: {scale}"))?;
    if scale == 0 {
        return Ok(value.to_string());
    }
    let factor = 10_u128
        .checked_pow(scale)
        .ok_or_else(|| format!("unsupported decimal scale: {scale}"))?;
    let negative = value.is_negative();
    let abs = value.unsigned_abs();
    let whole = abs / factor;
    let fraction = abs % factor;
    Ok(format!(
        "{}{}.{:0width$}",
        if negative { "-" } else { "" },
        whole,
        fraction,
        width = scale as usize
    ))
}

fn decimal_to_mysql_value(
    column: &ArrayRef,
    row_idx: usize,
    scale: i8,
) -> Result<MysqlResultValue, String> {
    let scale =
        usize::try_from(scale).map_err(|_| format!("unsupported decimal scale: {scale}"))?;
    let formatted = match column.data_type() {
        DataType::Int8 => downcast_array::<Int8Array>(column, "Int8Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::Int16 => downcast_array::<Int16Array>(column, "Int16Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::Int32 => downcast_array::<Int32Array>(column, "Int32Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::Int64 => downcast_array::<Int64Array>(column, "Int64Array")
            .map(|arr| format!("{:.*}", scale, arr.value(row_idx) as f64))?,
        DataType::UInt8 => downcast_array::<UInt8Array>(column, "UInt8Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::UInt16 => downcast_array::<UInt16Array>(column, "UInt16Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::UInt32 => downcast_array::<UInt32Array>(column, "UInt32Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::UInt64 => downcast_array::<UInt64Array>(column, "UInt64Array")
            .map(|arr| format!("{:.*}", scale, arr.value(row_idx) as f64))?,
        DataType::Float32 => downcast_array::<Float32Array>(column, "Float32Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::Float64 => downcast_array::<Float64Array>(column, "Float64Array")
            .map(|arr| format!("{:.*}", scale, arr.value(row_idx)))?,
        DataType::Utf8 => downcast_array::<StringArray>(column, "StringArray")
            .map(|arr| arr.value(row_idx).to_string())?,
        DataType::LargeUtf8 => downcast_array::<LargeStringArray>(column, "LargeStringArray")
            .map(|arr| arr.value(row_idx).to_string())?,
        other => {
            return Err(format!(
                "standalone mysql server does not support decimal output column type {other:?}"
            ));
        }
    };
    Ok(MysqlResultValue::Bytes(formatted.into_bytes()))
}

fn timestamp_unit(data_type: &DataType) -> Result<TimeUnit, String> {
    match data_type {
        DataType::Timestamp(unit, _) => Ok(*unit),
        other => Err(format!("expected timestamp data type, got {other:?}")),
    }
}

fn downcast_array<'a, T: 'static>(column: &'a ArrayRef, expected: &str) -> Result<&'a T, String> {
    column
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| format!("failed to downcast output column to {expected}"))
}

fn date32_to_mysql_value(days: i32) -> Result<MysqlResultValue, String> {
    if is_zero_date_sentinel_date32(days) {
        return Ok(MysqlResultValue::Bytes(b"0000-00-00".to_vec()));
    }
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    let date = epoch
        .checked_add_signed(Duration::days(i64::from(days)))
        .ok_or_else(|| format!("date32 value out of range: {days}"))?;
    Ok(MysqlResultValue::Date(date))
}

fn timestamp_to_naive_datetime(
    column: &ArrayRef,
    unit: TimeUnit,
    row_idx: usize,
) -> Result<NaiveDateTime, String> {
    let raw = timestamp_raw_micros(column, unit, row_idx)?;
    let secs = raw.div_euclid(1_000_000);
    let micros = raw.rem_euclid(1_000_000);
    let secs = i64::try_from(secs).map_err(|_| format!("timestamp value out of range: {raw}"))?;
    let micros =
        u32::try_from(micros).map_err(|_| format!("timestamp micros out of range: {raw}"))?;
    let dt = chrono::DateTime::<Utc>::from_timestamp(secs, micros * 1_000)
        .ok_or_else(|| format!("timestamp value out of range: {raw}"))?;
    Ok(dt.naive_utc())
}

fn timestamp_raw_micros(column: &ArrayRef, unit: TimeUnit, row_idx: usize) -> Result<i128, String> {
    let raw = match unit {
        TimeUnit::Second => {
            i128::from(
                downcast_array::<TimestampSecondArray>(column, "TimestampSecondArray")?
                    .value(row_idx),
            ) * 1_000_000
        }
        TimeUnit::Millisecond => {
            i128::from(
                downcast_array::<TimestampMillisecondArray>(column, "TimestampMillisecondArray")?
                    .value(row_idx),
            ) * 1_000
        }
        TimeUnit::Microsecond => i128::from(
            downcast_array::<TimestampMicrosecondArray>(column, "TimestampMicrosecondArray")?
                .value(row_idx),
        ),
        TimeUnit::Nanosecond => {
            i128::from(
                downcast_array::<TimestampNanosecondArray>(column, "TimestampNanosecondArray")?
                    .value(row_idx),
            ) / 1_000
        }
    };
    Ok(raw)
}

fn timestamp_to_mysql_value(
    column: &ArrayRef,
    unit: TimeUnit,
    row_idx: usize,
) -> Result<MysqlResultValue, String> {
    Ok(MysqlResultValue::DateTime(timestamp_to_naive_datetime(
        column, unit, row_idx,
    )?))
}

fn timestamp_to_date_mysql_value(
    column: &ArrayRef,
    unit: TimeUnit,
    row_idx: usize,
) -> Result<MysqlResultValue, String> {
    Ok(MysqlResultValue::Date(
        timestamp_to_naive_datetime(column, unit, row_idx)?.date(),
    ))
}

fn timestamp_to_time_mysql_value(
    column: &ArrayRef,
    unit: TimeUnit,
    row_idx: usize,
) -> Result<MysqlResultValue, String> {
    time_micros_to_mysql_value(timestamp_raw_micros(column, unit, row_idx)?)
}

fn time_to_mysql_value(
    column: &ArrayRef,
    unit: TimeUnit,
    row_idx: usize,
) -> Result<MysqlResultValue, String> {
    let micros = match unit {
        TimeUnit::Second => {
            i128::from(
                downcast_array::<Time32SecondArray>(column, "Time32SecondArray")?.value(row_idx),
            ) * 1_000_000
        }
        TimeUnit::Millisecond => {
            i128::from(
                downcast_array::<Time32MillisecondArray>(column, "Time32MillisecondArray")?
                    .value(row_idx),
            ) * 1_000
        }
        TimeUnit::Microsecond => i128::from(
            downcast_array::<Time64MicrosecondArray>(column, "Time64MicrosecondArray")?
                .value(row_idx),
        ),
        TimeUnit::Nanosecond => {
            i128::from(
                downcast_array::<Time64NanosecondArray>(column, "Time64NanosecondArray")?
                    .value(row_idx),
            ) / 1_000
        }
    };
    time_micros_to_mysql_value(micros)
}

fn time_micros_to_mysql_value(micros: i128) -> Result<MysqlResultValue, String> {
    let total_seconds = micros.div_euclid(1_000_000);
    let microseconds = micros.rem_euclid(1_000_000) as u32;
    let hours = total_seconds.div_euclid(3_600);
    let minutes = total_seconds.rem_euclid(3_600).div_euclid(60);
    let seconds = total_seconds.rem_euclid(60);
    let days = hours.div_euclid(24);
    let hour_of_day = hours.rem_euclid(24);

    Ok(MysqlResultValue::Time {
        negative: micros.is_negative(),
        days: u32::try_from(days.unsigned_abs())
            .map_err(|_| format!("time value out of range: {micros}"))?,
        hours: u8::try_from(hour_of_day.unsigned_abs())
            .map_err(|_| format!("time value out of range: {micros}"))?,
        minutes: u8::try_from(minutes.unsigned_abs())
            .map_err(|_| format!("time value out of range: {micros}"))?,
        seconds: u8::try_from(seconds.unsigned_abs())
            .map_err(|_| format!("time value out of range: {micros}"))?,
        micros: microseconds,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Date32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_query_application::api::ResultField;

    use super::{MysqlResultValue, array_value_to_mysql_value, build_mysql_row};

    #[test]
    fn date32_zero_sentinel_renders_as_mysql_zero_date() {
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
        let zero_date = chrono::NaiveDate::from_ymd_opt(-1, 11, 30).expect("sentinel date");
        let days = zero_date.signed_duration_since(epoch).num_days() as i32;
        let values = Arc::new(Date32Array::from(vec![days])) as ArrayRef;
        let field = ResultField::new("d", DataType::Date32, false, None);

        let value = array_value_to_mysql_value(&values, &field, 0, None).expect("mysql value");

        assert_eq!(value, MysqlResultValue::Bytes(b"0000-00-00".to_vec()));
    }

    #[test]
    fn row_encoding_consumes_query_application_fields() {
        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "count",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![7]))],
        )
        .expect("record batch");
        let fields = vec![ResultField::new("count", DataType::Int64, false, None)];

        let row = build_mysql_row(&batch, &fields, 0).expect("mysql row");

        assert_eq!(row, vec![MysqlResultValue::Int(7)]);
    }
}
