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
//! Original primitive-to-VARCHAR computation shared by selected casts and v1.
use crate::builtin::string_extended::{
    format_float32_for_varchar, format_float64_for_varchar, format_timestamp_for_varchar,
};
use arrow_array::*;
use arrow_schema::{DataType, TimeUnit};

/// Render one already evaluated, non-NULL primitive value. The caller owns
/// address, NULL and control validation; no arena or runtime policy is read.
pub fn render(array: &dyn Array, row: usize) -> Result<String, &'static str> {
    macro_rules! integer {
        ($ty:ty) => {
            array
                .as_any()
                .downcast_ref::<$ty>()
                .ok_or("text cast carrier downcast failed")?
                .value(row)
                .to_string()
        };
    }
    Ok(match array.data_type() {
        DataType::Boolean => integer!(BooleanArray),
        DataType::Int8 => integer!(Int8Array),
        DataType::Int16 => integer!(Int16Array),
        DataType::Int32 => integer!(Int32Array),
        DataType::Int64 => integer!(Int64Array),
        DataType::UInt8 => integer!(UInt8Array),
        DataType::UInt16 => integer!(UInt16Array),
        DataType::UInt32 => integer!(UInt32Array),
        DataType::UInt64 => integer!(UInt64Array),
        DataType::Float32 => format_float32_for_varchar(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or("text cast carrier downcast failed")?
                .value(row),
        ),
        DataType::Float64 => format_float64_for_varchar(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("text cast carrier downcast failed")?
                .value(row),
        ),
        DataType::Timestamp(unit, tz) => {
            let value = match unit {
                TimeUnit::Second => array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or("text cast carrier downcast failed")?
                    .value(row),
                TimeUnit::Millisecond => array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or("text cast carrier downcast failed")?
                    .value(row),
                TimeUnit::Microsecond => array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or("text cast carrier downcast failed")?
                    .value(row),
                TimeUnit::Nanosecond => array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or("text cast carrier downcast failed")?
                    .value(row),
            };
            format_timestamp_for_varchar(unit, value, tz.as_deref())
        }
        _ => return Err("text cast requires an exact primitive carrier"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    #[test]
    fn primitive_integer_text_matches_original_arrow_rendering() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(vec![Some(i8::MIN), None, Some(i8::MAX)])),
            Arc::new(Int16Array::from(vec![Some(i16::MIN), None, Some(i16::MAX)])),
            Arc::new(Int32Array::from(vec![Some(i32::MIN), None, Some(i32::MAX)])),
            Arc::new(Int64Array::from(vec![Some(i64::MIN), None, Some(i64::MAX)])),
            Arc::new(UInt8Array::from(vec![Some(0), None, Some(u8::MAX)])),
            Arc::new(UInt16Array::from(vec![Some(0), None, Some(u16::MAX)])),
            Arc::new(UInt32Array::from(vec![Some(0), None, Some(u32::MAX)])),
            Arc::new(UInt64Array::from(vec![Some(0), None, Some(u64::MAX)])),
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
        ];
        for array in arrays {
            let original = arrow_cast::cast(&array, &DataType::Utf8).unwrap();
            let original = original.as_any().downcast_ref::<StringArray>().unwrap();
            for row in [0, 2] {
                assert_eq!(render(array.as_ref(), row).unwrap(), original.value(row));
            }
        }
    }
}
