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

use arrow::datatypes::{DataType, TimeUnit};

use novarocks_types::PrimitiveType;
use novarocks_types::decimal::{LEGACY_DECIMALV2_PRECISION, LEGACY_DECIMALV2_SCALE};
use novarocks_types::largeint;

pub(crate) fn arrow_type_from_native_primitive(primitive: PrimitiveType) -> Option<DataType> {
    let data_type = match primitive {
        PrimitiveType::Null => DataType::Null,
        PrimitiveType::Boolean => DataType::Boolean,
        PrimitiveType::TinyInt => DataType::Int8,
        PrimitiveType::SmallInt => DataType::Int16,
        PrimitiveType::Int => DataType::Int32,
        PrimitiveType::BigInt => DataType::Int64,
        PrimitiveType::LargeInt => DataType::FixedSizeBinary(largeint::LARGEINT_BYTE_WIDTH),
        PrimitiveType::Float => DataType::Float32,
        PrimitiveType::Double => DataType::Float64,
        PrimitiveType::Date => DataType::Date32,
        PrimitiveType::DateTime => DataType::Timestamp(TimeUnit::Microsecond, None),
        PrimitiveType::Time => DataType::Time64(TimeUnit::Microsecond),
        PrimitiveType::Binary | PrimitiveType::Varbinary => DataType::Binary,
        PrimitiveType::Hll | PrimitiveType::Object | PrimitiveType::Percentile => DataType::Binary,
        PrimitiveType::Char
        | PrimitiveType::Varchar
        | PrimitiveType::Json
        | PrimitiveType::Function => DataType::Utf8,
        PrimitiveType::Variant => DataType::LargeBinary,
        PrimitiveType::DecimalV2 => {
            DataType::Decimal128(LEGACY_DECIMALV2_PRECISION, LEGACY_DECIMALV2_SCALE)
        }
        PrimitiveType::Decimal32
        | PrimitiveType::Decimal64
        | PrimitiveType::Decimal128
        | PrimitiveType::Decimal256
        | PrimitiveType::Decimal
        | PrimitiveType::Int256
        | PrimitiveType::Invalid => return None,
    };
    Some(data_type)
}

pub(crate) fn native_primitive_from_arrow_type(data_type: &DataType) -> Option<PrimitiveType> {
    match data_type {
        DataType::Null => Some(PrimitiveType::Null),
        DataType::Boolean => Some(PrimitiveType::Boolean),
        DataType::Int8 => Some(PrimitiveType::TinyInt),
        DataType::Int16 => Some(PrimitiveType::SmallInt),
        DataType::Int32 => Some(PrimitiveType::Int),
        DataType::Int64 => Some(PrimitiveType::BigInt),
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            Some(PrimitiveType::LargeInt)
        }
        DataType::Float32 => Some(PrimitiveType::Float),
        DataType::Float64 => Some(PrimitiveType::Double),
        DataType::Date32 => Some(PrimitiveType::Date),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Some(PrimitiveType::DateTime),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Some(PrimitiveType::DateTime),
        DataType::Time64(TimeUnit::Microsecond) => Some(PrimitiveType::Time),
        DataType::Binary => Some(PrimitiveType::Varbinary),
        DataType::Utf8 | DataType::LargeUtf8 => Some(PrimitiveType::Varchar),
        DataType::LargeBinary => Some(PrimitiveType::Variant),
        DataType::Decimal128(_, _) => Some(PrimitiveType::Decimal128),
        DataType::Decimal256(_, _) => Some(PrimitiveType::Decimal256),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_basic_native_primitives_to_arrow() {
        assert_eq!(
            arrow_type_from_native_primitive(PrimitiveType::Int),
            Some(DataType::Int32)
        );
        assert_eq!(
            arrow_type_from_native_primitive(PrimitiveType::Varchar),
            Some(DataType::Utf8)
        );
        assert_eq!(
            arrow_type_from_native_primitive(PrimitiveType::DateTime),
            Some(DataType::Timestamp(TimeUnit::Microsecond, None))
        );
    }
}
