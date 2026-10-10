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

// Returns heap only: TypeName inline is already part of Expr::Cast.
use super::borrowed_value_footprint::Failure;
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_parser::ast::{Ident, TypeNameArgument};
use std::mem::size_of;

type Result<T> = std::result::Result<T, Failure>;
fn add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b).ok_or(Failure::ResourceExhausted)
}
fn mul(a: u64, b: u64) -> Result<u64> {
    a.checked_mul(b).ok_or(Failure::ResourceExhausted)
}
fn name(s: &str) -> Result<u64> {
    add(size_of::<Ident>() as u64, s.len() as u64)
}
fn arguments(count: usize) -> Result<u64> {
    // TypeNameArgument contains its inline TypeName/Literal/StructField.
    add(
        mul(count as u64, size_of::<TypeNameArgument>() as u64)?,
        count.saturating_sub(1) as u64,
    )
}
fn decimal_digits(mut n: u8) -> u64 {
    let mut digits = 1;
    while n >= 10 {
        n /= 10;
        digits += 1;
    }
    digits
}

pub fn exact_type_heap(dt: &DataType) -> Result<u64> {
    let primitive = match dt {
        DataType::Boolean => Some("BOOLEAN"),
        DataType::Int8 => Some("TINYINT"),
        DataType::Int16 => Some("SMALLINT"),
        DataType::Int32 => Some("INT"),
        DataType::Int64 => Some("BIGINT"),
        DataType::FixedSizeBinary(width)
            if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
        {
            Some("LARGEINT")
        }
        DataType::Float32 => Some("FLOAT"),
        DataType::Float64 => Some("DOUBLE"),
        DataType::Utf8 | DataType::LargeUtf8 => Some("STRING"),
        DataType::Date32 => Some("DATE"),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Some("DATETIME"),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Some("DATETIME_NS"),
        DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond) => Some("TIME"),
        DataType::Binary => Some("VARBINARY"),
        DataType::LargeBinary => Some("VARIANT"),
        _ => None,
    };
    if let Some(s) = primitive {
        return name(s);
    }
    match dt {
        DataType::Decimal128(precision, scale) => {
            // Preserve the old Pratt type-parameter rejection for -scale,
            // including NULL-valued columns that bypass value conversion.
            if *scale < 0 {
                return Err(Failure::OriginalSemantic("negative DECIMAL type parameter"));
            }
            add(
                add(name("DECIMAL")?, arguments(2)?)?,
                add(decimal_digits(*precision), decimal_digits(*scale as u8))?,
            )
        }
        DataType::List(field) => add(
            add(name("ARRAY")?, arguments(1)?)?,
            exact_type_heap(field.data_type())?,
        ),
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(Failure::OriginalSemantic(
                    "unsupported Arrow map entries type",
                ));
            };
            if fields.len() != 2 {
                return Err(Failure::OriginalSemantic(
                    "unsupported Arrow map entries field count",
                ));
            }
            add(
                add(
                    add(name("MAP")?, arguments(2)?)?,
                    exact_type_heap(fields[0].data_type())?,
                )?,
                exact_type_heap(fields[1].data_type())?,
            )
        }
        DataType::Struct(fields) => {
            if fields.is_empty() {
                return Err(Failure::OriginalSemantic("empty STRUCT type parameter"));
            }
            let mut total = add(name("STRUCT")?, arguments(fields.len())?)?;
            for field in fields {
                // Field Ident and child TypeName inline are inside the enum
                // slot already charged by arguments(); only heap is added.
                total = add(
                    add(total, field.name().len() as u64)?,
                    exact_type_heap(field.data_type())?,
                )?;
            }
            Ok(total)
        }
        _ => Err(Failure::OriginalSemantic(
            "unsupported Arrow type for INSERT default conversion",
        )),
    }
}
