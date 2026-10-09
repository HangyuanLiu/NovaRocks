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

//! Frozen scalar semantic facts compared with canonical Native storage.
//! This check does not infer logical Json/Variant/opaque identity from Arrow.
//! The frozen schema supplies that authority; runtime owners separately verify
//! actual logical metadata and pay every accepted carrier's complete backing.

use arrow_schema::{DataType as D, Field, TimeUnit as U};
use novarocks_result_contract::{ScalarField, ScalarTimestampUnit as T, ScalarValueType as S};

pub fn scalar_field_matches_storage(field: &ScalarField, carrier: &D, nullable: bool) -> bool {
    field.nullable == nullable && matches_storage(&field.value_type, carrier)
}

fn matches_field(field: &ScalarField, carrier: &Field) -> bool {
    scalar_field_matches_storage(field, carrier.data_type(), carrier.is_nullable())
}
fn matches_storage(field: &S, carrier: &D) -> bool {
    match (field, carrier) {
        (S::Null, D::Null)
        | (S::Boolean, D::Boolean)
        | (S::SignedInteger(8), D::Int8)
        | (S::SignedInteger(16), D::Int16)
        | (S::SignedInteger(32), D::Int32)
        | (S::SignedInteger(64), D::Int64)
        | (S::LargeInt, D::FixedSizeBinary(16))
        | (S::Float32, D::Float32)
        | (S::Float64, D::Float64)
        | (S::String | S::Json, D::Utf8)
        | (S::Binary | S::Opaque(_), D::Binary)
        | (S::Variant, D::LargeBinary)
        | (S::Date, D::Date32)
        | (S::TimeMicros, D::Time64(U::Microsecond)) => true,
        (
            S::Decimal {
                bits: 128,
                precision,
                scale,
            },
            D::Decimal128(p, s),
        )
        | (
            S::Decimal {
                bits: 256,
                precision,
                scale,
            },
            D::Decimal256(p, s),
        ) => precision == p && i16::from(*scale) == i16::from(*s),
        (S::Timestamp { unit, timezone }, D::Timestamp(u, zone)) => {
            matches!(
                (unit, u),
                (T::Microsecond, U::Microsecond) | (T::Nanosecond, U::Nanosecond)
            ) && timezone.as_deref() == zone.as_deref()
        }
        (S::List(child), D::List(field)) => field.name() == "item" && matches_field(child, field),
        (S::Map { key, value }, D::Map(entries, false))
            if entries.name() == "entries" && !entries.is_nullable() =>
        {
            match entries.data_type() {
                D::Struct(fields)
                    if fields.len() == 2
                        && fields[0].name() == "key"
                        && fields[1].name() == "value" =>
                {
                    matches_field(key, &fields[0]) && matches_field(value, &fields[1])
                }
                _ => false,
            }
        }
        (S::Struct(expected), D::Struct(actual)) => {
            expected.len() == actual.len()
                && expected.iter().zip(actual).all(|(expected, actual)| {
                    expected.name == *actual.name() && matches_field(&expected.field, actual)
                })
        }
        _ => false,
    }
}
