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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SqlType {
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    LargeInt,
    Float,
    Double,
    Decimal {
        precision: u8,
        scale: i8,
    },
    String,
    Json,
    Binary,
    Bitmap,
    Hll,
    Boolean,
    Date,
    DateTime,
    /// Iceberg v3 nanosecond timestamp (`timestamp_ns`). Default DATETIME stays
    /// microsecond; this is a distinct variant so existing DATETIME behavior is
    /// untouched. Time zone (`timestamptz_ns`) is carried at the Arrow level on
    /// read/insert; native CREATE of the tz variant is out of scope.
    DateTimeNs,
    Time,
    Array(Box<SqlType>),
    Map(Box<SqlType>, Box<SqlType>),
    Struct(Vec<(String, SqlType)>),
    /// Iceberg v3 unshredded variant. Carried as Arrow `LargeBinary`
    /// in execution; persisted as a parquet group with `LogicalType::Variant`.
    Variant,
}

#[cfg(test)]
mod tests {
    use super::SqlType;

    #[test]
    fn sql_type_preserves_exact_variant_vocabulary() {
        let variants = vec![
            SqlType::TinyInt,
            SqlType::SmallInt,
            SqlType::Int,
            SqlType::BigInt,
            SqlType::LargeInt,
            SqlType::Float,
            SqlType::Double,
            SqlType::Decimal {
                precision: 38,
                scale: -2,
            },
            SqlType::String,
            SqlType::Json,
            SqlType::Binary,
            SqlType::Bitmap,
            SqlType::Hll,
            SqlType::Boolean,
            SqlType::Date,
            SqlType::DateTime,
            SqlType::DateTimeNs,
            SqlType::Time,
            SqlType::Array(Box::new(SqlType::Int)),
            SqlType::Map(Box::new(SqlType::String), Box::new(SqlType::BigInt)),
            SqlType::Struct(vec![("value".to_string(), SqlType::Boolean)]),
            SqlType::Variant,
        ];

        assert_eq!(variants.len(), 22);
        assert_eq!(variants.clone(), variants);

        let nested = SqlType::Array(Box::new(SqlType::Map(
            Box::new(SqlType::String),
            Box::new(SqlType::Struct(vec![
                ("x".to_string(), SqlType::DateTimeNs),
                ("v".to_string(), SqlType::Variant),
            ])),
        )));
        assert_eq!(
            nested,
            SqlType::Array(Box::new(SqlType::Map(
                Box::new(SqlType::String),
                Box::new(SqlType::Struct(vec![
                    ("x".to_string(), SqlType::DateTimeNs),
                    ("v".to_string(), SqlType::Variant),
                ])),
            )))
        );
        assert_eq!(
            format!("{nested:?}"),
            "Array(Map(String, Struct([(\"x\", DateTimeNs), (\"v\", Variant)])))"
        );
    }
}
