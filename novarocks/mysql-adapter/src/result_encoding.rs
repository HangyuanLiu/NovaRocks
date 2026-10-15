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

//! MySQL result-schema adaptation.

use arrow::datatypes::DataType;
use novarocks_query_application::api::ResultField;
use novarocks_type_contract::ValueLogicalType;
use opensrv_mysql::{Column, ColumnFlags, ColumnType};
use std::io;

/// Project the already-bounded immutable schema for a governed row stream.
pub fn mysql_columns_for_result_fields(fields: &[ResultField]) -> io::Result<Vec<Column>> {
    fields
        .iter()
        .map(mysql_column_for_result_field)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

/// Converts a Query Application result field into one MySQL column definition.
///
/// The adapter owns the wire type and flag choices. Application roles retain
/// ownership of the result schema they expose.
pub fn mysql_column_for_result_field(field: &ResultField) -> Result<Column, String> {
    let mut colflags = ColumnFlags::empty();
    if !field.nullable() {
        colflags.insert(ColumnFlags::NOT_NULL_FLAG);
    }
    let coltype = match field.data_type() {
        DataType::Boolean => ColumnType::MYSQL_TYPE_TINY,
        DataType::Int8 | DataType::Int16 | DataType::Int32 => ColumnType::MYSQL_TYPE_LONG,
        DataType::Int64 => ColumnType::MYSQL_TYPE_LONGLONG,
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            colflags.insert(ColumnFlags::UNSIGNED_FLAG);
            ColumnType::MYSQL_TYPE_LONGLONG
        }
        DataType::Float32 => ColumnType::MYSQL_TYPE_FLOAT,
        DataType::Float64 => ColumnType::MYSQL_TYPE_DOUBLE,
        DataType::FixedSizeBinary(width)
            if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH
                && field.value_type().logical_type() == ValueLogicalType::LargeInt =>
        {
            ColumnType::MYSQL_TYPE_STRING
        }
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::Map(_, _)
        | DataType::Struct(_) => ColumnType::MYSQL_TYPE_VAR_STRING,
        DataType::Decimal128(_, _) => ColumnType::MYSQL_TYPE_NEWDECIMAL,
        DataType::Date32 => ColumnType::MYSQL_TYPE_DATE,
        DataType::Time32(_) | DataType::Time64(_) => ColumnType::MYSQL_TYPE_TIME,
        DataType::Timestamp(_, _) => ColumnType::MYSQL_TYPE_DATETIME,
        DataType::Null => ColumnType::MYSQL_TYPE_NULL,
        other => {
            return Err(format!(
                "standalone mysql server does not support output column type {other:?}"
            ));
        }
    };

    Ok(Column {
        table: String::new(),
        column: field.name().to_string(),
        coltype,
        colflags,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_scalar_type_selects_mysql_decimal() {
        let field = ResultField::new(
            "amount",
            novarocks_type_contract::FunctionValueType::new(DataType::Decimal128(18, 2), false),
        );

        let column = mysql_column_for_result_field(&field).expect("column");

        assert_eq!(column.coltype, ColumnType::MYSQL_TYPE_NEWDECIMAL);
        assert!(column.colflags.contains(ColumnFlags::NOT_NULL_FLAG));
    }

    #[test]
    fn unsigned_field_selects_unsigned_mysql_integer() {
        let field = ResultField::new(
            "id",
            novarocks_type_contract::FunctionValueType::new(DataType::UInt64, true),
        );

        let column = mysql_column_for_result_field(&field).expect("column");

        assert_eq!(column.coltype, ColumnType::MYSQL_TYPE_LONGLONG);
        assert!(column.colflags.contains(ColumnFlags::UNSIGNED_FLAG));
        assert!(!column.colflags.contains(ColumnFlags::NOT_NULL_FLAG));
    }
    #[test]
    fn fixed_binary_is_not_an_implicit_large_integer_declaration() {
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType as L};
        let physical = ResultField::new(
            "bytes",
            FunctionValueType::new(DataType::FixedSizeBinary(16), false),
        );
        let uuid = ResultField::new(
            "uuid",
            FunctionValueType::try_with_logical_type(DataType::FixedSizeBinary(16), false, L::Uuid)
                .unwrap(),
        );
        assert!(mysql_column_for_result_field(&physical).is_err());
        assert!(mysql_column_for_result_field(&uuid).is_err());
        let largeint = ResultField::new(
            "large",
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                false,
                L::LargeInt,
            )
            .unwrap(),
        );
        assert_eq!(
            mysql_column_for_result_field(&largeint).unwrap().coltype,
            ColumnType::MYSQL_TYPE_STRING
        );
    }
}
