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
use novarocks_types::schema::SqlType;
use opensrv_mysql::{Column, ColumnFlags, ColumnType};

/// Converts a Query Application result field into one MySQL column definition.
///
/// The adapter owns the wire type and flag choices. Application roles retain
/// ownership of the result schema they expose.
pub fn mysql_column_for_result_field(field: &ResultField) -> Result<Column, String> {
    let mut colflags = ColumnFlags::empty();
    if !field.nullable() {
        colflags.insert(ColumnFlags::NOT_NULL_FLAG);
    }
    if matches!(field.logical_type(), Some(SqlType::Decimal { .. })) {
        return Ok(Column {
            table: String::new(),
            column: field.name().to_string(),
            coltype: ColumnType::MYSQL_TYPE_NEWDECIMAL,
            colflags,
        });
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
            if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
        {
            ColumnType::MYSQL_TYPE_STRING
        }
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::List(_)
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
    fn decimal_logical_type_selects_mysql_decimal() {
        let field = ResultField::new(
            "amount",
            DataType::Int64,
            false,
            Some(SqlType::Decimal {
                precision: 18,
                scale: 2,
            }),
        );

        let column = mysql_column_for_result_field(&field).expect("column");

        assert_eq!(column.coltype, ColumnType::MYSQL_TYPE_NEWDECIMAL);
        assert!(column.colflags.contains(ColumnFlags::NOT_NULL_FLAG));
    }

    #[test]
    fn unsigned_field_selects_unsigned_mysql_integer() {
        let field = ResultField::new("id", DataType::UInt64, true, None);

        let column = mysql_column_for_result_field(&field).expect("column");

        assert_eq!(column.coltype, ColumnType::MYSQL_TYPE_LONGLONG);
        assert!(column.colflags.contains(ColumnFlags::UNSIGNED_FLAG));
        assert!(!column.colflags.contains(ColumnFlags::NOT_NULL_FLAG));
    }
}
