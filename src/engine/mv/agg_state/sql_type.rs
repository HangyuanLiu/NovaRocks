use arrow::datatypes::DataType;

use crate::sql::parser::ast::SqlType;

pub(crate) fn arrow_data_type_to_sql_type(data_type: &DataType) -> Result<SqlType, String> {
    match data_type {
        DataType::Boolean => Ok(SqlType::Boolean),
        DataType::Int8 => Ok(SqlType::TinyInt),
        DataType::Int16 => Ok(SqlType::SmallInt),
        DataType::Int32 => Ok(SqlType::Int),
        DataType::Int64 => Ok(SqlType::BigInt),
        DataType::Float32 => Ok(SqlType::Float),
        DataType::Float64 => Ok(SqlType::Double),
        DataType::Utf8 => Ok(SqlType::String),
        DataType::Binary => Ok(SqlType::Binary),
        DataType::Date32 => Ok(SqlType::Date),
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => Ok(SqlType::DateTimeNs),
        DataType::Timestamp(_, _) => Ok(SqlType::DateTime),
        DataType::Time64(_) => Ok(SqlType::Time),
        DataType::FixedSizeBinary(width)
            if *width == crate::common::largeint::LARGEINT_BYTE_WIDTH =>
        {
            Ok(SqlType::LargeInt)
        }
        DataType::Decimal128(precision, scale) => Ok(SqlType::Decimal {
            precision: *precision,
            scale: *scale,
        }),
        DataType::List(field) => Ok(SqlType::Array(Box::new(arrow_data_type_to_sql_type(
            field.data_type(),
        )?))),
        DataType::Struct(fields) => Ok(SqlType::Struct(
            fields
                .iter()
                .map(|field| {
                    Ok((
                        field.name().clone(),
                        arrow_data_type_to_sql_type(field.data_type())?,
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?,
        )),
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err("MAP output type must use struct entries".to_string());
            };
            let (_, key) = fields
                .find("key")
                .ok_or_else(|| "MAP output type is missing key field".to_string())?;
            let (_, value) = fields
                .find("value")
                .ok_or_else(|| "MAP output type is missing value field".to_string())?;
            Ok(SqlType::Map(
                Box::new(arrow_data_type_to_sql_type(key.data_type())?),
                Box::new(arrow_data_type_to_sql_type(value.data_type())?),
            ))
        }
        other => Err(format!("unsupported MV output type: {other}")),
    }
}
