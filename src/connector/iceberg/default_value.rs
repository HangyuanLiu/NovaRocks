// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

//! Default value helpers shared by DDL, schema transport, parquet read path,
//! and INSERT write path.

use iceberg::spec::{FormatVersion, Literal as IcebergLiteral, PrimitiveLiteral};

use crate::sql::parser::ast::{DefaultLiteral, Literal as AstLiteral, SqlType};

/// Convert an AST `DefaultLiteral` to an `iceberg::spec::Literal` validated
/// against the column's SqlType.  Returns `Ok(None)` for `DefaultLiteral::Null`
/// (which is not persisted) and `Err` when the literal does not fit the
/// column's type or the type itself is unsupported.
pub(crate) fn default_literal_to_iceberg(
    literal: &DefaultLiteral,
    column_type: &SqlType,
) -> Result<Option<IcebergLiteral>, String> {
    if matches!(literal, DefaultLiteral::Null) {
        return Ok(None);
    }
    let prim = match (literal, column_type) {
        (DefaultLiteral::Bool(b), SqlType::Boolean) => PrimitiveLiteral::Boolean(*b),
        (DefaultLiteral::Int(v), SqlType::TinyInt) => {
            i8::try_from(*v).map_err(|_| out_of_range("TINYINT", *v))?;
            PrimitiveLiteral::Int(*v as i32)
        }
        (DefaultLiteral::Int(v), SqlType::SmallInt) => {
            i16::try_from(*v).map_err(|_| out_of_range("SMALLINT", *v))?;
            PrimitiveLiteral::Int(*v as i32)
        }
        (DefaultLiteral::Int(v), SqlType::Int) => {
            i32::try_from(*v).map_err(|_| out_of_range("INT", *v))?;
            PrimitiveLiteral::Int(*v as i32)
        }
        (DefaultLiteral::Int(v), SqlType::BigInt) => PrimitiveLiteral::Long(*v),
        (DefaultLiteral::Float(v), SqlType::Float) => {
            PrimitiveLiteral::Float(ordered_float::OrderedFloat(*v as f32))
        }
        (DefaultLiteral::Float(v), SqlType::Double) => {
            PrimitiveLiteral::Double(ordered_float::OrderedFloat(*v))
        }
        (
            DefaultLiteral::Decimal { unscaled, scale },
            SqlType::Decimal {
                scale: col_scale, ..
            },
        ) => {
            if *scale != *col_scale {
                return Err(format!(
                    "DEFAULT value scale {scale} does not match column scale {col_scale}"
                ));
            }
            PrimitiveLiteral::Int128(*unscaled)
        }
        (DefaultLiteral::String(s), SqlType::String) => PrimitiveLiteral::String(s.clone()),
        (DefaultLiteral::Date(d), SqlType::Date) => PrimitiveLiteral::Int(*d),
        (DefaultLiteral::DateTime(t), SqlType::DateTime) => PrimitiveLiteral::Long(*t),
        (DefaultLiteral::Binary(b), SqlType::Binary) => PrimitiveLiteral::Binary(b.clone()),
        (lit, ty) => {
            return Err(format!(
                "DEFAULT value type does not match column type: literal={lit:?} column={ty:?}"
            ));
        }
    };
    Ok(Some(IcebergLiteral::Primitive(prim)))
}

fn out_of_range(type_name: &str, value: i64) -> String {
    format!("DEFAULT value {value} is out of range for {type_name}")
}

/// Convert an `iceberg::spec::Literal` back to an AST `Literal` for use in the
/// INSERT write path (filling omitted columns with their write_default).
///
/// Returns `Err` for types that have no native `ast::Literal` variant (Decimal,
/// Binary, Timestamp, DateTime) — those paths are not yet supported by the
/// INSERT path.
pub(crate) fn iceberg_literal_to_ast(
    literal: &IcebergLiteral,
    column_type: &SqlType,
) -> Result<AstLiteral, String> {
    match (literal, column_type) {
        (IcebergLiteral::Primitive(PrimitiveLiteral::Boolean(b)), SqlType::Boolean) => {
            Ok(AstLiteral::Bool(*b))
        }
        (
            IcebergLiteral::Primitive(PrimitiveLiteral::Int(v)),
            SqlType::TinyInt | SqlType::SmallInt | SqlType::Int,
        ) => Ok(AstLiteral::Int(*v as i64)),
        (IcebergLiteral::Primitive(PrimitiveLiteral::Long(v)), SqlType::BigInt) => {
            Ok(AstLiteral::Int(*v))
        }
        (IcebergLiteral::Primitive(PrimitiveLiteral::Float(v)), SqlType::Float) => {
            Ok(AstLiteral::Float(v.0 as f64))
        }
        (IcebergLiteral::Primitive(PrimitiveLiteral::Double(v)), SqlType::Double) => {
            Ok(AstLiteral::Float(v.0))
        }
        (IcebergLiteral::Primitive(PrimitiveLiteral::String(s)), SqlType::String) => {
            Ok(AstLiteral::String(s.clone()))
        }
        (IcebergLiteral::Primitive(PrimitiveLiteral::Int(days)), SqlType::Date) => {
            // Convert days-since-epoch back to "YYYY-MM-DD" string.
            use chrono::NaiveDate;
            const UNIX_EPOCH_DAY_OFFSET: i32 = 719163;
            let date = NaiveDate::from_num_days_from_ce_opt(UNIX_EPOCH_DAY_OFFSET + days)
                .ok_or_else(|| {
                    format!("write-default date value {days} is out of representable range")
                })?;
            Ok(AstLiteral::Date(date.format("%Y-%m-%d").to_string()))
        }
        (IcebergLiteral::Primitive(PrimitiveLiteral::Int128(_)), SqlType::Decimal { .. })
        | (IcebergLiteral::Primitive(PrimitiveLiteral::Binary(_)), SqlType::Binary)
        | (IcebergLiteral::Primitive(PrimitiveLiteral::Long(_)), SqlType::DateTime) => {
            Err(format!(
                "write-default for column type {column_type:?} is not yet supported by the INSERT path"
            ))
        }
        (lit, ty) => Err(format!(
            "write-default literal type does not match column type: literal={lit:?} column={ty:?}"
        )),
    }
}

/// Reject non-NULL defaults on tables whose format-version is not v3.
/// `None` is the no-default case and is always accepted.
pub(crate) fn require_v3_for_default(
    format_version: FormatVersion,
    default: &Option<IcebergLiteral>,
) -> Result<(), String> {
    if default.is_some() && !matches!(format_version, FormatVersion::V3) {
        return Err("non-NULL DEFAULT requires Iceberg format-version 3; \
             set TBLPROPERTIES('format-version'='3')"
            .to_string());
    }
    Ok(())
}

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::DataType;

/// Build an Arrow constant array of length `row_count` whose every element is
/// the value encoded by `literal`. The literal's runtime type must agree with
/// `target_type`; mismatches fail fast.
pub(crate) fn literal_to_constant_array(
    literal: &IcebergLiteral,
    target_type: &DataType,
    row_count: usize,
) -> Result<ArrayRef, String> {
    let IcebergLiteral::Primitive(prim) = literal else {
        return Err(format!(
            "unsupported initial-default literal kind: {literal:?}"
        ));
    };
    Ok(match (prim, target_type) {
        (PrimitiveLiteral::Boolean(v), DataType::Boolean) => {
            Arc::new(BooleanArray::from(vec![*v; row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Int(v), DataType::Int32) => {
            Arc::new(Int32Array::from(vec![*v; row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Long(v), DataType::Int64) => {
            Arc::new(Int64Array::from(vec![*v; row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Float(v), DataType::Float32) => {
            Arc::new(Float32Array::from(vec![v.0; row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Double(v), DataType::Float64) => {
            Arc::new(Float64Array::from(vec![v.0; row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Int128(v), DataType::Decimal128(precision, scale)) => Arc::new(
            Decimal128Array::from(vec![*v; row_count])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| format!("decimal default cast: {e}"))?,
        )
            as ArrayRef,
        (PrimitiveLiteral::String(s), DataType::Utf8) => {
            Arc::new(StringArray::from(vec![s.as_str(); row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Int(v), DataType::Date32) => {
            Arc::new(Date32Array::from(vec![*v; row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Long(v), DataType::Timestamp(_, _)) => {
            Arc::new(TimestampMicrosecondArray::from(vec![*v; row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Binary(b), DataType::Binary) => {
            let slice = b.as_slice();
            Arc::new(BinaryArray::from(vec![slice; row_count])) as ArrayRef
        }
        (prim, ty) => {
            return Err(format!(
                "unsupported initial-default literal {prim:?} for arrow type {ty:?}"
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bool_default_round_trips() {
        let lit = default_literal_to_iceberg(&DefaultLiteral::Bool(true), &SqlType::Boolean)
            .expect("bool default")
            .expect("not null");
        assert!(matches!(
            lit,
            IcebergLiteral::Primitive(PrimitiveLiteral::Boolean(true))
        ));
    }

    #[test]
    fn int_overflow_rejected_for_tinyint() {
        let err = default_literal_to_iceberg(&DefaultLiteral::Int(200), &SqlType::TinyInt)
            .expect_err("overflow");
        assert!(err.contains("TINYINT"));
    }

    #[test]
    fn decimal_scale_mismatch_rejected() {
        let err = default_literal_to_iceberg(
            &DefaultLiteral::Decimal {
                unscaled: 1234,
                scale: 3,
            },
            &SqlType::Decimal {
                precision: 10,
                scale: 2,
            },
        )
        .expect_err("scale mismatch");
        assert!(err.contains("scale"));
    }

    #[test]
    fn null_returns_none() {
        let lit =
            default_literal_to_iceberg(&DefaultLiteral::Null, &SqlType::Int).expect("null default");
        assert!(lit.is_none());
    }

    #[test]
    fn type_mismatch_rejected() {
        let err = default_literal_to_iceberg(&DefaultLiteral::String("x".into()), &SqlType::Int)
            .expect_err("type mismatch");
        assert!(err.contains("type does not match"));
    }

    #[test]
    fn iceberg_to_ast_literal_int() {
        use crate::sql::parser::ast::Literal as AstLiteral;
        let iceberg = IcebergLiteral::Primitive(PrimitiveLiteral::Int(7));
        let ast = iceberg_literal_to_ast(&iceberg, &SqlType::Int).expect("convert");
        assert_eq!(ast, AstLiteral::Int(7));
    }

    #[test]
    fn iceberg_to_ast_literal_string() {
        use crate::sql::parser::ast::Literal as AstLiteral;
        let iceberg = IcebergLiteral::Primitive(PrimitiveLiteral::String("hi".into()));
        let ast = iceberg_literal_to_ast(&iceberg, &SqlType::String).expect("convert");
        assert_eq!(ast, AstLiteral::String("hi".into()));
    }

    #[test]
    fn iceberg_to_ast_literal_unsupported_type_errors() {
        // Decimal has no native ast::Literal variant.
        let iceberg = IcebergLiteral::Primitive(PrimitiveLiteral::Int128(12345));
        let err = iceberg_literal_to_ast(
            &iceberg,
            &SqlType::Decimal {
                precision: 10,
                scale: 2,
            },
        )
        .expect_err("decimal unsupported by ast");
        assert!(err.contains("not yet supported"));
    }

    #[test]
    fn iceberg_to_ast_literal_date_round_trips() {
        let epoch = IcebergLiteral::Primitive(PrimitiveLiteral::Int(0));
        let ast = iceberg_literal_to_ast(&epoch, &SqlType::Date).expect("epoch");
        assert_eq!(ast, AstLiteral::Date("1970-01-01".to_string()));

        let day_before = IcebergLiteral::Primitive(PrimitiveLiteral::Int(-1));
        let ast = iceberg_literal_to_ast(&day_before, &SqlType::Date).expect("pre-epoch");
        assert_eq!(ast, AstLiteral::Date("1969-12-31".to_string()));
    }

    #[test]
    fn iceberg_to_ast_literal_struct_against_decimal_reports_type_mismatch() {
        // Catch-all must surface "type does not match" rather than the
        // not-yet-supported branch when the literal is structurally wrong
        // for the column.
        let iceberg = IcebergLiteral::Primitive(PrimitiveLiteral::String("oops".into()));
        let err = iceberg_literal_to_ast(
            &iceberg,
            &SqlType::Decimal {
                precision: 10,
                scale: 2,
            },
        )
        .expect_err("type mismatch");
        assert!(err.contains("does not match"));
    }

    #[test]
    fn v2_rejects_non_null_default() {
        let err = require_v3_for_default(
            iceberg::spec::FormatVersion::V2,
            &Some(IcebergLiteral::Primitive(PrimitiveLiteral::Int(5))),
        )
        .expect_err("v2 reject");
        assert!(err.contains("format-version 3"));
    }

    #[test]
    fn v3_accepts_non_null_default() {
        require_v3_for_default(
            iceberg::spec::FormatVersion::V3,
            &Some(IcebergLiteral::Primitive(PrimitiveLiteral::Int(5))),
        )
        .expect("v3 accept");
    }

    #[test]
    fn v2_accepts_null_default() {
        require_v3_for_default(iceberg::spec::FormatVersion::V2, &None).expect("v2 + null ok");
    }

    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::DataType;

    #[test]
    fn literal_to_constant_array_int32() {
        let lit = IcebergLiteral::Primitive(PrimitiveLiteral::Int(5));
        let arr = literal_to_constant_array(&lit, &DataType::Int32, 3).expect("array");
        let i32arr = arr.as_any().downcast_ref::<Int32Array>().expect("i32");
        assert_eq!(i32arr.len(), 3);
        assert_eq!(i32arr.value(0), 5);
        assert_eq!(i32arr.value(2), 5);
    }

    #[test]
    fn literal_to_constant_array_string() {
        let lit = IcebergLiteral::Primitive(PrimitiveLiteral::String("hi".into()));
        let arr = literal_to_constant_array(&lit, &DataType::Utf8, 2).expect("array");
        let strarr = arr.as_any().downcast_ref::<StringArray>().expect("str");
        assert_eq!(strarr.value(0), "hi");
        assert_eq!(strarr.value(1), "hi");
    }

    #[test]
    fn literal_to_constant_array_zero_rows() {
        let lit = IcebergLiteral::Primitive(PrimitiveLiteral::Int(5));
        let arr = literal_to_constant_array(&lit, &DataType::Int32, 0).expect("array");
        assert_eq!(arr.len(), 0);
    }

    #[test]
    fn literal_to_constant_array_unsupported_type_fails_fast() {
        // Use a (Long, Float64) mismatch — Long should not produce a Float64 array.
        let lit = IcebergLiteral::Primitive(PrimitiveLiteral::Long(5));
        let err =
            literal_to_constant_array(&lit, &DataType::Float64, 1).expect_err("type mismatch");
        assert!(err.contains("unsupported"), "unexpected error: {err}");
    }
}
